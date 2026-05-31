//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"math"
	"os"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// newV0ScoreSegmentMmap builds a minimal V0 (gob map) inverted *segment in mmap
// mode and eager-loads it into the resident slices, so the V0 score path
// (propLengthForScore -> propLengthAt -> resident lossless uint32 slice) can be
// exercised end-to-end. It reuses buildV0Section (the T2 gob-section writer) and
// the T2 newV0TestSegmentMmap segment builder, then triggers the eager load.
func newV0ScoreSegmentMmap(t *testing.T, m map[uint64]uint32) *segment {
	t.Helper()
	section := buildV0Section(t, 12.5, uint64(len(m)), m)
	s := newV0TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengths())
	return s
}

// TestS2_PropLengthForScore_V2EqualsV0Map is the S2 zero-delta test: for the SAME
// (docID, length) corpus, the per-doc length the SCORE path reads on a V2 segment
// (propLengthForScore -> gallop) must equal, bit-for-bit, what the same corpus
// returns on a V0 segment (propLengthForScore -> resident slice) AND the raw map.
// Baseline-first: the V0 map is the authoritative baseline; V2 must match it.
//
// This test catches the score path reading the WRONG column (e.g. the clamped
// block-impact snapshot instead of the lossless V2 column -- CAVEAT-A) or a gallop
// off-by-one, because it probes every present docID plus interleaved absent docIDs
// and any single mismatch between the V2 score read and the V0 baseline fails.
func TestS2_PropLengthForScore_V2EqualsV0Map(t *testing.T) {
	// Sorted ascending corpus with sparse docIDs and a >65535 length (so the
	// baseline equality also pins losslessness, not just structural agreement).
	docIDs := []uint64{1, 5, 6, 100, 4242, 70000, 1_000_000}
	lengths := []uint32{3, 7, 65535, 65536, 200000, 99, 12}

	baseline := map[uint64]uint32{}
	for i, d := range docIDs {
		baseline[d] = lengths[i]
	}

	v0 := newV0ScoreSegmentMmap(t, baseline)

	v2section := buildV2Section(12.5, uint64(len(docIDs)), docIDs, lengths, 0)
	v2 := newV2TestSegmentMmap(t, v2section)
	require.NoError(t, v2.loadPropertyLengthsV2())

	// Probe present docIDs and a set of absent ones (below min, between, above max).
	probes := []uint64{0, 1, 2, 5, 6, 7, 99, 100, 4242, 70000, 999_999, 1_000_000, 2_000_000}
	for _, p := range probes {
		var v0Cur, v2Cur uint64
		want := v0.propLengthForScore(p, &v0Cur) // V0 baseline
		got := v2.propLengthForScore(p, &v2Cur)  // V2 score read
		require.Equalf(t, want, got, "V2 score read != V0 baseline for docID %d", p)
		require.Equalf(t, baseline[p], got, "V2 score read != raw map for docID %d", p)
	}
}

// TestS3_ZeroCopyMmap asserts the V2 length read on an mmap segment is ZERO-COPY:
// the bytes the reader returns are sliced directly out of the segment's mmap'd
// contents, not copied to the Go heap. It proves this structurally -- a length
// byte mutated IN the contents slice is observed by the very next read -- which can
// only hold if the read indexes s.contents in place (no cached copy).
//
// This test catches a regression that materializes the V2 columns onto the heap
// (resurrecting the ~0-resident-heap win this design exists for): a heap copy would
// not see the in-place mutation and the assertion would fail.
func TestS3_ZeroCopyMmap(t *testing.T) {
	docIDs := []uint64{10, 20, 30}
	lengths := []uint32{100, 200, 300}
	section := buildV2Section(7.0, 3, docIDs, lengths, 0)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())

	// The length column backing pointer must fall inside the mmap contents slice.
	lenBase := s.invertedData.propLengthV2LenBase
	contentsPtr := uintptr(unsafe.Pointer(unsafe.SliceData(s.contents)))
	contentsEnd := contentsPtr + uintptr(len(s.contents))
	lenColPtr := contentsPtr + uintptr(lenBase)
	require.GreaterOrEqual(t, uint64(lenColPtr), uint64(contentsPtr),
		"len column must start at/after the contents base")
	require.Less(t, uint64(lenColPtr), uint64(contentsEnd),
		"len column must start before the contents end (it is inside the mmap region)")

	// Structural zero-copy proof: read docID 20 (200), mutate the backing byte in
	// contents, read again -- the new value must surface with no reload.
	var cur uint64
	require.Equal(t, uint32(200), s.propLengthForScore(20, &cur))

	// len[1] (docID 20) lives at lenBase + 1*4; bump its low byte by 5.
	off := lenBase + 1*propLengthV2LenWidth
	s.contents[off] += 5
	var cur2 uint64
	require.Equal(t, uint32(205), s.propLengthForScore(20, &cur2),
		"zero-copy: an in-place mutation of the mmap'd length byte must be observed")
}

// TestS4_ScoreParity_MmapAndPread is the S4 dual-mode parity test at the SCORE
// level: it computes the exact BM25 tf term using the per-doc length read by
// propLengthForScore on BOTH a mmap and a pread V2 segment, and asserts the
// lengths AND the resulting BM25 tf floats are bit-identical across the two modes.
//
// This test catches a pread path that diverges from mmap (the #1 constraint -- a
// naive s.contents[off:] that only works in mmap mode) because the pread segment
// has no contents slice; a missing readFromMemory branch would panic or read
// garbage and the float equality would fail.
func TestS4_ScoreParity_MmapAndPread(t *testing.T) {
	const (
		k1         = 1.2
		b          = 0.75
		avgPropLen = 42.0
	)
	docIDs := []uint64{1, 5, 6, 100, 4242, 1_000_000}
	lengths := []uint32{3, 7, 65535, 65536, 200000, 12}
	section := buildV2Section(avgPropLen, uint64(len(docIDs)), docIDs, lengths, 0)

	mmapSeg := newV2TestSegmentMmap(t, section)
	preadSeg := newV2TestSegmentPread(t, section)
	require.NoError(t, mmapSeg.loadPropertyLengthsV2())
	require.NoError(t, preadSeg.loadPropertyLengthsV2())

	// Monotonic ascending queries (as BlockMax issues), threading the cursor.
	var mCur, pCur uint64
	freq := 5.0
	for _, d := range docIDs {
		mLen := mmapSeg.propLengthForScore(d, &mCur)
		pLen := preadSeg.propLengthForScore(d, &pCur)
		require.Equalf(t, mLen, pLen, "mmap vs pread length disagree for docID %d", d)

		mTF := bm25TF(freq, float64(mLen), k1, b, avgPropLen)
		pTF := bm25TF(freq, float64(pLen), k1, b, avgPropLen)
		require.Equalf(t, mTF, pTF, "mmap vs pread BM25 tf disagree for docID %d", d)
	}
}

// buildSingleDocPostingNode hand-encodes the 20-byte single-doc posting buffer
// loadBlockEntries reads when docCount <= ENCODE_AS_FULL_BYTES (==1):
// [docCount uint64 LE][docID uint64 BE][tf float32 LE]. This mirrors what
// createAndEncodeSingleValue produces; convertFixedLengthFromMemory decodes it.
func buildSingleDocPostingNode(docID uint64, tf float32) []byte {
	buf := make([]byte, 8+12*1)
	binary.LittleEndian.PutUint64(buf[0:8], 1) // docCount == 1
	binary.BigEndian.PutUint64(buf[8:16], docID)
	binary.LittleEndian.PutUint32(buf[16:20], math.Float32bits(tf))
	return buf
}

// TestS16_SingleDocInline_MmapAndPread is the S16 test: a single-doc posting's
// length must be read inline through the V2-aware lossless dispatch. It drives the
// ACTUAL loadBlockEntries single-doc branch (segment_blockmax.go:53) -- not just
// the dispatch helper -- against a V2 segment in BOTH mmap and pread modes, and
// asserts the BlockEntry's MaxImpactPropLength carries the exact lossless length.
//
// This test catches the single-doc call site still using the V0-only propLengthAt
// (which reads the empty resident slice on a V2 segment and yields 0, silently
// zeroing the block-impact length) because it reads MaxImpactPropLength straight
// out of loadBlockEntries' returned entry and asserts the exact >65535 length. A
// reverted call site returns 0 here and the assertion fails (verified red->green).
func TestS16_SingleDocInline_MmapAndPread(t *testing.T) {
	const (
		docID    = uint64(777)
		propLen  = uint32(123456) // >65535: lossless, single doc
		tf       = float32(2.0)
		nodeSize = 8 + 12*1
	)
	section := buildV2Section(9.0, 1, []uint64{docID}, []uint32{propLen}, 0)
	node := buildSingleDocPostingNode(docID, tf)

	for _, mode := range []string{"mmap", "pread"} {
		t.Run(mode, func(t *testing.T) {
			// Lay out contents as [single-doc node][V2 section]; the inverted header
			// points PropertyLengthsOffset at the section after the node.
			contents := append(append([]byte{}, node...), section...)
			sectionOff := uint64(len(node))

			var s *segment
			if mode == "mmap" {
				s = newV2TestSegmentMmap(t, section) // reuse for field defaults
				s.contents = contents
				s.readFromMemory = true
				s.invertedHeader.PropertyLengthsOffset = sectionOff
			} else {
				s = newV2TestSegmentPread(t, section)
				path := s.path
				require.NoError(t, os.WriteFile(path, contents, 0o644))
				f, err := os.Open(path)
				require.NoError(t, err)
				t.Cleanup(func() { f.Close() })
				s.contentFile = f
				s.size = int64(len(contents))
				s.invertedHeader.PropertyLengthsOffset = sectionOff
			}
			// re-load the section at the new offset.
			s.invertedData = &segmentInvertedData{}
			require.NoError(t, s.loadPropertyLengthsV2())

			entries, docCount, _, err := s.loadBlockEntries(segmentindex.Node{Start: 0})
			require.NoError(t, err)
			require.Equal(t, uint64(1), docCount)
			require.Len(t, entries, 1)
			require.Equal(t, propLen, entries[0].MaxImpactPropLength,
				"single-doc posting must carry the lossless V2 length inline (%s)", mode)
		})
	}
}

// TestS18_LockFreeV2HotPath asserts the V2 score read takes NO lockInvertedData:
// the test acquires the segment's lockInvertedData WRITE lock and, while holding
// it, runs a V2 score read in a goroutine. If the read path took the RLock it would
// block on the held write lock and the read would not complete; because the V2 path
// is lock-free (immutable mmap'd columns), the read completes promptly.
//
// This test catches a regression that reintroduces a per-doc RLock on the V2 hot
// path (the D1-review M1 lock the V2 design removes) because such a read would
// deadlock against the held write lock and the select would hit the timeout branch.
func TestS18_LockFreeV2HotPath(t *testing.T) {
	docIDs := []uint64{1, 2, 3, 4, 5}
	lengths := []uint32{10, 20, 30, 40, 50}
	section := buildV2Section(6.0, 5, docIDs, lengths, 0)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())

	// Hold the inverted-data WRITE lock for the duration of the read.
	s.invertedData.lockInvertedData.Lock()
	defer s.invertedData.lockInvertedData.Unlock()

	done := make(chan uint32, 1)
	go func() {
		var cur uint64
		done <- s.propLengthForScore(3, &cur)
	}()

	select {
	case got := <-done:
		require.Equal(t, uint32(30), got,
			"V2 score read must be lock-free and return the exact length while the write lock is held")
	case <-time.After(2 * time.Second):
		t.Fatal("V2 score read blocked on lockInvertedData: the V2 hot path is NOT lock-free")
	}
}

// TestS18_ConcurrentBM25Read_Race drives many concurrent V2 score reads (each with
// its own monotonic cursor) over a shared immutable segment. Run under -race it
// surfaces any data race on the read path; functionally it asserts every reader
// gets the exact lengths regardless of interleaving.
//
// This test catches a shared-mutable-state bug on the V2 read path (e.g. a cursor
// accidentally hung off the shared segment instead of the per-iterator struct)
// because -race flags the unsynchronized access and the value assertions catch a
// torn read.
func TestS18_ConcurrentBM25Read_Race(t *testing.T) {
	docIDs := make([]uint64, 2000)
	lengths := make([]uint32, 2000)
	for i := range docIDs {
		docIDs[i] = uint64(i*3 + 1) // sorted ascending, sparse
		lengths[i] = uint32(i + 1)
	}
	section := buildV2Section(50.0, uint64(len(docIDs)), docIDs, lengths, 0)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())

	const readers = 16
	var wg sync.WaitGroup
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			// Ascending monotonic queries with a per-goroutine cursor (the real
			// per-iterator shape). No shared mutable state across goroutines.
			var cur uint64
			for i, d := range docIDs {
				got := s.propLengthForScore(d, &cur)
				if got != lengths[i] {
					panic("torn concurrent V2 read")
				}
			}
		}()
	}
	wg.Wait()
}

// TestV0Lossless_ScorePath is the CAVEAT-B end-to-end test at the SCORE level: a
// >65535-token doc on an UN-CONVERTED V0 segment must score with its EXACT length,
// not the uint16-clamped 65535. It builds a V0 gob segment with an over-cap doc,
// eager-loads it, and asserts propLengthForScore (the V0 score path) returns the
// length verbatim.
//
// This test catches the D1-review B1 corruption: if the resident V0 slice still
// clamped to uint16, the over-cap doc would read back as 65535 and the BM25 tf
// would be wrong on every un-converted V0 segment. It pins that the widen to
// lossless uint32 reaches the actual score read, not just the loader.
func TestV0Lossless_ScorePath(t *testing.T) {
	const overCap = uint32(70000) // > maxPropLength (65535)
	m := map[uint64]uint32{
		10: 5,
		20: overCap,
		30: maxPropLength, // exactly at the old cap
	}
	s := newV0ScoreSegmentMmap(t, m)

	require.Equal(t, overCap, s.propLengthForScore(20, nil),
		"V0 score path must return the >65535 length losslessly (CAVEAT-B)")
	require.Equal(t, uint32(maxPropLength), s.propLengthForScore(30, nil))
	require.Equal(t, uint32(5), s.propLengthForScore(10, nil))
	require.Equal(t, uint32(0), s.propLengthForScore(99, nil)) // absent
}
