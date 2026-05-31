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
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// buildV2Section serializes a V2 flat-column property-length section: the
// 24-byte avg/count/size prefix followed by [n][docID column][len column].
// docIDs must be sorted ascending (the writer guarantees this; the reader
// gallops over it). overrideSize, when non-zero, replaces the computed size in
// the prefix so a test can forge a torn (size != 8+n*12) section.
func buildV2Section(avg float64, count uint64, docIDs []uint64, lengths []uint32, overrideSize uint64) []byte {
	if len(docIDs) != len(lengths) {
		panic("buildV2Section: docIDs and lengths must be the same length")
	}
	n := uint64(len(docIDs))
	size := uint64(propLengthV2NField) + n*(propLengthV2DocIDWidth+propLengthV2LenWidth)
	if n == 0 {
		size = 0
	}
	if overrideSize != 0 {
		size = overrideSize
	}

	buf := make([]byte, propLengthV2PrefixSize)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(avg))
	binary.LittleEndian.PutUint64(buf[8:16], count)
	binary.LittleEndian.PutUint64(buf[16:24], size)

	if n == 0 && overrideSize == 0 {
		return buf
	}

	nBuf := make([]byte, propLengthV2NField)
	binary.LittleEndian.PutUint64(nBuf, n)
	buf = append(buf, nBuf...)

	docIDCol := make([]byte, n*propLengthV2DocIDWidth)
	for i, d := range docIDs {
		binary.LittleEndian.PutUint64(docIDCol[i*propLengthV2DocIDWidth:], d)
	}
	buf = append(buf, docIDCol...)

	lenCol := make([]byte, n*propLengthV2LenWidth)
	for i, l := range lengths {
		binary.LittleEndian.PutUint32(lenCol[i*propLengthV2LenWidth:], l)
	}
	buf = append(buf, lenCol...)

	return buf
}

// newV2TestSegmentMmap builds a minimal *segment whose contents hold the V2
// section at PropertyLengthsOffset, in mmap (readFromMemory=true) mode -- the
// reader slices the columns zero-copy from s.contents.
func newV2TestSegmentMmap(t *testing.T, section []byte) *segment {
	t.Helper()
	// Prepend padding so PropertyLengthsOffset is non-zero (exercises the offset
	// arithmetic rather than accidentally working at offset 0).
	const pad = 64
	contents := make([]byte, pad+len(section))
	copy(contents[pad:], section)

	return &segment{
		path:           "test-segment-mmap",
		strategy:       segmentindex.StrategyInverted,
		readFromMemory: true,
		contents:       contents,
		logger:         logrus.New(),
		invertedHeader: &segmentindex.HeaderInverted{
			Version:               segmentindex.SegmentInvertedVersionV2,
			PropertyLengthsOffset: pad,
		},
		invertedData: &segmentInvertedData{},
	}
}

// newV2TestSegmentPread builds a minimal *segment backed by a real file in pread
// (readFromMemory=false) mode -- the reader reads each column probe via a bounded
// copyNode through the contentFile, mirroring segment_blockmax.go:30-37.
func newV2TestSegmentPread(t *testing.T, section []byte) *segment {
	t.Helper()
	const pad = 64
	fileBytes := make([]byte, pad+len(section))
	copy(fileBytes[pad:], section)

	dir := t.TempDir()
	path := filepath.Join(dir, "segment-pread.db")
	require.NoError(t, os.WriteFile(path, fileBytes, 0o644))
	f, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })

	return &segment{
		path:           path,
		strategy:       segmentindex.StrategyInverted,
		readFromMemory: false,
		contentFile:    f,
		// size is the file length; bufferedReaderAt uses it as the
		// SectionReader length, so it must be set or every pread returns EOF.
		size:   int64(len(fileBytes)),
		logger: logrus.New(),
		invertedHeader: &segmentindex.HeaderInverted{
			Version:               segmentindex.SegmentInvertedVersionV2,
			PropertyLengthsOffset: pad,
		},
		invertedData: &segmentInvertedData{},
	}
}

// lookupV2 is a stateless convenience: gallop from index 0 every call, so the
// test exercises the full gallop+binary-search without threading a cursor.
func lookupV2(t *testing.T, s *segment, docID uint64) uint32 {
	t.Helper()
	length, _, err := s.propLengthAtV2(docID, 0, nil)
	require.NoError(t, err)
	return length
}

// TestV2ReadRoundTrip_MmapAndPread hand-writes a V2 section, reads every length
// back via BOTH the mmap (zero-copy) and pread (bounded copyNode) paths, and
// asserts the two paths agree byte-for-byte. It covers a >65535 length to pin
// the lossless uint32 column (NO uint16 clamp) and absent-docID zero-for-absent.
//
// This test catches the #1 implementation constraint -- a V2 reader that uses a
// naive s.contents[off:] without the readFromMemory branch -- because the pread
// segment has NO contents slice; the reader is forced through copyNode and a
// missing branch would panic or read garbage on it. It also catches a lossy
// uint16 truncation (the D1 clamp regression) because a >65535 length read back
// as the clamped 65535 would fail the exact-equality assertion.
func TestV2ReadRoundTrip_MmapAndPread(t *testing.T) {
	// docIDs sorted ascending, with gaps (sparse global docIDs) and a >65535
	// length that the lossless uint32 column must carry verbatim.
	docIDs := []uint64{1, 5, 6, 100, 4242, 1_000_000}
	lengths := []uint32{3, 7, 65535, 65536, 200000, 12}
	const avg = 12.5
	const count = uint64(6)

	section := buildV2Section(avg, count, docIDs, lengths, 0)

	for _, mode := range []string{"V2Read_Mmap", "V2Read_Pread"} {
		t.Run(mode, func(t *testing.T) {
			var s *segment
			if mode == "V2Read_Mmap" {
				s = newV2TestSegmentMmap(t, section)
			} else {
				s = newV2TestSegmentPread(t, section)
			}

			require.NoError(t, s.loadPropertyLengthsV2())

			// avg/count populated from the prefix exactly as V0 (G8).
			require.Equal(t, avg, s.invertedData.avgPropertyLengthsAvg)
			require.Equal(t, count, s.invertedData.avgPropertyLengthsCount)
			require.Equal(t, uint64(len(docIDs)), s.invertedData.propLengthV2Count)

			// Every present docID returns its exact lossless length.
			for i, d := range docIDs {
				require.Equal(t, lengths[i], lookupV2(t, s, d),
					"docID %d length mismatch in %s", d, mode)
			}

			// The >65535 entries are lossless (NO uint16 clamp).
			require.Equal(t, uint32(65536), lookupV2(t, s, 100))
			require.Equal(t, uint32(200000), lookupV2(t, s, 4242))

			// Absent docIDs return 0 (zero-for-absent), including below the
			// minimum, between entries, and above the maximum.
			require.Equal(t, uint32(0), lookupV2(t, s, 0))
			require.Equal(t, uint32(0), lookupV2(t, s, 7))
			require.Equal(t, uint32(0), lookupV2(t, s, 2_000_000))
		})
	}

	// MmapPread parity: the two paths must return identical lengths for the same
	// queries (the explicit cross-mode agreement assertion).
	t.Run("MmapPread_parity", func(t *testing.T) {
		mmapSeg := newV2TestSegmentMmap(t, section)
		preadSeg := newV2TestSegmentPread(t, section)
		require.NoError(t, mmapSeg.loadPropertyLengthsV2())
		require.NoError(t, preadSeg.loadPropertyLengthsV2())

		probes := []uint64{0, 1, 5, 6, 7, 100, 4242, 999_999, 1_000_000, 2_000_000}
		for _, p := range probes {
			require.Equal(t, lookupV2(t, mmapSeg, p), lookupV2(t, preadSeg, p),
				"mmap vs pread disagree on docID %d", p)
		}
	})
}

// TestV2ReadGallopMonotonicCursor pins the monotonic gallop: feeding the index
// returned by the previous lookup as the next start index (ascending queries,
// as BlockMax-WAND issues them) returns the same results as a stateless lookup.
// It catches a gallop that mishandles a non-zero start index (forward and the
// occasional backward step), which would silently return wrong lengths under
// the T3 cursor.
func TestV2ReadGallopMonotonicCursor(t *testing.T) {
	docIDs := []uint64{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}
	lengths := []uint32{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	section := buildV2Section(5.0, uint64(len(docIDs)), docIDs, lengths, 0)

	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())

	// Walk docIDs in ascending order, threading the returned index forward.
	var startIdx uint64
	for i, d := range docIDs {
		length, idx, err := s.propLengthAtV2(d, startIdx, nil)
		require.NoError(t, err)
		require.Equal(t, lengths[i], length, "monotonic lookup of docID %d", d)
		startIdx = idx
	}

	// A backward query after advancing must still find the earlier docID (the
	// gallop-backward branch).
	length, _, err := s.propLengthAtV2(4, startIdx, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(20), length)
}

// TestV2EmptySection covers n==0: the section is just the prefix with size==0,
// every lookup returns 0, and load succeeds (matches V0's size==0 short-circuit).
func TestV2EmptySection(t *testing.T) {
	section := buildV2Section(0, 0, nil, nil, 0)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())
	require.Equal(t, uint64(0), s.invertedData.propLengthV2Count)
	require.Equal(t, uint32(0), lookupV2(t, s, 42))
}

// TestV2ForwardReject_SizeCrossCheck pins the self-describing size cross-check:
// a V2 section whose declared `size` field disagrees with 8 + n*12 is a torn or
// mis-decoded section and loadPropertyLengthsV2 must reject it LOUDLY rather
// than read past the section end.
//
// This test catches a missing size cross-check because it forges a header whose
// declared size (off by 4) does not match its declared n; without the check the
// reader would compute column offsets from n and silently read out-of-bounds or
// garbage at score time. The forged shape is exactly the torn-section state a
// short write during convert-on-compaction (T2, S14) produces.
func TestV2ForwardReject_SizeCrossCheck(t *testing.T) {
	docIDs := []uint64{1, 2, 3}
	lengths := []uint32{10, 20, 30}
	correctSize := uint64(propLengthV2NField) + uint64(len(docIDs))*(propLengthV2DocIDWidth+propLengthV2LenWidth)

	// Forge a size that disagrees with n (too small by one length-column entry).
	section := buildV2Section(1.0, 3, docIDs, lengths, correctSize-propLengthV2LenWidth)

	s := newV2TestSegmentMmap(t, section)
	err := s.loadPropertyLengthsV2()
	require.Error(t, err)
	require.Contains(t, err.Error(), "torn V2 property-length section")
}

// forgeV2RawSection hand-assembles a V2 section with FULLY independent prefix
// `size`, declared `n`, and physical body bytes -- so a test can forge a header
// whose declared n has no relation to the bytes that actually follow it (the
// torn / mis-decoded / V0-read-as-V2 corruption shapes). bodyBytes is whatever
// physically follows the n-field (may be shorter than the columns n implies).
func forgeV2RawSection(size, n uint64, bodyBytes []byte) []byte {
	buf := make([]byte, propLengthV2PrefixSize)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(1.0)) // avg
	binary.LittleEndian.PutUint64(buf[8:16], 0)                    // count
	binary.LittleEndian.PutUint64(buf[16:24], size)                // size

	nBuf := make([]byte, propLengthV2NField)
	binary.LittleEndian.PutUint64(nBuf, n)
	buf = append(buf, nBuf...)
	buf = append(buf, bodyBytes...)
	return buf
}

// TestV2TornReject_OverflowN_MmapAndPread (M1, case a): a forged header whose
// declared `n` is so large that 8 + n*12 WRAPS uint64 to a small value, and
// whose declared `size` equals that wrapped value, slips past the naive
// `size == 8 + n*12` cross-check (16 == 16). The old reader then caches column
// bases from the huge n and the first lookup slices s.contents far out of range
// -- a `slice bounds out of range` PANIC on the mmap hot path (the production
// open/score path), and a copyNode read-past-EOF on pread.
//
// This test catches the overflow hole because it passes the EXACT input shape
// the old size cross-check could not reject -- a wrapped expectedSize that
// equals the forged size -- and asserts loadPropertyLengthsV2 now returns a
// torn-section error (no panic on mmap, no wrong read on pread) on BOTH paths.
// The remaining-bytes bound rejects n before the wrapping multiply runs.
func TestV2TornReject_OverflowN_MmapAndPread(t *testing.T) {
	// n*12 wraps: n = MaxUint64/12 + 1 gives n*12 == 8, so expectedSize == 16.
	// overflowN is a runtime var (not const) so the wrapping multiply happens at
	// runtime; a const expression would be rejected by the compiler as overflow.
	overflowN := uint64(math.MaxUint64/12) + 1
	pairWidth := uint64(propLengthV2DocIDWidth + propLengthV2LenWidth)
	wrappedExpectedSize := uint64(propLengthV2NField) + overflowN*pairWidth
	require.Equal(t, uint64(16), wrappedExpectedSize,
		"precondition: the forged n must wrap 8+n*12 to 16 so it defeats the naive size check")

	// A small physical body (one docID's worth) -- enough that the old code's
	// first lookup computes a far-out-of-range offset rather than failing earlier.
	body := make([]byte, propLengthV2DocIDWidth)
	section := forgeV2RawSection(wrappedExpectedSize, overflowN, body)

	for _, mode := range []string{"Mmap", "Pread"} {
		t.Run(mode, func(t *testing.T) {
			var s *segment
			if mode == "Mmap" {
				s = newV2TestSegmentMmap(t, section)
			} else {
				s = newV2TestSegmentPread(t, section)
			}
			err := s.loadPropertyLengthsV2()
			require.Error(t, err, "overflow n must be rejected at load, not panic on read")
			require.Contains(t, err.Error(), "torn V2 property-length section")
			require.False(t, s.invertedData.propLengthV2Loaded,
				"a rejected section must not be marked loaded")
		})
	}
}

// TestV2TornReject_PointsPastEOF_MmapAndPread (M1, case b): a header whose
// declared n is self-CONSISTENT with its `size` field (size == 8 + n*12, so the
// naive cross-check passes) but declares far more pairs than the segment file
// physically holds. The old reader caches bases from n and the first lookup
// reads past the mmap end -- panic on mmap, read-past-EOF on pread. The
// remaining-bytes bound catches this because n exceeds the pairs that fit in the
// real segment bytes, independent of the (self-consistent) size field.
//
// This test catches the points-past-EOF hole because the size cross-check alone
// CANNOT see it -- size and n agree -- so only a bound against the real file
// length rejects it; it asserts a torn-section error (no panic, no wrong read)
// on BOTH mmap and pread.
func TestV2TornReject_PointsPastEOF_MmapAndPread(t *testing.T) {
	// Declare 1000 pairs (self-consistent size) but write a body holding only 2.
	const declaredN = uint64(1000)
	consistentSize := uint64(propLengthV2NField) +
		declaredN*(propLengthV2DocIDWidth+propLengthV2LenWidth)

	// Physical body: room for only 2 pairs (2 docIDs + 2 lengths), far short of
	// the 1000 declared. The bound computes maxPairs from this real length.
	const realPairs = 2
	body := make([]byte, realPairs*propLengthV2DocIDWidth+realPairs*propLengthV2LenWidth)
	section := forgeV2RawSection(consistentSize, declaredN, body)

	for _, mode := range []string{"Mmap", "Pread"} {
		t.Run(mode, func(t *testing.T) {
			var s *segment
			if mode == "Mmap" {
				s = newV2TestSegmentMmap(t, section)
			} else {
				s = newV2TestSegmentPread(t, section)
			}
			err := s.loadPropertyLengthsV2()
			require.Error(t, err, "points-past-EOF n must be rejected at load, not read past the file")
			require.Contains(t, err.Error(), "torn V2 property-length section")
			require.False(t, s.invertedData.propLengthV2Loaded,
				"a rejected section must not be marked loaded")
		})
	}
}

// nonFiniteAvgWarnSubstring is the stable greppable token an operator searches
// for to find a V2 segment whose persisted property-length avg was corrupt and
// got self-healed. The test pins it so a future rewording that drops the
// greppable anchor fails here.
const nonFiniteAvgWarnSubstring = "non-finite persisted avg"

// recomputedAvgWarnSubstring is the second stable token: the WARN must state
// that it RECOMPUTED the avg (the self-heal), not merely that it observed a bad
// value. Pinning it guards against a regression back to warn-only wording.
const recomputedAvgWarnSubstring = "recomputed avg"

// countNonFiniteAvgWarns returns the WARN-level hook entries whose message
// carries the greppable non-finite-avg anchor. Filtering on the substring (not
// just "any warn") keeps the assertion specific: an unrelated WARN from another
// code path can't satisfy it, and the missing-guard case yields zero matches.
func countNonFiniteAvgWarns(hook *test.Hook) []*logrus.Entry {
	var out []*logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, nonFiniteAvgWarnSubstring) {
			out = append(out, e)
		}
	}
	return out
}

// TestV2NonFiniteAvgGuard_SelfHealsLikeV0 pins the disk-corruption SELF-HEAL:
// loadPropertyLengthsV2 reads the persisted prefix avg, and when it is
// non-finite (NaN/+Inf/-Inf -- which can ONLY come from on-disk corruption,
// both write paths persist only finite avgs) it RECOMPUTES the avg from the
// persisted V2 uint32 length column, exactly like the V0 loader does at
// segment_inverted.go:164-175 (avg = sum(lengths)/n, guarded by count>0 && n>0,
// else 0). The recomputed FINITE value is written back so the corrupt scalar
// never poisons the downstream uint64(avg*count) at segment_group.go:494-497 /
// segment_group_prepend.go:122-126. A greppable WARN naming the recompute fires.
//
// This test catches the WARN-ONLY regression (the prior commit left NaN/Inf in
// place) because it asserts the healed avg EQUALS sum(lengths)/n -- the exact
// number V0 would produce for the same lengths. The mechanism that would be
// absent without the self-heal: the finite-trusting read path leaves the
// persisted NaN/Inf untouched, so the equality-to-recomputed-value assertion
// fails RED on the warn-only tree (NaN != 20.0). The finite sub-case asserts
// the avg is UNCHANGED and ZERO matching WARNs fire, pinning that the finite
// path is byte-identical to V0 (the heal branch is never entered).
func TestV2NonFiniteAvgGuard_SelfHealsLikeV0(t *testing.T) {
	docIDs := []uint64{1, 2, 3}
	lengths := []uint32{10, 20, 30}
	// V0/V2 self-heal formula: sum(lengths)/n. (10+20+30)/3 == 20.0. This is the
	// exact value V0's recompute (segment_inverted.go:171) yields for the same
	// lengths, so equality to it proves V2 matches V0.
	const wantRecomputed = float64(10+20+30) / 3.0

	cases := []struct {
		name       string
		avg        float64
		nonFinite  bool // true => expect self-heal + WARN; false => unchanged
		wantHealed float64
	}{
		{name: "NaN_avg", avg: math.NaN(), nonFinite: true, wantHealed: wantRecomputed},
		{name: "PosInf_avg", avg: math.Inf(1), nonFinite: true, wantHealed: wantRecomputed},
		{name: "NegInf_avg", avg: math.Inf(-1), nonFinite: true, wantHealed: wantRecomputed},
		{name: "finite_avg_unchanged", avg: 12.5, nonFinite: false, wantHealed: 12.5},
	}

	for _, mode := range []string{"Mmap", "Pread"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				// count must be > 0 for the heal to recompute (matches V0's
				// count>0 guard); use len(docIDs) as the realistic count.
				section := buildV2Section(tc.avg, uint64(len(docIDs)), docIDs, lengths, 0)

				var s *segment
				if mode == "Mmap" {
					s = newV2TestSegmentMmap(t, section)
				} else {
					s = newV2TestSegmentPread(t, section)
				}

				// Swap in a hook-backed logger so we can capture the WARN. Reset
				// the hook first so only the load's entries are observed.
				logger, hook := test.NewNullLogger()
				s.logger = logger
				hook.Reset()

				require.NoError(t, s.loadPropertyLengthsV2(),
					"the self-heal is non-fatal; load still succeeds on a corrupt avg")

				healed := s.invertedData.avgPropertyLengthsAvg
				require.False(t, math.IsNaN(healed) || math.IsInf(healed, 0),
					"after load the avg must be finite (self-heal), never NaN/Inf")
				require.Equal(t, tc.wantHealed, healed,
					"avg must equal the V0-equivalent recompute sum(lengths)/n (or be unchanged on the finite path)")

				warns := countNonFiniteAvgWarns(hook)
				wantWarns := 0
				if tc.nonFinite {
					wantWarns = 1
				}
				require.Len(t, warns, wantWarns,
					"expected %d non-finite-avg WARN(s), got %d", wantWarns, len(warns))

				if tc.nonFinite {
					e := warns[0]
					// The WARN names the segment path and BOTH the corrupt
					// persisted value and the recomputed value so an operator can
					// grep to the offending segment and see the heal.
					require.Equal(t, s.path, e.Data["segmentPath"],
						"WARN must name the segment path")
					require.Contains(t, e.Data, "persistedAvg",
						"WARN must carry the corrupt persisted avg as a field")
					require.Equal(t, tc.wantHealed, e.Data["recomputedAvg"],
						"WARN must carry the recomputed avg as a field")
					require.Contains(t, e.Message, s.path,
						"WARN message must name the segment path for greppability")
					require.Contains(t, e.Message, recomputedAvgWarnSubstring,
						"WARN message must state it RECOMPUTED the avg (self-heal), not just observed it")
				}
			})
		}
	}
}

// TestV2NonFiniteAvgGuard_HealsToZero pins the V0 zero-fallback guards: when the
// persisted avg is non-finite but there are no lengths to recompute from (empty
// section, n==0) OR the persisted count is 0, the self-heal falls to avg=0
// exactly like V0 (segment_inverted.go:173), without panicking on the empty
// length column. This is the V2 analog of V0's `count>0 && len(propLengths)>0`
// guard failing.
//
// This test catches a heal that divides by zero / dereferences an absent length
// column on an empty section: it forges NaN-avg sections with (a) n==0 (empty)
// and (b) a present column but count==0, and asserts the healed avg is 0 with
// no panic and no NaN leaking through.
func TestV2NonFiniteAvgGuard_HealsToZero(t *testing.T) {
	docIDs := []uint64{1, 2, 3}
	lengths := []uint32{10, 20, 30}

	cases := []struct {
		name    string
		count   uint64
		docIDs  []uint64
		lengths []uint32
	}{
		// Empty section (size==0, n==0): no lengths -> the n>0 guard fails -> 0.
		// Exercises the size==0 short-circuit self-heal call site.
		{name: "empty_section_n0", count: 0, docIDs: nil, lengths: nil},
		// Present length column but persisted count==0: the count>0 guard fails
		// -> 0, matching V0's count guard (it does not trust the column when the
		// recorded count is 0).
		{name: "count0_with_column", count: 0, docIDs: docIDs, lengths: lengths},
	}

	for _, mode := range []string{"Mmap", "Pread"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				section := buildV2Section(math.NaN(), tc.count, tc.docIDs, tc.lengths, 0)

				var s *segment
				if mode == "Mmap" {
					s = newV2TestSegmentMmap(t, section)
				} else {
					s = newV2TestSegmentPread(t, section)
				}
				logger, hook := test.NewNullLogger()
				s.logger = logger
				hook.Reset()

				require.NotPanics(t, func() {
					require.NoError(t, s.loadPropertyLengthsV2())
				}, "self-heal must not panic on an empty/zero-count section")

				healed := s.invertedData.avgPropertyLengthsAvg
				require.False(t, math.IsNaN(healed) || math.IsInf(healed, 0),
					"a non-finite avg must never leak through, even with no lengths")
				require.Equal(t, float64(0), healed,
					"the no-lengths / count==0 guard heals to 0, matching V0")

				// The heal still fired a WARN (it observed and corrected the corrupt avg).
				require.Len(t, countNonFiniteAvgWarns(hook), 1,
					"the self-heal must surface the corruption even when it heals to 0")
			})
		}
	}
}
