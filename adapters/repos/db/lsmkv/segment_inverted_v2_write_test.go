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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/gobenc"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// buildV0Section serializes a legacy V0 property-length section: the 24-byte
// [avg][count][size] prefix followed by the gob-encoded map[uint64]uint32 body,
// where size == len(gob). This is the shape loadPropertyLengthsV0Lossless reads.
// Lengths are stored verbatim (uint32) -- a >65535 length is legal on disk (the
// D1 clamp lived only in the resident-slice read path, never on disk).
func buildV0Section(t *testing.T, avg float64, count uint64, m map[uint64]uint32) []byte {
	t.Helper()
	encoded, err := gobenc.Encode(m)
	require.NoError(t, err)

	buf := make([]byte, propLengthV2PrefixSize)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(avg))
	binary.LittleEndian.PutUint64(buf[8:16], count)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(encoded)))
	if len(m) == 0 {
		binary.LittleEndian.PutUint64(buf[16:24], 0)
		return buf
	}
	return append(buf, encoded...)
}

// newV0TestSegmentMmap builds a minimal V0 (gob-map) *segment in mmap mode whose
// contents hold the V0 section at PropertyLengthsOffset. Used as a
// convert-on-compaction INPUT for the per-input-Version read path.
func newV0TestSegmentMmap(t *testing.T, section []byte) *segment {
	t.Helper()
	const pad = 64
	contents := make([]byte, pad+len(section))
	copy(contents[pad:], section)
	return &segment{
		path:           "test-segment-v0-mmap",
		strategy:       segmentindex.StrategyInverted,
		readFromMemory: true,
		contents:       contents,
		logger:         logrus.New(),
		invertedHeader: &segmentindex.HeaderInverted{
			Version:               0,
			PropertyLengthsOffset: pad,
		},
		invertedData: &segmentInvertedData{},
	}
}

// roundTripV2Section encodes a flat run, wraps it in an mmap segment via the T1
// reader, and returns a lookup function over the loaded section. This is the S1
// write->read bridge: the WRITER (encodePropertyLengthsV2) feeds the READER
// (loadPropertyLengthsV2 + propLengthAtV2) so a byte-layout mismatch between them
// surfaces as a wrong/absent length rather than passing silently.
func roundTripV2Section(t *testing.T, avg float64, count uint64, run propLengthRun) *segment {
	t.Helper()
	section := encodePropertyLengthsV2(avg, count, run.docIDs, run.lengths)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())
	return s
}

// TestFlatRoundTrip_BlockBoundaries pins the V2 WRITER against the T1 READER at
// the BlockMax block boundaries (n = 0, 1, 127, 128, 129) plus a >65535 overflow
// entry. The writer emits the flat section; the reader reads every length back.
//
// This test catches a writer byte-layout bug (wrong column base, wrong size
// field, missing n) because the reader computes column offsets from n and the
// size cross-check, and a wrong layout would either reject at load or return a
// wrong/absent length at lookup. It catches a lossy uint16 truncation because a
// >65535 entry read back as 65535 fails the exact-equality assertion. The
// 127/128/129 sizes straddle terms.BLOCK_SIZE (128) -- the property-length
// section is flat and block-agnostic, so these MUST all round-trip identically;
// a writer that accidentally chunked at the block boundary would break exactly
// here.
func TestFlatRoundTrip_BlockBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 129} {
		t.Run("", func(t *testing.T) {
			docIDs := make([]uint64, n)
			lengths := make([]uint32, n)
			for i := 0; i < n; i++ {
				docIDs[i] = uint64(i)*7 + 1 // sparse, sorted ascending
				lengths[i] = uint32(i) + 1
			}
			// Inject a >65535 overflow length in the middle (when room) to prove
			// the lossless uint32 column survives the writer.
			if n >= 3 {
				lengths[n/2] = 200000
			}

			s := roundTripV2Section(t, 12.5, uint64(n), propLengthRun{docIDs: docIDs, lengths: lengths})

			require.Equal(t, uint64(n), s.invertedData.propLengthV2Count)
			require.Equal(t, 12.5, s.invertedData.avgPropertyLengthsAvg)
			require.Equal(t, uint64(n), s.invertedData.avgPropertyLengthsCount)

			for i := 0; i < n; i++ {
				require.Equal(t, lengths[i], lookupV2(t, s, docIDs[i]),
					"n=%d docID=%d", n, docIDs[i])
			}
			if n >= 3 {
				require.Equal(t, uint32(200000), lookupV2(t, s, docIDs[n/2]),
					"overflow length must survive the writer losslessly, n=%d", n)
			}
			// An absent docID returns 0 (zero-for-absent) on every size.
			require.Equal(t, uint32(0), lookupV2(t, s, 999_999_999))
		})
	}
}

// TestFlatRoundTrip_WriterRejectsUnsortedRun pins the writer's sorted-invariant
// guard: a run whose docIDs are not strictly ascending is an internal bug (the
// merge always produces a sorted deduped run), and writePropertyLengthsV2 must
// fail loudly rather than emit a section whose gallop silently mis-resolves.
//
// This test catches a regression that drops the sortedness assertion because it
// feeds a descending run and asserts a write error; without the guard the bytes
// would be written and the reader's gallop would return wrong lengths at score
// time (a silent corruption), which no round-trip on a sorted run would reveal.
func TestFlatRoundTrip_WriterRejectsUnsortedRun(t *testing.T) {
	var buf nopBuffer
	run := propLengthRun{docIDs: []uint64{5, 3, 1}, lengths: []uint32{10, 20, 30}}
	_, err := writePropertyLengthsV2(&buf, 1.0, 3, run)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not sorted")
}

type nopBuffer struct{ n int }

func (b *nopBuffer) Write(p []byte) (int, error) { b.n += len(p); return len(p), nil }

// TestConvertOnCompaction_V0toV2_Lossless is S8: a V0 input is read LOSSLESSLY
// per its own Version and converted to a V2 output; EVERY length matches the V0
// source INCLUDING a >65535 overflow doc with NO re-clamp.
//
// This test catches a re-clamp regression (sourcing V0 lengths from the D1
// uint16 resident path instead of the lossless gob) because the V0 fixture holds
// a 200000-length doc and the test asserts the converted V2 value equals 200000
// exactly. The baseline-first arm proves the uint16 path WOULD return 65535 for
// the same doc, so the test fails loudly if the convert ever routes through the
// clamp -- the exact silent corruption (D1 B1) this lossless read exists to kill.
func TestConvertOnCompaction_V0toV2_Lossless(t *testing.T) {
	// V0 source map incl a >65535 doc.
	v0 := map[uint64]uint32{
		1:    10,
		7:    20,
		42:   65535,
		100:  65536,  // just over the uint16 ceiling
		4242: 200000, // the overflow doc
	}
	v0Section := buildV0Section(t, 33.0, uint64(len(v0)), v0)
	v0Seg := newV0TestSegmentMmap(t, v0Section)

	// Read the V0 input per its own Version (G6 dispatch -> lossless gob).
	run, err := v0Seg.loadPropertyLengthRunForVersion()
	require.NoError(t, err)
	require.True(t, propLengthRunSorted(run), "lossless V0 read must produce a sorted run")

	// Convert: a single-input "merge" (older empty) -> V2 output, round-tripped
	// through the T1 reader.
	merged := mergePropertyLengthRunsV2(propLengthRun{}, run)
	v2Seg := roundTripV2Section(t, 33.0, uint64(len(v0)), merged)

	// Every length matches the V0 source EXACTLY -- including the overflow doc.
	for docID, want := range v0 {
		require.Equal(t, want, lookupV2(t, v2Seg, docID),
			"converted V2 length for docID %d must equal the lossless V0 source", docID)
	}
	require.Equal(t, uint32(200000), lookupV2(t, v2Seg, 4242),
		"overflow doc must be lossless through V0->V2 convert (NO re-clamp)")
	require.Equal(t, uint32(65536), lookupV2(t, v2Seg, 100))

	// Baseline-first: prove the uint16 block-impact snapshot path
	// (snapshotPropLengthSlices) WOULD re-clamp the overflow doc to 65535, so the
	// lossless assertions above are meaningful -- the convert MUST NOT route through
	// the clamping path. T3 made the resident SCORE slice lossless (CAVEAT-B), so
	// the clamp now lives only at the block-impact snapshot boundary; that is the
	// path the convert must avoid, and the one this baseline exercises.
	clampSeg := &segment{
		strategy:     segmentindex.StrategyInverted,
		logger:       logrus.New(),
		invertedData: &segmentInvertedData{},
	}
	clampSeg.buildPropLengthSlices(v0)
	clampSeg.invertedData.propertyLengthsLoaded = true // built directly; mark loaded
	require.Equal(t, uint32(200000), clampSeg.invertedData.propLengthAtLocked(4242),
		"the resident score slice is now LOSSLESS (CAVEAT-B): the overflow doc is "+
			"carried verbatim, not clamped")
	_, clampedVals, err := clampSeg.snapshotPropLengthSlices()
	require.NoError(t, err)
	// docIDs are sorted ascending: 1,7,42,100,4242 -> the overflow doc is index 4.
	require.Equal(t, uint16(65535), clampedVals[4],
		"baseline: the uint16 block-impact snapshot still clamps the overflow doc to "+
			"65535 -- the V2 convert must NOT route through this clamping path")
}

// TestCompactV2V2_TransitiveNewerWins is S9: a V2->V2 merge across multiple
// levels (transitive), NEWER-wins on every docID collision. Modelled as two
// rounds: (gen0 merge gen1) -> gen01, then (gen01 merge gen2) -> final, mirroring
// repeated compaction levels.
//
// This test catches an older-wins or dropped-column merge because gen2 (the
// newest) overrides overlapping docIDs from earlier generations, and the test
// asserts the FINAL value is the gen2 value for every collision and the
// earlier-only docIDs survive untouched. A merge that kept the older value, or
// dropped a non-overlapping docID, fails the exact-equality assertion.
func TestCompactV2V2_TransitiveNewerWins(t *testing.T) {
	gen0 := propLengthRun{docIDs: []uint64{1, 2, 3, 10}, lengths: []uint32{100, 100, 100, 100}}
	gen1 := propLengthRun{docIDs: []uint64{2, 3, 4}, lengths: []uint32{200, 200, 200}}       // overrides 2,3; adds 4
	gen2 := propLengthRun{docIDs: []uint64{3, 5, 70000}, lengths: []uint32{300, 300, 90000}} // overrides 3; adds 5, overflow doc

	// Level 1: older=gen0, newer=gen1.
	gen01 := mergePropertyLengthRunsV2(gen0, gen1)
	// Level 2: older=gen01, newer=gen2.
	final := mergePropertyLengthRunsV2(gen01, gen2)

	s := roundTripV2Section(t, 1.0, uint64(len(final.docIDs)), final)

	// Collisions resolve to the NEWEST generation that wrote the docID.
	require.Equal(t, uint32(100), lookupV2(t, s, 1), "docID 1 only in gen0")
	require.Equal(t, uint32(200), lookupV2(t, s, 2), "docID 2: gen1 newer than gen0")
	require.Equal(t, uint32(300), lookupV2(t, s, 3), "docID 3: gen2 is newest of all three")
	require.Equal(t, uint32(200), lookupV2(t, s, 4), "docID 4 only in gen1")
	require.Equal(t, uint32(300), lookupV2(t, s, 5), "docID 5 only in gen2")
	require.Equal(t, uint32(100), lookupV2(t, s, 10), "docID 10 only in gen0, untouched")
	require.Equal(t, uint32(90000), lookupV2(t, s, 70000), "overflow doc lossless through V2->V2")
}

// TestG6_MixedInputMerge is the G6 load-bearing test: the per-input-Version merge
// over (V0+V0), (V2+V2), and (V0+V2) inputs, NEWER-wins on collision, RED-first.
//
// This test catches the single most likely silent-corruption bug (reading a V0
// gob section as a V2 flat array or vice versa) because it builds REAL V0 and V2
// input segments and exercises loadPropertyLengthRunForVersion's per-input
// Version dispatch -- a single-assumed-version read would decode one input as
// garbage and the asserted lengths would be wrong. The RED-first sub-test proves
// the test catches an older-wins merge: swapping older/newer yields the WRONG
// value on the collision docID, so a merge-order regression fails loudly.
func TestG6_MixedInputMerge(t *testing.T) {
	// Shared collision docID 42: older has 111, newer has 222 -> newer (222) wins.
	const collisionDocID = uint64(42)

	makeV0 := func(m map[uint64]uint32) *segment {
		return newV0TestSegmentMmap(t, buildV0Section(t, 5.0, uint64(len(m)), m))
	}
	makeV2 := func(docIDs []uint64, lengths []uint32) *segment {
		section := buildV2Section(5.0, uint64(len(docIDs)), docIDs, lengths, 0)
		s := newV2TestSegmentMmap(t, section)
		return s
	}

	readRun := func(s *segment) propLengthRun {
		run, err := s.loadPropertyLengthRunForVersion()
		require.NoError(t, err)
		require.True(t, propLengthRunSorted(run), "per-input read must produce a sorted run")
		return run
	}

	t.Run("V0_plus_V0", func(t *testing.T) {
		older := readRun(makeV0(map[uint64]uint32{1: 10, collisionDocID: 111}))
		newer := readRun(makeV0(map[uint64]uint32{collisionDocID: 222, 99: 90000}))
		merged := mergePropertyLengthRunsV2(older, newer)
		s := roundTripV2Section(t, 5.0, uint64(len(merged.docIDs)), merged)
		require.Equal(t, uint32(10), lookupV2(t, s, 1))
		require.Equal(t, uint32(222), lookupV2(t, s, collisionDocID), "newer wins")
		require.Equal(t, uint32(90000), lookupV2(t, s, 99))
	})

	t.Run("V2_plus_V2", func(t *testing.T) {
		older := readRun(makeV2([]uint64{1, collisionDocID}, []uint32{10, 111}))
		newer := readRun(makeV2([]uint64{collisionDocID, 99}, []uint32{222, 90000}))
		merged := mergePropertyLengthRunsV2(older, newer)
		s := roundTripV2Section(t, 5.0, uint64(len(merged.docIDs)), merged)
		require.Equal(t, uint32(10), lookupV2(t, s, 1))
		require.Equal(t, uint32(222), lookupV2(t, s, collisionDocID), "newer wins")
		require.Equal(t, uint32(90000), lookupV2(t, s, 99))
	})

	t.Run("V0_plus_V2_mixed", func(t *testing.T) {
		// The trap: a V0 older input + a V2 newer input. The len(DataFields) guard
		// does NOT catch this mix; only the per-input Version dispatch reads each
		// correctly. The V0 source carries a >65535 doc to prove the V0 read is
		// lossless AND correctly distinguished from the V2 flat read.
		older := readRun(makeV0(map[uint64]uint32{1: 10, collisionDocID: 111, 7: 200000}))
		newer := readRun(makeV2([]uint64{collisionDocID, 99}, []uint32{222, 90000}))
		merged := mergePropertyLengthRunsV2(older, newer)
		s := roundTripV2Section(t, 5.0, uint64(len(merged.docIDs)), merged)
		require.Equal(t, uint32(10), lookupV2(t, s, 1), "V0-only doc read correctly")
		require.Equal(t, uint32(200000), lookupV2(t, s, 7), "V0 overflow doc lossless across the mix")
		require.Equal(t, uint32(222), lookupV2(t, s, collisionDocID), "V2 newer wins over V0 older")
		require.Equal(t, uint32(90000), lookupV2(t, s, 99), "V2-only doc read correctly")
	})

	t.Run("RED_first_older_wins_must_fail", func(t *testing.T) {
		// Prove the assertion catches a merge-order regression: swapping older and
		// newer (so the OLDER value wins on the collision) yields 111, not 222.
		// This is the failure the NEWER-wins tests above would silently pass if the
		// merge had the order backwards.
		older := readRun(makeV0(map[uint64]uint32{collisionDocID: 111}))
		newer := readRun(makeV2([]uint64{collisionDocID}, []uint32{222}))

		correct := mergePropertyLengthRunsV2(older, newer)
		require.Equal(t, uint32(222), correct.lengths[indexOfDocID(correct, collisionDocID)],
			"sanity: correct order yields newer=222")

		swapped := mergePropertyLengthRunsV2(newer, older) // WRONG order on purpose
		require.Equal(t, uint32(111), swapped.lengths[indexOfDocID(swapped, collisionDocID)],
			"RED-first: older-wins (swapped) order yields 111 -- the regression the "+
				"NEWER-wins assertions catch")
		require.NotEqual(t, correct.lengths[indexOfDocID(correct, collisionDocID)],
			swapped.lengths[indexOfDocID(swapped, collisionDocID)],
			"the two orders MUST differ on the collision, proving the test is not vacuous")
	})
}

func indexOfDocID(run propLengthRun, docID uint64) int {
	for i, d := range run.docIDs {
		if d == docID {
			return i
		}
	}
	return -1
}

// TestWriteNewInverted_DefaultOff pins the reader-ahead-of-writer constraint: a
// freshly constructed Memtable, SegmentGroup, and compactorInverted all have the
// write-new flag OFF unless explicitly enabled, so flush and compaction write the
// legacy V0 gob format by default.
//
// This test catches an accidental default-on (e.g. a struct literal that sets the
// flag true, or an option wired to default-enable) because it asserts the
// zero-value flag is false on every carrier AND that the flush Version chosen
// from it is 0. Enabling V2 writes before the T3 score path ships makes V2 docs
// score propLength 0 -- this guard is what keeps that from happening silently.
func TestWriteNewInverted_DefaultOff(t *testing.T) {
	// Fresh Memtable via memtableConfig zero value: writeNewInverted is false.
	require.False(t, memtableConfig{}.writeNewInverted,
		"memtableConfig default writeNewInverted must be false")

	// Fresh compactor (writeV2 arg false) writes V0.
	c := &compactorInverted{writeV2: false}
	require.False(t, c.writeV2, "compactor default writeV2 must be false")

	// The flush Version derived from an off flag is 0 (legacy gob).
	var m Memtable
	require.False(t, m.writeNewInverted, "Memtable default writeNewInverted must be false")
	invertedVersion := uint8(0)
	if m.writeNewInverted {
		invertedVersion = segmentindex.SegmentInvertedVersionV2
	}
	require.Equal(t, uint8(0), invertedVersion, "flush with flag off must choose Version 0")

	// And when explicitly enabled, the flush Version is the V2 sentinel.
	m.writeNewInverted = true
	if m.writeNewInverted {
		invertedVersion = segmentindex.SegmentInvertedVersionV2
	}
	require.Equal(t, segmentindex.SegmentInvertedVersionV2, invertedVersion,
		"flush with flag on must choose the V2 sentinel")
}
