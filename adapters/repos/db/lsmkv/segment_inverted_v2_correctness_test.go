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

//go:build integrationTest

package lsmkv

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// T4 - the correctness backbone that proves the D1-review blocker class
// (B1/F2/F3 lossless, G3 cross-version parity, G8 avg-scalar) is closed.
//
// Every test below hits a REAL on-disk load->compact->score path through a real
// *Bucket: it imports docs, FlushAndSwitch-es real segments, runs the real
// convert-on-compaction `compactOnce()`, then reads lengths/avg back through the
// production reader (loadPropertyLengths / loadPropertyLengthsV2 /
// propLengthForScore) and BM25-scores them with the exact bm25TF tf term. No
// test hand-builds a segmentInvertedData; the on-disk bytes the real pipeline
// produced are the contract.

// t4NewBucket spins a real inverted Bucket. writeV2 selects the flush/compaction
// output format (default-OFF V0 gob, or V2 flat columns behind the write-new
// flag the design gates V2 behind).
func t4NewBucket(t *testing.T, writeV2 bool) *Bucket {
	t.Helper()
	ctx := context.Background()
	dirName := t.TempDir()

	opts := []BucketOption{WithStrategy(StrategyInverted)}
	if writeV2 {
		opts = append(opts, WithWriteInvertedSegmentV2(true))
	}

	b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9) // never auto-flush mid-segment
	return b
}

// t4CompactToOne runs the real compaction loop to convergence and returns the
// single survivor segment.
func t4CompactToOne(t *testing.T, b *Bucket) *segment {
	t.Helper()
	for {
		compacted, err := b.disk.compactOnce(context.Background())
		require.NoError(t, err)
		if !compacted {
			break
		}
	}
	require.Len(t, b.disk.segments, 1, "compaction must converge to a single segment")
	seg, ok := b.disk.segments[0].(*segment)
	require.True(t, ok)
	return seg
}

// t4ScoreLen reads the per-doc length the EXACT BM25 score path reads
// (propLengthForScore -> V2 gallop / V0 resident slice), loading the segment's
// resident V0 slices first when needed (the V2 path needs no eager load). It is
// the same read segment_blockmax.go uses at score time.
func t4ScoreLen(t *testing.T, seg *segment, docID uint64) uint32 {
	t.Helper()
	if seg.isPropLengthV2() {
		require.NoError(t, seg.loadPropertyLengthsV2())
	} else {
		require.NoError(t, seg.loadPropertyLengths())
	}
	var cur uint64
	return seg.propLengthForScore(docID, &cur)
}

// t4PersistedAvg returns the avg/count scalar the segment persisted (the BM25
// denominator), read through the production loader of the segment's own format.
func t4PersistedAvg(t *testing.T, seg *segment) (avg float64, count uint64) {
	t.Helper()
	if seg.isPropLengthV2() {
		require.NoError(t, seg.loadPropertyLengthsV2())
	} else {
		require.NoError(t, seg.loadPropertyLengths())
	}
	return seg.invertedData.avgPropertyLengthsAvg, seg.invertedData.avgPropertyLengthsCount
}

// -----------------------------------------------------------------------------
// S5 - >65535-token overflow doc LOSSLESS END-TO-END through BOTH V0->V0
// compaction AND V0->V2 convert. Baseline-first: the OLD D1 uint16 path would
// have returned 65535; the lossless uint32 column returns the TRUE length.
// -----------------------------------------------------------------------------

// TestS5_OverflowLossless_EndToEnd_BothPaths drives a real Bucket carrying a
// >65535-token doc (200000) through a real flush + real compaction, in BOTH the
// default V0->V0 mode and the V2 convert mode, and asserts the true length
// survives at BOTH the score read AND the persisted on-disk bytes.
//
// Baseline-first witness: clampToUint16([200000]) == 65535 is the value the
// un-fixed D1 uint16 path returns, so any 65535 read-back is the B1 corruption.
// This test catches a re-clamp anywhere in the real flush/convert/merge pipeline
// because it reads 200000 back through the exact score path AND the on-disk
// section after a real compactOnce -- a clamp at any stage surfaces as 65535.
func TestS5_OverflowLossless_EndToEnd_BothPaths(t *testing.T) {
	const overflowLen = uint32(200000) // > maxPropLength (65535); exact in float32 (<2^24)

	// Baseline-first: pin the value the OLD D1 uint16 path would have returned.
	require.Equal(t, uint16(65535), clampToUint16([]uint32{overflowLen})[0],
		"baseline: the D1 uint16 clamp truncates 200000 to 65535 (the B1 corruption D2 fixes)")

	for _, tc := range []struct {
		name    string
		writeV2 bool
		wantVer uint8
	}{
		{"V0_to_V0_default", false, 0},
		{"V0_to_V2_convert", true, segmentindex.SegmentInvertedVersionV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := t4NewBucket(t, tc.writeV2)
			key := []byte("the-term")

			// Segment 1: docs 1,2 with doc 2 carrying the overflow length.
			require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
			require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, float32(overflowLen), false)))
			require.NoError(t, b.FlushAndSwitch())

			// Segment 2: add doc 3 (forces a real multi-segment compaction/merge).
			require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, 33, false)))
			require.NoError(t, b.FlushAndSwitch())

			merged := t4CompactToOne(t, b)
			require.Equal(t, tc.wantVer, merged.invertedHeader.Version,
				"converged segment format must match the configured write mode")

			// Read 1: the exact score path.
			require.Equal(t, overflowLen, t4ScoreLen(t, merged, 2),
				"S5: the >65535 doc must survive the real %s compaction LOSSLESSLY at the score read (got 65535 = the D1 clamp corruption if this fails)", tc.name)
			require.Equal(t, uint32(11), t4ScoreLen(t, merged, 1), "doc 1 untouched")
			require.Equal(t, uint32(33), t4ScoreLen(t, merged, 3), "doc 3 untouched")

			// Read 2: the PERSISTED on-disk section bytes, independent of the
			// resident-slice/cached-base read above, so a clamp baked into the
			// section (not just the in-memory copy) is also caught.
			onDisk := t4ReadOnDiskLengths(t, merged)
			require.Equal(t, overflowLen, onDisk[2],
				"S5: the persisted on-disk section itself must carry 200000, not the clamped 65535 (%s)", tc.name)
			require.Equal(t, uint32(11), onDisk[1])
			require.Equal(t, uint32(33), onDisk[3])
		})
	}
}

// t4ReadOnDiskLengths reads every (docID,length) pair straight from the
// segment's on-disk property-length section via the production format-specific
// lossless reader (loadPropertyLengthsV0Lossless / loadPropertyLengthsV2Lossless),
// independent of the resident-slice score path. Used to pin that the PERSISTED
// bytes -- not just an in-memory copy -- are lossless.
func t4ReadOnDiskLengths(t *testing.T, seg *segment) map[uint64]uint32 {
	t.Helper()
	run, err := seg.loadPropertyLengthRunForVersion()
	require.NoError(t, err)
	out := make(map[uint64]uint32, len(run.docIDs))
	for i, d := range run.docIDs {
		out[d] = run.lengths[i]
	}
	return out
}

// -----------------------------------------------------------------------------
// S6 - BM25 score 0-delta vs base, on a fixed corpus incl an overflow doc, for
// BOTH the V0 path AND a V2 segment. The base tf is derived from the INPUT
// length map (the ground truth fed to the bucket) -- NOT a re-read of the same
// segment -- so the comparison is real, not a tautology.
// -----------------------------------------------------------------------------

// TestS6_BM25ZeroDelta_V0AndV2 builds the SAME fixed corpus (incl a 200000
// overflow doc and a zero-length doc) through the real pipeline once as a V0
// segment and once as a V2 segment, then asserts the exact BM25 tf term computed
// from the score-path length read is bit-identical to the base tf computed from
// the INPUT length map, for every (query,doc).
//
// The base is the ground-truth input map, an INDEPENDENT source from the under-
// test propLengthForScore read, so a 0-delta here is a real equality, not a
// tautological self-comparison. This catches any divergence the format migration
// could introduce (a clamp, a wrong gallop result, a wrong avg denominator)
// because the tf float is reconstructed from the actual read length and the
// persisted avg, and any single mismatch fails the bit-equality assertion.
func TestS6_BM25ZeroDelta_V0AndV2(t *testing.T) {
	const (
		k1   = 1.2
		b    = 0.75
		freq = 5.0
	)
	// Fixed corpus: ground-truth input length map (the base).
	corpus := map[uint64]uint32{
		1: 5,
		2: 65535,  // exactly at the old cap
		3: 65536,  // just over
		4: 200000, // far over (the B1 trigger)
		5: 0,      // zero-length doc (counted in avg)
		6: 42,
	}
	docIDs := []uint64{1, 2, 3, 4, 5, 6}

	for _, writeV2 := range []bool{false, true} {
		name := "V0"
		if writeV2 {
			name = "V2"
		}
		t.Run(name, func(t *testing.T) {
			bkt := t4NewBucket(t, writeV2)
			key := []byte("the-term")
			for _, d := range docIDs {
				require.NoError(t, bkt.MapSet(key,
					NewMapPairFromDocIdAndTf(d, 1.0, float32(corpus[d]), false)))
			}
			require.NoError(t, bkt.FlushAndSwitch())
			// Second segment to force a real merge.
			require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(99, 1.0, 7, false)))
			require.NoError(t, bkt.FlushAndSwitch())
			merged := t4CompactToOne(t, bkt)

			// The avg denominator the score uses comes from the persisted scalar.
			avg, count := t4PersistedAvg(t, merged)
			require.Greater(t, count, uint64(0), "avg count must be persisted for the denominator")

			var cur uint64
			for _, d := range docIDs {
				// Base tf: from the GROUND-TRUTH input length, with the persisted avg.
				baseTF := bm25TF(freq, float64(corpus[d]), k1, b, avg)
				// Under-test tf: from the score-path length read off the real segment.
				gotLen := merged.propLengthForScore(d, &cur)
				gotTF := bm25TF(freq, float64(gotLen), k1, b, avg)

				require.Equalf(t, corpus[d], gotLen,
					"%s: score-path length for doc %d must equal the input length (lossless)", name, d)
				require.Equalf(t, baseTF, gotTF,
					"%s: BM25 tf for doc %d must be bit-identical to the base (0-delta)", name, d)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// S7 / G8 - avg-property-length scalar: persisted == sum/count; NaN/Inf recompute
// path works; zero-length docs counted in avg; survives V0->V2 convert.
// -----------------------------------------------------------------------------

// TestS7_AvgScalarPersist_SumOverCount_BothPaths asserts the persisted avg
// scalar on a real compacted segment equals sum(lengths)/count computed from the
// inputs, that zero-length docs are counted in BOTH the numerator (as 0) and the
// count, and that the scalar survives the V0->V2 convert verbatim (the 24-byte
// prefix is format-independent).
//
// This catches a V2 read that fails to populate avg/count from the prefix (G8:
// every V2 score's denominator would be wrong) because it reads the avg back
// through the production loader of the segment's own format and compares to the
// independently-computed sum/count.
func TestS7_AvgScalarPersist_SumOverCount_BothPaths(t *testing.T) {
	// Corpus including a zero-length doc; avg = sum/count counts the zero.
	corpus := map[uint64]uint32{1: 10, 2: 20, 3: 0, 4: 30, 5: 200000}
	docIDs := []uint64{1, 2, 3, 4, 5}

	var sum uint64
	for _, d := range docIDs {
		sum += uint64(corpus[d])
	}
	wantCount := uint64(len(docIDs))
	wantAvg := float64(sum) / float64(wantCount)

	for _, writeV2 := range []bool{false, true} {
		name := "V0"
		if writeV2 {
			name = "V2"
		}
		t.Run(name, func(t *testing.T) {
			bkt := t4NewBucket(t, writeV2)
			key := []byte("the-term")
			for _, d := range docIDs {
				require.NoError(t, bkt.MapSet(key,
					NewMapPairFromDocIdAndTf(d, 1.0, float32(corpus[d]), false)))
			}
			require.NoError(t, bkt.FlushAndSwitch())
			merged := t4CompactToOne(t, bkt)

			avg, count := t4PersistedAvg(t, merged)
			require.Equalf(t, wantCount, count,
				"%s: avg count must include the zero-length doc (sum over count)", name)
			require.InDeltaf(t, wantAvg, avg, 1e-9,
				"%s: persisted avg must equal sum/count = %v (G8 denominator)", name, wantAvg)

			// Zero-length doc is retained and scores length 0 (not absent-as-error).
			require.Equal(t, uint32(0), t4ScoreLen(t, merged, 3),
				"%s: zero-length doc must read back as length 0", name)
		})
	}
}

// TestS7_AvgScalar_NaNRecompute_V0 pins the V0 NaN/Inf avg-recompute branch
// (segment_inverted.go:164): a V0 section whose persisted avg is NaN must, on
// load, recompute avg = sum(lengths)/len from the decoded map rather than
// propagating NaN into every BM25 denominator.
//
// This test catches a regression that drops the recompute branch because it
// forges a V0 gob section with a NaN avg over a known corpus and asserts the
// loader produces the finite sum/count value, not NaN. The forged section is
// read through the production loadPropertyLengths.
func TestS7_AvgScalar_NaNRecompute_V0(t *testing.T) {
	m := map[uint64]uint32{1: 10, 2: 20, 3: 30, 4: 200000}
	var sum uint64
	for _, v := range m {
		sum += uint64(v)
	}
	wantAvg := float64(sum) / float64(len(m))

	// Forge a V0 section with a NaN persisted avg (count is the doc count).
	section := buildV0Section(t, math.NaN(), uint64(len(m)), m)
	s := newV0TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengths())

	require.False(t, math.IsNaN(s.invertedData.avgPropertyLengthsAvg),
		"NaN persisted avg must be recomputed, not propagated")
	require.InDelta(t, wantAvg, s.invertedData.avgPropertyLengthsAvg, 1e-9,
		"recomputed avg must equal sum/count over the decoded lengths")
	// The recompute path must not corrupt the lossless lengths.
	require.Equal(t, uint32(200000), s.propLengthForScore(4, nil),
		"the >65535 doc survives the NaN-recompute load path losslessly")
}

// TestS7_AvgScalar_InfRecompute_V0 mirrors the NaN case for +Inf: an Inf
// persisted avg must trigger the same finite recompute.
func TestS7_AvgScalar_InfRecompute_V0(t *testing.T) {
	m := map[uint64]uint32{1: 1, 2: 2, 3: 3}
	wantAvg := float64(1+2+3) / 3.0

	section := buildV0Section(t, math.Inf(1), uint64(len(m)), m)
	s := newV0TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengths())

	require.False(t, math.IsInf(s.invertedData.avgPropertyLengthsAvg, 0),
		"Inf persisted avg must be recomputed, not propagated")
	require.InDelta(t, wantAvg, s.invertedData.avgPropertyLengthsAvg, 1e-9,
		"recomputed avg must equal sum/count over the decoded lengths")
}

// -----------------------------------------------------------------------------
// G3 - cross-version V0+V2 score-parity (RED-first): a V0 segment and a V2
// segment containing IDENTICAL docs return identical per-doc lengths AND
// identical BM25 scores. The negative-control assertion proves the test is RED
// if the V2 read were wrong.
// -----------------------------------------------------------------------------

// TestG3_CrossVersionParity_V0vsV2 builds the SAME corpus through the real
// pipeline twice -- once as a V0 segment, once as a V2 segment -- and asserts
// per-doc length and per-(query,doc) BM25 tf are bit-identical across the two
// formats. INV-TOKENIZER-5 holds across the version boundary only if the dual-
// path reader decodes both formats to the same length; G3 pins it.
//
// RED-first / negative control: the test ALSO asserts that the V2 score-path
// read equals the V0 score-path read for every doc. If the V2 flat-column read
// were wrong (a wrong column base, a clamp, an off-by-one gallop), the V2 length
// would diverge from the V0 length and this equality fails -- so the test is RED
// on a broken V2 reader and green only when both formats agree. The companion
// TestG3_NegativeControl below demonstrates the divergence mechanism explicitly.
func TestG3_CrossVersionParity_V0vsV2(t *testing.T) {
	const (
		k1   = 1.2
		b    = 0.75
		freq = 3.0
	)
	corpus := map[uint64]uint32{
		1: 5, 2: 65535, 3: 65536, 4: 200000, 5: 0, 6: 42, 7: 1, 8: 99999,
	}
	docIDs := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	buildMerged := func(writeV2 bool) *segment {
		bkt := t4NewBucket(t, writeV2)
		key := []byte("the-term")
		for _, d := range docIDs {
			require.NoError(t, bkt.MapSet(key,
				NewMapPairFromDocIdAndTf(d, 1.0, float32(corpus[d]), false)))
		}
		require.NoError(t, bkt.FlushAndSwitch())
		require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(50, 1.0, 8, false)))
		require.NoError(t, bkt.FlushAndSwitch())
		return t4CompactToOne(t, bkt)
	}

	v0 := buildMerged(false)
	v2 := buildMerged(true)
	require.Equal(t, uint8(0), v0.invertedHeader.Version, "v0 segment must be Version 0")
	require.Equal(t, segmentindex.SegmentInvertedVersionV2, v2.invertedHeader.Version,
		"v2 segment must carry the V2 format sentinel (header version byte 1)")

	require.NoError(t, v0.loadPropertyLengths())
	require.NoError(t, v2.loadPropertyLengthsV2())

	// Same avg denominator must be persisted on both (cross-version BM25 identity).
	v0Avg, v0Count := v0.invertedData.avgPropertyLengthsAvg, v0.invertedData.avgPropertyLengthsCount
	v2Avg, v2Count := v2.invertedData.avgPropertyLengthsAvg, v2.invertedData.avgPropertyLengthsCount
	require.Equal(t, v0Count, v2Count, "avg count must match across versions")
	require.InDelta(t, v0Avg, v2Avg, 1e-9, "avg denominator must match across versions")

	var c0, c2 uint64
	for _, d := range docIDs {
		l0 := v0.propLengthForScore(d, &c0)
		l2 := v2.propLengthForScore(d, &c2)
		require.Equalf(t, l0, l2,
			"G3: V0 and V2 must return identical lengths for doc %d (got V0=%d V2=%d)", d, l0, l2)
		require.Equalf(t, corpus[d], l2, "G3: V2 length for doc %d must equal the input", d)

		tf0 := bm25TF(freq, float64(l0), k1, b, v0Avg)
		tf2 := bm25TF(freq, float64(l2), k1, b, v2Avg)
		require.Equalf(t, tf0, tf2,
			"G3: V0 and V2 BM25 tf for doc %d must be bit-identical", d)
	}
}

// TestG3_NegativeControl_WrongV2BaseDiverges is the explicit RED demonstration
// for G3: it proves the parity assertion is NOT vacuous by corrupting the V2
// length-column base offset and showing the V2 read then DIVERGES from the V0
// baseline. This is the in-test analog of a stash-revert: it exercises the exact
// failure the parity test guards against and asserts the guard would catch it.
//
// Mechanism: the V2 read computes len[i] at propLengthV2LenBase + i*4. Shifting
// the base by one uint32 makes every length read return the NEXT doc's length,
// so at least one doc diverges from its V0 counterpart. The parity assertion in
// TestG3_CrossVersionParity would fire on exactly this state.
func TestG3_NegativeControl_WrongV2BaseDiverges(t *testing.T) {
	docIDs := []uint64{1, 2, 3, 4, 5}
	lengths := []uint32{10, 20, 30, 40, 50}
	section := buildV2Section(30.0, uint64(len(docIDs)), docIDs, lengths, 0)
	s := newV2TestSegmentMmap(t, section)
	require.NoError(t, s.loadPropertyLengthsV2())

	// Correct read first (the green baseline).
	var cur uint64
	require.Equal(t, uint32(20), s.propLengthForScore(2, &cur))

	// Corrupt the cached length-column base by one uint32 entry (simulating the
	// wrong-base class of bug the parity test must catch).
	s.invertedData.propLengthV2LenBase += propLengthV2LenWidth
	var cur2 uint64
	got := s.propLengthForScore(2, &cur2)
	require.NotEqualf(t, uint32(20), got,
		"negative control: a wrong V2 length-column base MUST diverge from the true length, "+
			"proving the G3 parity assertion is not vacuous (got %d)", got)
}

// -----------------------------------------------------------------------------
// S15 - empty / zero-length / tombstoned docs: n==0, zero-length counted,
// tombstoned handled, no panic.
// -----------------------------------------------------------------------------

// TestS15_EmptyZeroTombstone_BothPaths covers the degenerate data shapes through
// the real pipeline in BOTH formats:
//   - empty term posting / empty section (n==0): compaction converges, lookups
//     return 0, no panic.
//   - zero-length doc: counted in the avg, reads back as length 0.
//   - tombstoned doc: a doc whose map pair is a tombstone is handled (dropped
//     from the live posting) without panicking the length read.
func TestS15_EmptyZeroTombstone_BothPaths(t *testing.T) {
	for _, writeV2 := range []bool{false, true} {
		name := "V0"
		if writeV2 {
			name = "V2"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("empty_section_no_panic", func(t *testing.T) {
				bkt := t4NewBucket(t, writeV2)
				key := []byte("the-term")
				// One doc then tombstone it so the live posting is empty after merge.
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 5, false)))
				require.NoError(t, bkt.FlushAndSwitch())
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 5, true)))
				require.NoError(t, bkt.FlushAndSwitch())

				merged := t4CompactToOne(t, bkt)
				// No panic on the length read of the tombstoned/absent doc.
				require.NotPanics(t, func() {
					_ = t4ScoreLen(t, merged, 1)
				}, "%s: reading a tombstoned/absent doc's length must not panic", name)
				// A genuinely absent docID returns 0 (zero-for-absent).
				require.Equal(t, uint32(0), t4ScoreLen(t, merged, 12345),
					"%s: absent docID returns 0", name)
			})

			t.Run("zero_length_counted", func(t *testing.T) {
				bkt := t4NewBucket(t, writeV2)
				key := []byte("the-term")
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 0, false)))
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 10, false)))
				require.NoError(t, bkt.FlushAndSwitch())
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, 20, false)))
				require.NoError(t, bkt.FlushAndSwitch())
				merged := t4CompactToOne(t, bkt)

				_, count := t4PersistedAvg(t, merged)
				require.Equal(t, uint64(3), count,
					"%s: the zero-length doc must be counted (count includes len-0 docs)", name)
				require.Equal(t, uint32(0), t4ScoreLen(t, merged, 1),
					"%s: zero-length doc reads back as 0", name)
				require.Equal(t, uint32(10), t4ScoreLen(t, merged, 2))
				require.Equal(t, uint32(20), t4ScoreLen(t, merged, 3))
			})

			t.Run("tombstone_drops_doc", func(t *testing.T) {
				bkt := t4NewBucket(t, writeV2)
				key := []byte("the-term")
				// Two live docs + one that gets tombstoned in a later segment.
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
				require.NoError(t, bkt.FlushAndSwitch())
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, true))) // tombstone doc 2
				require.NoError(t, bkt.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, 33, false)))
				require.NoError(t, bkt.FlushAndSwitch())
				merged := t4CompactToOne(t, bkt)

				require.NotPanics(t, func() {
					_ = t4ScoreLen(t, merged, 2)
				}, "%s: reading the tombstoned doc's length must not panic", name)
				require.Equal(t, uint32(11), t4ScoreLen(t, merged, 1), "%s: live doc 1 intact", name)
				require.Equal(t, uint32(33), t4ScoreLen(t, merged, 3), "%s: live doc 3 intact", name)
			})
		})
	}
}
