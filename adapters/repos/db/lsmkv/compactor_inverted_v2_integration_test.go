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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// readResidentSegmentV2PropLengths reads every (docID,length) pair from a
// resident segment's V2 flat-column property-length section via the production
// reader (loadPropertyLengthsV2 + readDocIDV2/readLenV2). It asserts the segment
// IS V2 (Version sentinel set by the real flush/compaction write path) and
// returns a docID->length map for value assertions. This reads the on-disk
// section the real pipeline produced -- not a hand-built one.
func readResidentSegmentV2PropLengths(t *testing.T, seg *segment) map[uint64]uint32 {
	t.Helper()
	require.True(t, seg.isPropLengthV2(),
		"segment written with the V2 flag must carry the V2 Version sentinel")
	require.NoError(t, seg.loadPropertyLengthsV2())
	out := make(map[uint64]uint32, seg.invertedData.propLengthV2Count)
	for i := uint64(0); i < seg.invertedData.propLengthV2Count; i++ {
		d, err := seg.readDocIDV2(i, nil)
		require.NoError(t, err)
		l, err := seg.readLenV2(i, nil)
		require.NoError(t, err)
		out[d] = l
	}
	return out
}

// buildOverflowTwoSegmentBucket imports the canonical two-segment fixture shared
// by the convert-on-compaction tests and returns the bucket with two flushed
// segments, ready to compact. writeV2 selects the V2 write flag (V2 flat output)
// vs the default (V0 gob output).
//
// The fixture is deliberately the corner case the whole change set turns on:
//   - segment 1 = docs {1:11, 2:22, 3:200000} -- doc 3 is a >65535-token
//     overflow length (exact in float32, <2^24) that the old uint16 clamp
//     corrupted;
//   - segment 2 = {2:999, 4:44} -- doc 2 is re-stated so NEWER-wins merge
//     ordering is exercised, doc 4 is added.
func buildOverflowTwoSegmentBucket(t *testing.T, writeV2 bool) *Bucket {
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

	key := []byte("the-term")
	const overflowLen = float32(200000) // exact in float32 (<2^24)

	// Segment 1: docs 1,2,3 with a >65535 overflow propLength on doc 3.
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(1, 1.0, 11, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 22, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(3, 1.0, overflowLen, false)))
	require.NoError(t, b.FlushAndSwitch())

	// Segment 2: update doc 2 (NEWER value 999 must win) + add doc 4.
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(2, 1.0, 999, false)))
	require.NoError(t, b.MapSet(key, NewMapPairFromDocIdAndTf(4, 1.0, 44, false)))
	require.NoError(t, b.FlushAndSwitch())

	require.GreaterOrEqual(t, len(b.disk.segments), 2)
	return b
}

// TestConvertOnCompaction_RealPipeline_V0toV2 drives the REAL flush +
// REAL compaction `do()` with the V2 write flag ON: it imports docs across two
// flushed segments (one carrying a >65535-token overflow doc), compacts them to a
// single segment, and reads back the resulting on-disk V2 property-length section
// to assert every length survived the convert LOSSLESSLY, NEWER-wins on a docID
// updated across the two segments, and the output Version is the V2 sentinel.
//
// This exercises the production code my unit tests model -- memtable_flush_inverted
// (Version=1 + flat write) and compactorInverted.do() (per-input-Version read +
// flat write) -- end to end through a real Bucket. It catches a re-clamp in the
// real flush or convert path (the overflow doc would read back 65535) and a
// merge-order regression (the updated doc would read back the stale value) on the
// ACTUAL bytes, not a hand-assembled section.
//
// Scored BM25 is deliberately NOT asserted here: the per-doc score path
// (propLengthAt) is wired to V2 in T3, so a V2 doc scores propLength 0 until then.
// T2 validates the WRITE path; the section bytes are the contract.
func TestConvertOnCompaction_RealPipeline_V0toV2(t *testing.T) {
	b := buildOverflowTwoSegmentBucket(t, true)

	// Both flushed segments must already be V2 (the flush write path).
	for i := range b.disk.segments {
		seg, ok := b.disk.segments[i].(*segment)
		require.True(t, ok)
		require.Equal(t, segmentindex.SegmentInvertedVersionV2, seg.invertedHeader.Version,
			"flush with the V2 flag must write Version=1")
	}

	// Compact to a single segment (the real convert-on-compaction `do()`).
	merged := t4CompactToOne(t, b)

	got := readResidentSegmentV2PropLengths(t, merged)

	require.Equal(t, uint32(11), got[1], "doc 1 untouched")
	require.Equal(t, uint32(999), got[2], "doc 2 NEWER value (segment 2) wins over segment 1")
	require.Equal(t, uint32(200000), got[3],
		"overflow doc survives real flush + real convert LOSSLESSLY (NO re-clamp)")
	require.Equal(t, uint32(44), got[4], "doc 4 added in segment 2")
}

// TestCompaction_RealPipeline_V0toV0_LosslessOver65535 is the M1 regression test:
// the DEFAULT inverted compaction (V2 write flag OFF -> V0 gob output, the live
// reader-ahead-of-writer posture) must persist a >65535-token doc's EXACT per-doc
// length LOSSLESSLY through a V0->V0 merge.
//
// Before the fix, the V0 gob section was gob-encoded from the CLAMPED uint16
// block-impact map (snapshotPropLengthSlices -> clampPropLengthsToUint16 ->
// mergePropertyLengthRuns -> writePropertyLengths). T3 made the V0 RESIDENT read
// lossless, but the first compaction re-clamped a >65535 doc back to 65535 and
// persisted THAT as the exact score length -- the D1 B1 corruption deferred by one
// compaction, on the default path. The fix sources the V0 gob from the merged
// LOSSLESS uint32 run (propLengthRunToWrite) and confines the uint16 clamp to the
// block-impact MaxImpactPropLength (WAND) path only.
//
// This catches the M1 corruption because it reads the merged length back through
// BOTH the resident-slice score path (propLengthAt, after a fresh loadPropertyLengths
// of the merged gob) AND the direct on-disk gob decode (loadPropertyLengthsV0Lossless),
// asserting 200000 -- not 65535 -- on each. The pre-fix code persists 0xffff (65535)
// in the gob, so both reads return 65535 and the test goes RED; the fix persists
// 0x30d40 (200000) and both return 200000.
//
// NEWER-wins (doc 2 updated across the two segments) and untouched docs are also
// asserted so the lossless re-source does not regress merge ordering.
func TestCompaction_RealPipeline_V0toV0_LosslessOver65535(t *testing.T) {
	// V2 write flag OFF == the DEFAULT == V0 gob flush + V0 gob compaction output.
	b := buildOverflowTwoSegmentBucket(t, false)

	// Both flushed segments must be V0 (the default flush path).
	for i := range b.disk.segments {
		seg, ok := b.disk.segments[i].(*segment)
		require.True(t, ok)
		require.NotEqual(t, segmentindex.SegmentInvertedVersionV2, seg.invertedHeader.Version,
			"the default flush must write the V0 gob section, not V2")
	}

	// Real V0->V0 compaction (the default `do()` path, writeV2 == false).
	merged := t4CompactToOne(t, b)
	require.NotEqual(t, segmentindex.SegmentInvertedVersionV2, merged.invertedHeader.Version,
		"the converged segment must be V0 (default output)")

	// Read 1: the score path. Load the merged gob into the resident lossless uint32
	// slices, then read every doc the way Score does (propLengthAt -> resident slice).
	require.NoError(t, merged.loadPropertyLengths())
	require.Equal(t, uint32(11), merged.propLengthAt(1), "doc 1 untouched")
	require.Equal(t, uint32(999), merged.propLengthAt(2), "doc 2 NEWER value (segment 2) wins")
	require.Equal(t, uint32(200000), merged.propLengthAt(3),
		"M1: >65535 doc must survive V0->V0 compaction LOSSLESSLY at the score read (got 65535 before the fix)")
	require.Equal(t, uint32(44), merged.propLengthAt(4), "doc 4 added in segment 2")

	// Read 2: the on-disk gob bytes directly (independent of the resident-slice path),
	// to pin that the PERSISTED section -- not just the in-memory copy -- is lossless.
	run, err := merged.loadPropertyLengthsV0Lossless()
	require.NoError(t, err)
	gob := propLengthRunToMap(run)
	require.Equal(t, uint32(11), gob[1])
	require.Equal(t, uint32(999), gob[2])
	require.Equal(t, uint32(200000), gob[3],
		"M1: the persisted V0 gob section itself must carry 200000, not the clamped 65535")
	require.Equal(t, uint32(44), gob[4])
}

// TestConvertOnCompaction_RealPipeline_V0toV2_ThroughTheTree closes the coverage
// hole that hid MF1: it reads a term back THROUGH THE ON-DISK TREE (s.index.Get
// via getDocCount) after a real flush + real convert-on-compaction with the V2
// flag, asserting NO panic AND the correct posting.
//
// The sibling S8 test reads only the property-length SECTION (via
// PropertyLengthsOffset, captured BEFORE the proplen write), so it never touches
// the segment's disk tree and cannot observe a wrong Header.IndexStart. MF1 was
// exactly that: the V2 compaction branch wrote the proplen section but did not
// advance c.offset, so treeOffset (captured AFTER the write) equalled
// propertyLengthsOffset and IndexStart pointed INTO the proplen section. On
// reopen the disk-tree parser read float-bit proplen bytes as a node-key length
// and DiskTree.Get panicked with slice-bounds-out-of-range. This test drives the
// real compactOnce with the flag ON and then issues a tree read; it panics on the
// pre-fix tree and returns the correct doc count with the c.offset advance.
//
// getDocCount calls s.index.Get(key) (the through-the-tree lookup) and returns
// the first 8 bytes of the inverted node payload, which is the per-term doc count.
// The term "the-term" carries 4 distinct docs (1,2,3,4) after the convert, so the
// correct return is 4. (getDocCount swallows a non-nil Get error to 0, but the bug
// is a PANIC, not an error return, so a non-panicking 0 cannot pass either.)
func TestConvertOnCompaction_RealPipeline_V0toV2_ThroughTheTree(t *testing.T) {
	b := buildOverflowTwoSegmentBucket(t, true)
	key := []byte("the-term")

	merged := t4CompactToOne(t, b)
	require.Equal(t, segmentindex.SegmentInvertedVersionV2, merged.invertedHeader.Version,
		"the converged segment must be V2")

	// Through-the-tree read: getDocCount -> s.index.Get. On the pre-fix tree this
	// panics (corrupt IndexStart). Wrapping in require.NotPanics turns the panic
	// into a clean test failure for RED-first verification rather than aborting
	// the whole test binary.
	var docCount uint64
	require.NotPanics(t, func() {
		docCount = merged.getDocCount(key)
	}, "tree read must not panic: a corrupt Header.IndexStart makes DiskTree.Get read proplen bytes as a node header")

	require.Equal(t, uint64(4), docCount,
		"the term carries 4 distinct docs (1,2,3,4) after the convert; a correct on-disk tree returns 4")
}
