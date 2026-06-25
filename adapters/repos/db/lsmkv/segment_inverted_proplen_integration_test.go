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
// +build integrationTest

package lsmkv

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestSegmentPropertyLengthsRepresentations flushes real inverted segments with
// dense and sparse docID patterns and verifies that the on-load representation
// choice (dense array vs sorted pairs) is invisible to all consumers: the
// per-docID view matches what was written, and getPropertyLengths reconstructs
// the exact stored map that compaction round-trips to disk.
func TestSegmentPropertyLengthsRepresentations(t *testing.T) {
	tests := []struct {
		name      string
		docIDs    func() []uint64
		wantDense bool
	}{
		{
			// contiguous ids: span == count -> dense array
			name: "dense_sequential",
			docIDs: func() []uint64 {
				ids := make([]uint64, 500)
				for i := range ids {
					ids[i] = uint64(i)
				}
				return ids
			},
			wantDense: true,
		},
		{
			// stride 10: span ~10x count, far below 1/3 occupancy -> sorted pairs
			name: "sparse_strided",
			docIDs: func() []uint64 {
				ids := make([]uint64, 500)
				for i := range ids {
					ids[i] = uint64(i) * 10
				}
				return ids
			},
			wantDense: false,
		},
		{
			// exactly at the gate: span == 3*count keeps dense
			name: "boundary_one_third",
			docIDs: func() []uint64 {
				ids := make([]uint64, 500)
				for i := range ids {
					ids[i] = uint64(i) * 3
				}
				return ids
			},
			wantDense: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted))
			require.NoError(t, err)
			defer bucket.Shutdown(ctx)

			key := []byte("term")
			docIDs := tc.docIDs()
			want := make(map[uint64]uint32, len(docIDs))
			for i, docID := range docIDs {
				propLen := float32(i%50 + 1)
				want[docID] = uint32(propLen)
				require.NoError(t, bucket.MapSet(key, NewMapPairFromDocIdAndTf(docID, 1, propLen, false)))
			}
			require.NoError(t, bucket.FlushAndSwitch())

			view := bucket.GetConsistentView()
			defer view.ReleaseView()
			require.Len(t, view.Disk, 1)
			seg := view.Disk[0]

			got, err := seg.getPropertyLengths()
			require.NoError(t, err)
			assert.Equal(t, want, got, "getPropertyLengths must reconstruct the exact stored map")

			plView, err := seg.propLengthsView()
			require.NoError(t, err)
			assert.Equal(t, tc.wantDense, plView.dense != nil, "representation choice")
			assert.Equal(t, tc.wantDense, plView.ids == nil, "exactly one representation populated")

			// ascending scan (the production access pattern)
			for _, docID := range docIDs {
				require.Equal(t, want[docID], plView.get(docID), "docID %d", docID)
			}
			// absent ids inside and outside the range
			fresh, err := seg.propLengthsView()
			require.NoError(t, err)
			assert.Equal(t, uint32(0), fresh.get(docIDs[len(docIDs)-1]+1))
			if !tc.wantDense {
				assert.Equal(t, uint32(0), fresh.get(docIDs[0]+1), "strided gap must miss")
			}
		})
	}
}

// TestSegmentPropertyLengthsEmpty covers a segment whose postings carry no
// property lengths section payload.
func TestSegmentPropertyLengthsEmpty(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	// a single tombstoned doc: the posting flushes but contributes no length
	pair := NewMapPairFromDocIdAndTf(7, 1, 1, true)
	require.NoError(t, bucket.MapSet([]byte("term"), pair))
	require.NoError(t, bucket.FlushAndSwitch())

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1)

	got, err := view.Disk[0].getPropertyLengths()
	require.NoError(t, err)
	assert.Empty(t, got)

	plView, err := view.Disk[0].propLengthsView()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), plView.get(7))
}

// TestSegmentPropertyLengthsSpanOverflow pins that a docID range whose span
// (maxID-minID+1) overflows uint64 to 0 falls back to sorted pairs instead of a
// zero-length dense array the fill would panic on. Unreachable with sequential
// docIDs, but a corrupt/legacy segment must not crash the loader.
func TestSegmentPropertyLengthsSpanOverflow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	key := []byte("term")
	want := map[uint64]uint32{0: 3, math.MaxUint64: 7}
	require.NoError(t, bucket.MapSet(key, NewMapPairFromDocIdAndTf(0, 1, 3, false)))
	require.NoError(t, bucket.MapSet(key, NewMapPairFromDocIdAndTf(math.MaxUint64, 1, 7, false)))
	require.NoError(t, bucket.FlushAndSwitch())

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1)
	seg := view.Disk[0]

	// must not panic; a dense array for this span would need 2^64 entries
	plView, err := seg.propLengthsView()
	require.NoError(t, err)
	assert.Nil(t, plView.dense, "overflowing span must not select dense")
	assert.NotNil(t, plView.ids, "must fall back to sorted pairs")
	assert.Equal(t, uint32(3), plView.get(0))
	assert.Equal(t, uint32(7), plView.get(math.MaxUint64))

	got, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestSegmentPropertyLengthsZeroLengthForcesPairs pins that a stored length of 0
// keeps the segment on the pairs layout. Dense treats a 0 slot as "docID absent"
// and getPropertyLengths drops it, so a dense layout would lose the key on the
// compaction round-trip; pairs preserves it.
func TestSegmentPropertyLengthsZeroLengthForcesPairs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	// contiguous docIDs (span==count) would select dense were it not for the 0
	key := []byte("term")
	want := map[uint64]uint32{0: 0, 1: 5, 2: 5, 3: 5}
	for id := uint64(0); id < 4; id++ {
		require.NoError(t, bucket.MapSet(key, NewMapPairFromDocIdAndTf(id, 1, float32(want[id]), false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1)
	seg := view.Disk[0]

	plView, err := seg.propLengthsView()
	require.NoError(t, err)
	assert.Nil(t, plView.dense, "a stored 0-length must force the pairs layout")
	assert.NotNil(t, plView.ids)

	got, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Equal(t, want, got, "the 0-length key must survive reconstruction")
}

// TestInvertedCompactionPropertyLengths drives a real two-segment compaction and
// verifies the merged segment's property lengths are correct end to end — the
// array-merge serialization path (no map reconstruction) round-trips every
// docID, and a docID present in both segments resolves to the newer segment's
// value (c2 wins), matching the compactor's documented precedence.
func TestInvertedCompactionPropertyLengths(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	// segment 1 (older, c1): docs 1,2,5 under term "alpha"
	for docID, pl := range map[uint64]float32{1: 10, 2: 20, 5: 50} {
		require.NoError(t, bucket.MapSet([]byte("alpha"), NewMapPairFromDocIdAndTf(docID, 1, pl, false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// segment 2 (newer, c2): docs 3,7 under "beta" plus docID 5 again with a
	// different length — the duplicate the merge must resolve in c2's favor
	for docID, pl := range map[uint64]float32{3: 30, 5: 999, 7: 70} {
		require.NoError(t, bucket.MapSet([]byte("beta"), NewMapPairFromDocIdAndTf(docID, 1, pl, false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// compact until no longer eligible
	for {
		compacted, err := bucket.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1, "expected a single segment after compaction")
	seg := view.Disk[0]

	want := map[uint64]uint32{1: 10, 2: 20, 3: 30, 5: 999, 7: 70}

	got, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Equal(t, want, got, "merged property lengths (duplicate docID 5 = newer segment's 999)")

	plView, err := seg.propLengthsView()
	require.NoError(t, err)
	for id, l := range want {
		require.Equal(t, l, plView.get(id), "view docID %d", id)
	}
}

// TestInvertedCompactionPropertyLengthsSparsePairs is the sparse-docID sibling of
// TestInvertedCompactionPropertyLengths. The docID ranges are too sparse for the
// dense layout (span/3 > count), so both source segments and the merged segment
// store property lengths as sorted pairs — exercising the pairs branch of
// getPropertyLengthsPairs and the pairs cursor on read-back, the representation
// the dense case never reaches, while still pinning c2-wins precedence on the
// docID (10000) present in both segments.
func TestInvertedCompactionPropertyLengthsSparsePairs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	// segment 1 (older, c1): span 9001 over 3 docs, far past the 1/3-occupancy
	// dense cutoff, so this segment stores pairs
	for docID, pl := range map[uint64]float32{1000: 10, 1001: 11, 10000: 100} {
		require.NoError(t, bucket.MapSet([]byte("alpha"), NewMapPairFromDocIdAndTf(docID, 1, pl, false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// segment 2 (newer, c2): docID 10000 again with a different length — the
	// cross-segment duplicate the merge must resolve in c2's favor
	for docID, pl := range map[uint64]float32{5000: 50, 10000: 999, 20000: 200} {
		require.NoError(t, bucket.MapSet([]byte("beta"), NewMapPairFromDocIdAndTf(docID, 1, pl, false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	for {
		compacted, err := bucket.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	require.Len(t, view.Disk, 1, "expected a single segment after compaction")
	seg := view.Disk[0]

	want := map[uint64]uint32{1000: 10, 1001: 11, 5000: 50, 10000: 999, 20000: 200}

	got, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Equal(t, want, got, "merged property lengths (duplicate docID 10000 = newer segment's 999)")

	plView, err := seg.propLengthsView()
	require.NoError(t, err)
	assert.Nil(t, plView.dense, "sparse docIDs must keep the merged segment on the pairs layout")
	assert.NotNil(t, plView.ids, "pairs layout expected")
	for id, l := range want {
		require.Equal(t, l, plView.get(id), "view docID %d", id)
	}
}
