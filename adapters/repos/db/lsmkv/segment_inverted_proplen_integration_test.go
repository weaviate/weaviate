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
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
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
		name string
		// docIDs returns the docIDs to store; wantNarrow is only meaningful when
		// !wantDense (pairs), where max docID <= MaxUint32 selects uint32 ids.
		docIDs     func() []uint64
		wantDense  bool
		wantNarrow bool
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
			// stride 10: span ~10x count, far below 1/3 occupancy -> sorted pairs;
			// max docID 4990 fits uint32 -> narrow ids
			name: "sparse_strided",
			docIDs: func() []uint64 {
				ids := make([]uint64, 500)
				for i := range ids {
					ids[i] = uint64(i) * 10
				}
				return ids
			},
			wantDense:  false,
			wantNarrow: true,
		},
		{
			// sparse with a max docID beyond uint32 -> pairs must keep uint64 ids
			name: "sparse_wide_id",
			docIDs: func() []uint64 {
				return []uint64{0, 10, 20, 30, 1 << 33}
			},
			wantDense:  false,
			wantNarrow: false,
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
			switch {
			case tc.wantDense:
				assert.Nil(t, plView.ids, "dense: no uint64 pairs")
				assert.Nil(t, plView.ids32, "dense: no uint32 pairs")
			case tc.wantNarrow:
				assert.NotNil(t, plView.ids32, "max docID fits uint32: narrow ids")
				assert.Nil(t, plView.ids, "narrow ids: no uint64 pairs")
				require.Len(t, plView.ids32, len(docIDs))
				// 4B ids32 + 4B lens = 8B/entry (vs 12B with uint64 ids)
				assert.Equal(t, 8, (4*len(plView.ids32)+4*len(plView.lens))/len(plView.ids32),
					"uint32-ids pairs entry is 8B")
			default:
				assert.NotNil(t, plView.ids, "max docID exceeds uint32: uint64 fallback")
				assert.Nil(t, plView.ids32, "wide ids: no uint32 pairs")
			}

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
	assert.NotNil(t, plView.ids, "must fall back to sorted pairs (uint64 ids: max docID > uint32)")
	assert.Nil(t, plView.ids32, "MaxUint64 docID can't use uint32 ids")
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
	assert.NotNil(t, plView.ids32, "max docID 3 fits uint32: narrow ids")

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
	assert.NotNil(t, plView.ids32, "max docID 20000 fits uint32: narrow ids after the merge round-trip")
	assert.Nil(t, plView.ids, "narrow ids: no uint64 pairs")
	for id, l := range want {
		require.Equal(t, l, plView.get(id), "view docID %d", id)
	}
}

// TestInvertedCompactionReclaimsDeletedPropertyLengths verifies that a delete
// drops its per-doc property length at compaction, not just its posting. The
// control (live postings and consumed tombstones) rules out a false pass from a
// compaction that never ran.
func TestInvertedCompactionReclaimsDeletedPropertyLengths(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	const (
		total   = 100
		deleted = 60 // docIDs [0,deleted) are tombstoned; [deleted,total) survive
	)
	term := []byte("shared")

	// segment 1 (older, c1): every doc under one shared term, each with a propLen
	for id := 0; id < total; id++ {
		require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(uint64(id), 1, float32(id+1), false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// segment 2 (newer, c2): tombstone the first `deleted` docs
	for id := 0; id < deleted; id++ {
		pair := NewMapPairFromDocIdAndTf(uint64(id), 1, 1, true)
		require.NoError(t, bucket.MapDeleteKey(term, pair.Key))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// force the root compaction that physically drops the tombstoned docs
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

	// control: postings and tombstones prove a real cleanup compaction ran, so a
	// retained property length can only be the leak, not "hasn't compacted yet"
	live, err := bucket.MapList(ctx, term)
	require.NoError(t, err)
	liveCount := 0
	for _, mp := range live {
		if !mp.Tombstone {
			liveCount++
		}
	}
	require.Equal(t, total-deleted, liveCount, "postings physically reclaimed the deleted docs")

	tomb, err := seg.ReadOnlyTombstones()
	require.NoError(t, err)
	require.True(t, tomb == nil || tomb.IsEmpty(), "root compaction consumed the tombstones")

	// property lengths track the live set, not every doc ever ingested
	pls, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Len(t, pls, total-deleted, "deleted docs' property lengths must be reclaimed")
	for id := 0; id < deleted; id++ {
		_, ok := pls[uint64(id)]
		assert.False(t, ok, "tombstoned docID %d still carries a property length", id)
	}
	for id := deleted; id < total; id++ {
		assert.Equal(t, uint32(id+1), pls[uint64(id)], "surviving docID %d property length", id)
	}
}

// TestInvertedCompactionReclaimsAveragePropertyLength is the scoring-side sibling
// of TestInvertedCompactionReclaimsDeletedPropertyLengths (issue #311): once a
// compaction drops the deleted docs' property lengths, the BM25 avgdl denominator
// (SegmentGroup.GetAveragePropertyLength) must track the live set, not every doc
// ever flushed. With property lengths id+1, deleting docIDs [0,60) removes the 60
// shortest docs, so the live average jumps from the all-docs 50.5 to 80.5. The
// pre-fix accounting stayed at 50.5, under-normalizing every surviving doc.
func TestInvertedCompactionReclaimsAveragePropertyLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	const (
		total   = 100
		deleted = 60 // docIDs [0,deleted) are tombstoned; [deleted,total) survive
	)
	term := []byte("shared")

	// segment 1 (older, c1): docID id has property length id+1
	for id := 0; id < total; id++ {
		require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(uint64(id), 1, float32(id+1), false)))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// before any delete the average is over all 100 docs: (1+..+100)/100
	avg, count := bucket.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(total), count)
	require.InDelta(t, 50.5, avg, 1e-9)

	// segment 2 (newer, c2): tombstone the first `deleted` (shortest) docs
	for id := 0; id < deleted; id++ {
		pair := NewMapPairFromDocIdAndTf(uint64(id), 1, 1, true)
		require.NoError(t, bucket.MapDeleteKey(term, pair.Key))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	// deletes alone do not move the denominator: the tombstones live in the newer
	// segment while the lengths still sit in the older one
	avg, count = bucket.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(total), count)
	require.InDelta(t, 50.5, avg, 1e-9)

	// force the root compaction that physically drops the tombstoned docs
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

	// the surviving docs are [deleted,total) with lengths [deleted+1,total]:
	// avg = (61+..+100)/40 = 3220/40 = 80.5, count = 40
	wantSum := 0
	for id := deleted; id < total; id++ {
		wantSum += id + 1
	}
	wantCount := uint64(total - deleted)
	wantAvg := float64(wantSum) / float64(wantCount)

	avg, count = bucket.disk.GetAveragePropertyLength()
	require.Equal(t, wantCount, count, "avgdl denominator must track the live set")
	require.InDelta(t, wantAvg, avg, 1e-9, "avgdl must reflect only surviving docs")

	// the merged segment's stored scalar agrees with the group total (a single
	// segment now holds the whole live set)
	bAvg, bCount := bucket.GetAveragePropertyLength()
	require.Equal(t, wantCount, bCount)
	require.InDelta(t, wantAvg, bAvg, 1e-9)
}

// TestInvertedCompactionReclaimsFullyDeletedSegmentAvgPropertyLength covers the
// case where an entire older segment's property lengths belong to deleted docs
// (issue #311): compacting it against the newer segment that tombstoned them must
// remove its whole contribution from the avgdl accounting, leaving only the live
// segment's docs — and once every doc is gone the denominator collapses to zero,
// not a stale non-zero average. The two length scales (100 for the doomed docs, 2
// for the survivors) make a leaked contribution obvious in the average.
func TestInvertedCompactionReclaimsFullyDeletedSegmentAvgPropertyLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	term := []byte("shared")

	// older segment: docs 1,2 — every one of its property lengths will be deleted
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(1, 1, 100, false)))
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(2, 1, 100, false)))
	require.NoError(t, bucket.FlushAndSwitch())

	// newer segment: live docs 3,4 plus tombstones for the older segment's docs
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(3, 1, 2, false)))
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(4, 1, 2, false)))
	require.NoError(t, bucket.MapDeleteKey(term, NewMapPairFromDocIdAndTf(1, 1, 1, true).Key))
	require.NoError(t, bucket.MapDeleteKey(term, NewMapPairFromDocIdAndTf(2, 1, 1, true).Key))
	require.NoError(t, bucket.FlushAndSwitch())

	// before compaction the doomed docs still inflate the denominator: (100+100+2+2)/4
	avg, count := bucket.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(4), count)
	require.InDelta(t, 51.0, avg, 1e-9)

	for {
		compacted, err := bucket.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	// the fully-deleted older segment's whole contribution is gone; only docs 3,4
	// remain: avg = (2+2)/2 = 2, count = 2
	avg, count = bucket.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(2), count, "the dead segment must leave the avgdl accounting entirely")
	require.InDelta(t, 2.0, avg, 1e-9)

	// now delete the survivors too and compact them away: the denominator collapses
	// to the empty live set rather than holding a stale average
	require.NoError(t, bucket.MapDeleteKey(term, NewMapPairFromDocIdAndTf(3, 1, 1, true).Key))
	require.NoError(t, bucket.MapDeleteKey(term, NewMapPairFromDocIdAndTf(4, 1, 1, true).Key))
	require.NoError(t, bucket.FlushAndSwitch())
	// merge levels so the tombstones meet the surviving property lengths
	for i := 0; i < 3; i++ {
		require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(uint64(100+i), 1, 2, false)))
		require.NoError(t, bucket.MapDeleteKey(term, NewMapPairFromDocIdAndTf(uint64(100+i), 1, 1, true).Key))
		require.NoError(t, bucket.FlushAndSwitch())
	}
	for {
		compacted, err := bucket.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	live, err := bucket.MapList(ctx, term)
	require.NoError(t, err)
	liveCount := 0
	for _, mp := range live {
		if !mp.Tombstone {
			liveCount++
		}
	}
	require.Equal(t, 0, liveCount, "all docs deleted")

	avg, count = bucket.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(0), count, "an empty live set must zero the avgdl denominator")
	require.Equal(t, 0.0, avg)
}

// TestInvertedCompactionReinsertKeepsPropertyLength guards the delete-then-
// reinsert case raised in review: a docID deleted and re-added with a new
// length in a newer segment must keep its NEW property length through
// compaction, not be reclaimed as a delete. The reclaim only drops the older
// segment's entries, so the newer (live) length always survives.
func TestInvertedCompactionReinsertKeepsPropertyLength(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9) // never auto-flush mid-segment

	term := []byte("shared")
	const docID = uint64(7)

	// older segment: docID with an initial property length
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(docID, 1, 10, false)))
	require.NoError(t, bucket.FlushAndSwitch())

	// newer segment: delete then re-add the same docID with a new property length
	del := NewMapPairFromDocIdAndTf(docID, 1, 1, true)
	require.NoError(t, bucket.MapDeleteKey(term, del.Key))
	require.NoError(t, bucket.MapSet(term, NewMapPairFromDocIdAndTf(docID, 2, 20, false)))
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
	require.Len(t, view.Disk, 1)
	seg := view.Disk[0]

	// the reinserted doc stays live...
	live, err := bucket.MapList(ctx, term)
	require.NoError(t, err)
	liveIDs := map[uint64]bool{}
	for _, mp := range live {
		if !mp.Tombstone {
			liveIDs[binary.BigEndian.Uint64(mp.Key)] = true
		}
	}
	require.True(t, liveIDs[docID], "reinserted docID must survive compaction")

	// ...with its NEW property length, not dropped or zeroed
	pls, err := seg.getPropertyLengths()
	require.NoError(t, err)
	assert.Equal(t, uint32(20), pls[docID], "reinserted doc keeps its new property length")
}

// TestInvertedSegmentAddCountsAveragePropertyLength guards the WAL-recovery path:
// SegmentGroup.add (used when a recovered memtable is flushed into a segment after
// the group is already initialized) must fold that segment into the avgdl
// accounting, exactly as a normal flush does. Without it the segment is live but
// uncounted, so a later compaction that retires it subtracts a contribution that
// was never added and underflows the running total to a wrapped uint64 (issue
// #311 reconcile). add() is exercised directly by copying a real flushed segment
// into a fresh, already-initialized bucket.
func TestInvertedSegmentAddCountsAveragePropertyLength(t *testing.T) {
	ctx := context.Background()

	// build a real inverted segment in a source bucket
	srcDir := t.TempDir()
	src, err := NewBucketCreator().NewBucket(ctx, srcDir, srcDir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	src.SetMemtableThreshold(1e9)

	term := []byte("shared")
	const total = 200
	for id := 0; id < total; id++ {
		require.NoError(t, src.MapSet(term, NewMapPairFromDocIdAndTf(uint64(id), 1, float32(id+1), false)))
	}
	require.NoError(t, src.FlushAndSwitch())
	wantAvg, wantCount := src.disk.GetAveragePropertyLength()
	require.Equal(t, uint64(total), wantCount)
	require.NoError(t, src.Shutdown(ctx))

	// a fresh, already-initialized bucket has nothing counted yet
	tgtDir := t.TempDir()
	tgt, err := NewBucketCreator().NewBucket(ctx, tgtDir, tgtDir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer tgt.Shutdown(ctx)
	tgt.SetMemtableThreshold(1e9)
	if _, count := tgt.disk.GetAveragePropertyLength(); count != 0 {
		t.Fatalf("fresh bucket should count nothing, got %d", count)
	}

	// copy the source's flushed .db segment in and add() it post-init
	dbs, err := filepath.Glob(filepath.Join(srcDir, "*.db"))
	require.NoError(t, err)
	require.Len(t, dbs, 1, "source flushed exactly one segment")
	segPath := filepath.Join(tgtDir, filepath.Base(dbs[0]))
	data, err := os.ReadFile(dbs[0])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(segPath, data, 0o644))

	require.NoError(t, tgt.disk.add(segPath))
	avg, count := tgt.disk.GetAveragePropertyLength()
	require.Equal(t, wantCount, count, "add must fold the segment into the avgdl accounting")
	require.InDelta(t, wantAvg, avg, 1e-9)

	// delete half the docs and compact: the reconcile subtracts the retired
	// segment's contribution, which must be present (else the total underflows)
	const deleted = 100
	for id := 0; id < deleted; id++ {
		pair := NewMapPairFromDocIdAndTf(uint64(id), 1, 1, true)
		require.NoError(t, tgt.MapDeleteKey(term, pair.Key))
	}
	require.NoError(t, tgt.FlushAndSwitch())
	for {
		compacted, err := tgt.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}

	// survivors are docs [deleted,total); the denominator must be their count, not
	// an underflowed uint64
	wantSum := 0
	for id := deleted; id < total; id++ {
		wantSum += id + 1
	}
	liveCount := uint64(total - deleted)
	avg, count = tgt.disk.GetAveragePropertyLength()
	require.Equal(t, liveCount, count, "denominator must be the live set, not an underflowed uint64")
	require.InDelta(t, float64(wantSum)/float64(liveCount), avg, 1e-9)
}
