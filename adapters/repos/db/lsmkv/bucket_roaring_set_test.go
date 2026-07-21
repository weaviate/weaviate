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
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/concurrency/testinghelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestRoaringSetWritePathRefCount ensures that all write paths of the
// RoaringSet type correctly use and release refcounts on the active memtable
// and therefore do not block a flushlock for the entire duration of the wrige.
func TestRoaringSetWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableRoaringSet(nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}

	// add one
	err := b.RoaringSetAddOne([]byte("key1"), 1)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add list
	err = b.RoaringSetAddList([]byte("key1"), []uint64{2, 3, 4})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add bitmap
	err = b.RoaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{5, 6, 7}))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// remove one
	err = b.RoaringSetRemoveOne([]byte("key1"), 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, releaseBufPol, err := b.RoaringSetGet(context.Background(), []byte("key1"))
	defer releaseBufPol()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, v.ToArray())
}

// TestBucket_RoaringSetGetFromConsistentView_ReleasesDiskLayerOnError pins a
// disk-buffer leak on flushing/active read errors after acquisition.
func TestBucket_RoaringSetGetFromConsistentView_ReleasesDiskLayerOnError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("simulated memtable read error")

	newDiskSeg := func() *fakeSegment {
		return newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
			"key1": bitmapFromSlice([]uint64{1, 2, 3}),
		})
	}

	t.Run("active read error frees disk layer via defer, returns caller-safe noop", func(t *testing.T) {
		diskSeg := newDiskSeg()
		active := newTestMemtableRoaringSet(nil)
		active.roaringSetGetErr = readErr

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		}
		view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), view, []byte("key1"))
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.NotNil(t, release)

		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"disk layer's release must fire when the active read errors")

		release()
		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"returned release must be a noop; buffer already freed by defer")
	})

	t.Run("flushing read error frees disk layer via defer", func(t *testing.T) {
		diskSeg := newDiskSeg()
		flushing := newTestMemtableRoaringSet(nil)
		flushing.roaringSetGetErr = readErr
		active := newTestMemtableRoaringSet(nil)

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		}
		view := BucketConsistentView{Active: active, Flushing: flushing, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), view, []byte("key1"))
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"disk layer's release must fire when the flushing read errors")

		release()
		require.Equal(t, 1, diskSeg.roaringSetReleases)
	})

	t.Run("success path defers nothing, caller owns the release", func(t *testing.T) {
		diskSeg := newDiskSeg()
		active := newTestMemtableRoaringSet(nil)

		b := Bucket{
			strategy: StrategyRoaringSet,
			disk:     &SegmentGroup{segments: []Segment{diskSeg}},
		}
		view := BucketConsistentView{Active: active, Disk: []Segment{diskSeg}}

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), view, []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, bm)

		require.Equal(t, 0, diskSeg.roaringSetReleases,
			"success path must not release before the caller does")

		release()
		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"caller's release must free the disk layer exactly once")
	})
}

// TestBucket_RoaringSetGet_RespectsConcurrencyBudget pins RoaringSetGet's
// merge fan-out to the per-query budget without blowing the goroutine ceiling.
func TestBucket_RoaringSetGet_RespectsConcurrencyBudget(t *testing.T) {
	// Kill switch is this bound's red control, but no CI job sets it: CI
	// only ever runs the green (budget-enforced) path.
	if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
		t.Skip("budget cap disabled via kill switch")
	}
	// Merge fan-out only exists at SROAR_MERGE>=2 (GOMAXPROCS>=4); skipping
	// here silently would hide the guard, so CI fails loudly instead.
	if concurrency.SROAR_MERGE < 2 {
		if os.Getenv("CI") != "" {
			t.Fatalf("bounding tests require GOMAXPROCS>=4, refusing to skip silently on CI (SROAR_MERGE=%d)",
				concurrency.SROAR_MERGE)
		}
		t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
			concurrency.SROAR_MERGE)
	}

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	// never auto-flush; we flush explicitly to control the disk segment count
	b.SetMemtableThreshold(1e9)

	// numContainers keeps worker count (min(numContainers/24, SROAR_MERGE))
	// above 1; dense containers keep an ignored-budget worker alive across
	// sampler ticks.
	const (
		numContainers      = 128
		valuesPerContainer = 128
	)
	values := make([]uint64, 0, numContainers*valuesPerContainer)
	for c := 0; c < numContainers; c++ {
		for j := 0; j < valuesPerContainer; j++ {
			values = append(values, uint64(c)<<16+uint64(j))
		}
	}

	key := []byte("key")

	// many disk segments keep each Get's merges live across the sampler window
	const numSegments = 12
	for s := 0; s < numSegments; s++ {
		require.NoError(t, b.RoaringSetAddList(key, values))
		require.NoError(t, b.FlushAndSwitch())
	}

	budget1 := concurrency.CtxWithBudget(ctx, 1)

	// correctness: budget=1 result must match the unconstrained query
	got1, release1, err := b.RoaringSetGet(budget1, key)
	require.NoError(t, err)
	arr1 := got1.ToArray()
	release1()

	gotDefault, releaseDefault, err := b.RoaringSetGet(ctx, key)
	require.NoError(t, err)
	arrDefault := gotDefault.ToArray()
	releaseDefault()

	require.Equal(t, values, arr1)
	require.Equal(t, arrDefault, arr1)

	// budget=1 spawns no extra workers; slack absorbs sampler/GC noise
	testinghelpers.AssertGoroutineCeiling(t, 24, 1, 8, 200*time.Millisecond, func() error {
		bm, release, err := b.RoaringSetGet(budget1, key)
		if err != nil {
			return err
		}
		_ = bm
		release()
		return nil
	})
}

// TestBucket_RoaringSetGetFromView_MatchesRoaringSetGet proves the new
// batched-read primitive returns byte-identical results to the per-key
// RoaringSetGet entry point, whether the key is present across several
// on-disk segments plus the active memtable, or absent entirely, and that a
// single GetConsistentView() call serves every RoaringSetGetFromView read.
func TestBucket_RoaringSetGetFromView_MatchesRoaringSetGet(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	// never auto-flush; flush explicitly so the key spans several disk
	// segments plus a final write left in the active memtable
	b.SetMemtableThreshold(1e9)

	keyA := []byte("key-a")
	keyB := []byte("key-b")
	keyMissing := []byte("key-missing")

	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{1, 2, 3}))
	require.NoError(t, b.RoaringSetAddList(keyB, []uint64{10, 20}))
	require.NoError(t, b.FlushAndSwitch())

	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{4, 5}))
	require.NoError(t, b.FlushAndSwitch())

	// left in the active memtable, unflushed
	require.NoError(t, b.RoaringSetAddList(keyA, []uint64{6}))

	wantA, releaseWantA, err := b.RoaringSetGet(ctx, keyA)
	require.NoError(t, err)
	arrWantA := append([]uint64(nil), wantA.ToArray()...)
	releaseWantA()

	wantB, releaseWantB, err := b.RoaringSetGet(ctx, keyB)
	require.NoError(t, err)
	arrWantB := append([]uint64(nil), wantB.ToArray()...)
	releaseWantB()

	wantMissing, releaseWantMissing, err := b.RoaringSetGet(ctx, keyMissing)
	require.NoError(t, err)
	arrWantMissing := append([]uint64(nil), wantMissing.ToArray()...)
	releaseWantMissing()
	require.Empty(t, arrWantMissing, "absent key must resolve to an empty, non-nil bitmap")

	// one shared view serves all three RoaringSetGetFromView reads below
	view := b.GetConsistentView()

	gotA, releaseA, err := b.RoaringSetGetFromView(ctx, view, keyA)
	require.NoError(t, err)
	require.Equal(t, arrWantA, gotA.ToArray())
	releaseA()

	gotB, releaseB, err := b.RoaringSetGetFromView(ctx, view, keyB)
	require.NoError(t, err)
	require.Equal(t, arrWantB, gotB.ToArray())
	releaseB()

	gotMissing, releaseMissing, err := b.RoaringSetGetFromView(ctx, view, keyMissing)
	require.NoError(t, err)
	require.NotNil(t, gotMissing)
	require.Empty(t, gotMissing.ToArray())
	releaseMissing()

	view.ReleaseView()
}

// TestBucket_RoaringSet_DeleteThenReaddAcrossSegments is a read-path regression
// test for roaringset reads with tombstones spread across multiple disk segments
// plus the active memtable, including a doc deleted in one segment and re-added
// in a later layer. The first (oldest) segment carries no tombstones, so it
// takes the empty-deletions path in segment.roaringSetGet; the test pins that
// the full add / delete / re-add fold still resolves correctly. (Note: Flatten
// ignores the base layer's Deletions, so this exercises the surrounding fold
// rather than the base layer's deletions bitmap directly.)
func TestBucket_RoaringSet_DeleteThenReaddAcrossSegments(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // flush explicitly so writes span several segments

	tombstoned := []byte("tombstoned")
	clean := []byte("clean")

	// segment 1 (oldest): additions only -> empty deletions -> shared empty path
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{1, 2, 3, 4, 5}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{100, 101}))
	require.NoError(t, b.FlushAndSwitch())

	// segment 2: real deletions of 2 and 4, plus addition 6
	require.NoError(t, b.RoaringSetRemoveOne(tombstoned, 2))
	require.NoError(t, b.RoaringSetRemoveOne(tombstoned, 4))
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{6}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{102}))
	require.NoError(t, b.FlushAndSwitch())

	// active memtable (unflushed): re-add 2 (deleted in segment 2) and add 7
	require.NoError(t, b.RoaringSetAddList(tombstoned, []uint64{2, 7}))
	require.NoError(t, b.RoaringSetAddList(clean, []uint64{103}))

	// {1,2,3,4,5} -(del 2,4)-> {1,3,5} +6 -> {1,3,5,6}; re-add {2,7} -> {1,2,3,5,6,7}
	// (doc 2, deleted in segment 2, must survive because it is re-added later)
	requireRoaringSetElements(t, ctx, b, tombstoned, []uint64{1, 2, 3, 5, 6, 7})
	// pure additions across every layer -> every layer takes the empty path
	requireRoaringSetElements(t, ctx, b, clean, []uint64{100, 101, 102, 103})
}

// TestBucket_RoaringSet_DeletionsOnlyOldestSegment pins the empty-additions
// base across multiple disk segments: the oldest segment holding a key
// becomes the mutable accumulator base of the disk fold, so the segment read
// must substitute a non-nil additions bitmap when the node stores only
// deletions — the newer segment's merge runs AndNot/Or directly on it.
// Covers both segment read modes, whose branches both return nil additions
// for the empty region.
func TestBucket_RoaringSet_DeletionsOnlyOldestSegment(t *testing.T) {
	tests := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()

			opts := append([]BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			}, tt.opts...)
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

			b.SetMemtableThreshold(1e9) // flush explicitly so writes span several segments

			key := []byte("key")

			// segment 1 (oldest): deletions only -> empty additions region
			require.NoError(t, b.RoaringSetRemoveOne(key, 7))
			require.NoError(t, b.FlushAndSwitch())

			// segment 2: additions merged into the substituted base (the Or arm)
			require.NoError(t, b.RoaringSetAddList(key, []uint64{1, 2, 3}))
			require.NoError(t, b.FlushAndSwitch())

			// segment 3: deletes 2, added by segment 2, so the AndNot arm's
			// effect on the accumulated base is visible in the result
			require.NoError(t, b.RoaringSetRemoveOne(key, 2))
			require.NoError(t, b.FlushAndSwitch())

			requireRoaringSetElements(t, ctx, b, key, []uint64{1, 3})
		})
	}
}

// TestBucket_RoaringSet_ReleasesAllPooledBuffers pins combineReleases: the
// buffers a node's read pools must all be freed by the single returned
// release, in either read mode. The region shapes of the oldest segment
// steer which combineReleases arm the read takes (and the arms differ per
// mode — a pread read always pools the whole node buffer), so together the
// scenarios cover every arm in both modes.
func TestBucket_RoaringSet_ReleasesAllPooledBuffers(t *testing.T) {
	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true)}},
	}

	// The oldest segment's shape is what matters, because only the oldest
	// segment holding the key is read through segment.roaringSetGet (and so
	// through combineReleases) — newer segments are folded via
	// roaringSetMergeWith, which pools nothing through it.
	scenarios := []struct {
		name     string
		seg1     func(t *testing.T, b *Bucket, key []byte)
		expected []uint64
	}{
		{
			name: "oldest segment has additions and deletions",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{2}))
				require.NoError(t, b.RoaringSetRemoveOne(key, 5))
			},
			expected: []uint64{1, 2, 3},
		},
		{
			name: "oldest segment has additions only",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetAddList(key, []uint64{2}))
			},
			expected: []uint64{1, 2, 3},
		},
		{
			name: "oldest segment has deletions only",
			seg1: func(t *testing.T, b *Bucket, key []byte) {
				require.NoError(t, b.RoaringSetRemoveOne(key, 5))
			},
			expected: []uint64{1, 3},
		},
	}

	for _, mode := range modes {
		for _, scenario := range scenarios {
			t.Run(mode.name+"/"+scenario.name, func(t *testing.T) {
				ctx := context.Background()
				logger, _ := test.NewNullLogger()
				pool := roaringset.NewBitmapBufPoolTrackingForTests()

				opts := append([]BucketOption{
					WithStrategy(StrategyRoaringSet),
					WithBitmapBufPool(pool),
				}, mode.opts...)
				b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

				b.SetMemtableThreshold(1e9)

				key := []byte("key")
				scenario.seg1(t, b, key)
				require.NoError(t, b.FlushAndSwitch())

				require.NoError(t, b.RoaringSetAddList(key, []uint64{1, 3}))
				require.NoError(t, b.FlushAndSwitch())

				bm, release, err := b.RoaringSetGet(ctx, key)
				require.NoError(t, err)
				require.ElementsMatch(t, scenario.expected, bm.ToArray())
				release()

				require.Zero(t, pool.Outstanding(), "single release must free every pooled buffer")
			})
		}
	}
}

func requireRoaringSetElements(t *testing.T, ctx context.Context, b *Bucket, key []byte, want []uint64) {
	t.Helper()
	bm, release, err := b.RoaringSetGet(ctx, key)
	require.NoError(t, err)
	defer release()
	require.ElementsMatch(t, want, bm.ToArray())
}
