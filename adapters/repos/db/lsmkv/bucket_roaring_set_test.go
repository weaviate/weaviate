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

// TestBucket_RoaringSetGetFromConsistentView_ReleasesInMemoryRootOnError pins
// the pooled root buffer's release on memtable read errors in the in-memory
// branch, mirroring the disk-layer twin above.
func TestBucket_RoaringSetGetFromConsistentView_ReleasesInMemoryRootOnError(t *testing.T) {
	t.Parallel()

	readErr := errors.New("simulated memtable read error")
	logger, _ := test.NewNullLogger()

	newInMemoBucket := func(t *testing.T, pool *countingBitmapBufPool, active, flushing memtable) Bucket {
		t.Helper()
		segInMemo := roaringset.NewSegmentInMemory(logger)
		seed := newTestMemtableRoaringSet(map[string][]uint64{"key1": {1, 2, 3}})
		segInMemo.MergeMemtableEventually(seed.extractRoaringSet())
		// wait for the background merge so the read takes the root-only path
		require.Eventually(t, func() bool { return segInMemo.Size() > 0 }, time.Second, 10*time.Millisecond)

		return Bucket{
			strategy:      StrategyRoaringSet,
			disk:          &SegmentGroup{roaringSetSegmentInMemory: segInMemo},
			active:        active,
			flushing:      flushing,
			bitmapBufPool: pool,
		}
	}

	t.Run("active read error frees the root buffer via defer, returns caller-safe noop", func(t *testing.T) {
		pool := &countingBitmapBufPool{}
		active := newTestMemtableRoaringSet(nil)
		active.roaringSetGetErr = readErr
		b := newInMemoBucket(t, pool, active, nil)

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), BucketConsistentView{}, []byte("key1"))
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.Equal(t, 1, pool.clones, "root clone must have been acquired before the failing read")
		require.Equal(t, 1, pool.puts, "root buffer must be released exactly once on error")

		release()
		require.Equal(t, 1, pool.puts, "returned release must be a noop; buffer already freed by defer")
	})

	t.Run("flushing read error frees the root buffer via defer", func(t *testing.T) {
		pool := &countingBitmapBufPool{}
		flushing := newTestMemtableRoaringSet(nil)
		flushing.roaringSetGetErr = readErr
		b := newInMemoBucket(t, pool, newTestMemtableRoaringSet(nil), flushing)

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), BucketConsistentView{}, []byte("key1"))
		require.ErrorIs(t, err, readErr)
		require.Nil(t, bm)
		require.Equal(t, 1, pool.puts, "root buffer must be released exactly once on error")

		release()
		require.Equal(t, 1, pool.puts)
	})

	t.Run("success path defers nothing, caller owns the release", func(t *testing.T) {
		pool := &countingBitmapBufPool{}
		b := newInMemoBucket(t, pool, newTestMemtableRoaringSet(nil), nil)

		bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), BucketConsistentView{}, []byte("key1"))
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3}, bm.ToArray())
		require.Equal(t, 0, pool.puts, "success path must not release before the caller does")

		release()
		require.Equal(t, 1, pool.puts, "caller's release must free the root buffer exactly once")
	})
}

// countingBitmapBufPool counts clone acquisitions and buffer releases, so
// tests can pin acquire/release pairing of pooled bitmap buffers.
type countingBitmapBufPool struct {
	clones int
	puts   int
}

func (p *countingBitmapBufPool) Get(minCap int) ([]byte, func()) {
	return make([]byte, 0, minCap), func() { p.puts++ }
}

func (p *countingBitmapBufPool) CloneToBuf(bm *sroar.Bitmap) (*sroar.Bitmap, func()) {
	buf, put := p.Get(bm.LenInBytes())
	p.clones++
	return bm.CloneToBuf(buf), put
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

// TestBucketRoaringSetInMemoReadSnapshotConsistencyAcrossGenerations pins the
// in-memory read to serve a snapshot at call time, not the caller's view: the
// view was captured before two flush generations (delete id, then re-add id),
// so re-applying its stale memtable layer on the live root would blend both
// generations into a torn state (id absent) that never existed. The only
// correct answer at read time is id present.
func TestBucketRoaringSetInMemoReadSnapshotConsistencyAcrossGenerations(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	key := []byte("key-a")
	id := uint64(1)

	segInMemo := roaringset.NewSegmentInMemory(logger)
	// awaitRootState blocks until the background merge-forward has folded the
	// last queued memtable into the root: the key's presence matches
	// expectFound and nothing is pending anymore. Get doubles as the
	// merge-done signal.
	awaitRootState := func(expectFound bool, msg string) {
		t.Helper()
		require.Eventually(t, func() bool {
			_, release, found, pending := segInMemo.Get(key, roaringset.NewBitmapBufPoolNoop())
			release()
			return found == expectFound && len(pending) == 0
		}, time.Second, 10*time.Millisecond, "%s", msg)
	}

	// pre-seed the in-memory segment as if an older flushed segment holding
	// key -> {id} had been merged in
	seed := newTestMemtableRoaringSet(map[string][]uint64{
		"key-a": {id},
	})
	segInMemo.MergeMemtableEventually(seed.extractRoaringSet())
	awaitRootState(true, "seed memtable merged into root")

	// A0: net deletion of id, installed as the active memtable
	active := newTestMemtableRoaringSet(nil)
	require.NoError(t, active.roaringSetRemoveOne(key, id))

	b := Bucket{
		active: active,
		disk: &SegmentGroup{
			segments:                  []Segment{},
			roaringSetSegmentInMemory: segInMemo,
		},
		strategy:                   StrategyRoaringSet,
		keepMergedSegmentsInMemory: true,
		bitmapBufPool:              roaringset.NewBitmapBufPoolNoop(),
	}

	// capture the stale T1 view (active=A0, no flushing) and hold it across
	// both flush generations
	view := b.GetConsistentView()
	defer view.ReleaseView()

	flushActiveIntoSegmentInMemory := func(expectRootFound bool, msg string) {
		t.Helper()
		switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
			return newTestMemtableRoaringSet(nil), nil
		})
		require.NoError(t, err)
		require.True(t, switched)
		// the fake disk segment is irrelevant to the in-memory read path; what
		// matters is the merge-forward of the flushing memtable into the root
		require.NoError(t, b.atomicallyAddDiskSegmentAndRemoveFlushing(&fakeSegment{}))
		awaitRootState(expectRootFound, msg)
	}

	// generation 1: A0's deletion of id is folded into the merged root, the
	// key drops out of it
	flushActiveIntoSegmentInMemory(false, "A0 deletion merged into root")

	// generation 2: id is re-added, then folded into the root as well
	require.NoError(t, b.active.roaringSetAddOne(key, id))
	flushActiveIntoSegmentInMemory(true, "A1 re-add merged into root")

	// Read through the stale T1 view. Present is the only answer that ever
	// existed at call time; absent would be the torn blend of A0's stale
	// deletion over a root that already covers the A1 re-add.
	bm, release, err := b.roaringSetGetFromConsistentView(context.Background(), view, key)
	require.NoError(t, err)
	defer release()
	require.Equal(t, []uint64{id}, bm.ToArray(),
		"read must serve the state at call time (id re-added by A1), not a torn blend of two flush generations")
}

// TestNewBucketKeepMergedSegmentsInMemoryRequiresBufPool pins the fail-fast:
// the merged in-memory read path clones bitmaps through the buffer pool, so
// enabling it without a pool must fail at construction instead of panicking
// with a nil dereference on the first read.
func TestNewBucketKeepMergedSegmentsInMemoryRequiresBufPool(t *testing.T) {
	for _, strategy := range []string{StrategyRoaringSet, StrategyRoaringSetRange} {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			logger, _ := test.NewNullLogger()

			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy),
				WithKeepMergedSegmentsInMemory(true))
			require.ErrorContains(t, err, "WithBitmapBufPool")
			require.Nil(t, b)

			b, err = NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy),
				WithKeepMergedSegmentsInMemory(true),
				WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
			require.NoError(t, err)
			require.NoError(t, b.Shutdown(ctx))
		})
	}
}
