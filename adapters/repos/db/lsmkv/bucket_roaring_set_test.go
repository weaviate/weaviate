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

// TestBucket_RoaringSetGetFromConsistentView_ReleasesDiskLayerOnError pins the
// fix for a pooled-buffer leak in the bucket-level assembly: the disk layers are
// acquired first (returning their real release), then the flushing and active
// memtables are read. If one of those reads fails, the error path returns
// noopRelease to the caller, so the deferred cleanup — not the caller — must
// free the disk buffer. A regression that overwrites the release the defer reads
// would leak the disk buffer on every failed flushing/active read.
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

		// the disk layer's pooled buffer must have been released exactly once by
		// the deferred cleanup, despite the error path returning noopRelease.
		require.Equal(t, 1, diskSeg.roaringSetReleases,
			"disk layer's release must fire when the active read errors")

		// the returned release must be the caller-safe noop; calling it must not
		// double-release the disk layer.
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

		// no error => the defer must not fire; the caller still holds the buffer.
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
	// CI implication: the kill-switch CI leg (DISABLE_SROAR_MERGE_BUDGET=true)
	// skips this bound by design and serves as the red control instead.
	if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
		t.Skip("budget cap disabled via kill switch")
	}
	// CI implication: 2-vCPU runners have SROAR_MERGE=1 and skip this entirely;
	// the bound is only exercised on >=4-vCPU runners (SROAR_MERGE>=2).
	if concurrency.SROAR_MERGE < 2 {
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

	// one value per sroar container (stride 2^16); enough that merge ops
	// actually want >1 worker (min(200/24, SROAR_MERGE))
	const numContainers = 200
	values := make([]uint64, numContainers)
	for i := range values {
		values[i] = uint64(i) << 16
	}

	key := []byte("key")

	// >= 8 disk segments so each Get runs several segment merges
	const numSegments = 8
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
	testinghelpers.AssertGoroutineCeiling(t, 16, 1, 8, 200*time.Millisecond, func() error {
		bm, release, err := b.RoaringSetGet(budget1, key)
		if err != nil {
			return err
		}
		_ = bm
		release()
		return nil
	})
}
