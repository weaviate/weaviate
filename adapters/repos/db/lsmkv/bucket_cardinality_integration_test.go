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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucketPropertyCardinality exercises Bucket.GetKeysCount on a roaringset
// bucket, the strategy a filterable inverted-index property uses. Its keys are
// the distinct property values, so the estimates are asserted against the known
// distinct count of the inserted data.
func TestBucketPropertyCardinality(t *testing.T) {
	ctx := testCtx()

	t.Run("single segment: estimate close to exact", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		const n = 5000
		addDistinctKeys(t, b, 0, n)
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 1, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, n, float64(est), 5)
	})

	t.Run("equal-sized overlapping segments: merge estimates the union", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		// three overlapping 1000-key segments: 2000 distinct, not the 3000 sum.
		// Equal key counts give equal geometry, so the filters really do merge.
		addDistinctKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		addDistinctKeys(t, b, 500, 1500)
		require.NoError(t, b.FlushAndSwitch())
		addDistinctKeys(t, b, 1000, 2000)
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 3, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 2000, float64(est), 10)
	})

	t.Run("different-sized segments: keeps higher-cardinality estimate, compaction unions", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		addDistinctKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		addDistinctKeys(t, b, 1000, 1500) // different size => different m/k
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 2, b.disk.Len())

		// mismatched geometry can't merge, so this falls back to the larger
		// segment's ~1000 — a lower bound on the true 1500
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 1000, float64(est), 5)

		// one segment, one geometry, full union
		compactBucketFully(t, ctx, b)
		require.Equal(t, 1, b.disk.Len())

		est, err = b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 1500, float64(est), 5)
	})

	t.Run("memtable-only: estimates from the unflushed memtable", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		addDistinctKeys(t, b, 0, 100) // no flush: data stays in the memtable
		require.Equal(t, 0, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 100, float64(est), 5)
	})

	t.Run("memtable unioned with the segment below it", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		addDistinctKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		addDistinctKeys(t, b, 1000, 2000)

		// disjoint layers, so comparing them would report only the larger ~1000.
		// The memtable fits the disk filter's geometry, so the two merge.
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 2000, float64(est), 5)
	})

	t.Run("memtable much larger than the segment below it", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		addDistinctKeys(t, b, 0, 100)
		require.NoError(t, b.FlushAndSwitch())
		addDistinctKeys(t, b, 100, 5000)

		// the disk filter holds ~1400 bits for its 100 keys; feeding it the
		// memtable's 4900 sets every one and overflows ApproximatedSize to
		// MaxUint32, so the two layers are estimated apart and compared
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 4900, float64(est), 5)
	})

	// Lazy loading is the production default but only applies to segments read
	// off disk at startup — a flush always yields an eager segment — so covering
	// it requires reopening the directory.
	t.Run("lazily loaded segments: estimate survives a reopen", func(t *testing.T) {
		dir := t.TempDir()

		const n = 5000
		func() {
			b := newCardinalityBucket(ctx, t, dir)
			defer b.Shutdown(ctx)
			addDistinctKeys(t, b, 0, n)
			require.NoError(t, b.FlushAndSwitch())
		}()

		b := newCardinalityBucket(ctx, t, dir, WithLazySegmentLoading(true))
		defer b.Shutdown(ctx)
		require.Equal(t, 1, b.disk.Len())
		require.IsType(t, &lazySegment{}, b.disk.segments[0],
			"reopened segment must be lazy, otherwise this test does not cover the lazy path")

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, n, float64(est), 5)
	})
}

func newCardinalityBucket(ctx context.Context, t *testing.T, dir string, opts ...BucketOption) *Bucket {
	t.Helper()
	opts = append([]BucketOption{
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithUseBloomFilter(true),
	}, opts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...,
	)
	require.NoError(t, err)
	// Large enough that flushes only happen when we trigger them explicitly.
	b.SetMemtableThreshold(1e9)
	return b
}

// addDistinctKeys writes keys value-<i> for i in [start, end), one doc id each.
func addDistinctKeys(t *testing.T, b *Bucket, start, end int) {
	t.Helper()
	for i := start; i < end; i++ {
		key := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
}

func compactBucketFully(t *testing.T, ctx context.Context, b *Bucket) {
	t.Helper()
	for {
		compacted, err := b.disk.compactOnce(ctx)
		require.NoError(t, err)
		if !compacted {
			break
		}
	}
}

func assertWithinPct(t *testing.T, expected, actual, pct float64) {
	t.Helper()
	assert.InDelta(t, expected, actual, expected*pct/100,
		"expected %.0f within %.0f%%, got %.0f", expected, pct, actual)
}
