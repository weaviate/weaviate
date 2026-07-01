//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
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

// TestBucketPropertyCardinality exercises the bloom-filter cardinality estimate
// (Bucket.GetKeysCount) on a roaringset bucket — the strategy used by a
// filterable inverted-index property, whose keys are the distinct property
// values. The inserted data is known, so the test asserts against the true
// distinct count directly.
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

		// Three 1000-key segments with overlapping ranges => 2000 distinct union.
		// Equal key counts => equal bloom geometry => Merge ORs them into a single
		// union filter, so the estimate tracks the deduplicated distinct count
		// rather than the sum (3000).
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

		// Mismatched geometry => can't merge; instead of failing it keeps the
		// higher-cardinality filter, i.e. the larger segment's estimate (~1000),
		// which is a lower bound on the true 1500 distinct keys.
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 1000, float64(est), 5)

		// Compacting into a single segment gives a consistent geometry again, so
		// the estimate covers the full union.
		compactAll(t, ctx, b)
		require.Equal(t, 1, b.disk.Len())

		est, err = b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 1500, float64(est), 5)
	})

	t.Run("memtable-only: no flushed segments yet", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, t.TempDir())
		defer b.Shutdown(ctx)

		addDistinctKeys(t, b, 0, 100) // no flush: data stays in the memtable
		require.Equal(t, 0, b.disk.Len())

		// With no disk segments the bloom path has nothing to estimate.
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assert.Equal(t, uint32(0), est)
	})
}

func newCardinalityBucket(ctx context.Context, t *testing.T, dir string) *Bucket {
	t.Helper()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithUseBloomFilter(true),
	)
	require.NoError(t, err)
	// Large enough that flushes only happen when we trigger them explicitly.
	b.SetMemtableThreshold(1e9)
	return b
}

// addDistinctKeys writes keys value-<i> for i in [start, end), one doc id each.
// Each key is a distinct property value, so the count of keys is the cardinality.
func addDistinctKeys(t *testing.T, b *Bucket, start, end int) {
	t.Helper()
	for i := start; i < end; i++ {
		key := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
}

func compactAll(t *testing.T, ctx context.Context, b *Bucket) {
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
