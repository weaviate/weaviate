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
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type bucketKeyWriter struct {
	name    string
	opts    []BucketOption
	addKeys func(t *testing.T, b *Bucket, start, end int)
}

// A filterable property is a roaringset bucket, so it carries the scenarios
// below. StrategyRoaringSetRange is absent throughout: it holds per-bit bitmaps
// rather than a key set, and the shard builds it with WithUseBloomFilter(false),
// which GetKeysCount rejects.
var roaringsetKeyWriter = bucketKeyWriter{
	name: "roaringset",
	opts: []BucketOption{
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
	},
	addKeys: func(t *testing.T, b *Bucket, start, end int) {
		for i := start; i < end; i++ {
			require.NoError(t, b.RoaringSetAddList(cardinalityKey(i), []uint64{uint64(i)}))
		}
	},
}

// Everything the estimate does above the memtable is strategy-independent;
// only the path a key takes into a tree is not, so these get one run each.
var otherKeyWriters = []bucketKeyWriter{
	{
		name: "inverted",
		opts: []BucketOption{WithStrategy(StrategyInverted)},
		addKeys: func(t *testing.T, b *Bucket, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, b.MapSet(cardinalityKey(i),
					NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
			}
		},
	},
	{
		name: "map",
		opts: []BucketOption{WithStrategy(StrategyMapCollection)},
		addKeys: func(t *testing.T, b *Bucket, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, b.MapSet(cardinalityKey(i),
					NewMapPairFromDocIdAndTf(uint64(i), 1, 1, false)))
			}
		},
	},
	{
		name: "replace",
		opts: []BucketOption{WithStrategy(StrategyReplace)},
		addKeys: func(t *testing.T, b *Bucket, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, b.Put(cardinalityKey(i), []byte(fmt.Sprintf("doc-%06d", i))))
			}
		},
	},
	{
		name: "set",
		opts: []BucketOption{WithStrategy(StrategySetCollection)},
		addKeys: func(t *testing.T, b *Bucket, start, end int) {
			for i := start; i < end; i++ {
				require.NoError(t, b.SetAdd(cardinalityKey(i),
					[][]byte{[]byte(fmt.Sprintf("doc-%06d", i))}))
			}
		},
	},
}

// TestBucketPropertyCardinality exercises Bucket.GetKeysCount. Its keys are the
// distinct property values, so the estimates are asserted against the known
// distinct count of the inserted data.
func TestBucketPropertyCardinality(t *testing.T) {
	ctx := testCtx()
	w := roaringsetKeyWriter

	t.Run("single segment: estimate close to exact", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		const n = 5000
		w.addKeys(t, b, 0, n)
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 1, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, n, float64(est), 5)
	})

	t.Run("equal-sized overlapping segments: merge estimates the union", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		// three overlapping 1000-key segments: 2000 distinct, not the 3000 sum.
		// Equal key counts give equal geometry, so the filters really do merge.
		w.addKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 500, 1500)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 1000, 2000)
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 3, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 2000, float64(est), 10)
	})

	t.Run("different-sized segments: keeps higher-cardinality estimate, compaction unions", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		w.addKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 1000, 1500) // different size => different m/k
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

	t.Run("memtable unioned with the segment below it", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		w.addKeys(t, b, 0, 1000)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 1000, 2000)

		// disjoint layers, so comparing them would report only the larger ~1000.
		// The memtable fits the disk filter's geometry, so the two merge.
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 2000, float64(est), 5)
	})

	t.Run("memtable much larger than the segment below it", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		w.addKeys(t, b, 0, 100)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 100, 5000)

		// the disk filter holds ~1400 bits for its 100 keys; feeding it the
		// memtable's 4900 sets every one, making its estimate unusable, so the
		// two layers are estimated apart and compared
		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 4900, float64(est), 5)
	})

	t.Run("many equal-sized disjoint segments: saturated union falls back to a sane bound", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir())
		defer b.Shutdown(ctx)

		// 40 same-geometry filters of 500 disjoint keys each saturate their
		// union: every bit set, so its ApproximatedSize evaluates ln(0) with a
		// platform-defined uint32 result. The estimate must fall back to a
		// sound lower bound — a single segment's worth at minimum — and never
		// report the garbage value.
		const segs, perSeg = 40, 500
		for i := 0; i < segs; i++ {
			w.addKeys(t, b, i*perSeg, (i+1)*perSeg)
			require.NoError(t, b.FlushAndSwitch())
		}
		require.Equal(t, segs, b.disk.Len())

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		require.GreaterOrEqual(t, est, uint32(perSeg*95/100))
		require.LessOrEqual(t, est, uint32(segs*perSeg*105/100))

		// a differently-sized segment forms its own tiny unsaturated union; it
		// must not displace the saturated geometry's largest-member bound
		w.addKeys(t, b, segs*perSeg, segs*perSeg+100)
		require.NoError(t, b.FlushAndSwitch())

		est, err = b.GetKeysCount()
		require.NoError(t, err)
		require.GreaterOrEqual(t, est, uint32(perSeg*95/100))
		require.LessOrEqual(t, est, uint32((segs*perSeg+100)*105/100))
	})

	t.Run("segments below the mmap threshold: no bloom filters, keys counted exactly", func(t *testing.T) {
		// with an alloc checker present, segments this small are read fully
		// into memory and carry no bloom filter — the production default
		b := newCardinalityBucket(ctx, t, w, t.TempDir(),
			WithAllocChecker(memwatch.NewDummyMonitor()),
			WithMinMMapSize(1<<20))
		defer b.Shutdown(ctx)

		w.addKeys(t, b, 0, 300)
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 200, 500) // overlaps the first segment by 100
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 2, b.disk.Len())
		for i, seg := range b.disk.segments {
			require.Nilf(t, seg.getBloomFilter(),
				"segment %d must carry no bloom filter for this test to cover the exact path", i)
			keys := seg.getKeysSorted()
			require.Lenf(t, keys, 300, "segment %d must yield every key it holds", i)
			requireSortedDistinct(t, keys, "segment %d", i)
		}

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		require.Equal(t, uint32(500), est)

		w.addKeys(t, b, 400, 600) // memtable overlaps the second segment
		est, err = b.GetKeysCount()
		require.NoError(t, err)
		require.Equal(t, uint32(600), est)
	})

	t.Run("bloom and bloom-less segments mixed: exact keys join the union", func(t *testing.T) {
		b := newCardinalityBucket(ctx, t, w, t.TempDir(),
			WithAllocChecker(memwatch.NewDummyMonitor()),
			WithMinMMapSize(32<<10))
		defer b.Shutdown(ctx)

		w.addKeys(t, b, 0, 20000) // above the threshold: mmap'd, with filter
		require.NoError(t, b.FlushAndSwitch())
		w.addKeys(t, b, 20000, 20100) // below it: in memory, no filter
		require.NoError(t, b.FlushAndSwitch())
		require.Equal(t, 2, b.disk.Len())
		require.NotNil(t, b.disk.segments[0].getBloomFilter(),
			"large segment expected to carry a bloom filter")
		require.Nil(t, b.disk.segments[1].getBloomFilter(),
			"small segment expected to carry no bloom filter")

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, 20100, float64(est), 5)
	})

	// Lazy loading is the production default but only applies to segments read
	// off disk at startup — a flush always yields an eager segment — so covering
	// it requires reopening the directory.
	t.Run("lazily loaded segment: estimate survives a reopen", func(t *testing.T) {
		dir := t.TempDir()

		const n = 5000
		func() {
			b := newCardinalityBucket(ctx, t, w, dir)
			defer b.Shutdown(ctx)
			w.addKeys(t, b, 0, n)
			require.NoError(t, b.FlushAndSwitch())
		}()

		b := newCardinalityBucket(ctx, t, w, dir, WithLazySegmentLoading(true))
		defer b.Shutdown(ctx)
		require.Equal(t, 1, b.disk.Len())
		require.IsType(t, &lazySegment{}, b.disk.segments[0],
			"reopened segment must be lazy, otherwise this test does not cover the lazy path")

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		assertWithinPct(t, n, float64(est), 5)
	})

	t.Run("lazily loaded segment without a bloom filter: keys counted exactly", func(t *testing.T) {
		dir := t.TempDir()

		const n = 300
		func() {
			b := newCardinalityBucket(ctx, t, w, dir,
				WithWriteSegmentInfoIntoFileName(true))
			defer b.Shutdown(ctx)
			w.addKeys(t, b, 0, n)
			require.NoError(t, b.FlushAndSwitch())
		}()

		// the level and strategy in the file name spare a lazy segment the
		// load that answering for them would otherwise cost, so it is still
		// unloaded when the estimate runs
		b := newCardinalityBucket(ctx, t, w, dir,
			WithWriteSegmentInfoIntoFileName(true),
			WithLazySegmentLoading(true),
			WithAllocChecker(memwatch.NewDummyMonitor()),
			WithMinMMapSize(1<<20))
		defer b.Shutdown(ctx)
		require.Len(t, b.disk.segments, 1)
		lazy, ok := b.disk.segments[0].(*lazySegment)
		require.True(t, ok,
			"reopened segment must be lazy, otherwise this test does not cover the lazy path")
		// pinning a view loads the segment, so anything reading through the
		// segment group here — b.disk.Len() included — would defeat the check
		require.False(t, lazy.isLoaded(),
			"the estimate must be what loads the segment")

		est, err := b.GetKeysCount()
		require.NoError(t, err)
		require.Equal(t, uint32(n), est)

		require.Nil(t, lazy.getBloomFilter(),
			"segment must carry no bloom filter for this test to cover the exact path")
	})

	for _, w := range otherKeyWriters {
		t.Run(w.name+": estimate through this strategy's write path", func(t *testing.T) {
			b := newCardinalityBucket(ctx, t, w, t.TempDir())
			defer b.Shutdown(ctx)

			w.addKeys(t, b, 0, 1000)
			require.NoError(t, b.FlushAndSwitch())
			w.addKeys(t, b, 1000, 2000)
			require.Equal(t, 1, b.disk.Len())

			est, err := b.GetKeysCount()
			require.NoError(t, err)
			assertWithinPct(t, 2000, float64(est), 5)
		})
	}
}

func newCardinalityBucket(ctx context.Context, t *testing.T, w bucketKeyWriter, dir string, opts ...BucketOption) *Bucket {
	t.Helper()
	opts = append(append([]BucketOption{WithUseBloomFilter(true)}, w.opts...), opts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...,
	)
	require.NoError(t, err)
	// Large enough that flushes only happen when we trigger them explicitly.
	b.SetMemtableThreshold(1e9)
	return b
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
