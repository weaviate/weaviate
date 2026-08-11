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

package cache

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId common.VectorForID[float32] = nil
	id := 100_000
	expectedCount := int64(0)

	vectorCache := NewShardedFloat32LockCache(vecForId, nil, 1_000_000, 1, logger, false, time.Duration(10_000), nil)
	initialSize := vectorCache.Len()
	assert.Less(t, int(initialSize), id)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())

	vectorCache.Grow(uint64(id))
	size1stGrow := vectorCache.Len()
	assert.Greater(t, int(size1stGrow), id)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())

	vectorCache.Grow(uint64(id))
	size2ndGrow := vectorCache.Len()
	assert.Equal(t, size1stGrow, size2ndGrow)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())
}

func TestCache_ParallelGrowth(t *testing.T) {
	// no asserts
	// ensures there is no "index out of range" panic on get

	logger, _ := test.NewNullLogger()
	var vecForId common.VectorForID[float32] = func(context.Context, uint64) ([]float32, error) { return nil, nil }
	vectorCache := NewShardedFloat32LockCache(vecForId, nil, 1_000_000, 1, logger, false, time.Second, nil)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 10_000
	maxNode := 100_000

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		node := uint64(r.Intn(maxNode))
		go func(node uint64) {
			defer wg.Done()

			vectorCache.Grow(node)
			vectorCache.Get(context.Background(), node)
		}(node)
	}

	wg.Wait()
}

func TestCacheCleanup(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId common.VectorForID[float32] = nil

	maxSize := 10
	batchSize := maxSize - 1
	deletionInterval := 200 * time.Millisecond // overwrite default deletionInterval of 3s
	sleepDuration := deletionInterval + 100*time.Millisecond

	t.Run("count is not reset on unnecessary deletion", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, maxSize, 1, logger, false, deletionInterval, nil)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		for i := 0; i < batchSize; i++ {
			shardedLockCache.Preload(uint64(i), []float32{float32(i), float32(i)})
		}
		time.Sleep(sleepDuration) // wait for deletion to fire

		assert.Equal(t, batchSize, int(shardedLockCache.CountVectors()))
		assert.Equal(t, batchSize, countCached(shardedLockCache))

		shardedLockCache.Drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countCached(shardedLockCache))
	})

	t.Run("deletion clears cache and counter when maxSize exceeded", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, maxSize, 1, logger, false, deletionInterval, nil)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		for b := 0; b < 2; b++ {
			for i := 0; i < batchSize; i++ {
				id := b*batchSize + i
				shardedLockCache.Preload(uint64(id), []float32{float32(id), float32(id)})
			}
			time.Sleep(sleepDuration) // wait for deletion to fire, 2nd should clean the cache
		}

		assert.Equal(t, 0, int(shardedLockCache.CountVectors()))
		assert.Equal(t, 0, countCached(shardedLockCache))

		shardedLockCache.Drop()
	})
}

func countCached(c *shardedLockCache[float32]) int {
	c.LockAll()
	defer c.UnlockAll()

	count := 0
	for _, vec := range c.cache {
		if vec != nil {
			count++
		}
	}
	return count
}

func TestGetAllInCurrentLock(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pageSize := uint64(10)
	maxSize := 1000

	t.Run("fully cached page", func(t *testing.T) {
		// Setup a cache with some pre-loaded vectors
		vectorCache := NewShardedFloat32LockCache(nil, nil, maxSize, pageSize, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		// Preload vectors for a full page
		for i := uint64(0); i < pageSize; i++ {
			cache.Preload(i, []float32{float32(i)})
		}

		// Test retrieving the full page
		out := make([][]float32, pageSize)
		errs := make([]error, pageSize)
		resultOut, resultErrs, start, end := cache.GetAllInCurrentLock(context.Background(), 5, out, errs)

		assert.Equal(t, uint64(0), start)
		assert.Equal(t, pageSize, end)
		assert.Equal(t, pageSize, uint64(len(resultOut)))
		assert.Equal(t, pageSize, uint64(len(resultErrs)))

		// Verify all vectors are present and correct
		for i := uint64(0); i < pageSize; i++ {
			assert.NotNil(t, resultOut[i])
			assert.Equal(t, []float32{float32(i)}, resultOut[i])
			assert.Nil(t, resultErrs[i])
		}
	})

	t.Run("partially cached page", func(t *testing.T) {
		// Setup mock vector retrieval function
		vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{float32(id * 100)}, nil
		}

		vectorCache := NewShardedFloat32LockCache(vecForID, nil, maxSize, pageSize, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		// Preload only some vectors
		for i := uint64(0); i < pageSize/2; i++ {
			cache.Preload(i, []float32{float32(i)})
		}

		out := make([][]float32, pageSize)
		errs := make([]error, pageSize)
		resultOut, resultErrs, start, end := cache.GetAllInCurrentLock(context.Background(), 5, out, errs)

		assert.Equal(t, uint64(0), start)
		assert.Equal(t, pageSize, end)

		// Verify cached vectors
		for i := uint64(0); i < pageSize/2; i++ {
			assert.NotNil(t, resultOut[i])
			assert.Equal(t, []float32{float32(i)}, resultOut[i])
			assert.Nil(t, resultErrs[i])
		}

		// Verify vectors loaded from storage
		for i := pageSize / 2; i < pageSize; i++ {
			assert.NotNil(t, resultOut[i])
			assert.Equal(t, []float32{float32(i * 100)}, resultOut[i])
			assert.Nil(t, resultErrs[i])
		}
	})

	t.Run("page beyond cache size", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(nil, nil, maxSize, pageSize, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		// Request vectors beyond current cache size
		beyondCacheID := uint64(len(cache.cache) + int(pageSize))
		out := make([][]float32, pageSize)
		errs := make([]error, pageSize)
		resultOut, resultErrs, start, end := cache.GetAllInCurrentLock(context.Background(), beyondCacheID, out, errs)

		// Verify we get the last complete page
		expectedStart := (beyondCacheID / pageSize) * pageSize
		expectedEnd := uint64(len(cache.cache))
		assert.Equal(t, expectedStart, start)
		assert.Equal(t, expectedEnd, end)

		// All vectors should be nil since they're beyond cache size
		for i := uint64(0); i < uint64(len(resultOut)); i++ {
			assert.Nil(t, resultOut[i])
			assert.Nil(t, resultErrs[i])
		}
	})

	t.Run("error handling from storage", func(t *testing.T) {
		expectedErr := errors.New("storage error")
		vecForID := func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, expectedErr
		}

		vectorCache := NewShardedFloat32LockCache(vecForID, nil, maxSize, pageSize, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		// make the page part of the cache window; fetch-through only applies
		// to pages within it
		cache.Grow(pageSize - 1)

		out := make([][]float32, pageSize)
		errs := make([]error, pageSize)
		resultOut, resultErrs, start, end := cache.GetAllInCurrentLock(context.Background(), 5, out, errs)

		assert.Equal(t, uint64(0), start)
		assert.Equal(t, pageSize, end)

		// All vectors should be nil and have errors
		for i := uint64(0); i < pageSize; i++ {
			assert.Nil(t, resultOut[i])
			assert.Equal(t, expectedErr, resultErrs[i])
		}
	})
}

func TestMultiVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var multivecForId common.VectorForID[[]float32] = nil
	id := 100_000
	expectedCount := int64(0)

	vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, time.Duration(10_000), nil)
	initialSize := vectorCache.Len()
	assert.Less(t, int(initialSize), id)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())

	vectorCache.Grow(uint64(id))
	size1stGrow := vectorCache.Len()
	assert.Greater(t, int(size1stGrow), id)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())

	vectorCache.Grow(uint64(id))
	size2ndGrow := vectorCache.Len()
	assert.Equal(t, size1stGrow, size2ndGrow)
	assert.Equal(t, expectedCount, vectorCache.CountVectors())
}

func TestMultiCache_ParallelGrowth(t *testing.T) {
	// no asserts
	// ensures there is no "index out of range" panic on get

	logger, _ := test.NewNullLogger()
	var multivecForId common.VectorForID[[]float32] = func(context.Context, uint64) ([][]float32, error) { return nil, nil }
	vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, time.Second, nil)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 10_000
	maxNode := 100_000

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		node := uint64(r.Intn(maxNode))
		go func(node uint64) {
			defer wg.Done()

			vectorCache.Grow(node)
		}(node)
	}

	wg.Wait()
}

func TestMultiCacheCleanup(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var multivecForId common.VectorForID[[]float32] = nil

	maxSize := 10
	batchSize := maxSize - 1
	deletionInterval := 200 * time.Millisecond // overwrite default deletionInterval of 3s
	sleepDuration := deletionInterval + 100*time.Millisecond

	t.Run("count is not reset on unnecessary deletion", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, maxSize, logger, false, deletionInterval, nil)
		shardedLockCache, ok := vectorCache.(*shardedMultipleLockCache[float32])
		assert.True(t, ok)

		for i := 0; i < batchSize; i++ {
			shardedLockCache.PreloadMulti(uint64(i), []uint64{uint64(i)}, [][]float32{{float32(i), float32(i)}})
		}
		time.Sleep(sleepDuration) // wait for deletion to fire

		assert.Equal(t, batchSize, int(shardedLockCache.CountVectors()))
		assert.Equal(t, batchSize, countMultiCached(shardedLockCache))

		shardedLockCache.Drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countMultiCached(shardedLockCache))
	})

	t.Run("deletion clears cache and counter when maxSize exceeded", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, maxSize, logger, false, deletionInterval, nil)
		shardedLockCache, ok := vectorCache.(*shardedMultipleLockCache[float32])
		assert.True(t, ok)

		for b := 0; b < 2; b++ {
			for i := 0; i < batchSize; i++ {
				id := b*batchSize + i
				shardedLockCache.PreloadMulti(uint64(id), []uint64{uint64(id)}, [][]float32{{float32(id), float32(id)}})
			}
			time.Sleep(sleepDuration) // wait for deletion to fire, 2nd should clean the cache
		}

		assert.Equal(t, 0, int(shardedLockCache.CountVectors()))
		assert.Equal(t, 0, countMultiCached(shardedLockCache))

		shardedLockCache.Drop()
	})
}

func TestGet_OutOfBounds(t *testing.T) {
	logger, _ := test.NewNullLogger()

	notFoundVecForID := func(ctx context.Context, id uint64) ([]float32, error) {
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}
	notFoundMultiVecForID := func(ctx context.Context, id uint64) ([][]float32, error) {
		return nil, storobj.NewErrNotFoundf(id, "not found")
	}

	t.Run("shardedLockCache grows and handles miss for out-of-bounds id", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(notFoundVecForID, nil, 1_000_000, 1, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		// Cache should have grown to accommodate the ID.
		assert.Greater(t, len(cache.cache), int(outOfBoundsID))
	})

	t.Run("shardedLockCache grows and handles miss for id exactly at cache length", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(notFoundVecForID, nil, 1_000_000, 1, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		atBoundID := uint64(len(cache.cache))
		vec, err := cache.Get(context.Background(), atBoundID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		assert.Greater(t, len(cache.cache), int(atBoundID))
	})

	t.Run("shardedLockCache returns vector for valid id", func(t *testing.T) {
		expected := []float32{1.0, 2.0, 3.0}
		vectorCache := NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		cache.Preload(0, expected)
		vec, err := cache.Get(context.Background(), 0)

		assert.NoError(t, err)
		assert.Equal(t, expected, vec)
	})

	t.Run("shardedMultipleLockCache grows and handles miss for out-of-bounds id", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(notFoundMultiVecForID, 1_000_000, logger, false, 0, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		assert.Greater(t, len(cache.cache), int(outOfBoundsID))
	})

	t.Run("shardedMultipleLockCache grows and handles miss for id exactly at cache length", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(notFoundMultiVecForID, 1_000_000, logger, false, 0, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		atBoundID := uint64(len(cache.cache))
		vec, err := cache.Get(context.Background(), atBoundID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		assert.Greater(t, len(cache.cache), int(atBoundID))
	})

	t.Run("shardedMultipleLockCache returns vector for valid id", func(t *testing.T) {
		expected := []float32{1.0, 2.0, 3.0}
		var multivecForId common.VectorForID[[]float32] = nil
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, 0, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		cache.PreloadPassage(0, 0, 0, expected)
		vec, err := cache.Get(context.Background(), 0)

		assert.NoError(t, err)
		assert.Equal(t, expected, vec)
	})
}

func countMultiCached(c *shardedMultipleLockCache[float32]) int {
	c.shardedLocks.LockAll()
	defer c.shardedLocks.UnlockAll()

	count := 0
	for _, vec := range c.cache {
		if vec != nil {
			count++
		}
	}
	return count
}

// countingFetcher returns a deterministic vector for any id ([]T{T(id)}) and
// counts how often the cache had to fall back to it.
func countingFetcher[T float32 | byte | uint64]() (common.VectorForID[T], *int64) {
	var calls int64
	return func(_ context.Context, id uint64) ([]T, error) {
		atomic.AddInt64(&calls, 1)
		return []T{T(id)}, nil
	}, &calls
}

// failingFetcher fails the test if the cache falls back to a fetch.
func failingFetcher[T float32 | byte | uint64](t *testing.T) common.VectorForID[T] {
	return func(_ context.Context, id uint64) ([]T, error) {
		t.Errorf("unexpected cache miss for id %d", id)
		return nil, errors.New("unexpected cache miss")
	}
}

// slotOf reads the raw backing slot, telling an empty slot from one Get would refill.
func slotOf[T float32 | byte | uint64](t *testing.T, c Cache[T], id uint64) []T {
	t.Helper()
	s, ok := c.(*shardedLockCache[T])
	require.True(t, ok, "expected *shardedLockCache")
	return s.cache[id]
}

// ifAbsentPreloader exposes PreloadIfAbsent, which is not on Cache[T].
func ifAbsentPreloader[T float32 | byte | uint64](t *testing.T, c Cache[T]) IfAbsentPreloader[T] {
	t.Helper()
	p, ok := c.(IfAbsentPreloader[T])
	require.True(t, ok, "expected an IfAbsentPreloader")
	return p
}

// stripeCountOf gives white-box access to the lock stripe count of a
// shardedLockCache so tests can assert on lazy allocation and re-striping.
// It is 0 until the stripes are allocated on first use.
func stripeCountOf[T float32 | byte | uint64](t *testing.T, c Cache[T]) uint64 {
	t.Helper()
	s, ok := c.(*shardedLockCache[T])
	require.True(t, ok, "expected *shardedLockCache")
	return s.shardedLocks.Count()
}

func newLazyTestCache[T float32 | byte | uint64](t *testing.T, fetch common.VectorForID[T]) Cache[T] {
	t.Helper()
	logger, _ := test.NewNullLogger()
	switch any(*new(T)).(type) {
	case float32:
		f := any(fetch).(common.VectorForID[float32])
		return any(NewShardedFloat32LockCache(f, nil, 1_000_000, 1, logger, false, 0, nil)).(Cache[T])
	case byte:
		f := any(fetch).(common.VectorForID[byte])
		return any(NewShardedByteLockCache(f, 1_000_000, 1, logger, 0, nil)).(Cache[T])
	case uint64:
		f := any(fetch).(common.VectorForID[uint64])
		return any(NewShardedUInt64LockCache(f, 1_000_000, 1, logger, 0, nil)).(Cache[T])
	}
	t.Fatal("unsupported type")
	return nil
}

func testLazyAllocation[T float32 | byte | uint64](t *testing.T) {
	fetch, calls := countingFetcher[T]()
	c := newLazyTestCache[T](t, fetch)

	// a fresh cache must not allocate its backing slice or its lock stripes
	assert.EqualValues(t, 0, c.Len(), "fresh cache must have length 0")
	assert.EqualValues(t, 0, c.CountVectors())
	assert.Nil(t, c.All(), "fresh cache must not allocate the backing slice")
	assert.EqualValues(t, 0, stripeCountOf(t, c), "fresh cache must not allocate lock stripes")

	// first Get initializes on demand and serves the vector
	vec, err := c.Get(context.Background(), 42)
	require.NoError(t, err)
	assert.Equal(t, []T{T(42)}, vec)
	assert.EqualValues(t, 1, atomic.LoadInt64(calls))

	// the cache grew relative to the requested id, not by a fixed 2000 jump
	assert.Greater(t, c.Len(), int32(42))
	assert.LessOrEqual(t, c.Len(), int32(100), "small first use must not allocate a large slice")

	assert.EqualValues(t, 8, stripeCountOf(t, c), "small cache must use a small stripe count")

	// second Get is served from cache
	vec, err = c.Get(context.Background(), 42)
	require.NoError(t, err)
	assert.Equal(t, []T{T(42)}, vec)
	assert.EqualValues(t, 1, atomic.LoadInt64(calls), "second Get must hit the cache")
}

func TestShardedLockCache_LazyAllocation(t *testing.T) {
	t.Run("float32", testLazyAllocation[float32])
	t.Run("byte", testLazyAllocation[byte])
	t.Run("uint64", testLazyAllocation[uint64])
}

func TestShardedLockCache_GrowSizing(t *testing.T) {
	fetch, _ := countingFetcher[float32]()
	c := newLazyTestCache[float32](t, fetch)

	c.Grow(10)
	assert.Greater(t, c.Len(), int32(10))
	assert.LessOrEqual(t, c.Len(), int32(50), "growing to a small id must stay small")
	small := c.Len()

	// mid-size caches get a stripe count proportional to their size:
	// Grow(2000) yields ~2500 slots, one stripe per 64 slots → 64 stripes
	c.Grow(2000)
	assert.EqualValues(t, 64, stripeCountOf(t, c),
		"mid-size cache must use a proportional stripe count")

	// large caches keep the fixed growth headroom and the full stripe count
	c.Grow(100_000)
	assert.Greater(t, c.Len(), int32(100_000))
	assert.LessOrEqual(t, c.Len(), int32(100_000+MinimumIndexGrowthDelta))
	assert.EqualValues(t, common.DefaultShardedLocksCount, stripeCountOf(t, c),
		"large cache must use the full stripe count")

	// growing never shrinks
	c.Grow(10)
	assert.Greater(t, c.Len(), small)
}

func TestShardedLockCache_OpsOnFreshCache(t *testing.T) {
	fetch, calls := countingFetcher[float32]()
	c := newLazyTestCache[float32](t, fetch)

	// none of these must panic on an untouched cache, and they must allocate
	// at most the minimal stripe set (never the backing slice)
	c.Delete(context.Background(), 7)
	c.Prefetch(7)
	assert.EqualValues(t, 0, c.CountVectors())
	assert.EqualValues(t, 0, c.Len(), "read-only ops must not allocate the backing slice")
	assert.LessOrEqual(t, stripeCountOf(t, c), uint64(initialShardedLocksCount),
		"read-only ops must allocate at most the minimal stripe set")

	// MultiGet on a fresh cache must fetch and cache the vectors
	vecs, errs := c.MultiGet(context.Background(), []uint64{5, 10})
	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, vecs, 2)
	assert.Equal(t, []float32{5}, vecs[0])
	assert.Equal(t, []float32{10}, vecs[1])
	assert.EqualValues(t, 2, atomic.LoadInt64(calls))

	c.Drop()
}

func TestShardedLockCache_GetAllInCurrentLockOnFreshCache(t *testing.T) {
	t.Run("default max size does not fetch through", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		fetch, calls := countingFetcher[uint64]()
		c := NewShardedUInt64LockCache(fetch, int(defaultCacheMaxSize), 1, logger, 0, nil)

		out := make([][]uint64, 1)
		errs := make([]error, 1)
		_, _, _, end := c.GetAllInCurrentLock(context.Background(), 3, out, errs)
		assert.EqualValues(t, 0, end, "fresh cache has nothing in range")
		assert.EqualValues(t, 0, atomic.LoadInt64(calls))
	})

	t.Run("altered max size does not fetch through either", func(t *testing.T) {
		// fetch-through only applies to pages within the current window;
		// iterating never grows the cache as a side effect
		fetch, calls := countingFetcher[uint64]()
		c := newLazyTestCache[uint64](t, fetch)

		out := make([][]uint64, 1)
		errs := make([]error, 1)
		_, _, _, end := c.GetAllInCurrentLock(context.Background(), 3, out, errs)
		assert.EqualValues(t, 0, end, "fresh cache has nothing in range")
		assert.EqualValues(t, 0, atomic.LoadInt64(calls))
	})
}

func TestShardedLockCache_PreloadGrowsCache(t *testing.T) {
	fetch, calls := countingFetcher[float32]()
	c := newLazyTestCache[float32](t, fetch)

	// Preload without a prior Grow must work for any id: HNSW's insert path
	// preloads freshly assigned node ids without growing the cache first.
	c.Preload(500, []float32{500})
	c.Preload(5000, []float32{5000})

	vec, err := c.Get(context.Background(), 500)
	require.NoError(t, err)
	assert.Equal(t, []float32{500}, vec)
	vec, err = c.Get(context.Background(), 5000)
	require.NoError(t, err)
	assert.Equal(t, []float32{5000}, vec)
	assert.EqualValues(t, 0, atomic.LoadInt64(calls), "preloaded vectors must not hit the fetcher")
	assert.EqualValues(t, 2, c.CountVectors())
}

func TestShardedLockCache_LockAllPreloadFlow(t *testing.T) {
	// mirrors the flat index PostStartup preload sequence
	fetch, calls := countingFetcher[uint64]()
	c := newLazyTestCache[uint64](t, fetch)

	const maxID = 300
	c.Grow(maxID)
	c.LockAll()
	c.SetSizeAndGrowNoLock(maxID)
	for i := uint64(0); i <= maxID; i++ {
		c.PreloadNoLock(i, []uint64{i})
	}
	c.UnlockAll()

	for _, id := range []uint64{0, 150, maxID} {
		vec, err := c.Get(context.Background(), id)
		require.NoError(t, err)
		assert.Equal(t, []uint64{id}, vec)
	}
	assert.EqualValues(t, 0, atomic.LoadInt64(calls), "preloaded vectors must not hit the fetcher")
}

func TestShardedLockCache_LockAllOnFreshCache(t *testing.T) {
	// LockAll on an untouched cache must initialize the locks so the
	// LockAll/PreloadNoLock/UnlockAll contract holds even without a prior Grow
	fetch, _ := countingFetcher[byte]()
	c := newLazyTestCache[byte](t, fetch)

	c.LockAll()
	c.SetSizeAndGrowNoLock(50)
	c.PreloadNoLock(50, []byte{50})
	c.UnlockAll()

	vec, err := c.Get(context.Background(), 50)
	require.NoError(t, err)
	assert.Equal(t, []byte{50}, vec)
}

func TestShardedLockCache_ConcurrentLazyInit(t *testing.T) {
	// hammer a fresh cache from many goroutines so the nil→initialized
	// transition and the stripe upgrade both happen under contention
	fetch, _ := countingFetcher[float32]()
	c := newLazyTestCache[float32](t, fetch)

	const (
		goroutines = 16
		maxID      = 5000 // crosses the stripe upgrade threshold
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed uint64) {
			defer wg.Done()
			for i := uint64(0); i < 500; i++ {
				id := (seed*131 + i*7) % maxID
				switch i % 5 {
				case 0:
					c.Grow(id)
				case 1:
					c.Preload(id, []float32{float32(id)})
				case 2:
					vec, err := c.Get(context.Background(), id)
					if err == nil && len(vec) == 1 && vec[0] != float32(id) {
						t.Errorf("id %d: got %v", id, vec[0])
					}
				case 3:
					c.Delete(context.Background(), id)
				case 4:
					c.Len()
					c.CountVectors()
				}
			}
		}(uint64(g))
	}
	wg.Wait()

	// after the dust settles every id must resolve correctly
	for _, id := range []uint64{0, 1, 2500, maxID - 1} {
		vec, err := c.Get(context.Background(), id)
		require.NoError(t, err)
		assert.Equal(t, []float32{float32(id)}, vec)
	}
	// the cache grew to cover ids up to ~5000 (5000..6250 slots depending on
	// interleaving), which at one stripe per 64 slots quantizes to 128
	assert.EqualValues(t, 128, stripeCountOf(t, c),
		"cache must have re-striped proportionally to its size")
}

func TestShardedLockCache_ReadersDuringReStripe(t *testing.T) {
	// readers on low ids while a grower pushes the cache past the stripe
	// upgrade threshold: reads must stay correct through the lock swap
	fetch, _ := countingFetcher[float32]()
	c := newLazyTestCache[float32](t, fetch)

	for i := uint64(0); i < 100; i++ {
		c.Preload(i, []float32{float32(i)})
	}
	require.EqualValues(t, 8, stripeCountOf(t, c))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			for i := uint64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := (seed + i) % 100
				vec, err := c.Get(context.Background(), id)
				if err != nil {
					t.Errorf("get %d: %v", id, err)
					return
				}
				if len(vec) != 1 || vec[0] != float32(id) {
					t.Errorf("get %d: wrong vector %v", id, vec)
					return
				}
			}
		}(uint64(g))
	}

	// force re-striping while readers are active
	for step := uint64(1000); step <= 20_000; step += 1000 {
		c.Grow(step)
	}
	close(stop)
	wg.Wait()

	assert.EqualValues(t, common.DefaultShardedLocksCount, stripeCountOf(t, c))
}

func TestShardedLockCache_PageSizeOnFreshCache(t *testing.T) {
	fetch, _ := countingFetcher[uint64]()
	c := newLazyTestCache[uint64](t, fetch)
	assert.EqualValues(t, 1, c.PageSize(), "PageSize must work before the locks are allocated")
}

// count feeds replaceIfFull's full-cache wipe, so it must equal the number of slots
// that hold a vector — across every transition a write can make, in both directions.
func TestPreloadCountsOccupiedSlotsOnly(t *testing.T) {
	const id = 5

	tests := []struct {
		name      string
		write     func(t *testing.T, c Cache[float32], p IfAbsentPreloader[float32])
		wantCount int64
		wantSlot  []float32
	}{
		{
			name: "first Preload occupies the slot",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{1, 2})
			},
			wantCount: 1,
			wantSlot:  []float32{1, 2},
		},
		{
			name: "overwrite does not recount",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{9, 9})
				c.Preload(id, []float32{1, 2})
			},
			wantCount: 1,
			wantSlot:  []float32{1, 2},
		},
		{
			name: "Preload nil over an occupied slot frees it",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{1, 2})
				c.Preload(id, nil)
			},
			wantCount: 0,
			wantSlot:  nil,
		},
		{
			name: "repeated Preload of nil never counts",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				for i := 0; i < 5; i++ {
					c.Preload(id, nil)
				}
			},
			wantCount: 0,
			wantSlot:  nil,
		},
		{
			name: "Preload of an empty non-nil vector leaves the slot empty",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{})
			},
			wantCount: 0,
			wantSlot:  nil,
		},
		{
			name: "PreloadIfAbsent fills an empty slot",
			write: func(t *testing.T, _ Cache[float32], p IfAbsentPreloader[float32]) {
				assert.True(t, p.PreloadIfAbsent(id, []float32{3, 4}))
			},
			wantCount: 1,
			wantSlot:  []float32{3, 4},
		},
		{
			name: "PreloadIfAbsent leaves an occupied slot alone",
			write: func(t *testing.T, c Cache[float32], p IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{1, 2})
				assert.False(t, p.PreloadIfAbsent(id, []float32{9, 9}))
			},
			wantCount: 1,
			wantSlot:  []float32{1, 2},
		},
		{
			name: "repeated PreloadIfAbsent of nil stores nothing and counts nothing",
			write: func(t *testing.T, _ Cache[float32], p IfAbsentPreloader[float32]) {
				for i := 0; i < 5; i++ {
					assert.False(t, p.PreloadIfAbsent(id, nil))
				}
			},
			wantCount: 0,
			wantSlot:  nil,
		},
		{
			name: "PreloadIfAbsent never empties an occupied slot",
			write: func(t *testing.T, c Cache[float32], p IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{1, 2})
				assert.False(t, p.PreloadIfAbsent(id, nil))
			},
			wantCount: 1,
			wantSlot:  []float32{1, 2},
		},
		{
			name: "Delete frees an occupied slot exactly once",
			write: func(_ *testing.T, c Cache[float32], _ IfAbsentPreloader[float32]) {
				c.Preload(id, []float32{1, 2})
				c.Delete(context.Background(), id)
				c.Delete(context.Background(), id)
			},
			wantCount: 0,
			wantSlot:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newLazyTestCache(t, failingFetcher[float32](t))
			c.Grow(10)

			tt.write(t, c, ifAbsentPreloader(t, c))

			assert.Equal(t, tt.wantCount, c.CountVectors())
			assert.Equal(t, tt.wantSlot, slotOf(t, c, id))
		})
	}
}

// This cache calls a slot occupied when it holds a non-empty vector — Get and Delete
// both test len() — so count must move on exactly that transition.
func TestMultiVectorCacheCountInvariant(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("repeated PreloadPassage on same id stays at 1", func(t *testing.T) {
		c := NewShardedMultiUInt64LockCache(nil, 1_000_000, logger, 0, nil)

		for i := 0; i < 5; i++ {
			c.PreloadPassage(3, 7, 0, []uint64{uint64(i)})
		}
		assert.Equal(t, int64(1), c.CountVectors())

		vec, err := c.Get(context.Background(), 3)
		require.NoError(t, err)
		assert.Equal(t, []uint64{4}, vec, "last PreloadPassage call wins")
	})

	t.Run("PreloadMulti with overlapping ids counts distinct slots only", func(t *testing.T) {
		c := NewShardedMultiUInt64LockCache(nil, 1_000_000, logger, 0, nil)

		c.PreloadMulti(100, []uint64{1, 2, 3}, [][]uint64{{10}, {20}, {30}})
		c.PreloadMulti(101, []uint64{3, 4, 5}, [][]uint64{{31}, {40}, {50}})

		// distinct ids touched: 1, 2, 3, 4, 5
		assert.Equal(t, int64(5), c.CountVectors())
	})

	t.Run("miss with empty fetch result does not increment count", func(t *testing.T) {
		emptyFetch := func(context.Context, uint64) ([]uint64, error) { return []uint64{}, nil }
		c := NewShardedMultiUInt64LockCache(emptyFetch, 1_000_000, logger, 0, nil)

		vec, err := c.Get(context.Background(), 9)
		require.NoError(t, err)
		assert.Empty(t, vec)
		assert.Equal(t, int64(0), c.CountVectors())
	})

	t.Run("an empty passage occupies nothing and stays reclaimable", func(t *testing.T) {
		c := NewShardedMultiUInt64LockCache(nil, 1_000_000, logger, 0, nil)

		c.PreloadPassage(1, 7, 0, []uint64{})
		assert.Equal(t, int64(0), c.CountVectors(), "an empty vec leaves the slot unoccupied")

		// Delete tests len(), so a slot counted while empty could never be reclaimed
		c.Delete(context.Background(), 1)
		assert.Equal(t, int64(0), c.CountVectors())
	})

	t.Run("filling a slot left empty by a preload counts it once", func(t *testing.T) {
		fetch, _ := countingFetcher[uint64]()
		c := NewShardedMultiUInt64LockCache(fetch, 1_000_000, logger, 0, nil)

		c.PreloadPassage(2, 7, 0, nil)
		vec, err := c.Get(context.Background(), 2)
		require.NoError(t, err)
		require.NotEmpty(t, vec, "an empty slot must be a miss, so Get refills it")

		assert.Equal(t, int64(1), c.CountVectors())

		c.Delete(context.Background(), 2)
		assert.Equal(t, int64(0), c.CountVectors())
	})

	t.Run("same id missed twice via handleMultipleCacheMiss counts once", func(t *testing.T) {
		// simulates two goroutines that both concluded a cache miss for the same id
		// and are now racing to store: only the first store may count.
		fetch, _ := countingFetcher[uint64]()
		vectorCache := NewShardedMultiUInt64LockCache(fetch, 1_000_000, logger, 0, nil)
		c, ok := vectorCache.(*shardedMultipleLockCache[uint64])
		require.True(t, ok)

		vec1, err := c.handleMultipleCacheMiss(context.Background(), 42, 42, 0)
		require.NoError(t, err)
		require.NotEmpty(t, vec1)

		vec2, err := c.handleMultipleCacheMiss(context.Background(), 42, 42, 0)
		require.NoError(t, err)
		require.NotEmpty(t, vec2)

		assert.Equal(t, int64(1), c.CountVectors())
	})
}

// Hammers a single uncached id from many goroutines that all overlap inside the
// fetch thunk, so every one of them observes the slot as empty before any stores.
func TestConcurrentMissSameID_NoDoubleCount(t *testing.T) {
	const goroutines = 50

	t.Run("single-vector cache", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		ready := make(chan struct{}, goroutines)
		release := make(chan struct{})

		fetch := func(ctx context.Context, id uint64) ([]float32, error) {
			ready <- struct{}{}
			<-release
			return []float32{float32(id)}, nil
		}

		c := NewShardedFloat32LockCache(fetch, nil, 1_000_000, 1, logger, false, 0, nil)

		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				vec, err := c.Get(context.Background(), 42)
				assert.NoError(t, err)
				assert.Equal(t, []float32{42}, vec)
			}()
		}

		for i := 0; i < goroutines; i++ {
			<-ready
		}
		close(release)
		wg.Wait()

		assert.Equal(t, int64(1), c.CountVectors())
	})

	t.Run("multivector cache", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		ready := make(chan struct{}, goroutines)
		release := make(chan struct{})

		fetch := func(ctx context.Context, id uint64) ([]uint64, error) {
			ready <- struct{}{}
			<-release
			return []uint64{id}, nil
		}

		c := NewShardedMultiUInt64LockCache(fetch, 1_000_000, logger, 0, nil)

		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				vec, err := c.Get(context.Background(), 42)
				assert.NoError(t, err)
				assert.Equal(t, []uint64{42}, vec)
			}()
		}

		for i := 0; i < goroutines; i++ {
			<-ready
		}
		close(release)
		wg.Wait()

		assert.Equal(t, int64(1), c.CountVectors())
	})
}

// PreloadIfAbsent can only win on an empty slot, so once at least one Preload has
// run for an id, the final stored value can never be a PreloadIfAbsent value.
func TestPreloadIfAbsent_ContentionWithPreload(t *testing.T) {
	c := newLazyTestCache(t, failingFetcher[float32](t))
	p := ifAbsentPreloader(t, c)

	const (
		numIDs       = 200
		workersPerID = 8
	)

	var wg sync.WaitGroup
	for id := uint64(0); id < numIDs; id++ {
		for w := 0; w < workersPerID; w++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				c.Preload(id, []float32{1, float32(id)})
			}()
			go func() {
				defer wg.Done()
				p.PreloadIfAbsent(id, []float32{2, float32(id)})
			}()
		}
	}
	wg.Wait()

	assert.Equal(t, int64(numIDs), c.CountVectors(), "count must equal exactly the number of distinct ids touched")

	for id := uint64(0); id < numIDs; id++ {
		vec, err := c.Get(context.Background(), id)
		require.NoError(t, err)
		require.Len(t, vec, 2)
		assert.Equal(t, float32(1), vec[0], "id %d: a Preload ran for it, so the final value must be a Preload value", id)
		assert.Equal(t, float32(id), vec[1])
	}
}

// Covers the Grow-retry loop: id beyond the cache length -> unlock -> Grow -> retry.
func TestPreloadIfAbsent_GrowRetry(t *testing.T) {
	c := newLazyTestCache(t, failingFetcher[float32](t))
	p := ifAbsentPreloader(t, c)

	c.Grow(10)
	require.Less(t, int(c.Len()), 50_000)

	id := uint64(50_000)
	ok := p.PreloadIfAbsent(id, []float32{7, 8})
	assert.True(t, ok)
	assert.Greater(t, c.Len(), int32(id), "PreloadIfAbsent must grow the cache to cover id")

	vec, err := c.Get(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, []float32{7, 8}, vec)
	assert.Equal(t, int64(1), c.CountVectors())
}

// Hunts for a lost update or livelock in the retry loop by forcing many goroutines
// through concurrent Grow calls for distinct out-of-bounds ids at once.
func TestPreloadIfAbsent_GrowRetryConcurrent(t *testing.T) {
	c := newLazyTestCache(t, failingFetcher[float32](t))
	p := ifAbsentPreloader(t, c)
	c.Grow(10)

	const (
		goroutines = 200
		baseID     = uint64(20_000)
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			id := baseID + uint64(i)
			ok := p.PreloadIfAbsent(id, []float32{float32(id)})
			assert.True(t, ok)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(goroutines), c.CountVectors())
	for i := 0; i < goroutines; i++ {
		id := baseID + uint64(i)
		vec, err := c.Get(context.Background(), id)
		require.NoError(t, err)
		assert.Equal(t, []float32{float32(id)}, vec)
	}
}
