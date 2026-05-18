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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId common.VectorForID[float32] = nil
	id := 100_000
	expectedCount := int64(0)

	vectorCache := NewShardedFloat32LockCache(vecForId, nil, 1_000_000, 1, logger, false, nil)
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
	vectorCache := NewShardedFloat32LockCache(vecForId, nil, 1_000_000, 1, logger, false, nil)

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

	maxSize := 10

	t.Run("no eviction below maxSize", func(t *testing.T) {
		var vecForId common.VectorForID[float32] = nil
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, maxSize, 1, logger, false, nil)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		for i := 0; i < maxSize-1; i++ {
			shardedLockCache.Preload(uint64(i), []float32{float32(i), float32(i)})
		}

		assert.Equal(t, maxSize-1, int(shardedLockCache.CountVectors()))
		assert.Equal(t, maxSize-1, countCached(shardedLockCache))

		shardedLockCache.Drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countCached(shardedLockCache))
	})

	t.Run("eviction triggers on cache miss when at maxSize", func(t *testing.T) {
		vecForId := func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{float32(id)}, nil
		}
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, maxSize, 1, logger, false, nil)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		// Preload to just below maxSize
		for i := 0; i < maxSize-1; i++ {
			shardedLockCache.Preload(uint64(i), []float32{float32(i)})
		}
		assert.Equal(t, maxSize-1, int(shardedLockCache.CountVectors()))

		// This Get causes a cache miss, pushing count to maxSize, triggering eviction
		vec, err := shardedLockCache.Get(context.Background(), uint64(maxSize-1))
		assert.NoError(t, err)
		assert.NotNil(t, vec)

		// After eviction, count is reset to 0 (deleteAllVectors sets it to 0)
		assert.Equal(t, 0, int(shardedLockCache.CountVectors()))

		shardedLockCache.Drop()
	})

	t.Run("concurrent cache misses near eviction boundary", func(t *testing.T) {
		vecForId := func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{float32(id)}, nil
		}
		concurrentMaxSize := 100
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, concurrentMaxSize, 1, logger, false, nil)

		// Preload to just below maxSize
		for i := 0; i < concurrentMaxSize-1; i++ {
			vectorCache.(*shardedLockCache[float32]).Preload(uint64(i), []float32{float32(i)})
		}

		// Many goroutines trigger cache misses concurrently near the boundary
		goroutines := 50
		wg := new(sync.WaitGroup)
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			id := uint64(concurrentMaxSize + i)
			go func(id uint64) {
				defer wg.Done()
				vectorCache.Grow(id)
				vectorCache.Get(context.Background(), id)
			}(id)
		}
		wg.Wait()

		// No panic or deadlock means success. The cache should still be usable.
		vec, err := vectorCache.Get(context.Background(), 0)
		assert.NoError(t, err)
		assert.NotNil(t, vec)

		vectorCache.Drop()
	})

	t.Run("multiple eviction cycles", func(t *testing.T) {
		vecForId := func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{float32(id)}, nil
		}
		vectorCache := NewShardedFloat32LockCache(vecForId, nil, maxSize, 1, logger, false, nil)
		c := vectorCache.(*shardedLockCache[float32])

		for cycle := 0; cycle < 3; cycle++ {
			// Fill to just below maxSize via Preload
			base := cycle * maxSize
			for i := 0; i < maxSize-1; i++ {
				id := uint64(base + i)
				c.Grow(id)
				c.Preload(id, []float32{float32(id)})
			}
			assert.Equal(t, maxSize-1, int(c.CountVectors()))

			// One more Get triggers eviction
			triggerID := uint64(base + maxSize - 1)
			c.Grow(triggerID)
			vec, err := c.Get(context.Background(), triggerID)
			assert.NoError(t, err)
			assert.NotNil(t, vec)
			assert.Equal(t, 0, int(c.CountVectors()))
		}

		c.Drop()
	})
}

func countCached(c *shardedLockCache[float32]) int {
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

func TestGetAllInCurrentLock(t *testing.T) {
	logger, _ := test.NewNullLogger()
	pageSize := uint64(10)
	maxSize := 1000

	t.Run("fully cached page", func(t *testing.T) {
		// Setup a cache with some pre-loaded vectors
		vectorCache := NewShardedFloat32LockCache(nil, nil, maxSize, pageSize, logger, false, nil)
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

		vectorCache := NewShardedFloat32LockCache(vecForID, nil, maxSize, pageSize, logger, false, nil)
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
		vectorCache := NewShardedFloat32LockCache(nil, nil, maxSize, pageSize, logger, false, nil)
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

		vectorCache := NewShardedFloat32LockCache(vecForID, nil, maxSize, pageSize, logger, false, nil)
		cache := vectorCache.(*shardedLockCache[float32])

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

	vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, nil)
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
	vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, nil)

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

	maxSize := 10

	t.Run("no eviction below maxSize", func(t *testing.T) {
		var multivecForId common.VectorForID[[]float32] = nil
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, maxSize, logger, false, nil)
		shardedLockCache, ok := vectorCache.(*shardedMultipleLockCache[float32])
		assert.True(t, ok)

		for i := 0; i < maxSize-1; i++ {
			shardedLockCache.PreloadMulti(uint64(i), []uint64{uint64(i)}, [][]float32{{float32(i), float32(i)}})
		}

		assert.Equal(t, maxSize-1, int(shardedLockCache.CountVectors()))
		assert.Equal(t, maxSize-1, countMultiCached(shardedLockCache))

		shardedLockCache.Drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countMultiCached(shardedLockCache))
	})

	t.Run("eviction triggers on cache miss when at maxSize", func(t *testing.T) {
		multivecForId := func(ctx context.Context, id uint64) ([][]float32, error) {
			return [][]float32{{float32(id)}}, nil
		}
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, maxSize, logger, false, nil)
		shardedLockCache, ok := vectorCache.(*shardedMultipleLockCache[float32])
		assert.True(t, ok)

		// Preload to just below maxSize
		for i := 0; i < maxSize-1; i++ {
			shardedLockCache.PreloadMulti(uint64(i), []uint64{uint64(i)}, [][]float32{{float32(i)}})
		}
		assert.Equal(t, maxSize-1, int(shardedLockCache.CountVectors()))

		// Set keys for the new ID so handleMultipleCacheMiss can fetch it
		newID := uint64(maxSize - 1)
		shardedLockCache.SetKeys(newID, newID, 0)

		// This Get causes a cache miss, pushing count to maxSize, triggering eviction
		vec, err := shardedLockCache.Get(context.Background(), newID)
		assert.NoError(t, err)
		assert.NotNil(t, vec)

		// After eviction, count is reset to 0
		assert.Equal(t, 0, int(shardedLockCache.CountVectors()))

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
		vectorCache := NewShardedFloat32LockCache(notFoundVecForID, nil, 1_000_000, 1, logger, false, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		// Cache should have grown to accommodate the ID.
		assert.Greater(t, len(cache.cache), int(outOfBoundsID))
	})

	t.Run("shardedLockCache grows and handles miss for id exactly at cache length", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(notFoundVecForID, nil, 1_000_000, 1, logger, false, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		atBoundID := uint64(len(cache.cache))
		vec, err := cache.Get(context.Background(), atBoundID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		assert.Greater(t, len(cache.cache), int(atBoundID))
	})

	t.Run("shardedLockCache returns vector for valid id", func(t *testing.T) {
		expected := []float32{1.0, 2.0, 3.0}
		vectorCache := NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		cache.Preload(0, expected)
		vec, err := cache.Get(context.Background(), 0)

		assert.NoError(t, err)
		assert.Equal(t, expected, vec)
	})

	t.Run("shardedMultipleLockCache grows and handles miss for out-of-bounds id", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(notFoundMultiVecForID, 1_000_000, logger, false, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.Error(t, err)
		assert.Greater(t, len(cache.cache), int(outOfBoundsID))
	})

	t.Run("shardedMultipleLockCache grows and handles miss for id exactly at cache length", func(t *testing.T) {
		vectorCache := NewShardedMultiFloat32LockCache(notFoundMultiVecForID, 1_000_000, logger, false, nil)
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
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, nil)
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
