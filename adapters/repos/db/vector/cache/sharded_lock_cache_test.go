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

	t.Run("shardedLockCache returns error for out-of-bounds id", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.ErrorContains(t, err, "out of bounds")
	})

	t.Run("shardedLockCache returns error for id exactly at cache length", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
		cache := vectorCache.(*shardedLockCache[float32])

		atBoundID := uint64(len(cache.cache))
		vec, err := cache.Get(context.Background(), atBoundID)

		assert.Nil(t, vec)
		assert.ErrorContains(t, err, "out of bounds")
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

	t.Run("shardedMultipleLockCache returns error for out-of-bounds id", func(t *testing.T) {
		var multivecForId common.VectorForID[[]float32] = nil
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, 0, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(cache.cache) + 1)
		vec, err := cache.Get(context.Background(), outOfBoundsID)

		assert.Nil(t, vec)
		assert.ErrorContains(t, err, "out of bounds")
	})

	t.Run("shardedMultipleLockCache returns error for id exactly at cache length", func(t *testing.T) {
		var multivecForId common.VectorForID[[]float32] = nil
		vectorCache := NewShardedMultiFloat32LockCache(multivecForId, 1_000_000, logger, false, 0, nil)
		cache := vectorCache.(*shardedMultipleLockCache[float32])

		atBoundID := uint64(len(cache.cache))
		vec, err := cache.Get(context.Background(), atBoundID)

		assert.Nil(t, vec)
		assert.ErrorContains(t, err, "out of bounds")
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

func TestMultiCache_GetDocUnsupported(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("UInt64 cache returns error instead of panic", func(t *testing.T) {
		var vecForID common.VectorForID[uint64] = func(ctx context.Context, id uint64) ([]uint64, error) {
			return []uint64{1, 2, 3}, nil
		}
		cache := NewShardedMultiUInt64LockCache(vecForID, 1000, logger, 0, nil)

		// GetDoc should return error, not panic
		_, err := cache.GetDoc(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("Byte cache returns error instead of panic", func(t *testing.T) {
		var vecForID common.VectorForID[byte] = func(ctx context.Context, id uint64) ([]byte, error) {
			return []byte{1, 2, 3}, nil
		}
		cache := NewShardedMultiByteLockCache(vecForID, 1000, logger, 0, nil)

		// GetDoc should return error, not panic
		_, err := cache.GetDoc(context.Background(), 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})

	t.Run("Float32 cache with multipleVectorForDocID works", func(t *testing.T) {
		expectedVecs := [][]float32{{1.0, 2.0}, {3.0, 4.0}}
		var multiVecForID common.VectorForID[[]float32] = func(ctx context.Context, id uint64) ([][]float32, error) {
			return expectedVecs, nil
		}
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)

		vecs, err := cache.GetDoc(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, expectedVecs, vecs)
	})
}

func TestMultiCache_BoundsChecks(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("GetKeys returns zero values for out-of-bounds id", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(multiCache.cache) + 100)

		// Should not panic, should return 0, 0
		docID, relativeID := cache.GetKeys(outOfBoundsID)
		assert.Equal(t, uint64(0), docID)
		assert.Equal(t, uint64(0), relativeID)
	})

	t.Run("SetKeys silently ignores out-of-bounds id", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(multiCache.cache) + 100)

		// Should not panic
		cache.SetKeys(outOfBoundsID, 42, 7)

		// Verify nothing changed (can't really verify for out-of-bounds, just ensure no panic)
	})

	t.Run("Prefetch does not panic for out-of-bounds id", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(multiCache.cache) + 100)

		// Should not panic
		cache.Prefetch(outOfBoundsID)
	})

	t.Run("MultiGet returns error for out-of-bounds ids", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = func(ctx context.Context, id uint64) ([][]float32, error) {
			return [][]float32{{1.0}}, nil
		}
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		// Pre-populate keys for valid IDs so cache miss doesn't fail
		multiCache.vectorDocID[0] = CacheKeys{DocID: 0, RelativeID: 0}
		multiCache.vectorDocID[1] = CacheKeys{DocID: 1, RelativeID: 0}

		outOfBoundsID := uint64(len(multiCache.cache) + 100)
		ids := []uint64{0, outOfBoundsID, 1}

		vecs, errs := cache.MultiGet(context.Background(), ids)

		assert.Len(t, vecs, 3)
		assert.NotNil(t, errs)
		assert.Nil(t, errs[0])
		assert.ErrorContains(t, errs[1], "out of bounds")
		assert.Nil(t, errs[2])
	})

	t.Run("PreloadPassage silently ignores out-of-bounds id", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		outOfBoundsID := uint64(len(multiCache.cache) + 100)
		initialCount := cache.CountVectors()

		// Should not panic
		multiCache.PreloadPassage(outOfBoundsID, 1, 0, []float32{1.0, 2.0})

		// Count should not increase
		assert.Equal(t, initialCount, cache.CountVectors())
	})

	t.Run("PreloadMulti only counts successfully stored vectors", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		validID := uint64(0)
		outOfBoundsID := uint64(len(multiCache.cache) + 100)
		ids := []uint64{validID, outOfBoundsID}
		vecs := [][]float32{{1.0, 2.0}, {3.0, 4.0}}

		initialCount := cache.CountVectors()

		// Should not panic
		multiCache.PreloadMulti(1, ids, vecs)

		// Only one vector should be stored (the valid one)
		assert.Equal(t, initialCount+1, cache.CountVectors())
	})
}

func TestMultiCache_CountAccuracy(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("count not incremented when handleCacheMiss returns error", func(t *testing.T) {
		// Simulates a vectorForID that returns an error
		var multiVecForID common.VectorForID[[]float32] = func(ctx context.Context, id uint64) ([][]float32, error) {
			return nil, errors.New("storage error")
		}
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		// Setup keys for ID 0
		multiCache.vectorDocID[0] = CacheKeys{DocID: 0, RelativeID: 0}

		initialCount := cache.CountVectors()

		// Get should trigger cache miss with error
		vec, err := cache.Get(context.Background(), 0)
		assert.Error(t, err)
		assert.Nil(t, vec)

		// Count should NOT have increased since nothing was cached
		assert.Equal(t, initialCount, cache.CountVectors())
	})

	t.Run("count incremented only once for concurrent misses on same id", func(t *testing.T) {
		callCount := 0
		var mu sync.Mutex
		var multiVecForID common.VectorForID[[]float32] = func(ctx context.Context, id uint64) ([][]float32, error) {
			mu.Lock()
			callCount++
			mu.Unlock()
			// Simulate some delay to allow concurrent calls
			time.Sleep(10 * time.Millisecond)
			return [][]float32{{1.0, 2.0}}, nil
		}
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		// Setup keys for ID 0
		multiCache.vectorDocID[0] = CacheKeys{DocID: 0, RelativeID: 0}

		// Launch multiple concurrent gets for the same ID
		var wg sync.WaitGroup
		concurrentGets := 10
		wg.Add(concurrentGets)

		for i := 0; i < concurrentGets; i++ {
			go func() {
				defer wg.Done()
				_, _ = cache.Get(context.Background(), 0)
			}()
		}

		wg.Wait()

		// Count should be exactly 1, not 10
		assert.Equal(t, int64(1), cache.CountVectors())

		// Verify the vector was cached (subsequent calls shouldn't increment)
		_, _ = cache.Get(context.Background(), 0)
		assert.Equal(t, int64(1), cache.CountVectors())
	})

	t.Run("count matches actual cached vectors after concurrent operations", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = func(ctx context.Context, id uint64) ([][]float32, error) {
			return [][]float32{{float32(id)}}, nil
		}
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		multiCache := cache.(*shardedMultipleLockCache[float32])

		// Setup keys for multiple IDs
		numIDs := 100
		for i := 0; i < numIDs; i++ {
			multiCache.vectorDocID[i] = CacheKeys{DocID: uint64(i), RelativeID: 0}
		}

		// Concurrent gets for all IDs, with multiple goroutines per ID
		var wg sync.WaitGroup
		goroutinesPerID := 5

		for id := 0; id < numIDs; id++ {
			for g := 0; g < goroutinesPerID; g++ {
				wg.Add(1)
				go func(id uint64) {
					defer wg.Done()
					_, _ = cache.Get(context.Background(), id)
				}(uint64(id))
			}
		}

		wg.Wait()

		// Count should equal number of unique IDs, not total goroutines
		assert.Equal(t, int64(numIDs), cache.CountVectors())

		// Verify by actually counting cached vectors
		actualCached := countMultiCached(multiCache)
		assert.Equal(t, numIDs, actualCached)
		assert.Equal(t, int64(actualCached), cache.CountVectors())
	})
}

func TestPreloadCountAccuracy(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("shardedLockCache Preload does not increment count on overwrite", func(t *testing.T) {
		cache := NewShardedFloat32LockCache(nil, nil, 1000, 1, logger, false, 0, nil)
		slc := cache.(*shardedLockCache[float32])

		// First preload - should increment count
		slc.Preload(0, []float32{1.0, 2.0})
		assert.Equal(t, int64(1), cache.CountVectors())

		// Second preload to same slot - should NOT increment count
		slc.Preload(0, []float32{3.0, 4.0})
		assert.Equal(t, int64(1), cache.CountVectors())

		// Preload to new slot - should increment
		slc.Preload(1, []float32{5.0, 6.0})
		assert.Equal(t, int64(2), cache.CountVectors())

		// Verify actual cache content
		assert.Equal(t, 2, countCached(slc))
	})

	t.Run("shardedMultipleLockCache PreloadPassage does not increment count on overwrite", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		smlc := cache.(*shardedMultipleLockCache[float32])

		// First preload - should increment count
		smlc.PreloadPassage(0, 100, 0, []float32{1.0, 2.0})
		assert.Equal(t, int64(1), cache.CountVectors())

		// Second preload to same slot - should NOT increment count
		smlc.PreloadPassage(0, 100, 0, []float32{3.0, 4.0})
		assert.Equal(t, int64(1), cache.CountVectors())

		// Preload to new slot - should increment
		smlc.PreloadPassage(1, 100, 1, []float32{5.0, 6.0})
		assert.Equal(t, int64(2), cache.CountVectors())

		// Verify actual cache content
		assert.Equal(t, 2, countMultiCached(smlc))
	})

	t.Run("shardedMultipleLockCache PreloadMulti does not increment count on overwrite", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		smlc := cache.(*shardedMultipleLockCache[float32])

		// First batch preload - should count all 3
		ids1 := []uint64{0, 1, 2}
		vecs1 := [][]float32{{1.0}, {2.0}, {3.0}}
		smlc.PreloadMulti(100, ids1, vecs1)
		assert.Equal(t, int64(3), cache.CountVectors())

		// Second batch with overlap - only new entries should be counted
		// ids 1, 2 are overwrites, id 3 is new
		ids2 := []uint64{1, 2, 3}
		vecs2 := [][]float32{{10.0}, {20.0}, {30.0}}
		smlc.PreloadMulti(100, ids2, vecs2)
		assert.Equal(t, int64(4), cache.CountVectors()) // 3 original + 1 new

		// Verify actual cache content
		assert.Equal(t, 4, countMultiCached(smlc))
	})

	t.Run("shardedMultipleLockCache PreloadMulti mixed new and existing ids", func(t *testing.T) {
		var multiVecForID common.VectorForID[[]float32] = nil
		cache := NewShardedMultiFloat32LockCache(multiVecForID, 1000, logger, false, 0, nil)
		smlc := cache.(*shardedMultipleLockCache[float32])

		// Pre-populate some slots
		smlc.PreloadPassage(0, 100, 0, []float32{1.0})
		smlc.PreloadPassage(2, 100, 2, []float32{2.0})
		smlc.PreloadPassage(4, 100, 4, []float32{3.0})
		assert.Equal(t, int64(3), cache.CountVectors())

		// Batch with mix: 0, 2, 4 exist; 1, 3, 5 are new
		ids := []uint64{0, 1, 2, 3, 4, 5}
		vecs := [][]float32{{10.0}, {20.0}, {30.0}, {40.0}, {50.0}, {60.0}}
		smlc.PreloadMulti(200, ids, vecs)

		// Should only add 3 new entries (ids 1, 3, 5)
		assert.Equal(t, int64(6), cache.CountVectors())
		assert.Equal(t, 6, countMultiCached(smlc))
	})
}
