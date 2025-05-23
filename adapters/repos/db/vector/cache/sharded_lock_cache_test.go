//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
