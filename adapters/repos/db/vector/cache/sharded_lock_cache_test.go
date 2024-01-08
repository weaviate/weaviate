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

	vectorCache := NewShardedFloat32LockCache(vecForId, 1_000_000, logger, false, time.Duration(10_000))
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
	vectorCache := NewShardedFloat32LockCache(vecForId, 1_000_000, logger, false, time.Second)

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
	sleepMs := deletionInterval + 100*time.Millisecond

	t.Run("count is not reset on unnecessary deletion", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(vecForId, maxSize, logger, false, deletionInterval)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		for i := 0; i < batchSize; i++ {
			shardedLockCache.Preload(uint64(i), []float32{float32(i), float32(i)})
		}
		time.Sleep(sleepMs) // wait for deletion to fire

		assert.Equal(t, batchSize, int(shardedLockCache.CountVectors()))
		assert.Equal(t, batchSize, countCached(shardedLockCache))

		shardedLockCache.Drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countCached(shardedLockCache))
	})

	t.Run("deletion clears cache and counter when maxSize exceeded", func(t *testing.T) {
		vectorCache := NewShardedFloat32LockCache(vecForId, maxSize, logger, false, deletionInterval)
		shardedLockCache, ok := vectorCache.(*shardedLockCache[float32])
		assert.True(t, ok)

		for b := 0; b < 2; b++ {
			for i := 0; i < batchSize; i++ {
				id := b*batchSize + i
				shardedLockCache.Preload(uint64(id), []float32{float32(id), float32(id)})
			}
			time.Sleep(sleepMs) // wait for deletion to fire, 2nd should clean the cache
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
