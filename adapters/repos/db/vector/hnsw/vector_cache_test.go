//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId VectorForID = nil
	id := 100_000
	expectedCount := int64(0)

	vectorCache := newShardedLockCache(vecForId, 1_000_000, logger, false, time.Duration(10_000))
	initialSize := vectorCache.len()
	assert.Less(t, int(initialSize), id)
	assert.Equal(t, expectedCount, vectorCache.countVectors())

	vectorCache.grow(uint64(id))
	size1stGrow := vectorCache.len()
	assert.Greater(t, int(size1stGrow), id)
	assert.Equal(t, expectedCount, vectorCache.countVectors())

	vectorCache.grow(uint64(id))
	size2ndGrow := vectorCache.len()
	assert.Equal(t, size1stGrow, size2ndGrow)
	assert.Equal(t, expectedCount, vectorCache.countVectors())
}

func TestCache_ParallelGrowth(t *testing.T) {
	// no asserts
	// ensures there is no "index out of range" panic on get

	logger, _ := test.NewNullLogger()
	var vecForId VectorForID = func(context.Context, uint64) ([]float32, error) { return nil, nil }
	vectorCache := newShardedLockCache(vecForId, 1_000_000, logger, false, time.Second)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := 10_000
	maxNode := 100_000

	wg := new(sync.WaitGroup)
	wg.Add(count)
	for i := 0; i < count; i++ {
		node := uint64(r.Intn(maxNode))
		go func(node uint64) {
			defer wg.Done()

			vectorCache.grow(node)
			vectorCache.get(context.Background(), node)
		}(node)
	}

	wg.Wait()
}

func TestCacheCleanup(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId VectorForID = nil

	maxSize := 10
	batchSize := maxSize - 1
	deletionInterval := 200 * time.Millisecond // overwrite default deletionInterval of 3s
	sleepMs := deletionInterval + 100*time.Millisecond

	t.Run("count is not reset on unnecessary deletion", func(t *testing.T) {
		shardedLockCache := newShardedLockCache(vecForId, maxSize, logger, false, deletionInterval)

		for i := 0; i < batchSize; i++ {
			shardedLockCache.preload(uint64(i), []float32{float32(i), float32(i)})
		}
		time.Sleep(sleepMs) // wait for deletion to fire

		assert.Equal(t, batchSize, int(shardedLockCache.countVectors()))
		assert.Equal(t, batchSize, countCached(shardedLockCache))

		shardedLockCache.drop()

		assert.Equal(t, 0, int(shardedLockCache.count))
		assert.Equal(t, 0, countCached(shardedLockCache))
	})

	t.Run("deletion clears cache and counter when maxSize exceeded", func(t *testing.T) {
		shardedLockCache := newShardedLockCache(vecForId, maxSize, logger, false, deletionInterval)

		for b := 0; b < 2; b++ {
			for i := 0; i < batchSize; i++ {
				id := b*batchSize + i
				shardedLockCache.preload(uint64(id), []float32{float32(id), float32(id)})
			}
			time.Sleep(sleepMs) // wait for deletion to fire, 2nd should clean the cache
		}

		assert.Equal(t, 0, int(shardedLockCache.countVectors()))
		assert.Equal(t, 0, countCached(shardedLockCache))

		shardedLockCache.drop()
	})
}

func countCached(c *shardedLockCache) int {
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
