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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestVectorCacheGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	var vecForId VectorForID = nil
	vectorCache := newShardedLockCache(vecForId, 1000000, logger, false, time.Duration(10000))
	id := int64(100000)
	assert.True(t, int64(len(vectorCache.cache)) < id)
	vectorCache.grow(uint64(id))
	assert.True(t, int64(len(vectorCache.cache)) > id)
	last := vectorCache.count
	vectorCache.grow(uint64(id))
	assert.True(t, int64(len(vectorCache.cache)) == last)
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
	c.obtainAllLocks()
	defer c.releaseAllLocks()

	count := 0
	for _, vec := range c.cache {
		if vec != nil {
			count++
		}
	}
	return count
}
