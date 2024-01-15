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

package hnsw

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector"
)

type compressedShardedLockCache struct {
	shardLocks          *vector.ShardLocks
	cache               [][]byte
	maxSize             int64
	count               int64
	cancel              chan bool
	vectorForID         CompressedVectorForID
	logger              logrus.FieldLogger
	dims                int32
	trackDimensionsOnce sync.Once

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

func newCompressedShardedLockCache(vecForID CompressedVectorForID, maxSize int, logger logrus.FieldLogger) *compressedShardedLockCache {
	vc := &compressedShardedLockCache{
		cache:           make([][]byte, initialSize),
		count:           0,
		maxSize:         int64(maxSize),
		cancel:          make(chan bool),
		logger:          logger,
		vectorForID:     vecForID,
		shardLocks:      vector.NewShardLocks(),
		maintenanceLock: sync.RWMutex{},
	}

	vc.watchForDeletion()
	return vc
}

func (c *compressedShardedLockCache) get(ctx context.Context, id uint64) ([]byte, error) {
	lock, err := c.shardLocks.RLock(ctx, id)
	if err != nil {
		return nil, err
	}
	vec := c.cache[id]
	lock.Unlock()

	if vec != nil {
		return vec, nil
	}

	return c.handleCacheMiss(ctx, id)
}

//nolint:unused
func (c *compressedShardedLockCache) all() [][]byte {
	return c.cache
}

//nolint:unused
func (c *compressedShardedLockCache) delete(ctx context.Context, id uint64) {
	lock, _ := c.shardLocks.Lock(context.TODO(), id)
	defer lock.Unlock()

	if int(id) >= len(c.cache) || c.cache[id] == nil {
		return
	}

	c.cache[id] = nil
	atomic.AddInt64(&c.count, -1)
}

func (c *compressedShardedLockCache) handleCacheMiss(ctx context.Context, id uint64) ([]byte, error) {
	vec, err := c.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&c.count, 1)
	c.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&c.dims, int32(len(vec)))
	})

	lock, err := c.shardLocks.Lock(ctx, id)
	if err != nil {
		return nil, err
	}
	c.cache[id] = vec
	lock.Unlock()

	return vec, nil
}

//nolint:unused
func (c *compressedShardedLockCache) multiGet(ctx context.Context, ids []uint64) ([][]byte, []error) {
	out := make([][]byte, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		lock, err := c.shardLocks.RLock(ctx, id)
		if err != nil {
			errs[i] = err
			continue
		}
		vec := c.cache[id]
		lock.Unlock()

		if vec == nil {
			vecFromDisk, err := c.handleCacheMiss(ctx, id)
			errs[i] = err
			vec = vecFromDisk
		}

		out[i] = vec
	}

	return out, errs
}

//nolint:unused
func (c *compressedShardedLockCache) prefetch(id uint64) {
	lock, _ := c.shardLocks.RLock(context.TODO(), id)
	defer lock.Unlock()

	prefetchFunc(uintptr(unsafe.Pointer(&c.cache[id])))
}

//nolint:unused
func (c *compressedShardedLockCache) preload(id uint64, vec []byte) {
	lock, _ := c.shardLocks.RLock(context.TODO(), id)
	defer lock.Unlock()

	atomic.AddInt64(&c.count, 1)
	c.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&c.dims, int32(len(vec)))
	})

	c.cache[id] = vec
}

func (c *compressedShardedLockCache) grow(node uint64) {
	c.maintenanceLock.RLock()
	if node < uint64(len(c.cache)) {
		c.maintenanceLock.RUnlock()
		return
	}
	c.maintenanceLock.RUnlock()

	c.maintenanceLock.Lock()
	defer c.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(c.cache)) {
		return
	}

	lock, _ := c.shardLocks.LockAll(context.TODO())
	defer lock.Unlock()

	newSize := node + minimumIndexGrowthDelta
	newCache := make([][]byte, newSize)
	copy(newCache, c.cache)
	c.cache = newCache
}

func (c *compressedShardedLockCache) len() int32 {
	return int32(len(c.cache))
}

func (c *compressedShardedLockCache) countVectors() int64 {
	return atomic.LoadInt64(&c.count)
}

//nolint:unused
func (c *compressedShardedLockCache) drop() {
	c.deleteAllVectors()
	c.cancel <- true
}

func (c *compressedShardedLockCache) deleteAllVectors() {
	lock, _ := c.shardLocks.LockAll(context.TODO())
	defer lock.Unlock()

	for i := range c.cache {
		c.cache[i] = nil
	}

	atomic.StoreInt64(&c.count, 0)
}

func (c *compressedShardedLockCache) watchForDeletion() {
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.cancel:
				return
			case <-t.C:
				c.replaceIfFull()
			}
		}
	}()
}

func (c *compressedShardedLockCache) replaceIfFull() {
	if atomic.LoadInt64(&c.count) >= atomic.LoadInt64(&c.maxSize) {
		c.deleteAllVectors()
	}
}

//nolint:unused
func (c *compressedShardedLockCache) updateMaxSize(size int64) {
	atomic.StoreInt64(&c.maxSize, size)
}

//nolint:unused
func (c *compressedShardedLockCache) copyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&c.maxSize)
	return sizeCopy
}
