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
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
)

type compressedShardedLockCache struct {
	shardedLocks []sync.RWMutex
	cache        [][]byte
	maxSize      int64
	count        int64
	cancel       chan bool
	logger       logrus.FieldLogger
	//nolint:unused
	dims int32
	//nolint:unused
	trackDimensionsOnce sync.Once

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.Mutex
}

func newCompressedShardedLockCache(maxSize int, logger logrus.FieldLogger) *compressedShardedLockCache {
	vc := &compressedShardedLockCache{
		cache:           make([][]byte, initialSize),
		count:           0,
		maxSize:         int64(maxSize),
		cancel:          make(chan bool),
		logger:          logger,
		shardedLocks:    make([]sync.RWMutex, shardFactor),
		maintenanceLock: sync.Mutex{},
	}

	for i := uint64(0); i < shardFactor; i++ {
		vc.shardedLocks[i] = sync.RWMutex{}
	}
	vc.watchForDeletion()
	return vc
}

//nolint:unused
func (n *compressedShardedLockCache) get(ctx context.Context, id uint64) ([]byte, error) {
	n.shardedLocks[id%shardFactor].RLock()
	vec := n.cache[id]
	n.shardedLocks[id%shardFactor].RUnlock()

	if vec != nil {
		return vec, nil
	}

	return n.handleCacheMiss(ctx, id)
}

//nolint:unused
func (n *compressedShardedLockCache) all() [][]byte {
	return n.cache
}

//nolint:unused
func (n *compressedShardedLockCache) delete(ctx context.Context, id uint64) {
	n.shardedLocks[id%shardFactor].Lock()
	defer n.shardedLocks[id%shardFactor].Unlock()

	if int(id) >= len(n.cache) || n.cache[id] == nil {
		return
	}

	n.cache[id] = nil
	atomic.AddInt64(&n.count, -1)
}

//nolint:unused
func (n *compressedShardedLockCache) handleCacheMiss(ctx context.Context, id uint64) ([]byte, error) {
	return nil, errors.New("Not implemented")
}

//nolint:unused
func (n *compressedShardedLockCache) multiGet(ctx context.Context, ids []uint64) ([][]byte, []error) {
	out := make([][]byte, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		n.shardedLocks[id%shardFactor].RLock()
		vec := n.cache[id]
		n.shardedLocks[id%shardFactor].RUnlock()

		if vec == nil {
			vecFromDisk, err := n.handleCacheMiss(ctx, id)
			errs[i] = err
			vec = vecFromDisk
		}

		out[i] = vec
	}

	return out, errs
}

//nolint:unused
func (n *compressedShardedLockCache) prefetch(id uint64) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	prefetchFunc(uintptr(unsafe.Pointer(&n.cache[id])))
}

//nolint:unused
func (n *compressedShardedLockCache) preload(id uint64, vec []byte) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	atomic.AddInt64(&n.count, 1)
	n.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&n.dims, int32(len(vec)))
	})

	n.cache[id] = vec
}

//nolint:unused
func (n *compressedShardedLockCache) grow(node uint64) {
	if node < uint64(len(n.cache)) {
		return
	}

	n.maintenanceLock.Lock()
	defer n.maintenanceLock.Unlock()

	n.obtainAllLocks()
	defer n.releaseAllLocks()

	newSize := node + minimumIndexGrowthDelta
	newCache := make([][]byte, newSize)
	copy(newCache, n.cache)
	atomic.StoreInt64(&n.count, int64(newSize))
	n.cache = newCache
}

//nolint:unused
func (n *compressedShardedLockCache) len() int32 {
	return int32(len(n.cache))
}

//nolint:unused
func (n *compressedShardedLockCache) countVectors() int64 {
	return atomic.LoadInt64(&n.count)
}

//nolint:unused
func (n *compressedShardedLockCache) drop() {
	n.deleteAllVectors()
	n.cancel <- true
}

//nolint:unused
func (n *compressedShardedLockCache) deleteAllVectors() {
	n.obtainAllLocks()
	defer n.releaseAllLocks()

	for i := range n.cache {
		n.cache[i] = nil
	}

	atomic.StoreInt64(&n.count, 0)
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
		c.maintenanceLock.Lock()
		defer c.maintenanceLock.Unlock()

		c.obtainAllLocks()
		c.logger.WithField("action", "hnsw_delete_vector_cache").
			Debug("deleting full vector cache")
		for i := range c.cache {
			c.cache[i] = nil
		}
		c.releaseAllLocks()
		atomic.StoreInt64(&c.count, 0)
	}
}

func (c *compressedShardedLockCache) obtainAllLocks() {
	wg := &sync.WaitGroup{}
	for i := uint64(0); i < shardFactor; i++ {
		wg.Add(1)
		go func(index uint64) {
			defer wg.Done()
			c.shardedLocks[index].Lock()
		}(i)
	}

	wg.Wait()
}

func (c *compressedShardedLockCache) releaseAllLocks() {
	for i := uint64(0); i < shardFactor; i++ {
		c.shardedLocks[i].Unlock()
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
