//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus"
)

type shardedLockCache struct {
	shardedLocks        []sync.RWMutex
	cache               [][]float32
	vectorForID         VectorForID
	normalizeOnRead     bool
	maxSize             int64
	count               int64
	cancel              chan bool
	logger              logrus.FieldLogger
	dims                int32
	trackDimensionsOnce sync.Once

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.Mutex
}

var shardFactor = uint64(512)

func newShardedLockCache(vecForID VectorForID, maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool,
) *shardedLockCache {
	vc := &shardedLockCache{
		vectorForID:     vecForID,
		cache:           make([][]float32, initialSize),
		normalizeOnRead: normalizeOnRead,
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

func (n *shardedLockCache) get(ctx context.Context, id uint64) ([]float32, error) {
	n.shardedLocks[id%shardFactor].RLock()
	vec := n.cache[id]
	n.shardedLocks[id%shardFactor].RUnlock()

	if vec != nil {
		return vec, nil
	}

	return n.handleCacheMiss(ctx, id)
}

func (n *shardedLockCache) delete(ctx context.Context, id uint64) {
	n.shardedLocks[id%shardFactor].Lock()
	defer n.shardedLocks[id%shardFactor].Unlock()

	if int(id) >= len(n.cache) || n.cache[id] == nil {
		return
	}

	n.cache[id] = nil
	atomic.AddInt64(&n.count, -1)
}

func (n *shardedLockCache) handleCacheMiss(ctx context.Context, id uint64) ([]float32, error) {
	vec, err := n.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	n.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&n.dims, int32(len(vec)))
	})

	if n.normalizeOnRead {
		vec = distancer.Normalize(vec)
	}

	atomic.AddInt64(&n.count, 1)
	n.shardedLocks[id%shardFactor].Lock()
	n.cache[id] = vec
	n.shardedLocks[id%shardFactor].Unlock()

	return vec, nil
}

func (n *shardedLockCache) multiGet(ctx context.Context, ids []uint64) ([][]float32, []error) {
	out := make([][]float32, len(ids))
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

var prefetchFunc func(in uintptr) = func(in uintptr) {
	// do nothing on default arch
	// this function will be overridden for amd64
}

func (n *shardedLockCache) prefetch(id uint64) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	prefetchFunc(uintptr(unsafe.Pointer(&n.cache[id])))
}

func (n *shardedLockCache) preload(id uint64, vec []float32) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	atomic.AddInt64(&n.count, 1)
	n.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&n.dims, int32(len(vec)))
	})

	n.cache[id] = vec
}

func (n *shardedLockCache) grow(node uint64) {
	n.maintenanceLock.Lock()
	defer n.maintenanceLock.Unlock()

	n.obtainAllLocks()
	defer n.releaseAllLocks()

	newSize := node + minimumIndexGrowthDelta
	newCache := make([][]float32, newSize)
	copy(newCache, n.cache)
	n.cache = newCache
}

func (n *shardedLockCache) len() int32 {
	return int32(len(n.cache))
}

func (n *shardedLockCache) countVectors() int64 {
	return atomic.LoadInt64(&n.count)
}

func (n *shardedLockCache) drop() {
	n.deleteAllVectors()
	n.cancel <- true
}

func (n *shardedLockCache) deleteAllVectors() {
	n.obtainAllLocks()
	defer n.releaseAllLocks()

	for i := range n.cache {
		n.cache[i] = nil
	}

	atomic.StoreInt64(&n.count, 0)
}

func (c *shardedLockCache) watchForDeletion() {
	go func() {
		t := time.Tick(3 * time.Second)
		for {
			select {
			case <-c.cancel:
				return
			case <-t:
				c.replaceIfFull()
			}
		}
	}()
}

func (c *shardedLockCache) replaceIfFull() {
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
	}
	atomic.StoreInt64(&c.count, 0)
}

func (c *shardedLockCache) obtainAllLocks() {
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

// dimensions makes a best-effort guess at the dimensions of a vector. If the
// vector is in cache it serves it from cache. If the vector is not in the
// cache it tries to load it. If it can't load it (for example because it had
// been deleted in the underlying object stores) it picks another vector as it
// runs under the assumption that all vectors in the cache have the same
// dimensionality
func (c *shardedLockCache) dimensions(id uint64) int {
	c.shardedLocks[id%shardFactor].RLock()
	vec := c.cache[id]
	c.shardedLocks[id%shardFactor].RUnlock()

	if vec != nil {
		return len(vec)
	}

	vec, err := c.handleCacheMiss(context.Background(), id)
	if err != nil && vec != nil {
		return len(vec)
	}

	// this vector does not exist anymore, so we need to guess by taking another
	// vector
	return int(atomic.LoadInt32(&c.dims))
}

func (c *shardedLockCache) releaseAllLocks() {
	for i := uint64(0); i < shardFactor; i++ {
		c.shardedLocks[i].Unlock()
	}
}

func (c *shardedLockCache) updateMaxSize(size int64) {
	atomic.StoreInt64(&c.maxSize, size)
}

func (c *shardedLockCache) copyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&c.maxSize)
	return sizeCopy
}

// noopCache can be helpful in debugging situations, where we want to
// explicitly pass through each vectorForID call to the underlying vectorForID
// function without caching in between.
type noopCache struct {
	vectorForID VectorForID
}

func NewNoopCache(vecForID VectorForID, maxSize int,
	logger logrus.FieldLogger,
) *noopCache {
	return &noopCache{vectorForID: vecForID}
}

//nolint:unused
func (n *noopCache) get(ctx context.Context, id uint64) ([]float32, error) {
	return n.vectorForID(ctx, id)
}

//nolint:unused
func (n *noopCache) len() int32 {
	return 0
}
