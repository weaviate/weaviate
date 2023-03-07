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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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
	deletionInterval    time.Duration

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.Mutex
}

var shardFactor = uint64(512)

const defaultDeletionInterval = 3 * time.Second

func newShardedLockCache(vecForID VectorForID, maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
) *shardedLockCache {
	vc := &shardedLockCache{
		vectorForID:      vecForID,
		cache:            make([][]float32, initialSize),
		normalizeOnRead:  normalizeOnRead,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     make([]sync.RWMutex, shardFactor),
		maintenanceLock:  sync.Mutex{},
		deletionInterval: deletionInterval,
	}

	for i := uint64(0); i < shardFactor; i++ {
		vc.shardedLocks[i] = sync.RWMutex{}
	}
	vc.watchForDeletion()
	return vc
}

//nolint:unused
func (f *shardedLockCache) all() [][]float32 {
	return f.cache
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

//nolint:unused
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

//nolint:unused
var prefetchFunc func(in uintptr) = func(in uintptr) {
	// do nothing on default arch
	// this function will be overridden for amd64
}

//nolint:unused
func (n *shardedLockCache) prefetch(id uint64) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	prefetchFunc(uintptr(unsafe.Pointer(&n.cache[id])))
}

//nolint:unused
func (n *shardedLockCache) preload(id uint64, vec []float32) {
	n.shardedLocks[id%shardFactor].RLock()
	defer n.shardedLocks[id%shardFactor].RUnlock()

	atomic.AddInt64(&n.count, 1)
	n.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&n.dims, int32(len(vec)))
	})

	n.cache[id] = vec
}

//nolint:unused
func (n *shardedLockCache) grow(node uint64) {
	if node < uint64(len(n.cache)) {
		return
	}
	n.maintenanceLock.Lock()
	defer n.maintenanceLock.Unlock()

	n.obtainAllLocks()
	defer n.releaseAllLocks()

	newSize := node + minimumIndexGrowthDelta
	newCache := make([][]float32, newSize)
	copy(newCache, n.cache)
	atomic.StoreInt64(&n.count, int64(newSize))
	n.cache = newCache
}

//nolint:unused
func (n *shardedLockCache) len() int32 {
	return int32(len(n.cache))
}

//nolint:unused
func (n *shardedLockCache) countVectors() int64 {
	return atomic.LoadInt64(&n.count)
}

//nolint:unused
func (n *shardedLockCache) drop() {
	n.deleteAllVectors()
	n.cancel <- true
}

//nolint:unused
func (n *shardedLockCache) deleteAllVectors() {
	n.obtainAllLocks()
	defer n.releaseAllLocks()

	n.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range n.cache {
		n.cache[i] = nil
	}

	atomic.StoreInt64(&n.count, 0)
}

func (c *shardedLockCache) watchForDeletion() {
	go func() {
		t := time.NewTicker(c.deletionInterval)
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

func (c *shardedLockCache) replaceIfFull() {
	if atomic.LoadInt64(&c.count) >= atomic.LoadInt64(&c.maxSize) {
		c.maintenanceLock.Lock()
		c.deleteAllVectors()
		c.maintenanceLock.Unlock()
	}
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

func (c *shardedLockCache) releaseAllLocks() {
	for i := uint64(0); i < shardFactor; i++ {
		c.shardedLocks[i].Unlock()
	}
}

//nolint:unused
func (c *shardedLockCache) updateMaxSize(size int64) {
	atomic.StoreInt64(&c.maxSize, size)
}

//nolint:unused
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
