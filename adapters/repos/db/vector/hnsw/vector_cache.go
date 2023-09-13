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
func (s *shardedLockCache) all() [][]float32 {
	return s.cache
}

func (s *shardedLockCache) get(ctx context.Context, id uint64) ([]float32, error) {
	s.shardedLocks[id%shardFactor].RLock()
	vec := s.cache[id]
	s.shardedLocks[id%shardFactor].RUnlock()

	if vec != nil {
		return vec, nil
	}

	return s.handleCacheMiss(ctx, id)
}

//nolint:unused
func (s *shardedLockCache) delete(ctx context.Context, id uint64) {
	s.shardedLocks[id%shardFactor].Lock()
	defer s.shardedLocks[id%shardFactor].Unlock()

	if int(id) >= len(s.cache) || s.cache[id] == nil {
		return
	}

	s.cache[id] = nil
	atomic.AddInt64(&s.count, -1)
}

func (s *shardedLockCache) handleCacheMiss(ctx context.Context, id uint64) ([]float32, error) {
	vec, err := s.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	s.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&s.dims, int32(len(vec)))
	})

	if s.normalizeOnRead {
		vec = distancer.Normalize(vec)
	}

	atomic.AddInt64(&s.count, 1)
	s.shardedLocks[id%shardFactor].Lock()
	s.cache[id] = vec
	s.shardedLocks[id%shardFactor].Unlock()

	return vec, nil
}

func (s *shardedLockCache) multiGet(ctx context.Context, ids []uint64) ([][]float32, []error) {
	out := make([][]float32, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		s.shardedLocks[id%shardFactor].RLock()
		vec := s.cache[id]
		s.shardedLocks[id%shardFactor].RUnlock()

		if vec == nil {
			vecFromDisk, err := s.handleCacheMiss(ctx, id)
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
func (s *shardedLockCache) prefetch(id uint64) {
	s.shardedLocks[id%shardFactor].RLock()
	defer s.shardedLocks[id%shardFactor].RUnlock()

	prefetchFunc(uintptr(unsafe.Pointer(&s.cache[id])))
}

func (s *shardedLockCache) preload(id uint64, vec []float32) {
	s.shardedLocks[id%shardFactor].Lock()
	defer s.shardedLocks[id%shardFactor].Unlock()

	atomic.AddInt64(&s.count, 1)
	s.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&s.dims, int32(len(vec)))
	})

	s.cache[id] = vec
}

func (s *shardedLockCache) grow(node uint64) {
	if node < uint64(len(s.cache)) {
		return
	}
	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	s.obtainAllLocks()
	defer s.releaseAllLocks()

	newSize := node + minimumIndexGrowthDelta
	newCache := make([][]float32, newSize)
	copy(newCache, s.cache)
	atomic.StoreInt64(&s.count, int64(newSize))
	s.cache = newCache
}

//nolint:unused
func (s *shardedLockCache) len() int32 {
	return int32(len(s.cache))
}

func (s *shardedLockCache) countVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *shardedLockCache) drop() {
	s.deleteAllVectors()
	s.cancel <- true
}

func (s *shardedLockCache) deleteAllVectors() {
	s.obtainAllLocks()
	defer s.releaseAllLocks()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.cache {
		s.cache[i] = nil
	}

	atomic.StoreInt64(&s.count, 0)
}

func (s *shardedLockCache) watchForDeletion() {
	go func() {
		t := time.NewTicker(s.deletionInterval)
		defer t.Stop()
		for {
			select {
			case <-s.cancel:
				return
			case <-t.C:
				s.replaceIfFull()
			}
		}
	}()
}

func (s *shardedLockCache) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.maintenanceLock.Lock()
		s.deleteAllVectors()
		s.maintenanceLock.Unlock()
	}
}

func (s *shardedLockCache) obtainAllLocks() {
	wg := &sync.WaitGroup{}
	for i := uint64(0); i < shardFactor; i++ {
		wg.Add(1)
		go func(index uint64) {
			defer wg.Done()
			s.shardedLocks[index].Lock()
		}(i)
	}

	wg.Wait()
}

func (s *shardedLockCache) releaseAllLocks() {
	for i := uint64(0); i < shardFactor; i++ {
		s.shardedLocks[i].Unlock()
	}
}

//nolint:unused
func (s *shardedLockCache) updateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

//nolint:unused
func (s *shardedLockCache) copyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&s.maxSize)
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
