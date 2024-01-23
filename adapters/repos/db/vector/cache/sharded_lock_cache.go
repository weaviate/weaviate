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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type shardedLockCache[T float32 | byte | uint64] struct {
	shardedLocks     *common.ShardedLocks
	cache            [][][]T
	vectorForID      common.VectorForID[T]
	normalizeOnRead  bool
	maxSize          int64
	count            int64
	cancel           chan bool
	logger           logrus.FieldLogger
	deletionInterval time.Duration

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

const (
	InitialSize             = 1000
	MinimumIndexGrowthDelta = 2000
	indexGrowthRate         = 1.25
	pageSize                = 4096
)

func NewShardedFloat32LockCache(vecForID common.VectorForID[float32], maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
) Cache[float32] {
	vc := &shardedLockCache[float32]{
		vectorForID: func(ctx context.Context, id uint64) ([]float32, error) {
			vec, err := vecForID(ctx, id)
			if err != nil {
				return nil, err
			}
			if normalizeOnRead {
				vec = distancer.Normalize(vec)
			}
			return vec, nil
		},
		cache:            make([][][]float32, 1),
		normalizeOnRead:  normalizeOnRead,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewShardedLocks(pageSize),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
	}

	vc.cache[0] = make([][]float32, pageSize)

	vc.watchForDeletion()
	return vc
}

func NewShardedByteLockCache(vecForID common.VectorForID[byte], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
) Cache[byte] {
	vc := &shardedLockCache[byte]{
		vectorForID:      vecForID,
		cache:            make([][][]byte, 1),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewDefaultShardedLocks(),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
	}

	vc.cache[0] = make([][]byte, pageSize)

	vc.watchForDeletion()
	return vc
}

func NewShardedUInt64LockCache(vecForID common.VectorForID[uint64], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
) Cache[uint64] {
	vc := &shardedLockCache[uint64]{
		vectorForID:      vecForID,
		cache:            make([][][]uint64, 1),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewDefaultShardedLocks(),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
	}

	vc.cache = make([][][]uint64, 1)
	vc.cache[0] = make([][]uint64, pageSize)

	vc.watchForDeletion()
	return vc
}

func (s *shardedLockCache[T]) All() [][]T {
	s.shardedLocks.RLockAll()
	defer s.shardedLocks.RUnlockAll()

	out := make([][]T, 0, s.Len())
	for _, page := range s.cache {
		for _, vec := range page {
			if vec != nil {
				out = append(out, vec)
			}
		}
	}

	return out
}

// getPageForID returns the page and index of the vector for the given id.
// This function assumes that the caller holds a read lock on the page.
func (s *shardedLockCache[T]) getPageForID(id uint64) (page [][]T, idx int) {
	pageIdx := id / pageSize
	page = s.cache[pageIdx]
	if page == nil {
		return nil, -1
	}

	return page, int(id % pageSize)
}

// getOrCreatePage returns the page for the given id, or creates a new page if
// it doesn't exist yet.
// No locks must be held when calling this function.
func (s *shardedLockCache[T]) getOrCreatePage(id uint64) (page [][]T, idx int) {
	s.shardedLocks.Lock(id)
	page, idx = s.getPageForID(id)
	s.shardedLocks.Unlock(id)

	if page != nil {
		return
	}

	s.shardedLocks.Lock(id)
	// check if page was created while waiting for lock
	page, idx = s.getPageForID(id)
	if page != nil {
		s.shardedLocks.Unlock(id)
		return
	}

	// page doesn't exist yet, create it
	pageIdx := id / pageSize
	page = make([][]T, pageSize)
	s.cache[pageIdx] = page
	idx = int(id % pageSize)

	s.shardedLocks.Unlock(id)
	return
}

func (s *shardedLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	var vec []T

	s.shardedLocks.RLock(id)
	page, idx := s.getPageForID(id)
	if page != nil {
		vec = page[idx]
	}
	s.shardedLocks.RUnlock(id)

	if vec != nil {
		return vec, nil
	}

	return s.handleCacheMiss(ctx, id)
}

func (s *shardedLockCache[T]) Delete(ctx context.Context, id uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	if int(id) >= len(s.cache)*pageSize || s.cache[id] == nil {
		return
	}

	page, idx := s.getPageForID(id)
	if page == nil {
		return
	}

	page[idx] = nil
	atomic.AddInt64(&s.count, -1)

	// check if page is empty
	for _, v := range page {
		if v != nil {
			return
		}
	}

	// page is empty, delete it
	s.cache[id/pageSize] = nil

}

func (s *shardedLockCache[T]) handleCacheMiss(ctx context.Context, id uint64) ([]T, error) {
	vec, err := s.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)

	page, idx := s.getOrCreatePage(id)

	s.shardedLocks.Lock(id)
	page[idx] = vec
	s.shardedLocks.Unlock(id)

	return vec, nil
}

func (s *shardedLockCache[T]) MultiGet(ctx context.Context, ids []uint64) ([][]T, []error) {
	out := make([][]T, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		out[i], errs[i] = s.Get(ctx, id)
	}

	return out, errs
}

var prefetchFunc func(in uintptr) = func(in uintptr) {
	// do nothing on default arch
	// this function will be overridden for amd64
}

func (s *shardedLockCache[T]) Prefetch(id uint64) {
	page, idx := s.getOrCreatePage(id)

	s.shardedLocks.Lock(id)
	prefetchFunc(uintptr(unsafe.Pointer(&page[idx])))
	s.shardedLocks.Unlock(id)
}

func (s *shardedLockCache[T]) Preload(id uint64, vec []T) {
	atomic.AddInt64(&s.count, 1)
	page, idx := s.getOrCreatePage(id)

	s.shardedLocks.Lock(id)
	page[idx] = vec
	s.shardedLocks.Unlock(id)
}

func (s *shardedLockCache[T]) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < uint64(len(s.cache)*pageSize) {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(s.cache)*pageSize) {
		return
	}

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	maxSlot := node/pageSize + 1
	newCache := make([][][]T, maxSlot)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedLockCache[T]) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(len(s.cache) * pageSize)
}

func (s *shardedLockCache[T]) CountVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *shardedLockCache[T]) Drop() {
	s.deleteAllVectors()
	if s.deletionInterval != 0 {
		s.cancel <- true
	}
}

func (s *shardedLockCache[T]) deleteAllVectors() {
	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.cache {
		s.cache[i] = nil
	}

	atomic.StoreInt64(&s.count, 0)
}

func (s *shardedLockCache[T]) watchForDeletion() {
	if s.deletionInterval != 0 {
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
}

func (s *shardedLockCache[T]) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.deleteAllVectors()
	}
}

func (s *shardedLockCache[T]) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

func (s *shardedLockCache[T]) CopyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&s.maxSize)
	return sizeCopy
}

// noopCache can be helpful in debugging situations, where we want to
// explicitly pass through each vectorForID call to the underlying vectorForID
// function without caching in between.
type noopCache struct {
	vectorForID common.VectorForID[float32]
}

func NewNoopCache(vecForID common.VectorForID[float32], maxSize int,
	logger logrus.FieldLogger,
) *noopCache {
	return &noopCache{vectorForID: vecForID}
}
