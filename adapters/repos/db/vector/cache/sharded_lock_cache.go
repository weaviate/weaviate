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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type shardedLockCache[T float32 | byte | uint64] struct {
	shardedLocks     *common.ShardedRWLocks
	cache            [][]T
	vectorForID      common.VectorForID[T]
	normalizeOnRead  bool
	maxSize          int64
	count            int64
	cancel           chan bool
	logger           logrus.FieldLogger
	deletionInterval time.Duration
	allocChecker     memwatch.AllocChecker

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

const (
	InitialSize             = 1000
	RelativeInitialSize     = 20
	MinimumIndexGrowthDelta = 2000
	indexGrowthRate         = 1.25
)

func NewShardedFloat32LockCache(vecForID common.VectorForID[float32], maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
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
		cache:            make([][]float32, InitialSize),
		normalizeOnRead:  normalizeOnRead,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewDefaultShardedRWLocks(),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedByteLockCache(vecForID common.VectorForID[byte], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[byte] {
	vc := &shardedLockCache[byte]{
		vectorForID:      vecForID,
		cache:            make([][]byte, InitialSize),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewDefaultShardedRWLocks(),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedUInt64LockCache(vecForID common.VectorForID[uint64], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[uint64] {
	vc := &shardedLockCache[uint64]{
		vectorForID:      vecForID,
		cache:            make([][]uint64, InitialSize),
		normalizeOnRead:  false,
		count:            0,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewDefaultShardedRWLocks(),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func (s *shardedLockCache[T]) All() [][]T {
	return s.cache
}

func (s *shardedLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	s.shardedLocks.RLock(id)
	vec := s.cache[id]
	s.shardedLocks.RUnlock(id)

	if vec != nil {
		return vec, nil
	}

	return s.handleCacheMiss(ctx, id)
}

func (s *shardedLockCache[T]) Delete(ctx context.Context, id uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	if int(id) >= len(s.cache) || s.cache[id] == nil {
		return
	}

	s.cache[id] = nil
	atomic.AddInt64(&s.count, -1)
}

func (s *shardedLockCache[T]) handleCacheMiss(ctx context.Context, id uint64) ([]T, error) {
	if s.allocChecker != nil {
		// we don't really know the exact size here, but we don't have to be
		// accurate. If mem pressure is this high, we basically want to prevent any
		// new permanent heap alloc. If we underestimate the size of a vector a
		// bit, we might allow one more vector than we can really fit. But the one
		// after would fail then. Given that vectors are typically somewhat small
		// (in the single-digit KBs), it doesn't matter much, as long as we stop
		// allowing vectors when we're out of memory.
		estimatedSize := int64(1024)
		if err := s.allocChecker.CheckAlloc(estimatedSize); err != nil {
			s.logger.WithFields(logrus.Fields{
				"action": "vector_cache_miss",
				"event":  "vector_load_skipped_oom",
				"doc_id": id,
			}).WithError(err).
				Warnf("cannot load vector into cache due to memory pressure")
			return nil, err
		}
	}

	vec, err := s.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)
	s.shardedLocks.Lock(id)
	s.cache[id] = vec
	s.shardedLocks.Unlock(id)

	return vec, nil
}

func (s *shardedLockCache[T]) MultiGet(ctx context.Context, ids []uint64) ([][]T, []error) {
	out := make([][]T, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {
		s.shardedLocks.RLock(id)
		vec := s.cache[id]
		s.shardedLocks.RUnlock(id)

		if vec == nil {
			vecFromDisk, err := s.handleCacheMiss(ctx, id)
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

func (s *shardedLockCache[T]) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *shardedLockCache[T]) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

func (s *shardedLockCache[T]) Prefetch(id uint64) {
	s.shardedLocks.RLock(id)
	defer s.shardedLocks.RUnlock(id)

	prefetchFunc(uintptr(unsafe.Pointer(&s.cache[id])))
}

func (s *shardedLockCache[T]) Preload(id uint64, vec []T) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	atomic.AddInt64(&s.count, 1)
	s.cache[id] = vec
}

func (s *shardedLockCache[T]) PreloadNoLock(id uint64, vec []T) {
	s.cache[id] = vec
}

func (s *shardedLockCache[T]) SetSizeAndGrowNoLock(size uint64) {
	atomic.StoreInt64(&s.count, int64(size))

	if size < uint64(len(s.cache)) {
		return
	}
	newSize := size + MinimumIndexGrowthDelta
	newCache := make([][]T, newSize)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedLockCache[T]) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < uint64(len(s.cache)) {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(s.cache)) {
		return
	}

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	newSize := node + MinimumIndexGrowthDelta
	newCache := make([][]T, newSize)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedLockCache[T]) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(len(s.cache))
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
		f := func() {
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
		}
		enterrors.GoWrapper(f, s.logger)
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

func (s *shardedLockCache[T]) GetMultiple(ctx context.Context, docID uint64, relativeID uint64) ([]T, error) {
	return nil, errors.New("GetMultiple not implemented for shardedLockCache")
}

func (s *shardedLockCache[T]) MultiGetMultiple(ctx context.Context, docIDs []uint64, relativeID []uint64) ([][]float32, []error) {
	return nil, []error{errors.New("MultiGetMultiple not implemented for shardedLockCache")}
}

func (s *shardedLockCache[T]) PreloadMultiple(docID uint64, relativeID uint64, vec []float32) {
	panic("not implemented")
}

func (s *shardedLockCache[T]) PrefetchMultiple(docID uint64, relativeID uint64) {
	panic("not implemented")
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

type shardedMultipleLockCache struct {
	shardedLocks        *common.ShardedRWLocks
	cache               [][][]float32
	multipleVectorForID common.MultipleVectorForID[float32]
	normalizeOnRead     bool
	maxSize             int64
	count               int64
	cancel              chan bool
	logger              logrus.FieldLogger
	deletionInterval    time.Duration
	allocChecker        memwatch.AllocChecker

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

func NewShardedMultiFloat32LockCache(multipleVecForID common.MultipleVectorForID[float32], maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[float32] {
	multipleVecForIDValue := func(ctx context.Context, docId uint64, vecId uint64) ([]float32, error) {
		vec, err := multipleVecForID(ctx, docId, vecId)
		if err != nil {
			return nil, err
		}
		if normalizeOnRead {
			vec = distancer.Normalize(vec)
		}
		return vec, nil
	}

	cache := make([][][]float32, InitialSize)
	for i := range cache {
		cache[i] = make([][]float32, RelativeInitialSize)
	}

	vc := &shardedMultipleLockCache{
		multipleVectorForID: multipleVecForIDValue,
		cache:               cache,
		normalizeOnRead:     normalizeOnRead,
		count:               0,
		maxSize:             int64(maxSize),
		cancel:              make(chan bool),
		logger:              logger,
		shardedLocks:        common.NewDefaultShardedRWLocks(),
		maintenanceLock:     sync.RWMutex{},
		deletionInterval:    deletionInterval,
		allocChecker:        allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func (s *shardedMultipleLockCache) AllMultiple() [][][]float32 {
	return s.cache
}

func (s *shardedMultipleLockCache) All() [][]float32 {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) GetMultiple(ctx context.Context, docID uint64, vecID uint64) ([]float32, error) {
	s.shardedLocks.RLock(docID)
	vec := s.cache[docID][vecID]
	s.shardedLocks.RUnlock(docID)

	if vec != nil {
		return vec, nil
	}

	return s.handleMultipleCacheMiss(ctx, docID, vecID)
}

func (s *shardedMultipleLockCache) Delete(ctx context.Context, docID uint64) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) handleMultipleCacheMiss(ctx context.Context, docID uint64, vecID uint64) ([]float32, error) {
	if s.allocChecker != nil {
		// we don't really know the exact size here, but we don't have to be
		// accurate. If mem pressure is this high, we basically want to prevent any
		// new permanent heap alloc. If we underestimate the size of a vector a
		// bit, we might allow one more vector than we can really fit. But the one
		// after would fail then. Given that vectors are typically somewhat small
		// (in the single-digit KBs), it doesn't matter much, as long as we stop
		// allowing vectors when we're out of memory.
		estimatedSize := int64(1024)
		if err := s.allocChecker.CheckAlloc(estimatedSize); err != nil {
			s.logger.WithFields(logrus.Fields{
				"action": "vector_cache_miss",
				"event":  "vector_load_skipped_oom",
				"doc_id": docID,
				"vec_id": vecID,
			}).WithError(err).
				Warnf("cannot load vector into cache due to memory pressure")
			return nil, err
		}
	}

	vec, err := s.multipleVectorForID(ctx, docID, vecID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)
	s.shardedLocks.Lock(docID)
	s.cache[docID][vecID] = vec
	s.shardedLocks.Unlock(docID)

	return vec, nil
}

func (s *shardedMultipleLockCache) MultiGetMultiple(ctx context.Context, docIDs []uint64, vecIDs []uint64) ([][]float32, []error) {
	out := make([][]float32, len(vecIDs))
	errs := make([]error, len(vecIDs))

	for i, id := range vecIDs {
		s.shardedLocks.RLock(docIDs[i])
		vec := s.cache[docIDs[i]][id]
		s.shardedLocks.RUnlock(docIDs[i])

		if vec == nil {
			vecFromDisk, err := s.handleMultipleCacheMiss(ctx, docIDs[i], id)
			errs[i] = err
			vec = vecFromDisk
		}

		out[i] = vec
	}

	return out, errs
}

func (s *shardedMultipleLockCache) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *shardedMultipleLockCache) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

func (s *shardedMultipleLockCache) Prefetch(id uint64) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) PrefetchMultiple(docID uint64, vecID uint64) {
	s.shardedLocks.RLock(docID)
	defer s.shardedLocks.RUnlock(docID)

	prefetchFunc(uintptr(unsafe.Pointer(&s.cache[docID][vecID])))
}

func (s *shardedMultipleLockCache) PreloadMultiple(docID uint64, vecID uint64, vec []float32) {
	s.shardedLocks.Lock(docID)
	defer s.shardedLocks.Unlock(docID)

	atomic.AddInt64(&s.count, 1)
	s.cache[docID][vecID] = vec
}

func (s *shardedMultipleLockCache) Preload(docID uint64, vec []float32) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) PreloadNoLockMultiple(docID uint64, vecID uint64, vec []float32) {
	s.cache[docID][vecID] = vec
}

func (s *shardedMultipleLockCache) PreloadNoLock(docID uint64, vec []float32) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) SetSizeAndGrowNoLock(size uint64) {
	atomic.StoreInt64(&s.count, int64(size))

	if size < uint64(len(s.cache)) {
		return
	}
	newSize := size + MinimumIndexGrowthDelta
	newCache := make([][][]float32, newSize)
	for i := range newCache {
		newCache[i] = make([][]float32, RelativeInitialSize)
	}
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedMultipleLockCache) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < uint64(len(s.cache)) {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(s.cache)) {
		return
	}

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	newSize := node + MinimumIndexGrowthDelta
	newCache := make([][][]float32, newSize)
	for i := range s.cache {
		newCache[i] = make([][]float32, RelativeInitialSize)
	}
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedMultipleLockCache) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(len(s.cache))
}

func (s *shardedMultipleLockCache) CountVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *shardedMultipleLockCache) Drop() {
	s.deleteAllVectors()
	if s.deletionInterval != 0 {
		s.cancel <- true
	}
}

func (s *shardedMultipleLockCache) deleteAllVectors() {
	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.cache {
		s.cache[i] = make([][]float32, RelativeInitialSize)
	}

	atomic.StoreInt64(&s.count, 0)
}

func (s *shardedMultipleLockCache) watchForDeletion() {
	if s.deletionInterval != 0 {
		f := func() {
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
		}
		enterrors.GoWrapper(f, s.logger)
	}
}

func (s *shardedMultipleLockCache) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.deleteAllVectors()
	}
}

func (s *shardedMultipleLockCache) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

func (s *shardedMultipleLockCache) CopyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&s.maxSize)
	return sizeCopy
}

func (s *shardedMultipleLockCache) Get(ctx context.Context, vecID uint64) ([]float32, error) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache) MultiGet(ctx context.Context, ids []uint64) ([][]float32, []error) {
	panic("not implemented")
}
