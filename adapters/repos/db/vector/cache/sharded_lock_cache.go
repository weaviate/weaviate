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

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type shardedLockCache[T float32 | byte | uint64] struct {
	shardedLocks           *common.ShardedRWLocks
	cache                  [][]T
	vectorForID            common.VectorForID[T]
	multipleVectorForDocID common.VectorForID[[]float32]
	normalizeOnRead        bool
	maxSize                int64
	count                  int64
	cancel                 chan bool
	logger                 logrus.FieldLogger
	deletionInterval       time.Duration
	allocChecker           memwatch.AllocChecker

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

const (
	InitialSize                = 1000
	RelativeInitialSize        = 100
	MinimumIndexGrowthDelta    = 2000
	MinimumRelativeGrowthDelta = 20
	indexGrowthRate            = 1.25
	defaultCacheMaxSize        = 1e12
)

func NewShardedFloat32LockCache(vecForID common.VectorForID[float32], multiVecForID common.VectorForID[[]float32], maxSize int, pageSize uint64,
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
		multipleVectorForDocID: multiVecForID,
		cache:                  make([][]float32, InitialSize),
		normalizeOnRead:        normalizeOnRead,
		count:                  0,
		maxSize:                int64(maxSize),
		cancel:                 make(chan bool),
		logger:                 logger,
		shardedLocks:           common.NewShardedRWLocksWithPageSize(pageSize),
		maintenanceLock:        sync.RWMutex{},
		deletionInterval:       deletionInterval,
		allocChecker:           allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedByteLockCache(vecForID common.VectorForID[byte], maxSize int, pageSize uint64,
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
		shardedLocks:     common.NewShardedRWLocksWithPageSize(pageSize),
		maintenanceLock:  sync.RWMutex{},
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
	}

	vc.watchForDeletion()
	return vc
}

func NewShardedUInt64LockCache(vecForID common.VectorForID[uint64], maxSize int, pageSize uint64,
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
		shardedLocks:     common.NewShardedRWLocksWithPageSize(pageSize),
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

	if vec != nil {
		s.shardedLocks.Lock(id)
		s.cache[id] = vec
		s.shardedLocks.Unlock(id)
	}

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

func (s *shardedLockCache[T]) GetAllInCurrentLock(ctx context.Context, id uint64, out [][]T, errs []error) ([][]T, []error, uint64, uint64) {
	start := (id / s.shardedLocks.PageSize) * s.shardedLocks.PageSize
	end := start + s.shardedLocks.PageSize
	cacheMiss := false

	if end > uint64(len(s.cache)) {
		end = uint64(len(s.cache))
	}

	s.shardedLocks.RLock(start)
	for i := start; i < end; i++ {
		vec := s.cache[i]
		if vec == nil {
			cacheMiss = true
		}
		out[i-start] = vec
	}
	s.shardedLocks.RUnlock(start)

	// We don't expect cache misses in general as the default cache size is very large (1e12).
	// Until the vector index cache is improved to handle nil vectors better, it makes sense here
	// to exclude handling cache misses unless the cache size has been altered.
	if cacheMiss && atomic.LoadInt64(&s.maxSize) != defaultCacheMaxSize {
		for i := start; i < end; i++ {
			if out[i-start] == nil {
				vecFromDisk, err := s.handleCacheMiss(ctx, i)
				errs[i-start] = err
				out[i-start] = vecFromDisk
			}
		}
	}

	return out, errs, start, end
}

func (s *shardedLockCache[T]) PageSize() uint64 {
	return s.shardedLocks.PageSize
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

func (s *shardedLockCache[T]) GetKeys(id uint64) (uint64, uint64) {
	panic("not implemented")
}

func (s *shardedLockCache[T]) PreloadMulti(docID uint64, ids []uint64, vecs [][]T) {
	panic("not implemented")
}

func (s *shardedLockCache[T]) SetKeys(id uint64, docID uint64, relativeID uint64) {
	panic("not implemented")
}

func (s *shardedLockCache[T]) PreloadPassage(id uint64, docID uint64, relativeID uint64, vec []T) {
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

type CacheKeys struct {
	DocID      uint64
	RelativeID uint64
}

type shardedMultipleLockCache[T float32 | uint64 | byte] struct {
	shardedLocks           *common.ShardedRWLocks
	cache                  [][]T
	multipleVectorForID    common.MultipleVectorForID[T]
	multipleVectorForDocID common.VectorForID[[]float32]
	normalizeOnRead        bool
	maxSize                int64
	count                  int64
	ctx                    context.Context
	cancelFn               func()
	logger                 logrus.FieldLogger
	deletionInterval       time.Duration
	allocChecker           memwatch.AllocChecker
	vectorDocID            []CacheKeys

	// The maintenanceLock makes sure that only one maintenance operation, such
	// as growing the cache or clearing the cache happens at the same time.
	maintenanceLock sync.RWMutex
}

func NewShardedMultiFloat32LockCache(multipleVecForID common.VectorForID[[]float32], maxSize int,
	logger logrus.FieldLogger, normalizeOnRead bool, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[float32] {
	multipleVecForIDValue := func(ctx context.Context, id uint64, relativeID uint64) ([]float32, error) {
		vecs, err := multipleVecForID(ctx, id)
		if err != nil {
			return nil, err
		}
		vec := vecs[relativeID]
		if normalizeOnRead {
			vec = distancer.Normalize(vec)
		}
		return vec, nil
	}

	cache := make([][]float32, InitialSize)

	vc := &shardedMultipleLockCache[float32]{
		multipleVectorForID:    multipleVecForIDValue,
		multipleVectorForDocID: multipleVecForID,
		cache:                  cache,
		normalizeOnRead:        normalizeOnRead,
		count:                  0,
		maxSize:                int64(maxSize),
		logger:                 logger,
		shardedLocks:           common.NewDefaultShardedRWLocks(),
		maintenanceLock:        sync.RWMutex{},
		deletionInterval:       deletionInterval,
		allocChecker:           allocChecker,
		vectorDocID:            make([]CacheKeys, InitialSize),
	}

	vc.ctx, vc.cancelFn = context.WithCancel(context.Background())

	vc.watchForDeletion()
	return vc
}

func NewShardedMultiUInt64LockCache(multipleVecForID common.VectorForID[uint64], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[uint64] {
	multipleVecForIDValue := func(ctx context.Context, id uint64, relativeID uint64) ([]uint64, error) {
		vec, err := multipleVecForID(ctx, id)
		if err != nil {
			return nil, err
		}
		return vec, nil
	}

	cache := make([][]uint64, InitialSize)

	vc := &shardedMultipleLockCache[uint64]{
		multipleVectorForID: multipleVecForIDValue,
		cache:               cache,
		count:               0,
		maxSize:             int64(maxSize),
		logger:              logger,
		shardedLocks:        common.NewDefaultShardedRWLocks(),
		maintenanceLock:     sync.RWMutex{},
		deletionInterval:    deletionInterval,
		allocChecker:        allocChecker,
		vectorDocID:         make([]CacheKeys, InitialSize),
	}

	vc.ctx, vc.cancelFn = context.WithCancel(context.Background())

	vc.watchForDeletion()
	return vc
}

func NewShardedMultiByteLockCache(multipleVecForID common.VectorForID[byte], maxSize int,
	logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) Cache[byte] {
	multipleVecForIDValue := func(ctx context.Context, id uint64, relativeID uint64) ([]byte, error) {
		vec, err := multipleVecForID(ctx, id)
		if err != nil {
			return nil, err
		}
		return vec, nil
	}

	cache := make([][]byte, InitialSize)

	vc := &shardedMultipleLockCache[byte]{
		multipleVectorForID: multipleVecForIDValue,
		cache:               cache,
		count:               0,
		maxSize:             int64(maxSize),
		logger:              logger,
		shardedLocks:        common.NewDefaultShardedRWLocks(),
		maintenanceLock:     sync.RWMutex{},
		deletionInterval:    deletionInterval,
		allocChecker:        allocChecker,
		vectorDocID:         make([]CacheKeys, InitialSize),
	}

	vc.ctx, vc.cancelFn = context.WithCancel(context.Background())

	vc.watchForDeletion()
	return vc
}

func (s *shardedMultipleLockCache[T]) All() [][]T {
	return s.cache
}

func (s *shardedMultipleLockCache[T]) GetKeys(id uint64) (uint64, uint64) {
	s.shardedLocks.RLock(id)
	keys := s.vectorDocID[id]
	s.shardedLocks.RUnlock(id)
	return keys.DocID, keys.RelativeID
}

func (s *shardedMultipleLockCache[T]) SetKeys(id uint64, docID uint64, relativeID uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	s.vectorDocID[id] = CacheKeys{DocID: docID, RelativeID: relativeID}
}

func (s *shardedMultipleLockCache[T]) GetKeysNoLock(id uint64) (uint64, uint64) {
	keys := s.vectorDocID[id]
	return keys.DocID, keys.RelativeID
}

func (s *shardedMultipleLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	s.shardedLocks.RLock(id)
	vec := s.cache[id]
	s.shardedLocks.RUnlock(id)

	if len(vec) == 0 {
		docID, relativeID := s.GetKeys(id)
		return s.handleMultipleCacheMiss(ctx, id, docID, relativeID)
	}

	return vec, nil
}

func (s *shardedLockCache[T]) GetDoc(ctx context.Context, docID uint64) ([][]float32, error) {
	return s.multipleVectorForDocID(ctx, docID)
}

func (s *shardedMultipleLockCache[T]) GetDoc(ctx context.Context, docID uint64) ([][]float32, error) {
	return s.multipleVectorForDocID(ctx, docID)
}

func (s *shardedMultipleLockCache[T]) MultiGet(ctx context.Context, ids []uint64) ([][]T, []error) {
	out := make([][]T, len(ids))
	errs := make([]error, len(ids))

	for i, id := range ids {

		s.shardedLocks.RLock(id)
		vec := s.cache[id]
		s.shardedLocks.RUnlock(id)
		if len(vec) == 0 {
			docID, relativeID := s.GetKeys(id)
			vec, errs[i] = s.handleMultipleCacheMiss(ctx, id, docID, relativeID)
		}

		out[i] = vec
	}

	return out, errs
}

func (s *shardedMultipleLockCache[T]) Delete(ctx context.Context, id uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	if int(id) >= len(s.cache) || len(s.cache[id]) == 0 {
		return
	}

	s.cache[id] = nil
	s.vectorDocID[id] = CacheKeys{}
	atomic.AddInt64(&s.count, -1)
}

func (s *shardedMultipleLockCache[T]) handleMultipleCacheMiss(ctx context.Context, id uint64, docID uint64, relativeID uint64) ([]T, error) {
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
				"vec_id": relativeID,
			}).WithError(err).
				Warnf("cannot load vector into cache due to memory pressure")
			return nil, err
		}
	}

	vec, err := s.multipleVectorForID(ctx, docID, relativeID)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)
	if len(vec) != 0 {
		s.shardedLocks.Lock(id)
		s.cache[id] = vec
		s.shardedLocks.Unlock(id)
	}

	return vec, nil
}

func (s *shardedMultipleLockCache[T]) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *shardedMultipleLockCache[T]) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

func (s *shardedMultipleLockCache[T]) Prefetch(id uint64) {
	s.shardedLocks.RLock(id)
	defer s.shardedLocks.RUnlock(id)

	prefetchFunc(uintptr(unsafe.Pointer(&s.cache[id])))
}

func (s *shardedMultipleLockCache[T]) PreloadMulti(docID uint64, ids []uint64, vecs [][]T) {
	atomic.AddInt64(&s.count, int64(len(ids)))
	for i, id := range ids {
		s.shardedLocks.Lock(id)
		s.cache[id] = vecs[i]
		s.vectorDocID[id] = CacheKeys{DocID: docID, RelativeID: uint64(i)}
		s.shardedLocks.Unlock(id)
	}
}

func (s *shardedMultipleLockCache[T]) PreloadPassage(id uint64, docID uint64, relativeID uint64, vec []T) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	s.cache[id] = vec
	s.vectorDocID[id] = CacheKeys{DocID: docID, RelativeID: relativeID}
	atomic.AddInt64(&s.count, int64(1))
}

func (s *shardedMultipleLockCache[T]) Preload(docID uint64, vec []T) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache[T]) PreloadNoLock(docID uint64, vec []T) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache[T]) SetSizeAndGrowNoLock(size uint64) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache[T]) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < uint64(len(s.vectorDocID)) {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// make sure cache still needs growing
	// (it could have grown while waiting for maintenance lock)
	if node < uint64(len(s.vectorDocID)) {
		return
	}

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	newSizeVector := node + MinimumIndexGrowthDelta
	newVectorDocID := make([]CacheKeys, newSizeVector)
	copy(newVectorDocID, s.vectorDocID)
	s.vectorDocID = newVectorDocID
	newCache := make([][]T, newSizeVector)
	copy(newCache, s.cache)
	s.cache = newCache
}

func (s *shardedMultipleLockCache[T]) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(len(s.cache))
}

func (s *shardedMultipleLockCache[T]) CountVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *shardedMultipleLockCache[T]) Drop() {
	s.deleteAllVectors()
	if s.deletionInterval != 0 {
		s.cancelFn()
	}
}

func (s *shardedMultipleLockCache[T]) deleteAllVectors() {
	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.cache {
		s.cache[i] = nil
		s.vectorDocID[i] = CacheKeys{}
	}

	atomic.StoreInt64(&s.count, 0)
}

func (s *shardedMultipleLockCache[T]) watchForDeletion() {
	if s.deletionInterval != 0 {
		f := func() {
			t := time.NewTicker(s.deletionInterval)
			defer t.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-t.C:
					s.replaceIfFull()
				}
			}
		}
		enterrors.GoWrapper(f, s.logger)
	}
}

func (s *shardedMultipleLockCache[T]) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.deleteAllVectors()
	}
}

func (s *shardedMultipleLockCache[T]) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

func (s *shardedMultipleLockCache[T]) CopyMaxSize() int64 {
	sizeCopy := atomic.LoadInt64(&s.maxSize)
	return sizeCopy
}

func (s *shardedMultipleLockCache[T]) GetAllInCurrentLock(ctx context.Context, id uint64, out [][]T, errs []error) ([][]T, []error, uint64, uint64) {
	panic("not implemented")
}

func (s *shardedMultipleLockCache[T]) PageSize() uint64 {
	panic("not implemented")
}
