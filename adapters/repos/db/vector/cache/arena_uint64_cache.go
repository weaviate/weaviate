//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cache

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// arenaUint64Cache adapts the byte arena to Cache[uint64] for quantizers
// whose codes are word-based (1-bit RQ, BQ). Records are reinterpreted, not
// copied: a []uint64 view over an arena record is valid because records are
// 64-byte aligned and the stride is a multiple of 64, and because the
// reinterpretation happens in-process (no endianness concern). The
// underlying arena enforces the byte-level record size, so word-level
// callers get the same fixed-size guarantee.
type arenaUint64Cache struct {
	inner *arenaByteCache
	words int
}

// NewArenaUint64Cache constructs an arena-backed Cache[uint64] for codes of
// exactly recordWords words. Parameters mirror NewShardedUInt64LockCache.
func NewArenaUint64Cache(vecForID common.VectorForID[uint64], recordWords int, maxSize int,
	pageSize uint64, logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) (Cache[uint64], error) {
	if recordWords <= 0 {
		return nil, fmt.Errorf("arena cache: record words must be positive, got %d", recordWords)
	}
	byteVecForID := func(ctx context.Context, id uint64) ([]byte, error) {
		vec, err := vecForID(ctx, id)
		if err != nil {
			return nil, err
		}
		return u64AsBytes(vec), nil
	}
	inner, err := NewArenaByteCache(byteVecForID, recordWords*8, maxSize, pageSize,
		logger, deletionInterval, allocChecker)
	if err != nil {
		return nil, err
	}
	return &arenaUint64Cache{inner: inner.(*arenaByteCache), words: recordWords}, nil
}

// u64AsBytes reinterprets a word slice as its underlying bytes. nil-safe.
func u64AsBytes(v []uint64) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*8)
}

// bytesAsU64 reinterprets a byte slice as words. The slice must be 8-byte
// aligned and a multiple of 8 long; arena records and round-tripped fetch
// results always are. nil-safe.
func bytesAsU64(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), len(b)/8)
}

func (s *arenaUint64Cache) Get(ctx context.Context, id uint64) ([]uint64, error) {
	vec, err := s.inner.Get(ctx, id)
	return bytesAsU64(vec), err
}

func (s *arenaUint64Cache) MultiGet(ctx context.Context, ids []uint64) ([][]uint64, []error) {
	vecs, errs := s.inner.MultiGet(ctx, ids)
	out := make([][]uint64, len(vecs))
	for i, v := range vecs {
		out[i] = bytesAsU64(v)
	}
	return out, errs
}

func (s *arenaUint64Cache) GetAllInCurrentLock(ctx context.Context, id uint64, out [][]uint64, errs []error) ([][]uint64, []error, uint64, uint64) {
	byteOut := make([][]byte, len(out))
	byteOut, errs, start, end := s.inner.GetAllInCurrentLock(ctx, id, byteOut, errs)
	for i, v := range byteOut {
		out[i] = bytesAsU64(v)
	}
	return out, errs, start, end
}

func (s *arenaUint64Cache) Prefetch(id uint64) {
	s.inner.Prefetch(id)
}

func (s *arenaUint64Cache) PrefetchGet(id uint64) []uint64 {
	return bytesAsU64(s.inner.PrefetchGet(id))
}

func (s *arenaUint64Cache) Preload(id uint64, vec []uint64) {
	s.inner.Preload(id, u64AsBytes(vec))
}

func (s *arenaUint64Cache) PreloadNoLock(id uint64, vec []uint64) {
	s.inner.PreloadNoLock(id, u64AsBytes(vec))
}

func (s *arenaUint64Cache) SetSizeAndGrowNoLock(size uint64) {
	s.inner.SetSizeAndGrowNoLock(size)
}

func (s *arenaUint64Cache) Delete(ctx context.Context, id uint64) {
	s.inner.Delete(ctx, id)
}

func (s *arenaUint64Cache) Grow(size uint64) {
	s.inner.Grow(size)
}

func (s *arenaUint64Cache) Len() int32 {
	return s.inner.Len()
}

func (s *arenaUint64Cache) CountVectors() int64 {
	return s.inner.CountVectors()
}

func (s *arenaUint64Cache) PageSize() uint64 {
	return s.inner.PageSize()
}

func (s *arenaUint64Cache) All() [][]uint64 {
	vecs := s.inner.All()
	out := make([][]uint64, len(vecs))
	for i, v := range vecs {
		out[i] = bytesAsU64(v)
	}
	return out
}

func (s *arenaUint64Cache) LockAll() {
	s.inner.LockAll()
}

func (s *arenaUint64Cache) UnlockAll() {
	s.inner.UnlockAll()
}

func (s *arenaUint64Cache) Drop() {
	s.inner.Drop()
}

func (s *arenaUint64Cache) UpdateMaxSize(size int64) {
	s.inner.UpdateMaxSize(size)
}

func (s *arenaUint64Cache) CopyMaxSize() int64 {
	return s.inner.CopyMaxSize()
}

func (s *arenaUint64Cache) GetDoc(ctx context.Context, docID uint64) ([][]float32, error) {
	return s.inner.GetDoc(ctx, docID)
}

func (s *arenaUint64Cache) GetKeys(id uint64) (uint64, uint64) {
	panic("not implemented")
}

func (s *arenaUint64Cache) SetKeys(id uint64, docID uint64, relativeID uint64) {
	panic("not implemented")
}

func (s *arenaUint64Cache) PreloadMulti(docID uint64, ids []uint64, vecs [][]uint64) {
	panic("not implemented")
}

func (s *arenaUint64Cache) PreloadPassage(id uint64, docID uint64, relativeID uint64, vec []uint64) {
	panic("not implemented")
}
