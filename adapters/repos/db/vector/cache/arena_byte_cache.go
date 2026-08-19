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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

const (
	// arenaChunkShift sets the chunk capacity: 4096 records per chunk. At a
	// 128-byte stride that is a 512 KiB allocation — large enough that the
	// chunk count stays tiny (245 chunks per million records), small enough
	// that a sparsely used cache doesn't overcommit.
	arenaChunkShift = 12
	arenaChunkSize  = 1 << arenaChunkShift
	arenaChunkMask  = arenaChunkSize - 1

	// arenaAlign is the stride quantum: record strides are rounded up to a
	// multiple of 64 (the x86 cache line). arenaBaseAlign is the chunk base
	// alignment: 128 (the Apple M-series line), so a record's address modulo
	// either line size equals its in-chunk offset modulo that size.
	//
	// The resulting invariant: on x86 every record starts exactly on a
	// 64-byte line. On M-series, strides that are multiples of 128 put every
	// record at offset 0 mod 128 (fully line-aligned); strides that are odd
	// multiples of 64 alternate record offsets between 0 and 64 mod 128, and
	// an offset-64 record spans one extra 128-byte line only when its size
	// modulo 128 exceeds 64. The current odd-64 strides are the 8-bit RQ
	// records (784 B and 1552 B, both ≡ 16 mod 128), which pay no extra
	// line; rounding their strides up to 128 would cost ~4% memory for zero
	// line savings, so strides deliberately stay 64-quantized.
	arenaAlign     = 64
	arenaBaseAlign = 128
)

// arenaChunk is one fixed-capacity block of records. The data slice is
// 128-byte aligned (arenaBaseAlign) and never reallocated, so record views
// handed out to readers stay valid for the lifetime of the cache. Liveness is a bitmap
// (one bit per record) accessed atomically: neighbouring records within one
// bitmap word can belong to different lock stripes, so plain reads/writes
// would race.
type arenaChunk struct {
	data      []byte
	live      []uint64 // atomic access only; arenaChunkSize/64 words
	liveCount atomic.Int64
}

func (c *arenaChunk) isLive(slot uint64) bool {
	word := atomic.LoadUint64(&c.live[slot>>6])
	return word&(1<<(slot&63)) != 0
}

// setLive marks slot live and returns whether it was dead before.
func (c *arenaChunk) setLive(slot uint64) bool {
	old := atomic.OrUint64(&c.live[slot>>6], 1<<(slot&63))
	wasDead := old&(1<<(slot&63)) == 0
	if wasDead {
		c.liveCount.Add(1)
	}
	return wasDead
}

// clearLive marks slot dead and returns whether it was live before.
func (c *arenaChunk) clearLive(slot uint64) bool {
	old := atomic.AndUint64(&c.live[slot>>6], ^uint64(1<<(slot&63)))
	wasLive := old&(1<<(slot&63)) != 0
	if wasLive {
		c.liveCount.Add(-1)
	}
	return wasLive
}

// arenaByteCache is a Cache[byte] that stores fixed-size compressed vector
// codes in chunked arenas instead of one heap allocation per code. Records
// are padded to a stride that is a multiple of 64 and chunk bases are
// 128-byte aligned, so every record is exactly line-aligned on x86 and the
// per-config M-series line counts follow from the offset invariant on
// arenaAlign/arenaBaseAlign (no extra line for any current record size).
// The garbage collector sees one pointer per 4096 records instead of one
// per record.
//
// Semantics mirror shardedLockCache[byte] operation for operation — the
// same growth targets, the same counting quirks (Preload increments the
// count even when overwriting, PreloadNoLock does not increment it at all,
// a cache miss increments it even when the fetched vector is nil), the same
// panics on the multi-vector methods. Two deliberate divergences, both
// consequences of the fixed-stride layout:
//
//   - Records must have exactly the construction-time size; storing any
//     other length panics (the map-backed cache would silently accept it).
//   - A slice returned by Get/PrefetchGet/All aliases arena memory: it
//     remains stable across Delete (bytes are not scrubbed) but is
//     overwritten in place if the same id is preloaded again. The
//     map-backed cache kept old views immutable forever because it swapped
//     pointers. No production caller re-preloads a live id without
//     tombstoning it first, and cache-miss loads return the freshly fetched
//     slice rather than the arena view, so hot paths never observe the
//     difference.
//
// Chunk reclamation is intentionally out of scope: the per-chunk live
// counter exists so a later change can reclaim or compact chunks without a
// format change, but doing so interacts with id reuse and is not attempted
// here.
type arenaByteCache struct {
	shardedLocks *common.LazyShardedRWLocks
	// maintenanceLock serializes growth against itself, exactly like the
	// map-backed cache.
	maintenanceLock sync.RWMutex

	// chunks is swapped only while every lock stripe is held exclusively
	// (see grow), so holding any stripe pins the current slice. Individual
	// slots are read and installed atomically via chunkAt/installChunk:
	// writers under different stripes can race on the same slot when their
	// ids land in the same, not yet allocated chunk.
	chunks []*arenaChunk

	// logicalLen mirrors len(shardedLockCache.cache): ids below it are
	// covered, ids at or above it take the grow-and-miss path. Read under a
	// stripe lock or the maintenance lock, written only under all stripes
	// (grow) or by the caller-synchronized NoLock path.
	logicalLen uint64

	recordSize int
	stride     int

	vectorForID            common.VectorForID[byte]
	multipleVectorForDocID common.VectorForID[[]float32]
	maxSize                int64
	count                  int64
	cancel                 chan bool
	logger                 logrus.FieldLogger
	deletionInterval       time.Duration
	allocChecker           memwatch.AllocChecker
	prefetchBytes          int
}

// NewArenaByteCache constructs an arena-backed Cache[byte] for codes of
// exactly recordSize bytes. It is not wired as a default anywhere; callers
// opt in per index. The parameters after recordSize match
// NewShardedByteLockCache.
func NewArenaByteCache(vecForID common.VectorForID[byte], recordSize int, maxSize int,
	pageSize uint64, logger logrus.FieldLogger, deletionInterval time.Duration,
	allocChecker memwatch.AllocChecker,
) (Cache[byte], error) {
	if recordSize <= 0 {
		return nil, fmt.Errorf("arena cache: record size must be positive, got %d", recordSize)
	}
	stride := (recordSize + arenaAlign - 1) / arenaAlign * arenaAlign
	vc := &arenaByteCache{
		vectorForID:      vecForID,
		recordSize:       recordSize,
		stride:           stride,
		maxSize:          int64(maxSize),
		cancel:           make(chan bool),
		logger:           logger,
		shardedLocks:     common.NewLazyShardedRWLocks(initialShardedLocksCount, pageSize),
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
		prefetchBytes:    compressedPrefetchMaxBytes,
	}
	vc.watchForDeletion()
	return vc, nil
}

// chunkAt atomically loads the chunk pointer at slot ci of the current
// table. Atomic because a concurrent installChunk under a different lock
// stripe may be publishing the same slot.
func (s *arenaByteCache) chunkAt(ci uint64) *arenaChunk {
	return (*arenaChunk)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&s.chunks[ci]))))
}

// installChunk publishes ch at slot ci unless another writer got there
// first, and returns the winning chunk.
func (s *arenaByteCache) installChunk(ci uint64, ch *arenaChunk) *arenaChunk {
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&s.chunks[ci])),
		nil, unsafe.Pointer(ch)) {
		return ch
	}
	return s.chunkAt(ci)
}

// newArenaChunk allocates one chunk with its record area 128-byte aligned
// (arenaBaseAlign), which pins every record's offset modulo both cache-line
// sizes — see the invariant on the constants. Go's allocator aligns large
// allocations to 8 (and in practice often more) but guarantees neither 64
// nor 128, so the chunk is allocated with alignment slack and sliced at the
// aligned offset. The full backing array stays reachable through the
// subslice, so no separate reference is needed.
func (s *arenaByteCache) newArenaChunk() *arenaChunk {
	chunkBytes := arenaChunkSize * s.stride
	raw := make([]byte, chunkBytes+arenaBaseAlign)
	off := 0
	if rem := uintptr(unsafe.Pointer(&raw[0])) % arenaBaseAlign; rem != 0 {
		off = arenaBaseAlign - int(rem)
	}
	return &arenaChunk{
		data: raw[off : off+chunkBytes : off+chunkBytes],
		live: make([]uint64, arenaChunkSize/64),
	}
}

// liveView returns the record view for id, or nil if the chunk is not
// allocated or the record is dead. Caller must hold the id's stripe lock or
// otherwise accept a racy read (All does, mirroring the map-backed cache).
func (s *arenaByteCache) liveView(id uint64) []byte {
	ch := s.chunkAt(id >> arenaChunkShift)
	if ch == nil {
		return nil
	}
	slot := id & arenaChunkMask
	if !ch.isLive(slot) {
		return nil
	}
	off := int(slot) * s.stride
	return ch.data[off : off+s.recordSize : off+s.recordSize]
}

// checkRecordSize panics on records the fixed-stride arena cannot
// represent. It must run BEFORE any lock is taken: a panic escaping through
// a held stripe lock would deadlock the stripe for every later operation
// once recovered (e.g. by a test's assert.Panics or an HTTP handler).
// Failing loudly beats storing a truncated code and returning corrupt
// distances later.
func (s *arenaByteCache) checkRecordSize(id uint64, vec []byte) {
	if len(vec) != s.recordSize {
		panic(fmt.Sprintf("arena cache: record for id %d has %d bytes, cache is fixed at %d",
			id, len(vec), s.recordSize))
	}
}

// storeRecord copies vec into id's record and marks it live. Caller must
// hold the id's stripe lock (or be the caller-synchronized NoLock path),
// have ensured coverage (id < logicalLen), and have validated the record
// size via checkRecordSize.
func (s *arenaByteCache) storeRecord(id uint64, vec []byte) {
	ci := id >> arenaChunkShift
	ch := s.chunkAt(ci)
	if ch == nil {
		ch = s.installChunk(ci, s.newArenaChunk())
	}
	slot := id & arenaChunkMask
	off := int(slot) * s.stride
	copy(ch.data[off:off+s.recordSize], vec)
	ch.setLive(slot)
}

func (s *arenaByteCache) Get(ctx context.Context, id uint64) ([]byte, error) {
	s.shardedLocks.RLock(id)
	if id >= s.logicalLen {
		s.shardedLocks.RUnlock(id)
		s.Grow(id)
		return s.handleCacheMiss(ctx, id)
	}
	vec := s.liveView(id)
	s.shardedLocks.RUnlock(id)

	if vec != nil {
		return vec, nil
	}
	return s.handleCacheMiss(ctx, id)
}

// handleCacheMiss mirrors the map-backed implementation, including counting
// the fetch before the nil check. It returns the freshly fetched slice, not
// the arena view, so miss results are never aliased to arena memory.
func (s *arenaByteCache) handleCacheMiss(ctx context.Context, id uint64) ([]byte, error) {
	if s.allocChecker != nil {
		// The estimate mirrors the map-backed cache: accuracy doesn't matter
		// here, only that permanent allocations stop under memory pressure.
		estimatedSize := int64(1024)
		if err := s.allocChecker.CheckAlloc(estimatedSize); err != nil {
			s.logger.WithFields(logrus.Fields{
				"action": "vector_cache_miss",
				"event":  "vector_load_skipped_oom",
				"doc_id": id,
			}).Warnf("cannot load vector into cache due to memory pressure: %v", err)
			return nil, err
		}
	}

	vec, err := s.vectorForID(ctx, id)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.count, 1)

	if vec != nil {
		s.checkRecordSize(id, vec)
		s.shardedLocks.Lock(id)
		s.storeRecord(id, vec)
		s.shardedLocks.Unlock(id)
	}
	return vec, nil
}

func (s *arenaByteCache) MultiGet(ctx context.Context, ids []uint64) ([][]byte, []error) {
	out := make([][]byte, len(ids))
	var errs []error // only allocated on the first error, like the map-backed cache

	for i, id := range ids {
		var vec []byte
		s.shardedLocks.RLock(id)
		if id < s.logicalLen {
			vec = s.liveView(id)
		}
		s.shardedLocks.RUnlock(id)

		if vec == nil {
			s.Grow(id)
			vecFromDisk, err := s.handleCacheMiss(ctx, id)
			if err != nil {
				if errs == nil {
					errs = make([]error, len(ids))
				}
				errs[i] = err
			}
			vec = vecFromDisk
		}
		out[i] = vec
	}
	return out, errs
}

func (s *arenaByteCache) GetAllInCurrentLock(ctx context.Context, id uint64, out [][]byte, errs []error) ([][]byte, []error, uint64, uint64) {
	start := (id / s.shardedLocks.PageSize) * s.shardedLocks.PageSize
	end := start + s.shardedLocks.PageSize
	cacheMiss := false

	s.shardedLocks.RLock(start)
	if end > s.logicalLen {
		end = s.logicalLen
	}
	for i := start; i < end; i++ {
		vec := s.liveView(i)
		if vec == nil {
			cacheMiss = true
		}
		out[i-start] = vec
	}
	s.shardedLocks.RUnlock(start)

	// Mirrors the map-backed cache: misses are only backfilled when the max
	// size was altered from the effectively-unbounded default.
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

func (s *arenaByteCache) PageSize() uint64 {
	return s.shardedLocks.PageSize
}

func (s *arenaByteCache) Delete(ctx context.Context, id uint64) {
	s.shardedLocks.Lock(id)
	defer s.shardedLocks.Unlock(id)

	if id >= s.logicalLen {
		return
	}
	ch := s.chunkAt(id >> arenaChunkShift)
	if ch == nil {
		return
	}
	// Bytes are deliberately not scrubbed: views handed out before the
	// delete stay stable, matching the map-backed cache where the deleted
	// slice lived on in the reader's hands.
	if ch.clearLive(id & arenaChunkMask) {
		atomic.AddInt64(&s.count, -1)
	}
}

func (s *arenaByteCache) Preload(id uint64, vec []byte) {
	s.checkRecordSize(id, vec)
	for {
		s.shardedLocks.Lock(id)
		// reading logicalLen under a stripe lock is safe: it only changes
		// while all stripes are held (see grow).
		if id < s.logicalLen {
			s.storeRecord(id, vec)
			atomic.AddInt64(&s.count, 1)
			s.shardedLocks.Unlock(id)
			return
		}
		s.shardedLocks.Unlock(id)
		s.Grow(id)
	}
}

// PreloadNoLock stores without locks and, mirroring the map-backed cache,
// without touching the vector count; the caller owns synchronization and
// must have grown the cache to cover id (it panics otherwise, as the
// map-backed version panics on the out-of-range index).
func (s *arenaByteCache) PreloadNoLock(id uint64, vec []byte) {
	s.checkRecordSize(id, vec)
	if id >= s.logicalLen {
		panic(fmt.Sprintf("arena cache: PreloadNoLock id %d beyond cache length %d", id, s.logicalLen))
	}
	s.storeRecord(id, vec)
}

// SetSizeAndGrowNoLock mirrors the map-backed semantics: the count is
// overwritten with size (callers use it when rebuilding a cache from a
// snapshot), and coverage grows if needed. Caller owns synchronization.
func (s *arenaByteCache) SetSizeAndGrowNoLock(size uint64) {
	atomic.StoreInt64(&s.count, int64(size))

	if size < s.logicalLen {
		return
	}
	newLen := growTargetFor(size)
	s.growChunkTable(newLen)
	s.logicalLen = newLen
}

// growChunkTable extends the chunk pointer table to cover newLen records.
// Existing chunk pointers are carried over; the chunks themselves never
// move, which is what keeps previously returned record views valid across
// growth. Caller must exclude concurrent slot writers (all stripes held, or
// NoLock-path ownership).
func (s *arenaByteCache) growChunkTable(newLen uint64) {
	newCover := (newLen + arenaChunkSize - 1) >> arenaChunkShift
	if newCover <= uint64(len(s.chunks)) {
		return
	}
	newChunks := make([]*arenaChunk, newCover)
	copy(newChunks, s.chunks)
	s.chunks = newChunks
}

func (s *arenaByteCache) Grow(node uint64) {
	s.maintenanceLock.RLock()
	if node < s.logicalLen {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// re-check: it could have grown while waiting for the maintenance lock
	if node < s.logicalLen {
		return
	}

	newLen := growTargetFor(node)
	s.shardedLocks.EnsureCount(stripeCountFor(newLen))

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.growChunkTable(newLen)
	s.logicalLen = newLen
}

func (s *arenaByteCache) Len() int32 {
	s.maintenanceLock.RLock()
	defer s.maintenanceLock.RUnlock()

	return int32(s.logicalLen)
}

func (s *arenaByteCache) CountVectors() int64 {
	return atomic.LoadInt64(&s.count)
}

func (s *arenaByteCache) Prefetch(id uint64) {
	s.shardedLocks.RLock(id)
	if id >= s.logicalLen {
		s.shardedLocks.RUnlock(id)
		return
	}
	vec := s.liveView(id)
	s.shardedLocks.RUnlock(id)

	prefetchVector(vec, s.prefetchBytes)
}

func (s *arenaByteCache) PrefetchGet(id uint64) []byte {
	s.shardedLocks.RLock(id)
	if id >= s.logicalLen {
		s.shardedLocks.RUnlock(id)
		return nil
	}
	vec := s.liveView(id)
	s.shardedLocks.RUnlock(id)

	prefetchVector(vec, s.prefetchBytes)
	return vec
}

// All materializes one view per covered id, nil for missing records. The
// map-backed cache returns its backing slice without locks; this is equally
// racy by design and callers treat it as a best-effort snapshot.
func (s *arenaByteCache) All() [][]byte {
	s.maintenanceLock.RLock()
	length := s.logicalLen
	s.maintenanceLock.RUnlock()

	out := make([][]byte, length)
	for id := uint64(0); id < length; id++ {
		out[id] = s.liveView(id)
	}
	return out
}

func (s *arenaByteCache) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *arenaByteCache) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

func (s *arenaByteCache) Drop() {
	s.deleteAllVectors()
	if s.deletionInterval != 0 {
		s.cancel <- true
	}
}

// deleteAllVectors drops every chunk, releasing the arena memory while
// keeping the covered length, like the map-backed cache which nils entries
// but keeps its slice.
func (s *arenaByteCache) deleteAllVectors() {
	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.logger.WithField("action", "hnsw_delete_vector_cache").
		Debug("deleting full vector cache")
	for i := range s.chunks {
		// plain writes are safe here: slot writers hold a stripe lock and
		// all stripes are held
		s.chunks[i] = nil
	}
	atomic.StoreInt64(&s.count, 0)
}

func (s *arenaByteCache) watchForDeletion() {
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

func (s *arenaByteCache) replaceIfFull() {
	if atomic.LoadInt64(&s.count) >= atomic.LoadInt64(&s.maxSize) {
		s.deleteAllVectors()
	}
}

func (s *arenaByteCache) UpdateMaxSize(size int64) {
	atomic.StoreInt64(&s.maxSize, size)
}

func (s *arenaByteCache) CopyMaxSize() int64 {
	return atomic.LoadInt64(&s.maxSize)
}

func (s *arenaByteCache) GetDoc(ctx context.Context, docID uint64) ([][]float32, error) {
	return s.multipleVectorForDocID(ctx, docID)
}

func (s *arenaByteCache) GetKeys(id uint64) (uint64, uint64) {
	panic("not implemented")
}

func (s *arenaByteCache) SetKeys(id uint64, docID uint64, relativeID uint64) {
	panic("not implemented")
}

func (s *arenaByteCache) PreloadMulti(docID uint64, ids []uint64, vecs [][]byte) {
	panic("not implemented")
}

func (s *arenaByteCache) PreloadPassage(id uint64, docID uint64, relativeID uint64, vec []byte) {
	panic("not implemented")
}
