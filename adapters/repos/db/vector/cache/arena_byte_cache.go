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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	data []byte
	live []uint64 // atomic access only; arenaChunkSize/64 words
}

func (c *arenaChunk) isLive(slot uint64) bool {
	word := atomic.LoadUint64(&c.live[slot>>6])
	return word&(1<<(slot&63)) != 0
}

// setLive marks slot live and returns whether it was dead before.
func (c *arenaChunk) setLive(slot uint64) bool {
	old := atomic.OrUint64(&c.live[slot>>6], 1<<(slot&63))
	return old&(1<<(slot&63)) == 0
}

// clearLive marks slot dead and returns whether it was live before.
func (c *arenaChunk) clearLive(slot uint64) bool {
	old := atomic.AndUint64(&c.live[slot>>6], ^uint64(1<<(slot&63)))
	return old&(1<<(slot&63)) != 0
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
// same growth targets, the same occupied-slot counting (every write path
// moves the count only when a slot flips between empty and occupied, so
// overwrites and repeated misses on the same id cannot inflate it; see
// storeLocked in the sharded cache), the same panics on the multi-vector
// methods. Two deliberate divergences, both consequences of the
// fixed-stride layout:
//
//   - Records must have exactly the construction-time size. A record of any
//     other length is refused, never stored: the miss path returns an
//     error, the preload paths log and skip it (the map-backed cache would
//     silently accept it).
//   - A slice returned by Get/PrefetchGet/All aliases arena memory: it
//     remains stable across Delete (bytes are not scrubbed). Writes never
//     touch record bytes that already hold the incoming code (see
//     storeRecord), so concurrent misses, prefill and re-preloads of an
//     id's unchanged code cannot race with readers of an earlier view. Only
//     preloading DIFFERENT bytes for an id overwrites in place; the
//     map-backed cache kept old views immutable because it swapped
//     pointers. No production caller changes the code of an id (vector
//     updates allocate a new doc id), so hot paths never observe the
//     difference.
//
// Chunk reclamation is intentionally out of scope: memory follows the
// highest id range touched since startup (or the last Drop/wipe), not the
// number of live records. Occupancy and the node-level
// weaviate_vector_cache_arena_{slab_bytes,live_vectors} gauges make that
// gap observable.
type arenaByteCache struct {
	shardedLocks *common.LazyShardedRWLocks
	// maintenanceLock serializes growth against itself, exactly like the
	// map-backed cache.
	maintenanceLock sync.RWMutex

	// chunkInstallLock serializes chunk allocation, so writers racing for
	// the same empty chunk allocate it once, and the allocation runs outside
	// every stripe lock. Lock order: chunkInstallLock before any stripe.
	chunkInstallLock sync.Mutex

	// table is a coherent (chunks, logicalLen) snapshot republished on every
	// table swap. All traverses it without taking any lock, so it can neither
	// deadlock against a caller holding LockAll nor index a table that Drop
	// or a grow swapped out underneath it.
	table atomic.Pointer[arenaTable]

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

	// dropped tells a dropped cache apart from a fresh one, exactly like
	// shardedLockCache.dropped: reads still pass through to vectorForID, but
	// nothing reallocates the chunk table.
	dropped atomic.Bool

	// slabBytes is the memory held by installed chunks (record area,
	// alignment slack, liveness bitmap). Together with count it is this
	// cache's share of the node-level occupancy gauges, which may be nil
	// when metrics are unavailable.
	slabBytes        atomic.Int64
	slabBytesGauge   prometheus.Gauge
	liveVectorsGauge prometheus.Gauge
}

// ArenaOccupancy reports how much memory an arena cache holds against how
// many records are live in it. Without chunk reclamation the two drift
// apart under delete/update churn; SlabBytes/LiveVectors far above the
// stride is the signal.
type ArenaOccupancy struct {
	SlabBytes   int64
	LiveVectors int64
}

// Occupancy returns the cache's current slab bytes and live record count.
func (s *arenaByteCache) Occupancy() ArenaOccupancy {
	return ArenaOccupancy{
		SlabBytes:   s.slabBytes.Load(),
		LiveVectors: atomic.LoadInt64(&s.count),
	}
}

func (s *arenaByteCache) addSlabBytes(delta int64) {
	s.slabBytes.Add(delta)
	if s.slabBytesGauge != nil {
		s.slabBytesGauge.Add(float64(delta))
	}
}

func (s *arenaByteCache) addLive(delta int64) {
	atomic.AddInt64(&s.count, delta)
	if s.liveVectorsGauge != nil {
		s.liveVectorsGauge.Add(float64(delta))
	}
}

// resetLive zeroes the live count (wipe, Drop) and takes it off the gauge.
func (s *arenaByteCache) resetLive() {
	if old := atomic.SwapInt64(&s.count, 0); old != 0 && s.liveVectorsGauge != nil {
		s.liveVectorsGauge.Sub(float64(old))
	}
}

// releaseChunks takes the footprint of every installed chunk in chunks off
// the slab accounting. Caller holds every stripe, so no install races it.
func (s *arenaByteCache) releaseChunks(chunks []*arenaChunk) {
	var n int64
	for i := range chunks {
		if loadChunk(chunks, uint64(i)) != nil {
			n++
		}
	}
	if n != 0 {
		s.addSlabBytes(-n * s.chunkFootprint())
	}
}

// arenaTable is an immutable view of the chunk table and the length it
// covers. Slots in chunks are still loaded atomically: installChunk and
// deleteAllVectors write them in place.
type arenaTable struct {
	chunks []*arenaChunk
	length uint64
}

var _ IfAbsentPreloader[byte] = (*arenaByteCache)(nil)

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
		cancel:           make(chan bool),
		logger:           logger,
		maxSize:          maxSizeOrDefault(int64(maxSize)),
		shardedLocks:     common.NewLazyShardedRWLocks(initialShardedLocksCount, pageSize),
		deletionInterval: deletionInterval,
		allocChecker:     allocChecker,
		prefetchBytes:    compressedPrefetchMaxBytes,
	}
	if m := monitoring.GetMetrics(); m != nil {
		vc.slabBytesGauge = m.VectorCacheArenaSlabBytes
		vc.liveVectorsGauge = m.VectorCacheArenaLiveVectors
	}
	vc.table.Store(&arenaTable{})
	vc.watchForDeletion()
	return vc, nil
}

// publishTable republishes the lock-free snapshot read by All. Caller must
// exclude concurrent table swaps (all stripes held, or NoLock ownership).
func (s *arenaByteCache) publishTable() {
	s.table.Store(&arenaTable{chunks: s.chunks, length: s.logicalLen})
}

func loadChunk(chunks []*arenaChunk, ci uint64) *arenaChunk {
	return (*arenaChunk)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&chunks[ci]))))
}

// chunkAt atomically loads the chunk pointer at slot ci of the current
// table. Atomic because a concurrent installChunk under a different lock
// stripe may be publishing the same slot.
func (s *arenaByteCache) chunkAt(ci uint64) *arenaChunk {
	return loadChunk(s.chunks, ci)
}

// installChunk publishes ch at slot ci unless another writer got there
// first, and returns the winning chunk.
func (s *arenaByteCache) installChunk(ci uint64, ch *arenaChunk) *arenaChunk {
	if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&s.chunks[ci])),
		nil, unsafe.Pointer(ch)) {
		s.addSlabBytes(s.chunkFootprint())
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
// allocated or the record is dead. Caller must hold the id's stripe lock.
func (s *arenaByteCache) liveView(id uint64) []byte {
	return s.liveViewIn(s.chunks, id)
}

// liveViewIn is liveView over an explicit table, so All can read a
// published snapshot without holding a stripe lock.
func (s *arenaByteCache) liveViewIn(chunks []*arenaChunk, id uint64) []byte {
	ch := loadChunk(chunks, id>>arenaChunkShift)
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

// checkRecordSize rejects records the fixed-stride arena cannot represent.
// Refusing beats storing a truncated code and returning corrupt distances
// later; it is an error rather than a panic so that one bad record (e.g. a
// code persisted by a different layout) cannot abort prefill or crash every
// search that reaches its id.
func (s *arenaByteCache) checkRecordSize(id uint64, vec []byte) error {
	if len(vec) != s.recordSize {
		return fmt.Errorf("arena cache: record for id %d has %d bytes, cache is fixed at %d",
			id, len(vec), s.recordSize)
	}
	return nil
}

// skipBadRecord logs a record refused by checkRecordSize on a preload path,
// which has no error return; the id is then served through the miss path.
func (s *arenaByteCache) skipBadRecord(err error) {
	if s.logger != nil {
		s.logger.WithField("action", "vector_cache_preload").
			Warnf("skipping record: %v", err)
	}
}

// chunkMissing reports whether id is covered but its chunk is not yet
// allocated.
func (s *arenaByteCache) chunkMissing(id uint64) bool {
	s.shardedLocks.RLock(id)
	defer s.shardedLocks.RUnlock(id)
	return id < s.logicalLen && s.chunkAt(id>>arenaChunkShift) == nil
}

// ensureChunk allocates id's chunk if it is covered but missing, outside
// every stripe lock and at most once per chunk (chunkInstallLock), so
// readers on the id's stripe never wait on a multi-megabyte allocation and
// racing writers don't each allocate a chunk only to discard all but one.
// storeRecord keeps a fallback install for the rare case that the chunk
// disappears again (wipe) before the caller takes its stripe lock.
func (s *arenaByteCache) ensureChunk(id uint64) {
	if !s.chunkMissing(id) {
		return
	}
	s.chunkInstallLock.Lock()
	defer s.chunkInstallLock.Unlock()
	if !s.chunkMissing(id) {
		return
	}
	ch := s.newArenaChunk()
	s.shardedLocks.RLock(id)
	// re-checked under the stripe: a grow or Drop may have swapped the
	// table since chunkMissing, and holding the stripe pins it
	if id < s.logicalLen {
		s.installChunk(id>>arenaChunkShift, ch)
	}
	s.shardedLocks.RUnlock(id)
}

// chunkFootprint is the allocation charged when a miss has to create a
// chunk: the record area, its alignment slack and the liveness bitmap.
func (s *arenaByteCache) chunkFootprint() int64 {
	return int64(arenaChunkSize*s.stride + arenaBaseAlign + arenaChunkSize/8)
}

// storeRecord writes vec into id's record and marks it live, moving the
// count only when the slot flips from dead to live — the arena's analogue
// of the sharded cache's storeLocked occupancy delta (count feeds
// replaceIfFull, which wipes the whole cache when it believes the cache is
// full). Caller must hold the id's stripe lock (or be the
// caller-synchronized NoLock path), have ensured coverage (id <
// logicalLen), and have validated the record size via checkRecordSize.
//
// Bytes are copied only when they differ from what the record already
// holds. Views of a record are read outside the stripe lock, so writing
// even identical bytes over one would race with those readers; comparing
// is a read, which never races with them. Since an id's code doesn't
// change, every repeated store (overlapping misses, prefill after a miss,
// re-preload, re-fill after Delete) is write-free.
func (s *arenaByteCache) storeRecord(id uint64, vec []byte) {
	ci := id >> arenaChunkShift
	ch := s.chunkAt(ci)
	if ch == nil {
		ch = s.installChunk(ci, s.newArenaChunk())
	}
	slot := id & arenaChunkMask
	off := int(slot) * s.stride
	if dst := ch.data[off : off+s.recordSize]; !bytes.Equal(dst, vec) {
		copy(dst, vec)
	}
	if ch.setLive(slot) {
		s.addLive(1)
	}
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

// handleCacheMiss mirrors the map-backed implementation: the count moves
// inside storeRecord, and only when the fetch actually fills an empty slot.
// It only fills an empty slot: a record another miss or preload published
// in the meantime is left alone. It returns the freshly fetched slice, not
// the arena view, so miss results are never aliased to arena memory.
func (s *arenaByteCache) handleCacheMiss(ctx context.Context, id uint64) ([]byte, error) {
	// a dropped cache keeps nothing, so memory pressure is no reason to refuse
	if s.allocChecker != nil && !s.dropped.Load() {
		// For a record in an allocated chunk the estimate mirrors the
		// map-backed cache: accuracy doesn't matter, only that permanent
		// allocations stop under memory pressure. A miss into a missing
		// chunk allocates the whole chunk, so it is charged in full.
		estimatedSize := int64(1024)
		if s.chunkMissing(id) {
			estimatedSize = s.chunkFootprint()
		}
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

	if len(vec) != 0 {
		if err := s.checkRecordSize(id, vec); err != nil {
			return nil, err
		}
		s.ensureChunk(id)
		s.shardedLocks.Lock(id)
		// Drop can release the chunk table before or while vectorForID runs
		if id < s.logicalLen && s.liveView(id) == nil {
			s.storeRecord(id, vec)
		}
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
		s.addLive(-1)
	}
}

func (s *arenaByteCache) Preload(id uint64, vec []byte) {
	if err := s.checkRecordSize(id, vec); err != nil {
		s.skipBadRecord(err)
		return
	}
	s.preload(id, vec, false)
}

// PreloadIfAbsent writes vec only when the slot is empty, so it cannot clobber a
// newer vector written concurrently. Reports whether it stored the vector.
func (s *arenaByteCache) PreloadIfAbsent(id uint64, vec []byte) bool {
	if len(vec) == 0 {
		// nothing to store, and refusing here avoids growing the cache to
		// cover id — mirrored from the map-backed cache, and checked before
		// the record-size check so an empty vec is refused without a warning
		return false
	}
	if err := s.checkRecordSize(id, vec); err != nil {
		s.skipBadRecord(err)
		return false
	}
	return s.preload(id, vec, true)
}

func (s *arenaByteCache) preload(id uint64, vec []byte, ifAbsent bool) bool {
	for {
		s.ensureChunk(id)
		s.shardedLocks.Lock(id)
		// reading logicalLen under a stripe lock is safe: it only changes
		// while all stripes are held (see grow).
		if id < s.logicalLen {
			if ifAbsent && s.liveView(id) != nil {
				s.shardedLocks.Unlock(id)
				return false
			}
			s.storeRecord(id, vec)
			s.shardedLocks.Unlock(id)
			return true
		}
		s.shardedLocks.Unlock(id)

		// Grow never covers id on a dropped cache, so looping would spin
		if s.dropped.Load() {
			return false
		}

		// slow path: the cache doesn't cover id yet — preloaders don't grow
		// the cache themselves. Grow guarantees coverage of id, so the next
		// iteration stores.
		s.Grow(id)
	}
}

// PreloadNoLock stores without locks; like every other write path it moves
// the count only when the slot was empty (the map-backed cache routes this
// through storeLocked). The caller owns synchronization and must have grown
// the cache to cover id; an uncovered id is skipped, mirroring the map-backed
// cache (SetSizeAndGrowNoLock leaves a dropped cache uncovered).
func (s *arenaByteCache) PreloadNoLock(id uint64, vec []byte) {
	if err := s.checkRecordSize(id, vec); err != nil {
		s.skipBadRecord(err)
		return
	}
	if id >= s.logicalLen {
		return
	}
	s.storeRecord(id, vec)
}

// SetSizeAndGrowNoLock mirrors the map-backed semantics: it only grows
// coverage to size — the count is never touched (it tracks occupied slots,
// which growing doesn't change) — and returns early when size is already
// covered. Caller owns synchronization.
func (s *arenaByteCache) SetSizeAndGrowNoLock(size uint64) {
	if s.dropped.Load() || size < s.logicalLen {
		return
	}
	newLen := growTargetFor(size)
	s.growChunkTable(newLen)
	s.logicalLen = newLen
	s.publishTable()
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
	if s.dropped.Load() || node < s.logicalLen {
		s.maintenanceLock.RUnlock()
		return
	}
	s.maintenanceLock.RUnlock()

	s.maintenanceLock.Lock()
	defer s.maintenanceLock.Unlock()

	// re-check: it could have grown or been dropped while waiting for the
	// maintenance lock
	if s.dropped.Load() || node < s.logicalLen {
		return
	}

	newLen := growTargetFor(node)
	s.shardedLocks.EnsureCount(stripeCountFor(newLen))

	s.shardedLocks.LockAll()
	defer s.shardedLocks.UnlockAll()

	s.growChunkTable(newLen)
	s.logicalLen = newLen
	s.publishTable()
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

// All materializes one view per covered id, nil for missing records. Like
// the map-backed cache it takes no lock — callers may already hold LockAll,
// and a grow waiting for those stripes while holding maintenanceLock would
// otherwise deadlock — so it reads the published table snapshot: a
// concurrent grow, wipe or Drop yields a stale or partially cleared result,
// never an out-of-range index. Best-effort, as with the map-backed cache.
func (s *arenaByteCache) All() [][]byte {
	t := s.table.Load()
	out := make([][]byte, t.length)
	for id := uint64(0); id < t.length; id++ {
		out[id] = s.liveViewIn(t.chunks, id)
	}
	return out
}

func (s *arenaByteCache) LockAll() {
	s.shardedLocks.LockAll()
}

func (s *arenaByteCache) UnlockAll() {
	s.shardedLocks.UnlockAll()
}

// Drop releases the chunk table along with the chunks, since even an empty
// table costs a pointer per 4096 ids. Later reads pass through to vectorForID
// without reallocating it — the arena mirror of shardedLockCache.Drop.
func (s *arenaByteCache) Drop() {
	// shutdown and an in-flight compression can both drop the cache; the
	// watcher exits on the first notification, so a second send would block
	if !s.dropped.CompareAndSwap(false, true) {
		return
	}
	s.maintenanceLock.Lock()
	s.shardedLocks.LockAll()
	s.releaseChunks(s.chunks)
	s.chunks = nil
	s.logicalLen = 0
	s.publishTable()
	s.resetLive()
	s.shardedLocks.UnlockAll()
	s.maintenanceLock.Unlock()

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
	s.releaseChunks(s.chunks)
	for i := range s.chunks {
		// atomic although every stripe is held: All reads the slots of the
		// published table without any lock
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.chunks[i])), nil)
	}
	s.resetLive()
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
	atomic.StoreInt64(&s.maxSize, maxSizeOrDefault(size))
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
