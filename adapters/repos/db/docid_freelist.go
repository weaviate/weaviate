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

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	docIDFreeListFileName = "docid_freelist.bin"
	docIDFreeListVersion  = uint8(1)
)

var docIDFreeListMagic = [4]byte{'w', 'v', 'f', 'l'}

// shardDocIDFreeList tracks docIDs of deleted objects that may be reissued
// to new inserts once every index of the shard proved them clean and the
// async queues drained past the delete.
//
// Lifecycle of an id:
//
//	delete completes  -> RegisterCandidate (captures the drain watermark)
//	harvest cycle     -> gate passed + every index CleanForReuse -> free
//	                     (the free set is persisted BEFORE ids are published)
//	insert            -> Acquire (verified: no trace anywhere; fail-hard)
//	insert error      -> Return
//
// Everything is inert unless DOCID_REUSE_ENABLED is on. Losing state is
// always safe in exactly one direction: an id that is clean but absent from
// the free list merely leaks (is never reused); an id must never be present
// while dirty — that is what harvest ordering, load-time validation and the
// acquire-time assertion enforce.
type shardDocIDFreeList struct {
	shard  *Shard
	logger logrus.FieldLogger
	path   string

	mu sync.Mutex
	// free holds acquirable ids; persisted. LIFO to keep the working set hot.
	free []uint64
	// inFree mirrors free for O(1) membership (Return / duplicate guards).
	inFree map[uint64]struct{}
	// acquired holds ids handed to an in-flight insert. Deliberately NOT
	// persisted: after a crash the persisted file may still contain such an
	// id, and load-time validation decides its fate by looking at the actual
	// shard state.
	acquired map[uint64]struct{}
	// pending holds registered candidates awaiting harvest.
	pending map[uint64]*reuseCandidate
	// paused counts active reasons to suspend reuse (maintenance operations,
	// dynamic index upgrades). While >0, Acquire and harvest do nothing.
	paused int
}

// reuseCandidate is a deleted docID waiting for the drain gate: the set of
// queue chunk files that were pending when the delete completed. Ops of the
// old life can only live in those chunks (per-queue FIFO), so once none of
// the captured names remain, no old-life op can be applied anymore.
// Chunk names are wall-clock derived, hence a NAME SET rather than a
// high-water comparison — a clock regression cannot un-drain a set.
type reuseCandidate struct {
	watermark map[string]map[string]struct{} // queue ID -> chunk file names
}

// initDocIDFreeList creates the shard's free list, loads (and validates) its
// persisted state, hooks the harvest into the vector tombstone-cleanup cycle,
// and closes the reuse window around dynamic index upgrades. Must run after
// the LSM store, all vector indexes and all geo properties are initialized.
func (s *Shard) initDocIDFreeList() {
	s.freeList = newShardDocIDFreeList(s)
	s.freeList.Load()

	s.freeListCallbackCtrl = s.cycleCallbacks.vectorTombstoneCleanupCallbacks.Register(
		"docid_freelist/"+s.ID(), s.freeList.harvestCycle)

	// A dynamic index's flat→HNSW upgrade copies vectors while writes keep
	// flowing; reusing a docID during that window could put the new life's
	// vector into the old life's slot mid-copy. Close the shard-wide window
	// for the whole upgrade.
	_ = s.ForEachVectorIndex(func(_ string, idx VectorIndex) error {
		if h, ok := idx.(interface{ SetUpgradeHooks(start, done func()) }); ok {
			h.SetUpgradeHooks(s.freeList.Pause, s.freeList.Resume)
		}
		return nil
	})
}

func newShardDocIDFreeList(s *Shard) *shardDocIDFreeList {
	return &shardDocIDFreeList{
		shard:    s,
		logger:   s.index.logger,
		path:     filepath.Join(s.path(), docIDFreeListFileName),
		inFree:   map[uint64]struct{}{},
		acquired: map[uint64]struct{}{},
		pending:  map[uint64]*reuseCandidate{},
	}
}

// Pause suspends reuse (Acquire + harvest). Each Pause must be matched by a
// Resume; concurrent reasons stack. Maintenance operations that structurally
// assume monotone docIDs (RepairIndex sizes visited lists from the counter
// high-water mark, FillQueue scans checkpoint→counter, RequantizeIndex
// re-encodes by scan) hold a Pause for their whole run, as does a dynamic
// index's flat→HNSW upgrade window.
func (fl *shardDocIDFreeList) Pause() {
	if fl == nil {
		return
	}
	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.paused++
}

func (fl *shardDocIDFreeList) Resume() {
	if fl == nil {
		return
	}
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.paused > 0 {
		fl.paused--
	}
}

// RegisterCandidate records a completed delete's docID together with its
// drain watermark. Call AFTER the delete pushed its ops onto every queue.
func (fl *shardDocIDFreeList) RegisterCandidate(docID uint64) {
	if fl == nil || !docIDReuseEnabled() {
		return
	}

	watermark := map[string]map[string]struct{}{}
	capture := func(_ string, q *VectorIndexQueue) error {
		if q == nil || q.DiskQueue == nil {
			return nil
		}
		names, err := q.PendingChunkFiles()
		if err != nil {
			// Cannot establish a watermark: withhold the id forever rather
			// than guess (safe direction).
			return err
		}
		watermark[q.ID()] = names
		return nil
	}
	if err := fl.shard.ForEachVectorQueue(capture); err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: drop reuse candidate %d: capture queue watermark: %v", docID, err)
		return
	}
	if err := fl.shard.ForEachGeoQueue(capture); err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: drop reuse candidate %d: capture geo queue watermark: %v", docID, err)
		return
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()
	if _, ok := fl.inFree[docID]; ok {
		return
	}
	fl.pending[docID] = &reuseCandidate{watermark: watermark}
}

// Acquire hands out a reusable docID, verifying at this single point that
// the id has no trace anywhere in the shard: no object row and every index
// clean. A dirty id is a broken invariant — the insert fails hard and the id
// is withheld (never silently degraded to a fresh id).
func (fl *shardDocIDFreeList) Acquire() (uint64, bool, error) {
	if fl == nil || !docIDReuseEnabled() {
		return 0, false, nil
	}

	fl.mu.Lock()
	if fl.paused > 0 || len(fl.free) == 0 {
		fl.mu.Unlock()
		return 0, false, nil
	}
	docID := fl.free[len(fl.free)-1]
	fl.free = fl.free[:len(fl.free)-1]
	delete(fl.inFree, docID)
	fl.acquired[docID] = struct{}{}
	fl.mu.Unlock()

	if err := fl.assertNoTraces(docID); err != nil {
		fl.mu.Lock()
		delete(fl.acquired, docID)
		fl.mu.Unlock()
		return 0, false, fmt.Errorf("docID reuse invariant violation, refusing insert: %w", err)
	}
	return docID, true, nil
}

// Return puts an acquired id back after a failed insert.
func (fl *shardDocIDFreeList) Return(docID uint64) {
	if fl == nil {
		return
	}
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if _, ok := fl.acquired[docID]; !ok {
		return
	}
	delete(fl.acquired, docID)
	if _, ok := fl.inFree[docID]; ok {
		return
	}
	fl.free = append(fl.free, docID)
	fl.inFree[docID] = struct{}{}
}

// assertNoTraces is the acquire-time hard assertion: no object row under the
// docID secondary index, and every index reports CleanForReuse. The per-index
// check carries more than slot emptiness — HNSW, for example, also rejects an
// id that is still the index's entrypoint (a stranded entrypoint passes the
// row and slot checks but the graph still routes through it).
func (fl *shardDocIDFreeList) assertNoTraces(docID uint64) error {
	bucket, release, err := fl.shard.objectsBucket()
	if err != nil {
		return fmt.Errorf("objects bucket: %w", err)
	}
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	row, err := bucket.GetBySecondary(context.Background(),
		helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
	release()
	if err != nil {
		return fmt.Errorf("docID %d: secondary index lookup: %w", docID, err)
	}
	if row != nil {
		return fmt.Errorf("docID %d: an object row still references it", docID)
	}

	clean, dirtyIndex := fl.allIndexesClean(docID)
	if !clean {
		return fmt.Errorf("docID %d: index %q still holds a trace", docID, dirtyIndex)
	}
	return nil
}

// allIndexesClean reports whether EVERY index of the shard (all target
// vectors and all geo properties) implements the reuse surface and reports
// the id clean. An index without the surface makes the whole shard
// never-clean — this is the structural scope enforcement for v1.
func (fl *shardDocIDFreeList) allIndexesClean(docID uint64) (bool, string) {
	clean := true
	dirty := ""
	check := func(name string, idx interface{}) error {
		rc, ok := idx.(common.ReuseCleanliness)
		if !ok || !rc.CleanForReuse(docID) {
			clean = false
			dirty = name
			return fmt.Errorf("not clean")
		}
		return nil
	}
	if err := fl.shard.ForEachVectorIndex(func(target string, idx VectorIndex) error {
		return check("vector:"+target, idx)
	}); err != nil {
		return false, dirty
	}
	if err := fl.shard.ForEachGeoIndex(func(prop string, idx *geo.Index) error {
		return check("geo:"+prop, idx)
	}); err != nil {
		return false, dirty
	}
	return clean, dirty
}

// harvestCycle runs on the shard's tombstone-cleanup cycle: it promotes
// pending candidates whose drain gate passed and whose id every index
// reports clean, persists the enlarged free set FIRST, and only then
// publishes the ids for acquisition. A crash between index cleanup and
// persist leaves the id un-reusable — the safe direction.
func (fl *shardDocIDFreeList) harvestCycle(shouldAbort cyclemanager.ShouldAbortCallback) bool {
	if fl == nil || !docIDReuseEnabled() {
		return false
	}

	fl.mu.Lock()
	if fl.paused > 0 || len(fl.pending) == 0 {
		fl.mu.Unlock()
		return false
	}
	candidates := make(map[uint64]*reuseCandidate, len(fl.pending))
	for id, c := range fl.pending {
		candidates[id] = c
	}
	fl.mu.Unlock()

	// Current pending chunk names per queue, for the drain gate.
	current := map[string]map[string]struct{}{}
	capture := func(_ string, q *VectorIndexQueue) error {
		if q == nil || q.DiskQueue == nil {
			return nil
		}
		names, err := q.PendingChunkFiles()
		if err != nil {
			return err
		}
		current[q.ID()] = names
		return nil
	}
	if err := fl.shard.ForEachVectorQueue(capture); err != nil {
		return false
	}
	if err := fl.shard.ForEachGeoQueue(capture); err != nil {
		return false
	}

	var passers []uint64
	for id, cand := range candidates {
		if shouldAbort() {
			break
		}
		if !cand.drained(current) {
			continue
		}
		if clean, _ := fl.allIndexesClean(id); !clean {
			continue
		}
		passers = append(passers, id)
	}
	if len(passers) == 0 {
		return false
	}

	// Make the index-side deletions at least as durable as the free list
	// we are about to persist (commit-log buffers etc.).
	if err := fl.shard.ForEachVectorIndex(func(_ string, idx VectorIndex) error {
		return idx.Flush()
	}); err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: flush vector indexes before persist: %v", err)
		return false
	}
	if err := fl.shard.ForEachGeoIndex(func(_ string, idx *geo.Index) error {
		if f, ok := idx.UnderlyingVectorIndex().(interface{ Flush() error }); ok {
			return f.Flush()
		}
		return nil
	}); err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: flush geo indexes before persist: %v", err)
		return false
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.paused > 0 {
		return false
	}

	// Persist free ∪ passers, then publish.
	next := sroar.NewBitmap()
	next.SetMany(fl.free)
	next.SetMany(passers)
	if err := fl.persistLocked(next); err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Errorf("docid freelist: persist failed, ids stay un-reusable: %v", err)
		return false
	}
	for _, id := range passers {
		delete(fl.pending, id)
		if _, ok := fl.inFree[id]; ok {
			continue
		}
		fl.free = append(fl.free, id)
		fl.inFree[id] = struct{}{}
	}
	return true
}

// drained reports whether none of the captured chunk names remain pending.
// A queue missing from current (e.g. a dropped target vector) counts as
// drained: its ops can never be applied anymore.
func (c *reuseCandidate) drained(current map[string]map[string]struct{}) bool {
	for queueID, names := range c.watermark {
		cur, ok := current[queueID]
		if !ok {
			continue
		}
		for name := range names {
			if _, still := cur[name]; still {
				return false
			}
		}
	}
	return true
}

// persistLocked writes the bitmap with tmp-fsync-rename. Caller holds fl.mu.
func (fl *shardDocIDFreeList) persistLocked(bm *sroar.Bitmap) error {
	payload := bm.ToBuffer()
	buf := make([]byte, 0, len(payload)+13)
	buf = append(buf, docIDFreeListMagic[:]...)
	buf = append(buf, docIDFreeListVersion)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payload))
	buf = append(buf, crc[:]...)
	var plen [4]byte
	binary.LittleEndian.PutUint32(plen[:], uint32(len(payload)))
	buf = append(buf, plen[:]...)
	buf = append(buf, payload...)

	tmp := fl.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, fl.path); err != nil {
		os.Remove(tmp)
		return err
	}
	if dir, err := os.Open(filepath.Dir(fl.path)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}

// Load reads the persisted free list and VALIDATES every id against the live
// shard state before publishing it: ids at or above the counter, ids whose
// object row exists, and ids any index still holds are dropped (loudly).
// This makes a stale or foreign file — a crash-torn state, or a physical
// shard copy where the same docID lived a different life — safe: the worst
// case is a leaked id, never a reused live one. Call only after the LSM
// store and every vector/geo index finished initializing.
func (fl *shardDocIDFreeList) Load() {
	if fl == nil {
		return
	}
	raw, err := os.ReadFile(fl.path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: read %s: %v — starting empty", fl.path, err)
		return
	}
	if len(raw) < 13 || string(raw[:4]) != string(docIDFreeListMagic[:]) || raw[4] != docIDFreeListVersion {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: %s malformed or unknown version — starting empty", fl.path)
		return
	}
	wantCRC := binary.LittleEndian.Uint32(raw[5:9])
	plen := binary.LittleEndian.Uint32(raw[9:13])
	if uint32(len(raw)-13) != plen {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: %s truncated — starting empty", fl.path)
		return
	}
	payload := raw[13:]
	if crc32.ChecksumIEEE(payload) != wantCRC {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: %s checksum mismatch — starting empty", fl.path)
		return
	}

	bm := sroar.FromBufferWithCopy(payload)
	counter := fl.shard.counter.Get()

	var free []uint64
	dropped := 0
	for _, id := range bm.ToArray() {
		if id >= counter {
			dropped++
			continue
		}
		if err := fl.assertNoTraces(id); err != nil {
			fl.logger.WithField("shard", fl.shard.name).
				Warnf("docid freelist: dropping persisted id %d, shard state disagrees: %v", id, err)
			dropped++
			continue
		}
		free = append(free, id)
	}
	if dropped > 0 {
		fl.logger.WithField("shard", fl.shard.name).
			Warnf("docid freelist: dropped %d of %d persisted ids after validation", dropped, bm.GetCardinality())
	}

	fl.mu.Lock()
	defer fl.mu.Unlock()
	fl.free = free
	fl.inFree = make(map[uint64]struct{}, len(free))
	for _, id := range free {
		fl.inFree[id] = struct{}{}
	}
}
