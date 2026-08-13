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

package hnsw

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	prefillAllocCheckEvery = 4096
	// prefillReservedCacheSlots is what every prefill leaves free. replaceIfFull treats
	// count == maxSize as a full cache and wipes it, so a prefill that fills the last
	// slot is thrown away by the next deletion tick. Both the serial prefiller's limit
	// and the scan's budget are set from this.
	prefillReservedCacheSlots = 1
)

var (
	errPrefillMemoryPressure = errors.New("vector cache prefill aborted under memory pressure")
	errPrefillCacheFull      = errors.New("vector cache prefill aborted: cache full")
	// errPrefillCompressionActive is defensive: every compression-activation path is
	// gated on cachePrefilled, so it cannot fire through current callers.
	errPrefillCompressionActive = errors.New("vector cache prefill aborted: compression activated")
)

// Prefill paths, as they appear in the routing log. The serial by-id prefiller issues
// one random seek per vector, so which path an index took is the first thing a slow
// restore needs to answer.
const (
	prefillPathSerial       = "serial"
	prefillPathCursorScan   = "cursor-scan"
	prefillPathTargetedScan = "targeted-scan"
)

func (h *hnsw) logPrefillPath(path, reason string) {
	entry := h.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"index_id": h.id,
		"path":     path,
	})
	if reason != "" {
		entry = entry.WithField("reason", reason)
	}
	entry.Info("selected vector cache prefill path")
}

// useParallelPrefill reports whether the cache can be filled by scanning the objects
// bucket instead of the serial by-id prefiller, and why not when it cannot.
func (h *hnsw) useParallelPrefill() (bool, string) {
	if h.store == nil || h.store.Bucket(helpers.ObjectsBucketLSM) == nil {
		return false, "no objects bucket to scan"
	}
	// an hfresh cache holds centroids rather than object vectors
	if h.hfreshMode {
		return false, "hfresh cache is not filled from object rows"
	}
	if !scanPrefillEnabled() {
		return false, "scan prefill disabled by " + prefillScanWorkersEnv
	}
	return parallelPrefillEligible(h.parallelPrefillInputs())
}

func (h *hnsw) parallelPrefillInputs() parallelPrefillInputs {
	_, canPreloadIfAbsent := h.cache.(cache.IfAbsentPreloader[float32])

	return parallelPrefillInputs{
		multivector:        h.multivector.Load(),
		muvera:             h.muvera.Load(),
		canPreloadIfAbsent: canPreloadIfAbsent,
		cacheMaxSize:       h.cache.CopyMaxSize(),
		nodeCount:          h.nodeCount(),
	}
}

func (h *hnsw) nodeCount() int64 {
	h.RLock()
	defer h.RUnlock()

	return int64(len(h.nodes))
}

type parallelPrefillInputs struct {
	multivector        bool
	muvera             bool
	canPreloadIfAbsent bool
	cacheMaxSize       int64
	nodeCount          int64
}

// parallelPrefillEligible is useParallelPrefill's decision core, split out so the
// combinations can be tested directly.
func parallelPrefillEligible(in parallelPrefillInputs) (bool, string) {
	// a multivector slot is addressed by (docID, relativeID), which a scan with one id
	// per row cannot supply, and muvera fills its cache from _muvera_vectors
	if in.multivector || in.muvera {
		return false, "cache is not addressed by doc id alone"
	}
	// running alongside live writes is only safe if the scan can fill empty slots
	// without overwriting: its cursor holds a snapshot that may already be stale
	if !in.canPreloadIfAbsent {
		return false, "cache cannot preload if-absent"
	}
	// prefillCache screens this first, but nodes can grow between that check and the
	// scan starting on the async path
	if !cacheHoldsEveryNode(in.cacheMaxSize, in.nodeCount) {
		return false, "cache is smaller than the node count"
	}
	return true, ""
}

// cacheHoldsEveryNode reports whether a prefill's product can survive first use. Below
// this bound an uncached node is guaranteed, so the first query touching one takes the
// count to the level prefillReservedCacheSlots exists to stay under, and everything the
// prefill loaded is discarded.
//
// Exact fit is admitted deliberately. The reserved slot leaves one node uncached, so a
// query on that node can still trigger the wipe, but every other node stays resident
// and that wipe is one the workload reaches on its own once it has touched them all.
func cacheHoldsEveryNode(maxSize, nodeCount int64) bool {
	return maxSize >= nodeCount
}

// prefillCacheParallel fills the cache by scanning the objects bucket. The by-id
// prefiller issues one random seek per vector (the bucket is UUID-keyed), which is
// latency-bound and can take hours on network storage with the CPU idle.
func (h *hnsw) prefillCacheParallel(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err // a cancelled parent must not reach the store
	}

	bucket := h.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return fmt.Errorf("prefill cache: objects bucket %q not found", helpers.ObjectsBucketLSM)
	}

	targetVector := h.getTargetVector()
	if h.useTargetedPrefillScan(bucket) {
		h.logPrefillPath(prefillPathTargetedScan, "")
		return h.prefillFromScan(ctx, func(ctx context.Context, onVector prefillOnVector) error {
			return h.scanObjectVectorsTargeted(ctx, bucket, targetVector, onVector)
		})
	}
	h.logPrefillPath(prefillPathCursorScan, "")
	return h.prefillFromScan(ctx, func(ctx context.Context, onVector prefillOnVector) error {
		return scanBucketVectorsParallel(ctx, bucket, objectsRowDecoder(targetVector, h.logger), onVector, h.logger)
	})
}

// prefillSink is the cache end of a scan: it decides which vectors are admitted, holds
// the reservation against the cache limit, and latches the first abort so every worker
// stops together.
type prefillSink struct {
	h         *hnsw
	preloader cache.IfAbsentPreloader[float32]
	// budget is how many vectors this scan may add. Claimed atomically rather than
	// compared against CountVectors per row, so racing workers cannot collectively
	// overshoot the way an advisory check allows.
	budget  int64
	loaded  atomic.Int64
	claimed atomic.Int64
	// abortCause stops every worker at its next row. Returning an error only ends the
	// worker that raised it, and the group's cancellation reaches a sibling no earlier
	// than its next row either, so without this the alloc probe's verdict would arrive
	// after thousands more vectors were cached.
	abortCause atomic.Pointer[error]
}

func newPrefillSink(h *hnsw) (*prefillSink, error) {
	// not on Cache[T]: only the single-vector caches can fill a slot by id alone
	preloader, ok := h.cache.(cache.IfAbsentPreloader[float32])
	if !ok {
		return nil, fmt.Errorf("prefill cache: %T cannot preload if-absent", h.cache)
	}
	return &prefillSink{
		h:         h,
		preloader: preloader,
		budget:    h.cache.CopyMaxSize() - prefillReservedCacheSlots - h.cache.CountVectors(),
	}, nil
}

func (s *prefillSink) stop(err error) error {
	if s.abortCause.CompareAndSwap(nil, &err) {
		return err
	}
	return *s.abortCause.Load()
}

// accept is the per-row admission policy, called concurrently from every worker.
func (s *prefillSink) accept(id uint64, vec []float32) error {
	if cause := s.abortCause.Load(); cause != nil {
		return *cause
	}
	if s.h.compressed.Load() {
		return s.stop(errPrefillCompressionActive)
	}
	if !s.h.prefillEligible(id) {
		return nil
	}
	if s.claimed.Add(1) > s.budget {
		s.claimed.Add(-1)
		return s.stop(errPrefillCacheFull)
	}
	// cosine-dot keeps normalized vectors in the cache; the serial path gets this
	// from the cache's normalizeOnRead wrapper, which the preload bypasses. vec is
	// a fresh per-vector allocation, so normalizing in place is safe.
	s.h.normalizeVecInPlace(vec)
	if !s.preloader.PreloadIfAbsent(id, vec) {
		s.claimed.Add(-1) // slot already populated; hand the reservation back
		return nil
	}
	if n := s.loaded.Add(1); n%prefillAllocCheckEvery == 0 && s.h.allocChecker != nil {
		if err := s.h.allocChecker.CheckAlloc(prefillAllocCheckEvery * int64(len(vec)) * 4); err != nil {
			return s.stop(fmt.Errorf("%w: %w", errPrefillMemoryPressure, err))
		}
	}
	return nil
}

// prefillFromScan drives a parallel bucket scan into the cache. A delete racing the
// snapshot cursor can repopulate its slot with the pre-delete value: bounded waste, and
// the node itself stays tombstoned. Memory pressure and a full cache abort with a nil
// error, since the vectors left behind load on demand through cache.Get.
func (h *hnsw) prefillFromScan(ctx context.Context,
	scan func(context.Context, prefillOnVector) error,
) error {
	before := time.Now()

	sink, err := newPrefillSink(h)
	if err != nil {
		return err
	}

	scanErr := scan(ctx, sink.accept)
	return h.logPrefillOutcome(sink.loaded.Load(), time.Since(before), scanErr)
}

// logPrefillOutcome turns the scan's error into the three outcomes a prefill has: an
// abort that leaves the remaining vectors to load on demand, a genuine failure, and
// success. Only the middle one is the caller's problem.
func (h *hnsw) logPrefillOutcome(count int64, took time.Duration, err error) error {
	entry := h.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"index_id": h.id,
		"count":    count,
	})

	switch {
	case err == nil:
		entry.WithFields(logrus.Fields{
			"nodes":    h.nodeCount(),
			"took":     took,
			"parallel": true,
		}).Info("prefilled vector cache")
		return nil
	case errors.Is(err, errPrefillMemoryPressure), errors.Is(err, errPrefillCacheFull):
		entry.Warnf("%v; remaining vectors load on demand", err)
		return nil
	case errors.Is(err, errPrefillCompressionActive):
		entry.Info("stopping vector cache prefill: compression activated mid-scan")
		return nil
	}
	return err
}

// prefillEligible is shared by both scan paths, so the read strategy cannot change
// what ends up resident. Doc ids are never reused, so a superseded row's id has no
// live node; tombstoned nodes keep their entry until cleanup while their row stays
// live; an id past the node range is corrupt and must not size an allocation.
func (h *hnsw) prefillEligible(id uint64) bool {
	return h.nodeIndexed(id) && !h.hasTombstone(id)
}

// nodeIndexed is nodeByID's question without the vertex. It takes only the striped
// lock, where nodeByID also takes h's: a scan asks this once per row, and the shared
// lock would serialize every worker against every insert. Reading the bound under the
// stripe is what makes that safe, since h.nodes can shrink under LockAll on a reset.
func (h *hnsw) nodeIndexed(id uint64) bool {
	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)

	if id >= uint64(len(h.nodes)) {
		return false
	}
	return h.nodes[id] != nil
}

// prefillOnVector consumes one decoded vector. Must be safe for concurrent use; a
// non-nil error aborts the whole scan.
type prefillOnVector func(id uint64, vec []float32) error

// prefillRowDecoder extracts (docID, vector) from one bucket entry; ok=false skips
// the row. The returned vec must not alias v — cursor buffers are reused.
type prefillRowDecoder func(v []byte) (id uint64, vec []float32, ok bool)

func objectsRowDecoder(targetVector string, logger logrus.FieldLogger) prefillRowDecoder {
	return func(v []byte) (uint64, []float32, bool) {
		id, err := storobj.DocIDFromBinary(v)
		if err != nil {
			logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping object with undecodable doc id: %v", err)
			return 0, nil, false
		}

		// nil buffer forces a fresh allocation; a reused buffer would be aliased by
		// VectorFromBinary across iterations and corrupt previously cached vectors.
		vec, err := storobj.VectorFromBinary(v, nil, targetVector)
		if err != nil {
			if !vectorTargetMissing(err) {
				logger.WithField("action", "hnsw_vector_cache_prefill").
					Debugf("skipping doc id %d with undecodable vector: %v", id, err)
			}
			return 0, nil, false
		}
		return id, vec, true
	}
}

// prefillScanParallelism is 2x GOMAXPROCS: while one reader blocks on disk another
// keeps a core busy decoding — the IO-bound default used across the vector package.
func prefillScanParallelism() int {
	const cursorsPerProc = 2
	parallel := cursorsPerProc * runtime.GOMAXPROCS(0)
	if parallel < 1 {
		parallel = 1
	}
	return parallel
}

// scanBucketVectorsParallel scans a replace-strategy bucket over disjoint key ranges,
// one cursor per worker acquired from the node-wide pool.
func scanBucketVectorsParallel(ctx context.Context, bucket *lsmkv.Bucket,
	decode prefillRowDecoder, onVector prefillOnVector, logger logrus.FieldLogger,
) error {
	parallel, release, err := acquirePrefillWorkers(ctx, prefillScanParallelism(), logger)
	if err != nil {
		return err
	}
	defer release()

	// n-1 seeds yield n ranges: [first,seeds[0]), interiors, [seeds[last],end).
	seeds := bucket.QuantileKeys(parallel - 1)

	type keyRange struct{ start, end []byte } // nil = open-ended (first / end)
	var ranges []keyRange
	if len(seeds) == 0 {
		ranges = []keyRange{{start: nil, end: nil}} // no seeds: single full scan
	} else {
		ranges = append(ranges, keyRange{start: nil, end: seeds[0]})
		for i := 0; i < len(seeds)-1; i++ {
			ranges = append(ranges, keyRange{start: seeds[i], end: seeds[i+1]})
		}
		ranges = append(ranges, keyRange{start: seeds[len(seeds)-1], end: nil})
	}

	// The error group cancels siblings on the first error and turns a recovered
	// panic into that error. A bare GoWrapper would swallow it: the panic unwinds
	// into the recover, the wait returns nil, and the range that worker owned is
	// dropped from a prefill that reports success.
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	for i := range ranges {
		r := ranges[i]
		eg.Go(func() error {
			return scanBucketVectorsRange(egCtx, bucket, r.start, r.end, decode, onVector)
		})
	}
	return eg.Wait()
}

func scanBucketVectorsRange(ctx context.Context, bucket *lsmkv.Bucket, start, end []byte,
	decode prefillRowDecoder, onVector prefillOnVector,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c := bucket.CursorReplaceReusable()
	defer c.Close()

	var k, v []byte
	if start == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(start)
	}

	// Polled every row, not in batches: teardown waits on this loop while holding the
	// shard's shutdown lock, and a channel poll is nanoseconds against the disk read
	// that follows it, so batching buys nothing and costs the wait one batch of rows.
	done := ctx.Done()
	for ; k != nil; k, v = c.Next() {
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		select {
		case <-done:
			return ctx.Err()
		default:
		}
		if len(v) == 0 {
			continue
		}

		id, vec, ok := decode(v)
		if !ok || len(vec) == 0 {
			continue
		}
		if err := onVector(id, vec); err != nil {
			return err
		}
	}
	return nil
}
