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

const prefillAllocCheckEvery = 4096

var (
	errPrefillMemoryPressure = errors.New("vector cache prefill aborted under memory pressure")
	errPrefillCacheFull      = errors.New("vector cache prefill aborted: cache full")
	// errPrefillCompressionActive is defensive: every compression-activation path is
	// gated on cachePrefilled, so it cannot fire through current callers.
	errPrefillCompressionActive = errors.New("vector cache prefill aborted: compression activated")
)

// useParallelPrefill reports whether the cache can be filled by scanning the objects
// bucket instead of the serial by-id prefiller. Three caches cannot be, each for its
// own reason: a multivector slot is addressed by (docID, relativeID) and so cannot be
// filled from a scan that has one id per row, muvera reads _muvera_vectors, and an
// hfresh cache holds centroids rather than object vectors.
func (h *hnsw) useParallelPrefill() bool {
	if h.store == nil || h.store.Bucket(helpers.ObjectsBucketLSM) == nil || h.hfreshMode {
		return false
	}
	if !scanPrefillEnabled() {
		return false // operator turned the scan off; the serial prefiller still runs
	}

	return parallelPrefillEligible(h.parallelPrefillInputs())
}

func (h *hnsw) parallelPrefillInputs() parallelPrefillInputs {
	h.RLock()
	nodeCount := int64(len(h.nodes))
	h.RUnlock()

	_, ifAbsent := h.cache.(cache.IfAbsentPreloader[float32])

	return parallelPrefillInputs{
		multivector:  h.multivector.Load(),
		muvera:       h.muvera.Load(),
		ifAbsent:     ifAbsent,
		cacheMaxSize: h.cache.CopyMaxSize(),
		nodeCount:    nodeCount,
	}
}

type parallelPrefillInputs struct {
	multivector  bool
	muvera       bool
	ifAbsent     bool
	cacheMaxSize int64
	nodeCount    int64
}

// parallelPrefillEligible is useParallelPrefill's decision core, split out so the
// combinations can be tested directly.
func parallelPrefillEligible(in parallelPrefillInputs) bool {
	if in.multivector || in.muvera {
		return false
	}
	// running alongside live writes is only safe if the scan can fill empty slots
	// without overwriting: its cursor holds a snapshot that may already be stale
	if !in.ifAbsent {
		return false
	}
	return cacheFitsNodes(in)
}

// cacheFitsNodes admits an exact fit: the scan claims at most maxSize-1 slots, so it
// cannot reach the count replaceIfFull wipes on. Requiring headroom would send those
// caches to the serial prefiller for no benefit.
func cacheFitsNodes(in parallelPrefillInputs) bool {
	return in.cacheMaxSize >= in.nodeCount
}

// prefillCacheParallel fills the cache by scanning the objects bucket. The by-id
// prefiller issues one random seek per vector (the bucket is UUID-keyed), which is
// latency-bound and can take hours on network storage with the CPU idle.
func (h *hnsw) prefillCacheParallel(ctx context.Context) error {
	bucket := h.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return fmt.Errorf("prefill cache: objects bucket %q not found", helpers.ObjectsBucketLSM)
	}

	targetVector := h.getTargetVector()
	if h.useTargetedPrefillScan(bucket) {
		return h.prefillFromScan(ctx, func(ctx context.Context, onVector prefillOnVector) error {
			return h.scanObjectVectorsTargeted(ctx, bucket, targetVector, onVector)
		})
	}
	return h.prefillFromScan(ctx, func(ctx context.Context, onVector prefillOnVector) error {
		return scanBucketVectorsParallel(ctx, bucket, objectsRowDecoder(targetVector, h.logger), onVector, h.logger)
	})
}

// prefillFromScan drives a parallel bucket scan into the cache. A delete racing the
// snapshot cursor can repopulate its slot with the pre-delete value — bounded waste,
// the node itself stays tombstoned. Memory pressure and a full cache abort with a nil
// error; the vectors left behind load on demand through cache.Get.
func (h *hnsw) prefillFromScan(ctx context.Context,
	scan func(context.Context, prefillOnVector) error,
) error {
	before := time.Now()

	// not on Cache[T]: only the single-vector caches can fill a slot by id alone
	preloader, ok := h.cache.(cache.IfAbsentPreloader[float32])
	if !ok {
		return fmt.Errorf("prefill cache: %T cannot preload if-absent", h.cache)
	}

	// How many vectors this scan may add, leaving the last slot free: replaceIfFull
	// reads count == maxSize as a full cache and wipes it, discarding the scan.
	// Claimed atomically rather than compared against CountVectors per row, so racing
	// workers cannot collectively overshoot the way an advisory check allows.
	budget := h.cache.CopyMaxSize() - 1 - h.cache.CountVectors()

	var (
		loaded  atomic.Int64
		claimed atomic.Int64
		// abortCause stops every worker at its next row. Returning an error only ends
		// the worker that raised it, and the group's cancellation reaches a sibling
		// no earlier than its next row either, so without this the alloc probe's
		// verdict would arrive after thousands more vectors were cached.
		abortCause atomic.Pointer[error]
	)
	stop := func(err error) error {
		if abortCause.CompareAndSwap(nil, &err) {
			return err
		}
		return *abortCause.Load()
	}

	onVector := func(id uint64, vec []float32) error {
		if cause := abortCause.Load(); cause != nil {
			return *cause
		}
		if h.compressed.Load() {
			return stop(errPrefillCompressionActive)
		}
		if !h.prefillEligible(id) {
			return nil
		}
		if claimed.Add(1) > budget {
			claimed.Add(-1)
			return stop(errPrefillCacheFull)
		}
		// cosine-dot keeps normalized vectors in the cache; the serial path gets this
		// from the cache's normalizeOnRead wrapper, which the preload bypasses. vec is
		// a fresh per-vector allocation, so normalizing in place is safe.
		h.normalizeVecInPlace(vec)
		if !preloader.PreloadIfAbsent(id, vec) {
			claimed.Add(-1) // slot already populated; hand the reservation back
			return nil
		}
		if n := loaded.Add(1); n%prefillAllocCheckEvery == 0 && h.allocChecker != nil {
			if err := h.allocChecker.CheckAlloc(prefillAllocCheckEvery * int64(len(vec)) * 4); err != nil {
				return stop(fmt.Errorf("%w: %w", errPrefillMemoryPressure, err))
			}
		}
		return nil
	}

	entry := h.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"index_id": h.id,
	})

	if err := scan(ctx, onVector); err != nil {
		switch {
		case errors.Is(err, errPrefillMemoryPressure), errors.Is(err, errPrefillCacheFull):
			entry.WithField("count", loaded.Load()).
				Warnf("%v; remaining vectors load on demand", err)
			return nil
		case errors.Is(err, errPrefillCompressionActive):
			entry.WithField("count", loaded.Load()).
				Info("stopping vector cache prefill: compression activated mid-scan")
			return nil
		}
		return err
	}

	entry.WithFields(logrus.Fields{
		"count":    loaded.Load(),
		"took":     time.Since(before),
		"parallel": true,
	}).Info("prefilled vector cache")
	return nil
}

// prefillEligible is shared by both scan paths, so the read strategy cannot change
// what ends up resident. Doc ids are never reused, so a superseded row's id has no
// live node; tombstoned nodes keep their entry until cleanup while their row stays
// live; an id past the node range is corrupt and must not size an allocation.
func (h *hnsw) prefillEligible(id uint64) bool {
	return h.nodeAlive(id) && !h.hasTombstone(id)
}

func (h *hnsw) nodeAlive(id uint64) bool {
	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)
	// h.nodes can shrink under LockAll (index reset); read the bound under the lock
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
			var notFound storobj.ErrTargetVectorNotFound
			if errors.As(err, &notFound) {
				return 0, nil, false
			}
			logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping doc id %d with undecodable vector: %v", id, err)
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

// scanBucketVectorsParallel scans a replace-strategy bucket across GOMAXPROCS cursors
// over disjoint key ranges.
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
