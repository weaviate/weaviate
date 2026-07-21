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
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// prefillAllocCheckEvery is how many stored vectors pass between allocChecker probes
// during a scan prefill.
const prefillAllocCheckEvery = 4096

var (
	errPrefillMemoryPressure = errors.New("vector cache prefill aborted under memory pressure")
	errPrefillCacheFull      = errors.New("vector cache prefill aborted: cache full")
	// errPrefillCompressionActive is defensive: every compression-activation path is
	// gated on cachePrefilled, so it cannot fire through current callers.
	errPrefillCompressionActive = errors.New("vector cache prefill aborted: compression activated")
)

// useParallelPrefill reports whether the uncompressed cache can be prefilled by a
// parallel cursor scan of the objects bucket rather than the serial by-id
// vectorCachePrefiller. Multivector caches (per-passage) and muvera caches (sourced
// from the muvera bucket, see useMuveraParallelPrefill) are not objects-sourced, and
// neither is the hfresh centroid cache despite its store holding an objects bucket.
func (h *hnsw) useParallelPrefill() bool {
	if h.store == nil || h.store.Bucket(helpers.ObjectsBucketLSM) == nil || h.hfreshMode {
		return false
	}

	return parallelPrefillEligible(h.parallelPrefillInputs())
}

// useMuveraParallelPrefill reports whether the muvera float32 cache can be prefilled
// by a parallel cursor scan of the muvera vectors bucket (key = big-endian docID =
// node id, value = raw float32s).
func (h *hnsw) useMuveraParallelPrefill() bool {
	if h.store == nil || h.store.Bucket(h.muveraBucketName()) == nil {
		return false
	}

	return muveraParallelPrefillEligible(h.parallelPrefillInputs())
}

func (h *hnsw) muveraBucketName() string {
	return h.id + "_muvera_vectors"
}

func (h *hnsw) parallelPrefillInputs() parallelPrefillInputs {
	h.RLock()
	nodeCount := int64(len(h.nodes))
	h.RUnlock()

	return parallelPrefillInputs{
		multivector:  h.multivector.Load(),
		muvera:       h.muvera.Load(),
		cacheMaxSize: h.cache.CopyMaxSize(),
		nodeCount:    nodeCount,
	}
}

type parallelPrefillInputs struct {
	multivector  bool
	muvera       bool
	cacheMaxSize int64
	nodeCount    int64
}

// parallelPrefillEligible is the pure decision core of useParallelPrefill, split out
// for direct testing. A full scan ignores the size limit the serial path honors, so
// the cache must be unbounded relative to the node count.
func parallelPrefillEligible(in parallelPrefillInputs) bool {
	if in.multivector || in.muvera {
		return false
	}
	return in.cacheMaxSize >= in.nodeCount
}

// muveraParallelPrefillEligible is the pure decision core of useMuveraParallelPrefill.
func muveraParallelPrefillEligible(in parallelPrefillInputs) bool {
	if !in.muvera {
		return false
	}
	return in.cacheMaxSize >= in.nodeCount
}

// prefillCacheParallel populates the uncompressed vector cache via a parallel cursor
// scan of the objects bucket. The by-id vectorCachePrefiller issues one random seek
// per vector (the bucket is UUID-keyed), which is latency-bound and can take hours on
// network storage with the CPU idle; a cursor reads in storage order and decodes
// across cores.
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

// prefillMuveraCacheParallel populates the muvera float32 cache via a parallel cursor
// scan of the muvera vectors bucket.
func (h *hnsw) prefillMuveraCacheParallel(ctx context.Context) error {
	bucket := h.store.Bucket(h.muveraBucketName())
	if bucket == nil {
		return fmt.Errorf("prefill cache: muvera bucket %q not found", h.muveraBucketName())
	}

	return h.prefillFromScan(ctx, func(ctx context.Context, onVector prefillOnVector) error {
		return scanBucketVectorsParallel(ctx, bucket, muveraRowDecoder(h.logger), onVector, h.logger)
	})
}

// prefillFromScan drives a parallel bucket scan into the cache. PreloadIfAbsent makes
// it safe to run concurrently with live writes; a delete racing the snapshot cursor
// can repopulate its slot with the pre-delete value — bounded waste, the node itself
// stays tombstoned. Memory pressure and a full cache abort gracefully (nil error);
// the remaining vectors load on demand through cache.Get.
func (h *hnsw) prefillFromScan(ctx context.Context,
	scan func(context.Context, prefillOnVector) error,
) error {
	before := time.Now()

	h.RLock()
	preGrown := uint64(len(h.nodes))
	h.RUnlock()

	var loaded atomic.Int64
	onVector := func(id uint64, vec []float32) error {
		if h.compressed.Load() {
			return errPrefillCompressionActive
		}
		if h.cache.CountVectors() >= h.cache.CopyMaxSize() {
			return errPrefillCacheFull
		}
		// cosine-dot keeps normalized vectors in the cache; the serial path gets this
		// from the cache's normalizeOnRead wrapper, which the preload bypasses. vec is
		// a fresh per-vector allocation, so normalizing in place is safe.
		h.normalizeVecInPlace(vec)
		if id >= preGrown {
			// Cache is pre-grown to len(h.nodes) at restore, so live ids are in-bounds;
			// grow only for an id from a write that landed after we snapshotted the count.
			h.cache.Grow(id)
		}
		if !h.cache.PreloadIfAbsent(id, vec) {
			return nil
		}
		if n := loaded.Add(1); n%prefillAllocCheckEvery == 0 && h.allocChecker != nil {
			if err := h.allocChecker.CheckAlloc(prefillAllocCheckEvery * int64(len(vec)) * 4); err != nil {
				return fmt.Errorf("%w: %w", errPrefillMemoryPressure, err)
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

// prefillOnVector consumes one decoded vector. Must be safe for concurrent use; a
// non-nil error aborts the whole scan.
type prefillOnVector func(id uint64, vec []float32) error

// prefillRowDecoder extracts (docID, vector) from one bucket entry; ok=false skips
// the row. The returned vec must not alias v — cursor buffers are reused.
type prefillRowDecoder func(k, v []byte) (id uint64, vec []float32, ok bool)

func objectsRowDecoder(targetVector string, logger logrus.FieldLogger) prefillRowDecoder {
	return func(k, v []byte) (uint64, []float32, bool) {
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

func muveraRowDecoder(logger logrus.FieldLogger) prefillRowDecoder {
	return func(k, v []byte) (uint64, []float32, bool) {
		if len(k) != 8 {
			logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping muvera entry with %d-byte key", len(k))
			return 0, nil, false
		}
		// MuveraFromBytes allocates a fresh slice, so the value never aliases the
		// cursor buffer.
		return binary.BigEndian.Uint64(k), multivector.MuveraFromBytes(v), true
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
	parallel := prefillScanParallelism()

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

	// Cancel siblings as soon as one range errors, instead of scanning to completion.
	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		firstErr atomic.Pointer[error]
	)
	for i := range ranges {
		r := ranges[i]
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			if err := scanBucketVectorsRange(scanCtx, bucket, r.start, r.end, decode, onVector); err != nil {
				e := err
				if firstErr.CompareAndSwap(nil, &e) {
					cancel()
				}
			}
		}, logger)
	}
	wg.Wait()

	if e := firstErr.Load(); e != nil {
		return *e
	}
	return nil
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

	const checkContextEveryN = 1024
	n := 0
	for ; k != nil; k, v = c.Next() {
		if end != nil && bytes.Compare(k, end) >= 0 {
			break
		}
		n++
		if n%checkContextEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(v) == 0 {
			continue
		}

		id, vec, ok := decode(k, v)
		if !ok || len(vec) == 0 {
			continue
		}
		if err := onVector(id, vec); err != nil {
			return err
		}
	}
	return nil
}
