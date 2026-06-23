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
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// disableParallelPrefillEnvVar is a kill switch that forces the serial,
// by-id prefiller even when the parallel cursor path would otherwise be
// eligible. It exists so an operator can fall back without a redeploy if the
// parallel path ever misbehaves in production.
const disableParallelPrefillEnvVar = "DISABLE_PARALLEL_VECTOR_CACHE_PREFILL"

// useParallelPrefill reports whether the uncompressed vector cache can be
// prefilled by scanning the objects bucket with a parallel cursor instead of the
// serial, by-id vectorCachePrefiller. It is eligible only when:
//
//   - the prefill is synchronous (waitForCachePrefill). In async mode the prefill
//     runs alongside live writes; the serial path's load-if-absent cache.Get is
//     race-safe there, whereas an overwriting Preload from a snapshot cursor is not.
//   - the cache is effectively unbounded, i.e. it can hold every vector. When the
//     cache is smaller than the node count the serial prefiller is required because
//     it honors the size limit (it stops at `limit`); a full bucket scan would not.
//   - the index is single-vector or muvera. True multivector stores vectors per
//     passage with a different cache keying and keeps the serial path (mirrors the
//     compressed split in prefillCache).
func (h *hnsw) useParallelPrefill() bool {
	// The parallel path cursor-scans the objects bucket directly. Without it — e.g.
	// indexes wired only through a VectorForID thunk (tests) or before the store is
	// attached — fall back to the serial, thunk-based prefiller.
	if h.store == nil || h.store.Bucket(helpers.ObjectsBucketLSM) == nil {
		return false
	}

	h.RLock()
	nodeCount := int64(len(h.nodes))
	h.RUnlock()

	return parallelPrefillEligible(parallelPrefillInputs{
		waitForPrefill: h.waitForCachePrefill,
		killSwitch:     os.Getenv(disableParallelPrefillEnvVar) == "true",
		multivector:    h.multivector.Load(),
		muvera:         h.muvera.Load(),
		cacheMaxSize:   h.cache.CopyMaxSize(),
		nodeCount:      nodeCount,
	})
}

type parallelPrefillInputs struct {
	waitForPrefill bool
	killSwitch     bool
	multivector    bool
	muvera         bool
	cacheMaxSize   int64
	nodeCount      int64
}

// parallelPrefillEligible holds the decision logic for useParallelPrefill in a
// pure form so it can be exercised directly in tests. See useParallelPrefill for
// the rationale behind each condition.
func parallelPrefillEligible(in parallelPrefillInputs) bool {
	if !in.waitForPrefill {
		return false
	}
	if in.killSwitch {
		return false
	}
	if in.multivector && !in.muvera {
		return false
	}
	return in.cacheMaxSize >= in.nodeCount
}

// prefillCacheParallel populates the (uncompressed) vector cache by scanning the
// objects bucket with a parallel, per-segment cursor rather than looking up every
// vector by id through the HNSW graph.
//
// The by-id prefiller (vectorCachePrefiller) issues one random read per vector.
// The objects bucket is keyed by UUID, so a lookup by doc id is a random seek;
// doing that serially for millions of vectors is latency-bound and can take hours
// on network-attached storage, with the CPU idle. When the cache is unbounded the
// index lookup is pure overhead: a cursor scan reads in storage order (sequential)
// and parallelizes the decode across cores. This mirrors
// compressionhelpers.(*quantizedVectorsCompressor).PrefillCache, which does the
// same over the dedicated compressed-vector bucket.
//
// Unlike the compressed variant, it preloads into the cache as it scans instead of
// first collecting every vector into a temporary slice: full float32 vectors are
// large and a second full copy would roughly double peak memory during startup.
// The cache is already grown to len(h.nodes) at restore, so a direct Preload is
// in-bounds for every live doc id; the rare id beyond that grows the cache
// defensively.
func (h *hnsw) prefillCacheParallel(ctx context.Context) error {
	before := time.Now()

	bucket := h.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return fmt.Errorf("prefill cache: objects bucket %q not found", helpers.ObjectsBucketLSM)
	}
	targetVector := h.getTargetVector()

	h.RLock()
	preGrown := uint64(len(h.nodes))
	h.RUnlock()

	var loaded atomic.Int64
	onVector := func(id uint64, vec []float32) {
		if id >= preGrown {
			// The cache is grown to len(h.nodes) at restore, so every live doc id is
			// already in-bounds. Grow only for the unexpected larger id (e.g. a write
			// landed after we snapshotted the node count).
			h.cache.Grow(id)
		}
		h.cache.Preload(id, vec)
		loaded.Add(1)
	}

	if err := scanObjectVectorsParallel(ctx, bucket, targetVector, onVector, h.logger); err != nil {
		return err
	}

	h.logger.WithFields(logrus.Fields{
		"action":   "hnsw_vector_cache_prefill",
		"count":    loaded.Load(),
		"took":     time.Since(before),
		"index_id": h.id,
		"parallel": true,
	}).Info("prefilled vector cache")
	return nil
}

// scanObjectVectorsParallel splits the objects keyspace into ranges using quantile
// seed keys and scans each range with its own cursor concurrently, invoking
// onVector for every (docID, vector) pair. onVector must be safe for concurrent use.
func scanObjectVectorsParallel(ctx context.Context, bucket *lsmkv.Bucket, targetVector string,
	onVector func(id uint64, vec []float32), logger logrus.FieldLogger,
) error {
	parallel := 2 * runtime.GOMAXPROCS(0)
	if parallel < 1 {
		parallel = 1
	}

	// One fewer seed than the desired parallelism: the first routine reads from the
	// start to seeds[0], the last reads from the final seed to the end.
	seeds := bucket.QuantileKeys(parallel - 1)

	type keyRange struct{ start, end []byte } // nil start = from first; nil end = to the end
	var ranges []keyRange
	if len(seeds) == 0 {
		// Empty or very small bucket: a single full scan.
		ranges = []keyRange{{start: nil, end: nil}}
	} else {
		ranges = append(ranges, keyRange{start: nil, end: seeds[0]})
		for i := 0; i < len(seeds)-1; i++ {
			ranges = append(ranges, keyRange{start: seeds[i], end: seeds[i+1]})
		}
		ranges = append(ranges, keyRange{start: seeds[len(seeds)-1], end: nil})
	}

	var (
		wg       sync.WaitGroup
		firstErr atomic.Pointer[error]
	)
	for i := range ranges {
		r := ranges[i]
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			if err := scanObjectVectorsRange(ctx, bucket, r.start, r.end, targetVector, onVector, logger); err != nil {
				e := err
				firstErr.CompareAndSwap(nil, &e)
			}
		}, logger)
	}
	wg.Wait()

	if e := firstErr.Load(); e != nil {
		return *e
	}
	return nil
}

func scanObjectVectorsRange(ctx context.Context, bucket *lsmkv.Bucket, start, end []byte,
	targetVector string, onVector func(id uint64, vec []float32), logger logrus.FieldLogger,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c := bucket.Cursor()
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

		id, err := storobj.DocIDFromBinary(v)
		if err != nil {
			logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping object with undecodable doc id: %v", err)
			continue
		}

		// nil buffer: VectorFromBinary returns a view into the supplied buffer when
		// it is large enough, which would alias across iterations. A fresh allocation
		// per vector keeps every cached slice independent.
		vec, err := storobj.VectorFromBinary(v, nil, targetVector)
		if err != nil {
			var notFound storobj.ErrTargetVectorNotFound
			if errors.As(err, &notFound) {
				// object simply has no vector for this target vector; nothing to cache
				continue
			}
			logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping doc id %d with undecodable vector: %v", id, err)
			continue
		}
		if len(vec) == 0 {
			continue
		}
		onVector(id, vec)
	}
	return nil
}
