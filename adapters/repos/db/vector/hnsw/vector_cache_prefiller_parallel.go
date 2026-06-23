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
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// useParallelPrefill reports whether the uncompressed cache can be prefilled by a
// parallel cursor scan of the objects bucket rather than the serial by-id
// vectorCachePrefiller. Gated to cases where that scan is both safe and complete:
//   - sync only: an overwriting Preload from a snapshot cursor races live writes;
//     the serial path's load-if-absent Get does not.
//   - unbounded cache only: a full scan ignores the size limit the serial path honors.
//   - single-vector only: a multivector cache holds per-passage vectors, and a muvera
//     cache holds encoded vectors from the dedicated _muvera_vectors bucket — neither
//     lives in the objects bucket this scan reads, so both stay on the serial path.
func (h *hnsw) useParallelPrefill() bool {
	// No real objects bucket (tests wiring only a VectorForID thunk, or pre-attach):
	// fall back to the serial prefiller.
	if h.store == nil || h.store.Bucket(helpers.ObjectsBucketLSM) == nil {
		return false
	}

	h.RLock()
	nodeCount := int64(len(h.nodes))
	h.RUnlock()

	return parallelPrefillEligible(parallelPrefillInputs{
		waitForPrefill: h.waitForCachePrefill,
		multivector:    h.multivector.Load(),
		muvera:         h.muvera.Load(),
		cacheMaxSize:   h.cache.CopyMaxSize(),
		nodeCount:      nodeCount,
	})
}

type parallelPrefillInputs struct {
	waitForPrefill bool
	multivector    bool
	muvera         bool
	cacheMaxSize   int64
	nodeCount      int64
}

// parallelPrefillEligible is the pure decision core of useParallelPrefill, split out
// for direct testing.
func parallelPrefillEligible(in parallelPrefillInputs) bool {
	if !in.waitForPrefill {
		return false
	}
	if in.multivector || in.muvera {
		return false
	}
	return in.cacheMaxSize >= in.nodeCount
}

// prefillCacheParallel populates the uncompressed vector cache via a parallel cursor
// scan of the objects bucket. The by-id vectorCachePrefiller issues one random seek
// per vector (the bucket is UUID-keyed), which is latency-bound and can take hours on
// network storage with the CPU idle; a cursor reads in storage order and decodes
// across cores. Mirrors the compressed PrefillCache, but preloads as it scans rather
// than collecting into a slice first; a second copy of full float32 vectors would
// roughly double startup memory.
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
			// Cache is pre-grown to len(h.nodes) at restore, so live ids are in-bounds;
			// grow only for an id from a write that landed after we snapshotted the count.
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

// scanObjectVectorsParallel scans the objects bucket across GOMAXPROCS cursors over
// disjoint key ranges. onVector must be safe for concurrent use.
func scanObjectVectorsParallel(ctx context.Context, bucket *lsmkv.Bucket, targetVector string,
	onVector func(id uint64, vec []float32), logger logrus.FieldLogger,
) error {
	parallel := 2 * runtime.GOMAXPROCS(0)
	if parallel < 1 {
		parallel = 1
	}

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
			if err := scanObjectVectorsRange(scanCtx, bucket, r.start, r.end, targetVector, onVector, logger); err != nil {
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

		// nil buffer forces a fresh allocation; a reused buffer would be aliased by
		// VectorFromBinary across iterations and corrupt previously cached vectors.
		vec, err := storobj.VectorFromBinary(v, nil, targetVector)
		if err != nil {
			var notFound storobj.ErrTargetVectorNotFound
			if errors.As(err, &notFound) {
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
