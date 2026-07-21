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
	"context"
	"errors"
	"os"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	// prefillPeekBytes covers the fixed header, a small legacy vector, the class
	// name, and the schema length prefix; objects whose front sections exceed it
	// fall back to whole-value reads.
	prefillPeekBytes = 512
	// prefillTargetedMinSchemaLen gates the two-read path: below it, skipping the
	// properties schema saves too little to justify a second read.
	prefillTargetedMinSchemaLen = 8 << 10
)

func prefillTargetedReadsEnabled() bool {
	return entcfg.Enabled(os.Getenv("HNSW_PREFILL_TARGETED_READS"))
}

// scanObjectVectorsTargeted feeds onVector from targeted reads of the objects
// bucket: a bounded peek per entry plus, for large schemas, only the vector-bearing
// tail. The underlying scan has no cross-segment merge and visits superseded and
// deleted versions too, so every row is filtered by node liveness first — which also
// skips the tail read for rows that no longer matter.
func (h *hnsw) scanObjectVectorsTargeted(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	h.RLock()
	nodesLen := uint64(len(h.nodes))
	h.RUnlock()

	return bucket.ScanTargetedReplace(ctx, prefillPeekBytes, prefillScanParallelism(),
		func(e *lsmkv.TargetedScanEntry) error {
			id, err := storobj.DocIDFromBinary(e.Peek)
			if err != nil {
				h.logger.WithField("action", "hnsw_vector_cache_prefill").
					Debugf("skipping object with undecodable doc id: %v", err)
				return nil
			}
			if !h.nodeAlive(id, nodesLen) {
				return nil
			}
			vec, ok := h.targetedVectorFromEntry(e, targetVector)
			if !ok || len(vec) == 0 {
				return nil
			}
			return onVector(id, vec)
		}, h.logger)
}

// nodeAlive reports whether id has a live node. Doc ids are never reused, so a
// superseded or deleted row's id has no live node; ids at or beyond the startup
// snapshot come from live inserts, which preload their own vectors.
func (h *hnsw) nodeAlive(id uint64, nodesLen uint64) bool {
	if id >= nodesLen {
		return false
	}
	h.shardedNodeLocks.RLock(id)
	node := h.nodes[id]
	h.shardedNodeLocks.RUnlock(id)
	return node != nil
}

func (h *hnsw) targetedVectorFromEntry(e *lsmkv.TargetedScanEntry, targetVector string) ([]float32, bool) {
	if targetVector == "" {
		return h.legacyVectorFromEntry(e)
	}

	tailStart, schemaLen, ok, err := storobj.VectorTailOffsetFromPeek(e.Peek)
	if err != nil {
		h.logger.WithField("action", "hnsw_vector_cache_prefill").
			Debugf("skipping object with undecodable header: %v", err)
		return nil, false
	}
	if !ok || schemaLen < prefillTargetedMinSchemaLen || tailStart >= e.ValueSize {
		return h.wholeVectorFromEntry(e, targetVector)
	}

	tail, err := e.ReadRange(tailStart, 0)
	if err != nil {
		h.logger.WithField("action", "hnsw_vector_cache_prefill").
			Debugf("skipping object with unreadable vector tail: %v", err)
		return nil, false
	}
	vec, err := storobj.VectorFromTail(tail, targetVector)
	if err != nil {
		var notFound storobj.ErrTargetVectorNotFound
		if !errors.As(err, &notFound) {
			h.logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping object with undecodable vector tail: %v", err)
		}
		return nil, false
	}
	return vec, true
}

// legacyVectorFromEntry serves the legacy (unnamed) vector, which sits at a fixed
// offset in the value's front: usually straight from the peek, else via a bounded
// prefix read — never the whole value.
func (h *hnsw) legacyVectorFromEntry(e *lsmkv.TargetedScanEntry) ([]float32, bool) {
	need, ok, err := storobj.LegacyVectorPrefixLen(e.Peek)
	if err != nil {
		h.logger.WithField("action", "hnsw_vector_cache_prefill").
			Debugf("skipping object with undecodable header: %v", err)
		return nil, false
	}
	if !ok || need > e.ValueSize {
		return h.wholeVectorFromEntry(e, "")
	}

	buf := e.Peek
	if uint64(len(buf)) < need {
		buf, err = e.ReadRange(0, need)
		if err != nil {
			h.logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping object with unreadable vector prefix: %v", err)
			return nil, false
		}
	}
	return h.decodeVectorRow(buf, "")
}

func (h *hnsw) wholeVectorFromEntry(e *lsmkv.TargetedScanEntry, targetVector string) ([]float32, bool) {
	whole, err := e.ReadRange(0, 0)
	if err != nil {
		h.logger.WithField("action", "hnsw_vector_cache_prefill").
			Debugf("skipping object with unreadable value: %v", err)
		return nil, false
	}
	return h.decodeVectorRow(whole, targetVector)
}

func (h *hnsw) decodeVectorRow(value []byte, targetVector string) ([]float32, bool) {
	// nil buffer forces a fresh allocation, so the vector never aliases scan buffers
	vec, err := storobj.VectorFromBinary(value, nil, targetVector)
	if err != nil {
		var notFound storobj.ErrTargetVectorNotFound
		if !errors.As(err, &notFound) {
			h.logger.WithField("action", "hnsw_vector_cache_prefill").
				Debugf("skipping object with undecodable vector: %v", err)
		}
		return nil, false
	}
	return vec, true
}
