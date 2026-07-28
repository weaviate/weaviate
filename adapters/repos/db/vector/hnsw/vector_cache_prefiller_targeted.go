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
	// prefillTargetedMinAvgEntrySize keeps small-object buckets on the cursor
	// scan, where the targeted path would only add index-walk overhead.
	prefillTargetedMinAvgEntrySize = 4 << 10
)

func prefillTargetedReadsEnabled() bool {
	return entcfg.Enabled(os.Getenv("HNSW_PREFILL_TARGETED_READS"))
}

func (h *hnsw) useTargetedPrefillScan(bucket *lsmkv.Bucket) bool {
	return prefillTargetedReadsEnabled() &&
		bucket.EstimatedEntrySize() >= prefillTargetedMinAvgEntrySize
}

// scanObjectVectorsTargeted reads a bounded peek per row plus, for large
// schemas, only the vector-bearing tail. The scan hides superseded and deleted
// rows itself; the liveness filter covers HNSW-side exclusions only.
func (h *hnsw) scanObjectVectorsTargeted(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	return bucket.ScanTargetedReplace(ctx, prefillPeekBytes, prefillScanParallelism(),
		h.targetedRowCallback(targetVector, onVector), h.logger)
}

func (h *hnsw) targetedRowCallback(targetVector string, onVector prefillOnVector) func(*lsmkv.TargetedScanEntry) error {
	h.RLock()
	nodesLen := uint64(len(h.nodes))
	h.RUnlock()

	// tombstoned nodes stay in h.nodes until cleanup while their bucket rows may
	// still be live — they must not be prefilled
	h.tombstoneLock.RLock()
	tombstoned := make(map[uint64]struct{}, len(h.tombstones))
	for id := range h.tombstones {
		tombstoned[id] = struct{}{}
	}
	h.tombstoneLock.RUnlock()

	return func(e *lsmkv.TargetedScanEntry) error {
		id, err := storobj.DocIDFromBinary(e.Peek)
		if err != nil {
			h.prefillSkipDebug("undecodable doc id", err)
			return nil
		}
		if !h.nodeAlive(id, nodesLen) {
			return nil
		}
		if _, dead := tombstoned[id]; dead {
			return nil
		}
		vec, ok := h.targetedVectorFromEntry(e, targetVector)
		if !ok || len(vec) == 0 {
			return nil
		}
		return onVector(id, vec)
	}
}

// nodeAlive: doc ids are never reused, so a superseded or deleted row's id has
// no live node; ids beyond the snapshot come from inserts that self-preload.
func (h *hnsw) nodeAlive(id uint64, nodesLen uint64) bool {
	if id >= nodesLen {
		return false
	}
	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)
	// h.nodes can shrink under LockAll (index reset); re-check under the shard lock
	nodes := h.nodes
	if id >= uint64(len(nodes)) {
		return false
	}
	return nodes[id] != nil
}

func (h *hnsw) prefillSkipDebug(reason string, err error) {
	h.logger.WithField("action", "hnsw_vector_cache_prefill").
		Debugf("skipping object with %s: %v", reason, err)
}

func (h *hnsw) targetedVectorFromEntry(e *lsmkv.TargetedScanEntry, targetVector string) ([]float32, bool) {
	if targetVector == "" {
		return h.legacyVectorFromEntry(e)
	}

	tailStart, schemaLen, ok, err := storobj.VectorTailOffsetFromPeek(e.Peek)
	if err != nil {
		h.prefillSkipDebug("undecodable header", err)
		return nil, false
	}
	if !ok || schemaLen < prefillTargetedMinSchemaLen || tailStart >= e.ValueSize {
		return h.wholeVectorFromEntry(e, targetVector)
	}

	tail, err := e.ReadRange(tailStart, 0)
	if err != nil {
		h.prefillSkipDebug("unreadable vector tail", err)
		return nil, false
	}
	vec, err := storobj.VectorFromTail(tail, targetVector)
	if err != nil {
		var notFound storobj.ErrTargetVectorNotFound
		if !errors.As(err, &notFound) {
			h.prefillSkipDebug("undecodable vector tail", err)
		}
		return nil, false
	}
	return vec, true
}

// legacyVectorFromEntry: the legacy vector sits at a fixed front offset — served
// from the peek, or via a bounded prefix read, never the whole value.
func (h *hnsw) legacyVectorFromEntry(e *lsmkv.TargetedScanEntry) ([]float32, bool) {
	need, ok, err := storobj.LegacyVectorPrefixLen(e.Peek)
	if err != nil {
		h.prefillSkipDebug("undecodable header", err)
		return nil, false
	}
	if !ok || need > e.ValueSize {
		return h.wholeVectorFromEntry(e, "")
	}

	buf := e.Peek
	if uint64(len(buf)) < need {
		buf, err = e.ReadRange(0, need)
		if err != nil {
			h.prefillSkipDebug("unreadable vector prefix", err)
			return nil, false
		}
	}
	return h.decodeVectorRow(buf, "")
}

func (h *hnsw) wholeVectorFromEntry(e *lsmkv.TargetedScanEntry, targetVector string) ([]float32, bool) {
	whole, err := e.ReadRange(0, 0)
	if err != nil {
		h.prefillSkipDebug("unreadable value", err)
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
			h.prefillSkipDebug("undecodable vector", err)
		}
		return nil, false
	}
	return vec, true
}
