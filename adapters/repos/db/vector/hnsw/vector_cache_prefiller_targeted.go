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
	// prefillTargetedMinAvgEntrySize keeps small-object buckets on the cursor scan:
	// their rows mostly take the whole-read fallback anyway, so the targeted path
	// would only add its per-row index-walk overhead.
	prefillTargetedMinAvgEntrySize = 4 << 10
)

func prefillTargetedReadsEnabled() bool {
	return entcfg.Enabled(os.Getenv("HNSW_PREFILL_TARGETED_READS"))
}

func (h *hnsw) useTargetedPrefillScan(bucket *lsmkv.Bucket) bool {
	return prefillTargetedReadsEnabled() &&
		bucket.EstimatedEntrySize() >= prefillTargetedMinAvgEntrySize
}

// scanObjectVectorsTargeted feeds onVector from targeted reads of the objects
// bucket: a bounded peek per entry plus, for large schemas, only the vector-bearing
// tail. The underlying scan has no cross-segment merge and visits superseded and
// deleted versions too, so every row is filtered by node liveness first — which also
// skips the tail read for rows that no longer matter.
// scanObjectVectorsTargeted uses the newest-wins scan: superseded and deleted
// rows are hidden by in-memory key probes before any read, so the liveness filter
// only covers HNSW-side exclusions (unindexed or tombstoned nodes whose bucket
// row is still live).
func (h *hnsw) scanObjectVectorsTargeted(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	return bucket.ScanTargetedReplaceNewestWins(ctx, prefillPeekBytes, prefillScanParallelism(),
		h.targetedRowCallback(targetVector, onVector), h.logger)
}

// scanObjectVectorsTargetedAllVersions keeps the no-merge scan reachable for
// comparison benchmarks: stale rows surface and are filtered by node liveness
// alone, each costing a peek read.
func (h *hnsw) scanObjectVectorsTargetedAllVersions(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	return bucket.ScanTargetedReplace(ctx, prefillPeekBytes, prefillScanParallelism(),
		h.targetedRowCallback(targetVector, onVector), h.logger)
}

func (h *hnsw) targetedRowCallback(targetVector string, onVector prefillOnVector) func(*lsmkv.TargetedScanEntry) error {
	h.RLock()
	nodesLen := uint64(len(h.nodes))
	h.RUnlock()

	// tombstoned nodes stay in h.nodes until cleanup, but their objects-bucket rows
	// are already deleted — the merged cursor would hide them, so this scan must too
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

// nodeAlive reports whether id has a live node. Doc ids are never reused, so a
// superseded or deleted row's id has no live node; ids at or beyond the startup
// snapshot come from live inserts, which preload their own vectors.
func (h *hnsw) nodeAlive(id uint64, nodesLen uint64) bool {
	if id >= nodesLen {
		return false
	}
	h.shardedNodeLocks.RLock(id)
	defer h.shardedNodeLocks.RUnlock(id)
	// h.nodes can shrink under LockAll (index reset) after the snapshot; re-check
	// the bound against the current slice under the shard lock
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

// legacyVectorFromEntry serves the legacy (unnamed) vector, which sits at a fixed
// offset in the value's front: usually straight from the peek, else via a bounded
// prefix read — never the whole value.
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
