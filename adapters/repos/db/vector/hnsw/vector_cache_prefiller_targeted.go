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
	"sync/atomic"

	"github.com/sirupsen/logrus"

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
// schemas, only the vector-bearing tail. The bucket hides superseded and deleted
// rows itself, so nothing here re-filters for them.
func (h *hnsw) scanObjectVectorsTargeted(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	// a bucket that is not uuid-keyed would skip every row, so report the count once
	// rather than leaving an empty cache explained only by per-row debug lines
	var foreign atomic.Int64
	err := bucket.ScanTargetedReplace(ctx, prefillPeekBytes, prefillScanParallelism(),
		h.targetedRowCallback(targetVector, onVector, &foreign), h.logger)
	if n := foreign.Load(); n > 0 {
		h.logger.WithFields(logrus.Fields{
			"action": "hnsw_vector_cache_prefill",
			"rows":   n,
		}).Warn("skipped object rows whose uuid does not match their bucket key")
	}
	return err
}

func (h *hnsw) targetedRowCallback(targetVector string, onVector prefillOnVector,
	foreign *atomic.Int64,
) func(*lsmkv.TargetedScanEntry) error {
	return func(e *lsmkv.TargetedScanEntry) error {
		if err := objectRowMatchesKey(e.Key, e.Peek); err != nil {
			foreign.Add(1)
			h.prefillSkipDebug("row does not match its key", err)
			return nil
		}
		id, err := storobj.DocIDFromBinary(e.Peek)
		if err != nil {
			h.prefillSkipDebug("undecodable doc id", err)
			return nil
		}
		// onVector enforces this too; here it saves the tail read
		if !h.prefillEligible(id) {
			return nil
		}
		vec, ok := h.targetedVectorFromEntry(e, targetVector)
		if !ok || len(vec) == 0 {
			return nil
		}
		return onVector(id, vec)
	}
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
	if ok && tailStart >= e.ValueSize {
		// corrupt: the front sections decoded but place the tail past the value end.
		// Skipped here rather than surfaced as a downstream ReadRange bounds error.
		h.prefillSkipDebug("vector tail beyond value end",
			fmt.Errorf("tail offset %d, value size %d", tailStart, e.ValueSize))
		return nil, false
	}
	if !ok || schemaLen < prefillTargetedMinSchemaLen {
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
	if ok && need > e.ValueSize {
		// corrupt, and as above: skipped here rather than downstream
		h.prefillSkipDebug("legacy vector beyond value end",
			fmt.Errorf("needs %d bytes, value size %d", need, e.ValueSize))
		return nil, false
	}
	if !ok {
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

// objectRowMatchesKey guards the one corruption the scan cannot catch on its
// own: an index node whose offsets point at a different live row passes every
// bounds check, and its bytes would be cached under this key's doc id. The
// objects bucket is keyed by uuid and the row repeats it, so they must agree.
func objectRowMatchesKey(key, peek []byte) error {
	if len(key) == 0 {
		return nil // not the objects bucket layout; nothing to check against
	}
	rowID, ok := storobj.UUIDFromPeek(peek)
	if !ok {
		return nil // peek too short to tell; other decoders report the truncation
	}
	if !bytes.Equal(key, rowID) {
		return fmt.Errorf("key %x holds a row for %x", key, rowID)
	}
	return nil
}

// prefillStoppedByShutdown tells a prefill that was stopped from one that failed. A
// scan reports context.Canceled both for a teardown and for a read that fails against
// an already-cancelled parent, so only the prefill's own context — which nothing but
// teardown cancels — separates the two. Both drivers latch the first error before
// cancelling, so a genuine failure never arrives here as context.Canceled.
func prefillStoppedByShutdown(err error, prefillCtx context.Context) bool {
	return errors.Is(err, context.Canceled) && prefillCtx.Err() != nil
}
