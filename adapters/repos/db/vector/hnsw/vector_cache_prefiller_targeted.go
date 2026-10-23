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
	// prefillTargetedMinAvgEntrySize is deliberately well above the crossover measured
	// on gp3 (~40KB): below it the two reads per row cost more than the bytes they
	// save, and admitting too early costs a multiple where admitting too late only
	// leaves a win unclaimed. See the PR for the curve.
	prefillTargetedMinAvgEntrySize = 128 << 10
	// prefillTailProbeRows is how many rows the gate samples to check the tail read
	// will actually fire; enough to tell a uniform collection apart from a stray row.
	prefillTailProbeRows = 16
	// uuidKeyLen is the objects bucket's key width: a marshalled uuid.
	uuidKeyLen = 16
)

// prefillTargetedReadsEnv selects a read strategy for the scan. It cannot turn the scan
// off; HNSW_PREFILL_SCAN_WORKERS is the revert path.
const prefillTargetedReadsEnv = "HNSW_PREFILL_TARGETED_READS"

func prefillTargetedReadsEnabled() bool {
	return entcfg.Enabled(os.Getenv(prefillTargetedReadsEnv))
}

func (h *hnsw) useTargetedPrefillScan(ctx context.Context, bucket *lsmkv.Bucket) bool {
	if !prefillTargetedReadsEnabled() ||
		bucket.EstimatedEntrySize() < prefillTargetedMinAvgEntrySize {
		return false
	}
	// A legacy vector sits at a fixed front offset, so it is always served from the
	// peek or a bounded prefix; only the named-vector tail can fail to resolve.
	if h.getTargetVector() == "" {
		return true
	}
	return h.tailReadsFire(ctx, bucket)
}

// tailReadsFire reports whether this bucket's rows will actually take the tail read.
// VectorTailOffsetFromPrefix resolves only when the peek reaches the schema-length
// field, which a legacy vector alongside the named ones pushes out of range from about
// 116 dimensions up. That is a property of the collection rather than of a row, which
// is why it is a gate: see prefillTargetedMinAvgEntrySize for the cost of admitting a
// bucket whose rows all fall back.
//
// The cursor materializes whole rows before this slices them to the peek, so on a
// bucket past the admission gate the probe reads megabytes. It therefore takes a
// permit from the same node-wide pool as the scan it is deciding on, and polls the
// prefill's context: routing runs before the scan does, so without both, every index
// on a restoring node probes at once and none of them can be stopped by teardown.
func (h *hnsw) tailReadsFire(ctx context.Context, bucket *lsmkv.Bucket) bool {
	_, release, err := acquirePrefillWorkers(ctx, 1, h.logger)
	if err != nil {
		return false
	}
	defer release()

	c := bucket.CursorReplaceReusable()
	defer c.Close()

	seen, firing := 0, 0
	for k, v := c.First(); k != nil && seen < prefillTailProbeRows; k, v = c.Next() {
		if ctx.Err() != nil {
			return false
		}
		if len(v) == 0 {
			continue
		}
		seen++
		peek := v
		if len(peek) > prefillPeekBytes {
			peek = peek[:prefillPeekBytes]
		}
		_, schemaLen, ok, err := storobj.VectorTailOffsetFromPrefix(peek)
		if err == nil && ok && schemaLen >= prefillTargetedMinSchemaLen {
			firing++
		}
	}
	if seen == 0 {
		return false
	}
	if firing*2 <= seen {
		h.logger.WithFields(logrus.Fields{
			"action":  "hnsw_vector_cache_prefill",
			"sampled": seen,
			"firing":  firing,
		}).Debug("targeted scan disabled: rows do not resolve a vector tail from the peek")
		return false
	}
	return true
}

// targetedScanStats records how each row was served. The whole-value fallback is what
// decides whether this path helps or hurts: a row that takes it reads the same bytes as
// the cursor scan through a per-row pread, and pays for the peek on top. A scan that
// fell back on every row is otherwise indistinguishable from one that did not, since
// both decode the same vectors.
type targetedScanStats struct {
	tail    atomic.Int64
	whole   atomic.Int64
	foreign atomic.Int64
	// ioFailed counts rows whose bytes could not be read at all, as opposed to rows
	// that decoded fine and simply carry no vector for this target. The two are
	// indistinguishable in the cache that results, but only this one means the volume
	// failed underneath the scan, and it must not be reported as a clean finish.
	ioFailed atomic.Int64
}

func (s *targetedScanStats) fields() logrus.Fields {
	return logrus.Fields{
		"tail_reads":            s.tail.Load(),
		"whole_value_fallbacks": s.whole.Load(),
		"unreadable_rows":       s.ioFailed.Load(),
	}
}

// scanObjectVectorsTargeted reads a bounded peek per row plus, for large
// schemas, only the vector-bearing tail. The bucket hides superseded and deleted
// rows itself, so nothing here re-filters for them.
func (h *hnsw) scanObjectVectorsTargeted(ctx context.Context, bucket *lsmkv.Bucket,
	targetVector string, onVector prefillOnVector,
) error {
	// same node-wide pool as the cursor scan: both are bound by the volume, and a
	// node restoring many tenants runs one of these per named vector per shard
	parallel, release, err := acquirePrefillWorkers(ctx, prefillScanParallelism(), h.logger)
	if err != nil {
		return err
	}
	defer release()

	var stats targetedScanStats
	err = bucket.ScanTargetedReplace(ctx, prefillPeekBytes, parallel,
		h.targetedRowCallback(targetVector, onVector, &stats), h.logger)

	entry := h.logger.WithFields(stats.fields()).
		WithFields(logrus.Fields{"action": "hnsw_vector_cache_prefill", "index_id": h.id})

	// A read that failed leaves that vector to be fetched on the first query touching
	// it, so the index still answers correctly; what it must not do is finish quietly,
	// because cachePrefilled is set either way and nothing retries the prefill.
	if n := stats.ioFailed.Load(); n > 0 {
		entry.Warnf("targeted vector cache prefill scan finished with %d rows unread; "+
			"those vectors load on demand and the prefill is not retried", n)
	} else {
		entry.Info("targeted vector cache prefill scan finished")
	}

	// a bucket that is not uuid-keyed would skip every row, so report the count once
	// rather than leaving an empty cache explained only by per-row debug lines
	if n := stats.foreign.Load(); n > 0 {
		entry.WithField("rows", n).
			Warn("skipped object rows whose uuid does not match their bucket key")
	}
	return err
}

func (h *hnsw) targetedRowCallback(targetVector string, onVector prefillOnVector,
	stats *targetedScanStats,
) func(*lsmkv.TargetedScanEntry) error {
	return func(e *lsmkv.TargetedScanEntry) error {
		if err := checkObjectRowKey(e.Key, e.Peek); err != nil {
			stats.foreign.Add(1)
			h.logSkippedRow("row does not match its key", err)
			return nil
		}
		id, err := storobj.DocIDFromBinary(e.Peek)
		if err != nil {
			h.logSkippedRow("undecodable doc id", err)
			return nil
		}
		// onVector enforces this too; here it saves the tail read
		if !h.prefillEligible(id) {
			return nil
		}
		vec, ok := h.targetedVectorFromEntry(e, targetVector, stats)
		if !ok || len(vec) == 0 {
			return nil
		}
		return onVector(id, vec)
	}
}

func (h *hnsw) logSkippedRow(reason string, err error) {
	h.logger.WithField("action", "hnsw_vector_cache_prefill").
		Debugf("skipping object with %s: %v", reason, err)
}

func (h *hnsw) targetedVectorFromEntry(e *lsmkv.TargetedScanEntry, targetVector string,
	stats *targetedScanStats,
) ([]float32, bool) {
	if targetVector == "" {
		return h.legacyVectorFromEntry(e, stats)
	}

	tailStart, schemaLen, ok, err := storobj.VectorTailOffsetFromPrefix(e.Peek)
	if err != nil {
		h.logSkippedRow("undecodable header", err)
		return nil, false
	}
	if ok && tailStart >= e.ValueSize {
		// corrupt: the front sections decoded but place the tail past the value end.
		// Skipped here rather than surfaced as a downstream ReadRange bounds error.
		h.logSkippedRow("vector tail beyond value end",
			fmt.Errorf("tail offset %d, value size %d", tailStart, e.ValueSize))
		return nil, false
	}
	if !ok || schemaLen < prefillTargetedMinSchemaLen {
		return h.vectorFromWholeValue(e, targetVector, stats)
	}

	stats.tail.Add(1)
	tail, err := e.ReadRange(tailStart, 0)
	if err != nil {
		stats.ioFailed.Add(1)
		h.logSkippedRow("unreadable vector tail", err)
		return nil, false
	}
	vec, err := storobj.VectorFromTail(tail, targetVector)
	if err != nil {
		if !vectorTargetMissing(err) {
			h.logSkippedRow("undecodable vector tail", err)
		}
		return nil, false
	}
	return vec, true
}

// vectorTargetMissing separates a row that simply has no vector for this target from a
// row that failed to decode. The first is ordinary — an index exists per target vector
// and rows need not carry every one — so it is skipped without a log line.
func vectorTargetMissing(err error) bool {
	var notFound storobj.ErrTargetVectorNotFound
	return errors.As(err, &notFound)
}

// legacyVectorFromEntry serves the legacy vector, which sits at a fixed front offset:
// from the peek, or via a bounded prefix read, never the whole value.
func (h *hnsw) legacyVectorFromEntry(e *lsmkv.TargetedScanEntry, stats *targetedScanStats) ([]float32, bool) {
	need, ok, err := storobj.LegacyVectorPrefixLen(e.Peek)
	if err != nil {
		h.logSkippedRow("undecodable header", err)
		return nil, false
	}
	if ok && need > e.ValueSize {
		// corrupt, and as above: skipped here rather than downstream
		h.logSkippedRow("legacy vector beyond value end",
			fmt.Errorf("needs %d bytes, value size %d", need, e.ValueSize))
		return nil, false
	}
	if !ok {
		return h.vectorFromWholeValue(e, "", stats)
	}

	buf := e.Peek
	if uint64(len(buf)) < need {
		buf, err = e.ReadRange(0, need)
		if err != nil {
			stats.ioFailed.Add(1)
			h.logSkippedRow("unreadable vector prefix", err)
			return nil, false
		}
	}
	return h.decodeVectorRow(buf, "")
}

func (h *hnsw) vectorFromWholeValue(e *lsmkv.TargetedScanEntry, targetVector string,
	stats *targetedScanStats,
) ([]float32, bool) {
	stats.whole.Add(1)

	// ReadRange has no short-circuit, so a row already complete in the peek would pay a
	// second read for bytes in hand. A bucket averaging past the admission gate still
	// carries small rows, and every one whose schema falls under the tail gate lands here.
	whole := e.Peek
	if uint64(len(whole)) < e.ValueSize {
		var err error
		if whole, err = e.ReadRange(0, 0); err != nil {
			stats.ioFailed.Add(1)
			h.logSkippedRow("unreadable value", err)
			return nil, false
		}
	}
	return h.decodeVectorRow(whole, targetVector)
}

func (h *hnsw) decodeVectorRow(value []byte, targetVector string) ([]float32, bool) {
	// nil buffer forces a fresh allocation, so the vector never aliases scan buffers
	vec, err := storobj.VectorFromBinary(value, nil, targetVector)
	if err != nil {
		if !vectorTargetMissing(err) {
			h.logSkippedRow("undecodable vector", err)
		}
		return nil, false
	}
	return vec, true
}

// checkObjectRowKey catches a segment index whose offsets land on a different live
// row, which passes every bounds check. The doc id comes from the same bytes as the
// vector, so the pair preloaded would be self-consistent; what it would not be is this
// key's, and the duplicate consumes the slot reserved for a key that then never gets
// cached. The objects bucket is keyed by uuid and the row repeats it, so the two agree
// unless the segment index is wrong.
func checkObjectRowKey(key, peek []byte) error {
	// Bucket.Put accepts any key, so an exact uuid length is part of the invariant
	// rather than a precondition for checking it: a row stored under a short key is
	// already not something this bucket's readers can address.
	if len(key) != uuidKeyLen {
		return fmt.Errorf("objects bucket key is %d bytes, not a uuid", len(key))
	}
	rowID, ok := storobj.UUIDFromPrefix(peek)
	if !ok {
		return nil // peek too short to tell; other decoders report the truncation
	}
	if !bytes.Equal(key, rowID) {
		return fmt.Errorf("key %x holds a row for %x", key, rowID)
	}
	return nil
}
