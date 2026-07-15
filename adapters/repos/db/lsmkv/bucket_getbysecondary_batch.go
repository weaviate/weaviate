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

package lsmkv

// Batched secondary-key resolution on the Replace-strategy objects bucket
// (gh#309). It resolves many secondary keys under ONE consistent view in four
// phases: (0) a memtable pass, (1) per-segment newest-to-oldest index descents
// with newest-wins elimination, (2) offset-sorted bounded-concurrent value reads
// into one per-batch arena, and (3) a batched recheck that drops any hit
// superseded by a newer version of its primary key. Nothing here holds or
// re-enters a bucket lock across phase I/O (view refcounts only; see issues
// #11486 and #11678), and every returned value is copied out of the
// segment/memtable before the view is released so results stay valid afterwards
// (copy-under-refcount, issue #1837).

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// concurrentSecondaryBatches counts the secondary batches currently resolving
// across this process (== this node), single-key delegates included. It is the
// leading indicator for adding a node-wide read-concurrency cap: the batch
// resolver removes the serial-chain slowness that today limits how many cold
// queries a node accepts at once, so node-wide concurrency climbs as the speedup
// lands. Surfaced per batch in the slow log
// (BucketSlowLogEntry.ConcurrentBatchCount); sustained values well above the
// modeled range are the signal to bound node-wide concurrency before device queue
// latency rises.
var concurrentSecondaryBatches atomic.Int64

// secondaryBatchReadHook is a nil-in-production instrumentation seam around each
// phase-1 index descent AND each phase-2 value read. Tests inject it to (i) count
// peak in-flight operations (proving the phase actually runs concurrently, not
// accidentally serial) and (ii) inject a fixed per-op latency for a wall-time ratio
// assertion. onReadStart/onReadDone wrap each phase-2 value read; onDescentStart/
// onDescentDone wrap each phase-1 index descent. Each pair runs immediately before
// and after its op, inside the worker goroutine.
type secondaryBatchReadHook struct {
	onReadStart    func()
	onReadDone     func()
	onDescentStart func()
	onDescentDone  func()
}

// secondaryBatchViewHoldCap bounds the number of keys resolved under a single
// consistent view. GetBySecondaryBatch chunks larger inputs into slabs of this
// size, each slab acquiring and releasing its own view, so the window during which
// compaction must wait for outstanding readers (waitForReferenceCountToReachZero)
// stays bounded regardless of caller batch size. 500 = the BM25/filter `limit`
// ceiling.
const secondaryBatchViewHoldCap = 500

// defaultSecondaryBatchReadConcurrency is the phase-2 value-read semaphore default,
// used when no runtime value is configured. It is deliberately per-batch (not
// node-wide) and runtime-tunable (see WithSecondaryBatchReadConcurrency): raise it
// on a device with spare IOPS headroom, lower it on a throttled one.
const defaultSecondaryBatchReadConcurrency = 16

// secondaryBatchReadConcurrencyValue returns the effective read-concurrency bound: the
// runtime-configured value when set and positive, else the default 16. In the 4-phase
// path it bounds phase-2 value reads; in the chunked-pipeline path (Addendum 2) it is W,
// the worker count. Read once per batch so a mid-flight DynamicValue change never re-sizes
// an in-flight error group.
func (b *Bucket) secondaryBatchReadConcurrencyValue() int {
	if b.secondaryBatchReadConcurrency != nil {
		if v := b.secondaryBatchReadConcurrency.Get(); v > 0 {
			return v
		}
	}
	return defaultSecondaryBatchReadConcurrency
}

// secondaryChunkSize is the contiguous sorted-key chunk width the pipeline driver hands
// to each worker. 32 matches the empirically cold-winning chunk width of the pre-batch
// per-worker path (gh#309 pass-3c). It is a tuning knob, not a concurrency bound:
// shrinking it cuts the straggler tail (a chunk of all-cold-oldest-segment keys), widening
// it improves a worker's upper-tree-page cache locality. Follow-up: promote to a
// DynamicValue if the cold A/B shows tail sensitivity.
const secondaryChunkSize = 32

// secondaryBatchPipelineEnabled reports whether the Addendum-2 chunked-pipeline path is
// toggled on for this bucket. Nil (unconfigured) or false means the 4-phase path (= pr2).
func (b *Bucket) secondaryBatchPipelineEnabled() bool {
	return b.secondaryBatchPipeline != nil && b.secondaryBatchPipeline.Get()
}

// slowLogMaskedContext masks the slow_query_details accumulator for the per-key
// getBySecondaryCore calls the pipeline driver issues. Without the mask, W workers would
// each append a per-key lsm_get_by_secondary_batch entry, contending the accumulator mutex
// on the cold path and burying the single batch entry under n per-key entries; the driver
// annotates ONCE after eg.Wait() instead. Cancellation, deadline, and every other context
// value are delegated to the embedded context unchanged.
type slowLogMaskedContext struct{ context.Context }

func (c slowLogMaskedContext) Value(key any) any {
	if s, ok := key.(string); ok && s == "slow_query_details" {
		return nil
	}
	return c.Context.Value(key)
}

// GetBySecondaryBatch resolves multiple secondary keys, acquiring and releasing
// its own consistent view(s). Results are positionally aligned to keys: out[i]
// corresponds to keys[i], nil for a missing or deleted object. Inputs larger than
// secondaryBatchViewHoldCap are chunked internally into slabs, each slab under its
// own view, so the caller never has to think about the view-hold cap.
//
// Like GetBySecondary, this is limited to the Replace strategy.
func (b *Bucket) GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return [][]byte{}, nil
	}

	out := make([][]byte, len(keys))
	for start := 0; start < len(keys); start += secondaryBatchViewHoldCap {
		end := start + secondaryBatchViewHoldCap
		if end > len(keys) {
			end = len(keys)
		}

		view := b.GetConsistentView()
		res, err := b.GetBySecondaryBatchWithView(ctx, pos, keys[start:end], view)
		view.ReleaseView()
		if err != nil {
			return nil, err
		}
		copy(out[start:end], res)
	}
	return out, nil
}

// GetBySecondaryBatchWithView is like GetBySecondaryBatch but resolves all keys
// under an existing caller-supplied consistent view. The caller owns the view and
// its hold duration, so this entry point does NOT slab-chunk (the 500-key
// view-hold cap is enforced by GetBySecondaryBatch, which manages its own views).
// Results are positionally aligned to keys, nil for missing/deleted. Every
// returned value is copied out of the segment/memtable so results stay valid after
// the caller releases the view (copy-under-refcount, #1837).
func (b *Bucket) GetBySecondaryBatchWithView(ctx context.Context, pos int, keys [][]byte,
	view BucketConsistentView,
) ([][]byte, error) {
	if pos >= int(b.secondaryIndices) {
		return nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	n := len(keys)
	if n == 0 {
		return [][]byte{}, nil
	}

	// Count this batch in the node-wide concurrent-resolution gauge for its whole
	// lifetime, single-key delegates included, so the leading indicator does not
	// undercount. Released when the batch returns.
	concurrentBatches := concurrentSecondaryBatches.Add(1)
	defer concurrentSecondaryBatches.Add(-1)

	out := make([][]byte, n)
	if n == 1 {
		// delegate to the single-key path; copy the result so it survives view release
		v, _, err := b.getBySecondaryCore(ctx, pos, keys[0], nil, view, 0, "lsm_get_by_secondary_batch")
		if err != nil {
			if lsmkv.IsDeletedOrNotFound(err) {
				return out, nil // out[0] stays nil
			}
			return nil, err
		}
		out[0] = bytes.Clone(v)
		return out, nil
	}

	// Addendum-2 chunked-pipeline path (gh#309), behind the runtime toggle. When on, it
	// retires the 4-phase cross-key algorithm below in favour of a per-key concurrent
	// chunked driver over the single-key primitive. Toggle off (default) = the 4-phase
	// path (= pr2), so QA A/Bs pipeline-on vs pipeline-off in one binary, one knob flip.
	if b.secondaryBatchPipelineEnabled() {
		return b.getBySecondaryBatchPipelined(ctx, pos, keys, view, out, concurrentBatches, nil)
	}

	memtables, count := viewMemtables(view)
	segments := view.Disk

	// Phase 0 - memtable pass, walked in key-sorted order so the later per-segment
	// index descents touch upper DiskTree pages in key order (cache locality). Each
	// unresolved key carries its original position for positional restore; duplicate
	// keys are handled independently (each position resolves on its own).
	beforeMemtables := time.Now()
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return bytes.Compare(keys[order[a]], keys[order[b]]) < 0
	})

	unresolved := make([]secondaryBatchKey, 0, n)
	for _, oi := range order {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v, resolved, err := b.resolveSecondaryFromMemtables(pos, keys[oi], memtables, count, nil)
		if resolved {
			if err == nil {
				out[oi] = bytes.Clone(v)
			} else if !lsmkv.IsDeletedOrNotFound(err) {
				return nil, err
			}
			// lsmkv.Deleted / lsmkv.NotFound -> out[oi] stays nil
			continue
		}
		unresolved = append(unresolved, secondaryBatchKey{origIdx: oi, key: keys[oi]})
	}
	memtablesTook := time.Since(beforeMemtables)
	if len(unresolved) == 0 {
		return out, nil
	}

	// Phase 1 - per-segment sorted index descents, newest-to-oldest, unresolved-set
	// elimination on CONFIRMED index hits only (never a bloom pass). No value read.
	beforeIndex := time.Now()
	hits, err := b.disk.getBySecondaryBatchIndexHits(ctx, pos, unresolved, segments, nil)
	if err != nil {
		return nil, err
	}
	indexTook := time.Since(beforeIndex)
	if len(hits) == 0 {
		b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, 0, 0, concurrentBatches, 0)
		return out, nil
	}

	// Phase 2 - offset-sorted bounded-concurrent value reads. Live hits carry
	// priKey/value aliasing a single per-batch arena; tombstone hits drop to nil.
	// arenaBytes is the arena size, surfaced in the slow log for memory visibility.
	beforeValues := time.Now()
	lives, arenaBytes, err := b.disk.readSecondaryBatchValuesConcurrent(
		ctx, hits, segments, b.secondaryBatchReadConcurrencyValue(), nil)
	if err != nil {
		return nil, err
	}
	valuesTook := time.Since(beforeValues)
	if len(lives) == 0 {
		b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, valuesTook, 0, concurrentBatches, int64(arenaBytes))
		return out, nil
	}

	// Phase 3 - recheck: drop any live hit superseded by a newer version of its
	// primary key. Memtables first (newer than every segment), then newer segments.
	beforeRecheck := time.Now()
	kept := lives[:0]
	for _, lh := range lives {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		present, err := b.secondaryPresentInMemtables(lh.priKey, memtables, count)
		if err != nil {
			return nil, err
		}
		if !present {
			kept = append(kept, lh)
		}
	}
	lives = kept
	if err := b.disk.recheckSecondaryBatchInSegments(ctx, lives, segments); err != nil {
		return nil, err
	}
	recheckTook := time.Since(beforeRecheck)

	for i := range lives {
		if !lives[i].superseded {
			out[lives[i].origIdx] = lives[i].value
		}
	}
	b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, valuesTook, recheckTook, concurrentBatches, int64(arenaBytes))
	return out, nil
}

// getBySecondaryBatchPipelined is the Addendum-2 chunked-pipeline resolver (gh#309). It
// retires the 4-phase cross-key algorithm and instead drives the single-key primitive
// getBySecondaryCore once per key, over sorted contiguous secondaryChunkSize chunks, with
// W = secondaryBatchReadConcurrencyValue work-stealing workers under the one shared view.
// Each worker resolves its chunk's keys serially (one outstanding read at a time), so peak
// in-flight I/O per batch = W, identical to the 4-phase max(phase1, phase2) bound; the BxQ
// node envelope holds verbatim with B=W.
//
// Correctness is inherited from getBySecondaryCore, not re-derived: newest-wins across
// memtables and segments, the deleted+re-added-same-secondary-key recheck via
// existsWithConsistentViewUpTo, and copy-under-refcount (#1837) all come from the proven
// serial primitive. This driver only chunks and parallelizes it. Positional alignment
// (INV-LSMKV-BATCH-1) holds because each key resolves into its own disjoint out[origIdx]
// slot; concurrency reorders completion, never identity. Every returned value is
// bytes.Clone'd so it survives view release (no arena; per-value clone is the copy-out,
// and the getBySecondaryCore path is 0.085% of allocations, so the clone is the
// minimum-correct allocation, not an extra one).
//
// hook is nil in production; when set it wraps each per-key resolve (onReadStart/onReadDone)
// so the worker-concurrency gate can count peak in-flight workers.
func (b *Bucket) getBySecondaryBatchPipelined(ctx context.Context, pos int, keys [][]byte,
	view BucketConsistentView, out [][]byte, concurrentBatches int64, hook *secondaryBatchReadHook,
) ([][]byte, error) {
	n := len(keys)
	before := time.Now()

	// Sort keys once by bytes.Compare (= little-endian docID order) so a contiguous chunk
	// is a contiguous slice of the secondary keyspace, giving each worker a largely
	// disjoint leaf region of every segment's index (the cold fault-friendliness the A/B
	// tests). Keep the permutation to restore positional alignment.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, c int) bool {
		return bytes.Compare(keys[order[a]], keys[order[c]]) < 0
	})

	numChunks := (n + secondaryChunkSize - 1) / secondaryChunkSize
	workers := b.secondaryBatchReadConcurrencyValue()
	if workers > numChunks {
		workers = numChunks
	}

	var cursor atomic.Int64
	eg, egctx := enterrors.NewErrorGroupWithContextWrapper(b.logger, ctx)
	// Per-key calls take cancellation/deadline from egctx but must NOT each annotate the
	// slow log (W-way mutex contention + n buried entries); mask the accumulator and
	// annotate once after Wait.
	keyCtx := slowLogMaskedContext{Context: egctx}
	for w := 0; w < workers; w++ {
		eg.Go(func() error {
			// Per-worker buffer, reused across the worker's keys. getBySecondaryCore may
			// return a value aliasing this buffer, but we bytes.Clone it immediately, so
			// reuse on the next key is safe and no buffer is shared across workers.
			var buffer []byte
			for {
				if err := egctx.Err(); err != nil {
					return err
				}
				ci := int(cursor.Add(1)) - 1
				if ci >= numChunks {
					return nil
				}
				start := ci * secondaryChunkSize
				end := start + secondaryChunkSize
				if end > n {
					end = n
				}
				for _, oi := range order[start:end] {
					if err := egctx.Err(); err != nil {
						return err
					}
					if hook != nil && hook.onReadStart != nil {
						hook.onReadStart()
					}
					v, buf, err := b.getBySecondaryCore(keyCtx, pos, keys[oi], buffer, view, 0,
						"lsm_get_by_secondary_batch")
					if hook != nil && hook.onReadDone != nil {
						hook.onReadDone()
					}
					if err != nil {
						if lsmkv.IsDeletedOrNotFound(err) {
							continue // out[oi] stays nil (missing or deleted)
						}
						return err
					}
					buffer = buf // keep any grown buffer for the worker's next key
					out[oi] = bytes.Clone(v)
				}
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// Annotate once. The interleaved per-key shape has no distinct phases, so per-phase
	// fields are not populated; Total (aggregate wall) and the node-wide concurrent-batch
	// gauge are what the cold A/B reads.
	helpers.AnnotateSlowQueryLogAppend(ctx, "lsm_get_by_secondary_batch", BucketSlowLogEntry{
		Total:                time.Since(before),
		ConcurrentBatchCount: concurrentBatches,
	})
	return out, nil
}

// annotateSecondaryBatchSlowLog appends a per-phase timing entry for a batched
// secondary resolution under the lsm_get_by_secondary_batch log key, so an engineer
// reading the slow log sees WHERE the cold cost sits (index_descents vs value_reads
// vs recheck), the node-wide concurrent-batch count, and the phase-2 arena size.
func (b *Bucket) annotateSecondaryBatchSlowLog(ctx context.Context,
	memtables, indexDescents, valueReads, recheck time.Duration,
	concurrentBatches, arenaBytes int64,
) {
	helpers.AnnotateSlowQueryLogAppend(ctx, "lsm_get_by_secondary_batch", BucketSlowLogEntry{
		Total:                memtables + indexDescents + valueReads + recheck,
		ActiveMemtable:       memtables,
		Segments:             indexDescents + valueReads,
		IndexDescents:        indexDescents,
		ValueReads:           valueReads,
		Recheck:              recheck,
		ConcurrentBatchCount: concurrentBatches,
		ArenaBytes:           arenaBytes,
	})
}
