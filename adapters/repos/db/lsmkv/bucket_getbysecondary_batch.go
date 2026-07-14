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

// Batched secondary-key resolution on the Replace-strategy objects bucket (child
// 3a of the gh#309 sorted/batched GetBySecondary design). This is the
// correct-but-SERIAL skeleton: it resolves many secondary keys under ONE
// consistent view with four phases (memtable pass, per-segment newest-wins index
// descents, offset-sorted value reads, batched recheck), but the value reads are
// single-threaded. Child 3b swaps phase 2 for bounded-concurrent offset-sorted
// reads plus an arena; nothing here holds or re-enters a bucket lock across phase
// I/O (view refcounts only, #11486/#11678), and every returned value is copied out
// of the segment/memtable before the view is released (copy-under-refcount, #1837).

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// concurrentSecondaryBatches counts the secondary batches currently in their I/O
// phases (1-3) across this process (== this node). It is the leading indicator for
// the store-level-semaphore revisit trigger (design § Resource-exhaustion Elephant
// 1): the fix removes the serial-chain slowness that today throttles node Q, so
// sustained post-fix Q climbs precisely because the fix worked. Surfaced per batch
// in the slow log (BucketSlowLogEntry.ConcurrentBatchCount); sustained values above
// the modeled Q band are the signal to add a store-level semaphore BEFORE r_await
// blows up in a load test.
var concurrentSecondaryBatches atomic.Int64

// secondaryBatchReadHook is a nil-in-production instrumentation seam around each
// phase-2 value read. The close-blocking concurrency-effectiveness gate (design
// Gap 1) injects it to (i) count peak in-flight reads and (ii) inject a fixed
// per-read latency for the wall-time ratio assertion. onReadStart runs immediately
// before, and onReadDone immediately after, each read, inside the read goroutine.
type secondaryBatchReadHook struct {
	onReadStart func()
	onReadDone  func()
}

// secondaryBatchViewHoldCap bounds the number of keys resolved under a single
// consistent view. GetBySecondaryBatch chunks larger inputs into slabs of this
// size, each slab acquiring and releasing its own view, so the compaction
// ref-hold window (waitForReferenceCountToReachZero) stays bounded regardless of
// caller batch size. 500 = the BM25/filter `limit` ceiling.
const secondaryBatchViewHoldCap = 500

// defaultSecondaryBatchReadConcurrency is the phase-2 value-read semaphore default
// (design § Resource-exhaustion: tuned to LTK's ~16k-IOPS device; DynamicValue-tunable
// per deployment). Composed with the caller's outer decode fan-out, NOT multiplied
// (the storobj I/O fan-out collapses into one batch; child 4).
const defaultSecondaryBatchReadConcurrency = 16

// secondaryBatchReadConcurrencyValue returns the effective phase-2 semaphore bound:
// the runtime-configured value when set and positive, else the default 16. Read once
// per batch so a mid-flight DynamicValue change never re-sizes an in-flight errgroup.
func (b *Bucket) secondaryBatchReadConcurrencyValue() int {
	if b.secondaryBatchReadConcurrency != nil {
		if v := b.secondaryBatchReadConcurrency.Get(); v > 0 {
			return v
		}
	}
	return defaultSecondaryBatchReadConcurrency
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

	// The batch now enters its I/O phases (1-3): record node-wide batch concurrency
	// for the slow-log leading indicator and release the slot when the batch returns.
	concurrentBatches := concurrentSecondaryBatches.Add(1)
	defer concurrentSecondaryBatches.Add(-1)

	// Phase 1 - per-segment sorted index descents, newest-to-oldest, unresolved-set
	// elimination on CONFIRMED index hits only (never a bloom pass). No value read.
	beforeIndex := time.Now()
	hits, err := b.disk.getBySecondaryBatchIndexHits(ctx, pos, unresolved, segments)
	if err != nil {
		return nil, err
	}
	indexTook := time.Since(beforeIndex)
	if len(hits) == 0 {
		b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, 0, 0, concurrentBatches)
		return out, nil
	}

	// Phase 2 - offset-sorted bounded-CONCURRENT value reads (child 3b). Live hits
	// carry priKey/value aliasing a single per-batch arena; tombstone hits drop to nil.
	beforeValues := time.Now()
	lives, _, err := b.disk.readSecondaryBatchValuesConcurrent(
		ctx, hits, segments, b.secondaryBatchReadConcurrencyValue(), nil)
	if err != nil {
		return nil, err
	}
	valuesTook := time.Since(beforeValues)
	if len(lives) == 0 {
		b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, valuesTook, 0, concurrentBatches)
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
	b.annotateSecondaryBatchSlowLog(ctx, memtablesTook, indexTook, valuesTook, recheckTook, concurrentBatches)
	return out, nil
}

// annotateSecondaryBatchSlowLog appends a per-phase timing entry for a batched
// secondary resolution under the lsm_get_by_secondary_batch log key, so an engineer
// reading the slow log sees WHERE the cold cost sits (index_descents vs value_reads
// vs recheck) plus the per-node concurrent-batch count (the store-level-semaphore
// revisit leading indicator, design § Resource-exhaustion Elephant 1).
func (b *Bucket) annotateSecondaryBatchSlowLog(ctx context.Context,
	memtables, indexDescents, valueReads, recheck time.Duration, concurrentBatches int64,
) {
	helpers.AnnotateSlowQueryLogAppend(ctx, "lsm_get_by_secondary_batch", BucketSlowLogEntry{
		Total:                memtables + indexDescents + valueReads + recheck,
		ActiveMemtable:       memtables,
		Segments:             indexDescents + valueReads,
		IndexDescents:        indexDescents,
		ValueReads:           valueReads,
		Recheck:              recheck,
		ConcurrentBatchCount: concurrentBatches,
	})
}
