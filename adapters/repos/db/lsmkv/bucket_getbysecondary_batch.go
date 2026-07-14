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

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// secondaryBatchViewHoldCap bounds the number of keys resolved under a single
// consistent view. GetBySecondaryBatch chunks larger inputs into slabs of this
// size, each slab acquiring and releasing its own view, so the compaction
// ref-hold window (waitForReferenceCountToReachZero) stays bounded regardless of
// caller batch size. 500 = the BM25/filter `limit` ceiling.
const secondaryBatchViewHoldCap = 500

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
	if len(unresolved) == 0 {
		return out, nil
	}

	// Phase 1 - per-segment sorted index descents, newest-to-oldest, unresolved-set
	// elimination on CONFIRMED index hits only (never a bloom pass). No value read.
	hits, err := b.disk.getBySecondaryBatchIndexHits(ctx, pos, unresolved, segments)
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return out, nil
	}

	// Phase 2 - offset-sorted SERIAL value reads (3a). Live hits carry copied-out
	// priKey/value; tombstone hits are dropped (they resolve to nil).
	lives, err := b.disk.readSecondaryBatchValuesSerial(ctx, hits, segments)
	if err != nil {
		return nil, err
	}
	if len(lives) == 0 {
		return out, nil
	}

	// Phase 3 - recheck: drop any live hit superseded by a newer version of its
	// primary key. Memtables first (newer than every segment), then newer segments.
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

	for i := range lives {
		if !lives[i].superseded {
			out[lives[i].origIdx] = lives[i].value
		}
	}
	return out, nil
}
