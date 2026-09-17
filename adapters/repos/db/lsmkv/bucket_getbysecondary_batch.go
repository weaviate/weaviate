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

import (
	"bytes"
	"context"
	"slices"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// secondaryBatchChunkSize is how many keys a worker claims per turn. One slow
// chunk then stalls one worker rather than the batch, and a worker stays on one
// index region. Tune via BenchmarkGetBySecondaryBatchShape.
const secondaryBatchChunkSize = 32

// secondaryBatchWorkers caps fan-out, bounding both concurrent reads and the
// per-worker read buffers (each grows to its largest read). The concurrency
// budget on the context may lower it; tune via BenchmarkGetBySecondaryBatchShape.
const secondaryBatchWorkers = 16

// GetBySecondaryBatch resolves keys (Replace strategy only) under one
// consistent view, using up to secondaryBatchWorkers goroutines concurrently.
//
// visit is called at most once per key with a value; i is the key's position
// in keys, and value is the bucket's read buffer, valid only until visit
// returns, so copy it to keep it. The first error ends the call; other keys may
// already have been visited.
func (b *Bucket) GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte, visit func(i int, value []byte) error) error {
	return b.getBySecondaryBatch(ctx, pos, keys, secondaryBatchChunkSize, secondaryBatchWorkers, visit)
}

func (b *Bucket) getBySecondaryBatch(ctx context.Context, pos int, keys [][]byte, chunkSize, maxWorkers int,
	visit func(i int, value []byte) error,
) error {
	if len(keys) == 0 {
		return nil
	}

	if chunkSize <= 0 {
		// A chunk size of zero or less would divide by zero below. Fall back to
		// the default and log it, since it can only be a caller bug.
		b.logger.WithField("action", "get_by_secondary_batch").
			Debugf("chunk size %d is not positive, resolving in chunks of %d", chunkSize, secondaryBatchChunkSize)
		chunkSize = secondaryBatchChunkSize
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	chunks := (len(keys) + chunkSize - 1) / chunkSize
	workers := concurrency.NumWorkers(ctx, chunks, maxWorkers)

	// Secondary keys are little-endian doc ids, so a byte-wise sort matches
	// on-disk order. The order slice maps each lookup back to its caller
	// position. A single chunk skips both, since there is nothing to separate.
	var order []int
	if chunks > 1 {
		order = make([]int, len(keys))
		for i := range order {
			order[i] = i
		}
		slices.SortFunc(order, func(x, y int) int { return bytes.Compare(keys[x], keys[y]) })
	}

	var nextChunk atomic.Int64
	resolveChunks := func(ctx context.Context) error {
		var buffer []byte
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk := int(nextChunk.Add(1)) - 1
			if chunk >= chunks {
				return nil
			}
			start := chunk * chunkSize
			end := min(start+chunkSize, len(keys))
			var err error
			if buffer, err = b.resolveSecondaryChunk(ctx, pos, keys, order, start, end, buffer, view, visit); err != nil {
				return err
			}
		}
	}

	// A single worker has nothing to overlap, so skip the error group's
	// goroutine/context/channel overhead; RunRecovered keeps panic handling
	// consistent with the group path.
	if workers == 1 {
		return enterrors.RunRecovered(b.logger, func() error { return resolveChunks(ctx) })
	}

	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(b.logger, ctx)
	for range workers {
		eg.Go(func() error { return resolveChunks(egCtx) })
	}
	return eg.Wait()
}

// resolveSecondaryChunk resolves keys[start:end] in order, or in caller order
// when order is nil. It reuses buffer across the lookups, so visit must consume
// each value before the next lookup overwrites it.
func (b *Bucket) resolveSecondaryChunk(ctx context.Context, pos int, keys [][]byte, order []int, start, end int,
	buffer []byte, view BucketConsistentView, visit func(i int, value []byte) error,
) ([]byte, error) {
	// Workers share one slow-query details lock; taking it per lookup would
	// serialise the fan-out, so entries are collected and recorded in one go.
	var entries []BucketSlowLogEntry
	if helpers.HasSlowQueryDetails(ctx) {
		entries = make([]BucketSlowLogEntry, 0, end-start)
	}
	defer func() {
		helpers.AnnotateSlowQueryLogAppendMany(ctx, SlowLogKeyGetBySecondaryWithView, entries)
	}()

	for p := start; p < end; p++ {
		i := p
		if order != nil {
			i = order[p]
		}
		value, newBuf, entry, fromSegments, err := b.getBySecondaryCore(pos, keys[i], buffer, view, 0)
		if err != nil {
			if lsmkv.IsDeletedOrNotFound(err) {
				continue
			}
			return nil, err
		}
		buffer = newBuf
		if fromSegments && entries != nil {
			entries = append(entries, entry)
		}
		if value == nil {
			// Nothing to hand to visit; callers already treat nil as absent.
			continue
		}
		if err := visit(i, value); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}
