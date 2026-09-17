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

// secondaryBatchChunkSize is how many keys a worker claims at a time. Small
// enough that one slow chunk does not stall the batch, large enough that a
// worker stays on the same index pages. BenchmarkGetBySecondaryBatchShape
// sweeps it.
const secondaryBatchChunkSize = 32

// secondaryBatchWorkers caps the fan-out. The workers also run the caller's
// visit, and each holds a read buffer grown to the largest value it read, so
// the cap bounds memory as well as concurrent reads. The concurrency budget on
// the context can lower it. BenchmarkGetBySecondaryBatchShape sweeps it.
const secondaryBatchWorkers = 16

// GetBySecondaryBatch resolves keys (Replace strategy only) under one
// consistent view, on up to secondaryBatchWorkers goroutines.
//
// visit is called at most once per key that has a value, with i the key's
// position in keys and value the bucket's own read buffer: it holds only until
// visit returns, so copy anything that outlives the call. visit may run on
// several goroutines at once. The first error ends the call, and other keys may
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

	view := b.GetConsistentView()
	defer view.ReleaseView()

	chunks := (len(keys) + chunkSize - 1) / chunkSize
	workers := concurrency.NumWorkers(ctx, chunks, maxWorkers)

	// The secondary key is the little-endian doc id, so sorting the keys
	// byte-wise puts them in the order the index stores them; order maps each
	// lookup back to its caller position. A single chunk is resolved in caller
	// order, since there is no second chunk for the sort to separate it from.
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

	// A single worker has nothing to overlap with, so the error group would only
	// add a goroutine, a derived context and a channel. Recover a panic the way
	// the group's goroutines do, so both paths report one as an error.
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
	// The workers share one slow-query details lock, so taking it per lookup
	// would serialise the fan-out. Collect the chunk's entries instead and
	// record them in one go.
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
			// A key whose stored value is nil has nothing to hand to visit, and
			// the callers treat a nil value as an absent one anyway.
			continue
		}
		if err := visit(i, value); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}
