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

	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Small enough that one slow chunk does not stall the batch, large enough that
// a worker stays on the same index pages.
const secondaryBatchChunkSize = 32

// More concurrent reads than this do not make a cold disk faster. The workers
// also run the caller's decode, and each keeps a read buffer grown to the
// largest value it read. The concurrency budget on the context can lower it.
const secondaryBatchWorkers = 16

// GetBySecondaryBatch resolves keys (Replace strategy only) under one
// consistent view, on up to secondaryBatchWorkers goroutines. visit runs
// concurrently, at most once per found key, with a value valid only until it
// returns. The first error ends the call; other keys may already be visited.
func (b *Bucket) GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte, visit func(i int, value []byte) error) error {
	if len(keys) == 0 {
		return nil
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	// Resolve in on-disk key order so each chunk walks one contiguous index
	// region; order maps each lookup back to its caller position.
	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(x, y int) int { return bytes.Compare(keys[x], keys[y]) })

	chunks := (len(keys) + secondaryBatchChunkSize - 1) / secondaryBatchChunkSize
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
			start := chunk * secondaryBatchChunkSize
			end := min(start+secondaryBatchChunkSize, len(keys))
			var err error
			if buffer, err = b.resolveSecondaryChunk(ctx, pos, keys, order[start:end], buffer, view, visit); err != nil {
				return err
			}
		}
	}

	workers := concurrency.NumWorkers(ctx, chunks, secondaryBatchWorkers)
	eg, egCtx := enterrors.NewErrorGroupWithContextWrapper(b.logger, ctx)
	for range workers {
		eg.Go(func() error { return resolveChunks(egCtx) })
	}
	return eg.Wait()
}

// resolveSecondaryChunk reuses buffer across lookups in idxs order, so visit
// must consume each value before the next lookup overwrites it.
func (b *Bucket) resolveSecondaryChunk(ctx context.Context, pos int, keys [][]byte, idxs []int,
	buffer []byte, view BucketConsistentView, visit func(i int, value []byte) error,
) ([]byte, error) {
	for _, i := range idxs {
		value, grown, err := b.getBySecondaryWithView(ctx, pos, keys[i], buffer, view)
		if err != nil {
			if lsmkv.IsDeletedOrNotFound(err) {
				continue
			}
			return nil, err
		}
		buffer = grown
		if err := visit(i, value); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}
