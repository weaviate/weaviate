//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) flatSearch(ctx context.Context, queryVector []float32, k, limit int,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	if !h.shouldRescore() {
		limit = k
	}
	results := priorityqueue.NewMax[any](limit)

	h.RLock()
	nodeSize := uint64(len(h.nodes))
	h.RUnlock()

	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		distancer, returnFn := h.compressor.NewDistancer(queryVector)
		defer returnFn()
		compressorDistancer = distancer
	}

	beforeIter := time.Now()
	it := allowList.Iterator()
	for candidate, ok := it.Next(); ok; candidate, ok = it.Next() {
		if err := ctx.Err(); err != nil {
			helpers.AnnotateSlowQueryLog(ctx, "context_error", "flat_search_iteration")
			took := time.Since(beforeIter)
			helpers.AnnotateSlowQueryLog(ctx, "flat_search_iteration_took", took)
			return nil, nil, fmt.Errorf("flat search: iterating candidates: %w", err)
		}

		// Hot fix for https://github.com/weaviate/weaviate/issues/1937
		// this if statement mitigates the problem but it doesn't resolve the issue
		if candidate >= nodeSize {
			h.logger.WithField("action", "flatSearch").
				Debugf("trying to get candidate: %v but we only have: %v elements.",
					candidate, nodeSize)
			continue
		}

		h.shardedNodeLocks.RLock(candidate)
		c := h.nodes[candidate]
		h.shardedNodeLocks.RUnlock(candidate)

		if c == nil || h.hasTombstone(candidate) {
			continue
		}

		dist, err := h.distToNode(compressorDistancer, candidate, queryVector)
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID, "flatSearch")
			continue
		}
		if err != nil {
			return nil, nil, err
		}

		if results.Len() < limit {
			results.Insert(candidate, dist)
		} else if results.Top().Dist > dist {
			results.Pop()
			results.Insert(candidate, dist)
		}
	}
	took := time.Since(beforeIter)
	helpers.AnnotateSlowQueryLog(ctx, "flat_search_iteration_took", took)

	beforeRescore := time.Now()
	if h.shouldRescore() {
		compressorDistancer, fn := h.compressor.NewDistancer(queryVector)
		if err := h.rescore(ctx, results, k, compressorDistancer); err != nil {
			helpers.AnnotateSlowQueryLog(ctx, "context_error", "flat_search_rescore")
			took := time.Since(beforeRescore)
			helpers.AnnotateSlowQueryLog(ctx, "flat_search_rescore_took", took)
			return nil, nil, fmt.Errorf("flat search: %w", err)
		}
		fn()
		took := time.Since(beforeRescore)
		helpers.AnnotateSlowQueryLog(ctx, "flat_search_rescore_took", took)
	}

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())

	// results is ordered in reverse, we need to flip the order before presenting
	// to the user!
	i := len(ids) - 1
	for results.Len() > 0 {
		res := results.Pop()
		ids[i] = res.ID
		dists[i] = res.Dist
		i--
	}

	return ids, dists, nil
}
