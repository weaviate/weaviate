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
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (h *hnsw) flatSearch(ctx context.Context, queryVector []float32, k, limit int,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	if !h.shouldRescore() {
		limit = k
	}

	h.RLock()
	nodeSize := uint64(len(h.nodes))
	h.RUnlock()

	var compressorDistancer compressionhelpers.CompressorDistancer
	if h.compressed.Load() {
		distancer, returnFn := h.compressor.NewDistancer(queryVector)
		defer returnFn()
		compressorDistancer = distancer
	}

	aggregateMu := &sync.Mutex{}
	results := priorityqueue.NewMax[any](limit)

	beforeIter := time.Now()
	// first extract all candidates, this reduces the amount of coordination
	// needed for the workers
	candidates := make([]uint64, 0, allowList.Len())
	it := allowList.Iterator()
	for candidate, ok := it.Next(); ok; candidate, ok = it.Next() {
		candidates = append(candidates, candidate)
	}

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < h.flatSearchConcurrency; workerID++ {
		workerID := workerID
		eg.Go(func() error {
			localResults := priorityqueue.NewMax[any](limit)
			var e storobj.ErrNotFound
			for idPos := workerID; idPos < len(candidates); idPos += h.flatSearchConcurrency {
				candidate := candidates[idPos]

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
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID, "flatSearch")
					continue
				}
				if err != nil {
					return err
				}

				addResult(localResults, candidate, dist, limit)
			}

			aggregateMu.Lock()
			defer aggregateMu.Unlock()
			for localResults.Len() > 0 {
				res := localResults.Pop()
				addResult(results, res.ID, res.Dist, limit)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
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

func (h *hnsw) flatMultiSearch(ctx context.Context, queryVector [][]float32, limit int,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	aggregateMu := &sync.Mutex{}
	results := priorityqueue.NewMax[any](limit)

	beforeIter := time.Now()
	// first extract all candidates, this reduces the amount of coordination
	// needed for the workers
	candidates := allowList.Slice()

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < h.flatSearchConcurrency; workerID++ {
		workerID := workerID
		eg.Go(func() error {
			localResults := priorityqueue.NewMax[any](limit)
			var e storobj.ErrNotFound
			for idPos := workerID; idPos < len(candidates); idPos += h.flatSearchConcurrency {
				candidate := candidates[idPos]

				dist, err := h.computeScore(queryVector, candidate)

				if errors.As(err, &e) {
					h.RLock()
					vecIDs := h.docIDVectors[candidate]
					h.RUnlock()
					for _, vecID := range vecIDs {
						h.handleDeletedNode(vecID, "flatSearch")
					}
					continue
				}
				if err != nil {
					return err
				}

				addResult(localResults, candidate, dist, limit)
			}

			aggregateMu.Lock()
			defer aggregateMu.Unlock()
			for localResults.Len() > 0 {
				res := localResults.Pop()
				addResult(results, res.ID, res.Dist, limit)
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	took := time.Since(beforeIter)
	helpers.AnnotateSlowQueryLog(ctx, "flat_search_iteration_took", took)

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

func addResult(results *priorityqueue.Queue[any], id uint64, dist float32, limit int) {
	if results.Len() < limit {
		results.Insert(id, dist)
		return
	}

	if results.Top().Dist > dist {
		results.Pop()
		results.Insert(id, dist)
	}
}
