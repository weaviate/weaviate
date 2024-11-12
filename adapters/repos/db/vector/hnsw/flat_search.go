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

func (h *hnsw) flatSearch(queryVector []float32, k, limit int,
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

	// first extract all candidates, this reduces the amount of coordination
	// needed for the workers
	beforeExtractCandidates := time.Now()
	candidates := make([]uint64, 0, allowList.Len())
	it := allowList.Iterator()
	for candidate, ok := it.Next(); ok; candidate, ok = it.Next() {
		candidates = append(candidates, candidate)
	}
	fmt.Printf("extract candidates took %s\n", time.Since(beforeExtractCandidates))

	beforeDistances := time.Now()
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
					return nil
				}

				h.shardedNodeLocks.RLock(candidate)
				c := h.nodes[candidate]
				h.shardedNodeLocks.RUnlock(candidate)

				if c == nil || h.hasTombstone(candidate) {
					return nil
				}

				dist, err := h.distToNode(compressorDistancer, candidate, queryVector)
				if errors.As(err, &e) {
					h.handleDeletedNode(e.DocID, "flatSearch")
					return nil
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
	fmt.Printf("distance calculations took %s\n", time.Since(beforeDistances))

	beforeRescore := time.Now()
	if h.shouldRescore() {
		compressorDistancer, fn := h.compressor.NewDistancer(queryVector)
		h.rescore(results, k, compressorDistancer)
		fn()
	}
	fmt.Printf("rescore took %s\n", time.Since(beforeRescore))

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
