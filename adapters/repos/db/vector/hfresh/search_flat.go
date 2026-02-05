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

package hfresh

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	flatSearchConcurrency = 1
)

func (h *HFresh) flatSearch(ctx context.Context, queryVector []float32, k int,
	allowList helpers.AllowList,
) ([]uint64, []float32, error) {
	aggregateMu := &sync.Mutex{}
	results := priorityqueue.NewMax[any](k)

	beforeIter := time.Now()
	// first extract all candidates, this reduces the amount of coordination
	// needed for the workers
	candidates := make([]uint64, 0, allowList.Len())
	it := allowList.Iterator()
	for candidate, ok := it.Next(); ok; candidate, ok = it.Next() {
		candidates = append(candidates, candidate)
	}

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < flatSearchConcurrency; workerID++ {
		workerID := workerID
		eg.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			localResults := priorityqueue.NewMax[any](k)
			for idPos := workerID; idPos < len(candidates); idPos += flatSearchConcurrency {
				candidate := candidates[idPos]

				dist, err := h.distToNode(ctx, candidate, queryVector)
				if err != nil {
					return err
				}

				addResult(localResults, candidate, dist, k)
			}
			if err := ctx.Err(); err != nil {
				return err
			}

			aggregateMu.Lock()
			defer aggregateMu.Unlock()
			for localResults.Len() > 0 {
				res := localResults.Pop()
				addResult(results, res.ID, res.Dist, k)
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

func (h *HFresh) distToNode(ctx context.Context, node uint64, vecB []float32) (float32, error) {
	var vecA []float32
	var err error
	vecA, err = h.vectorForId(ctx, node)
	if err != nil {
		// not a typed error, we can recover from, return with err
		return 0, errors.Wrapf(err,
			"could not get vector of object at docID %d", node)
	}

	if len(vecA) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", node)
	}

	if len(vecB) == 0 {
		return 0, fmt.Errorf(
			"got a nil or zero-length vector as search vector")
	}

	return h.distancer.distancer.SingleDist(vecA, vecB)
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
