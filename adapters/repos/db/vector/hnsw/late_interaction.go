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

package hnsw

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/tphakala/simd/f32"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func (h *hnsw) computeLateInteraction(ctx context.Context, queryVectors [][]float32, k int, candidateSet map[uint64]struct{}) ([]uint64, []float32, error) {
	// Convert map to slice for stride-based index access across workers.
	ids := make([]uint64, 0, len(candidateSet))
	for docID := range candidateSet {
		ids = append(ids, docID)
	}

	// Acquire a single consistent view for all disk reads to avoid per-candidate flushLock acquisitions.
	view := h.GetViewThunk()
	defer view.ReleaseView()

	resultsQueue := priorityqueue.NewMax[any](k)
	mu := sync.Mutex{}
	addResult := func(id uint64, sim float32) {
		mu.Lock()
		defer mu.Unlock()
		resultsQueue.Insert(id, sim)
		if resultsQueue.Len() > k {
			resultsQueue.Pop()
		}
	}

	// Respect the per-query concurrency budget if the context carries one
	// (see entities/concurrency): under concurrent load the budget shrinks
	// the fan-out, without one we fall back to the full rescore concurrency.
	// The floor of 1 keeps a zero budget from silently skipping rescoring.
	workers := max(1, min(concurrency.BudgetFromCtx(ctx, h.rescoreConcurrency), h.rescoreConcurrency, len(ids)))

	eg := enterrors.NewErrorGroupWrapper(h.logger)
	for workerID := 0; workerID < workers; workerID++ {
		workerID := workerID
		eg.Go(func() error {
			slice := h.pools.tempVectors.Get(int(h.dims.Load()))
			defer h.pools.tempVectors.Put(slice)

			for idPos := workerID; idPos < len(ids); idPos += workers {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("computeLateInteraction: %w", err)
				}
				docID := ids[idPos]
				sim, err := h.computeScoreWithView(ctx, queryVectors, docID, slice, view)
				if err != nil {
					h.logger.
						WithField("action", "computeLateInteraction").
						Warnf("could not compute score for docID %d: %v", docID, err)
					continue
				}
				addResult(docID, sim)
			}
			return nil
		}, h.logger)
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	distances := make([]float32, resultsQueue.Len())
	resultIDs := make([]uint64, resultsQueue.Len())
	i := len(resultIDs) - 1
	for resultsQueue.Len() > 0 {
		el := resultsQueue.Pop()
		resultIDs[i] = el.ID
		distances[i] = el.Dist
		i--
	}
	return resultIDs, distances, nil
}

func (h *hnsw) computeScore(searchVecs [][]float32, docID uint64) (float32, error) {
	h.RLock()
	vecIDs := h.docIDVectors[docID]
	h.RUnlock()
	var docVecs [][]float32
	if h.compressed.Load() {
		slice := h.pools.tempVectors.Get(int(h.dims.Load()))
		var err error
		docVecs, err = h.TempMultiVectorForIDThunk(context.Background(), docID, slice)
		if err != nil {
			return 0.0, errors.Wrap(err, "get vector for docID")
		}
		h.pools.tempVectors.Put(slice)
	} else {
		if !h.muvera.Load() {
			var errs []error
			docVecs, errs = h.multiVectorForID(context.Background(), vecIDs)
			for _, err := range errs {
				if err != nil {
					return 0.0, errors.Wrap(err, "get vector for docID")
				}
			}
		} else {
			var err error
			docVecs, err = h.cache.GetDoc(context.Background(), docID)
			if err != nil {
				return 0.0, errors.Wrap(err, "get muvera vector for docID")
			}
		}
	}

	return lateInteractionScore(h.multiDistancerProvider, searchVecs, docVecs)
}

func lateInteractionScore(provider distancer.Provider, searchVecs, docVecs [][]float32) (float32, error) {
	similarity := float32(0.0)

	// Fast path: the multi-vector aggregation uses plain dot products
	// (multiDistancerProvider is always a DotProductProvider), so one batched
	// SIMD kernel scores a query token against all doc tokens at once. The
	// generic path below is kept for equal-length validation errors and any
	// future non-dot provider.
	if provider.Type() == "dot" && equalVectorDims(searchVecs, docVecs) {
		dots := make([]float32, len(docVecs))
		for _, searchVec := range searchVecs {
			f32.DotProductBatch(dots, docVecs, searchVec)
			maxSim := float32(math.MaxFloat32)
			for _, dot := range dots {
				if dist := -dot; dist < maxSim {
					maxSim = dist
				}
			}
			similarity += maxSim
		}
		return similarity, nil
	}

	var dist distancer.Distancer
	for _, searchVec := range searchVecs {
		maxSim := float32(math.MaxFloat32)
		dist = provider.New(searchVec)

		for _, docVec := range docVecs {
			d, err := dist.Distance(docVec)
			if err != nil {
				return 0.0, errors.Wrap(err, "calculate distance between candidate and query")
			}
			if d < maxSim {
				maxSim = d
			}
		}

		similarity += maxSim
	}

	return similarity, nil
}

func (h *hnsw) computeScoreWithView(ctx context.Context, searchVecs [][]float32, docID uint64, slice *common.VectorSlice, view common.BucketView) (float32, error) {
	docVecs, err := h.TempMultiVectorForIDWithViewThunk(ctx, docID, slice, view)
	if err != nil {
		return 0, errors.Wrap(err, "get vectors for docID")
	}

	return lateInteractionScore(h.multiDistancerProvider, searchVecs, docVecs)
}

func equalVectorDims(searchVecs, docVecs [][]float32) bool {
	if len(searchVecs) == 0 {
		return false
	}
	dims := len(searchVecs[0])
	for _, v := range searchVecs[1:] {
		if len(v) != dims {
			return false
		}
	}
	for _, v := range docVecs {
		if len(v) != dims {
			return false
		}
	}
	return true
}
