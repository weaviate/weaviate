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

package traverser

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

type ResultContainer interface {
	AddScores(id uint64, targets []string, distances []float32, weights map[string]float32)
}

type ResultContainerHybrid struct {
	ResultsIn []*search.Result
	allIDs    map[uint64]*search.Result
}

func (r ResultContainerHybrid) AddScores(id uint64, targets []string, distances []float32, weights map[string]float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	newResult := *(r.allIDs[id])
	newResult.Dist = distances[0]
	r.ResultsIn = append(r.ResultsIn, &newResult)
}

type ResultContainerStandard struct {
	ResultsIn map[uint64]search.Result
}

type targetVectorData struct {
	target       []string
	searchVector [][]float32
}

func (r ResultContainerStandard) AddScores(id uint64, targets []string, distances []float32, weights map[string]float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	tmp := r.ResultsIn[id]
	for i := 0; i < len(targets); i++ {
		tmp.Dist += distances[i] * float32(weights[targets[i]])
	}
	r.ResultsIn[id] = tmp
}

func (e *Explorer) combineResults(ctx context.Context, results [][]search.Result, params dto.GetParams, targetVectors []string, searchVectors [][]float32) ([]search.Result, error) {
	if len(results) == 0 {
		return []search.Result{}, nil
	}

	if len(results) == 1 {
		return results[0], nil
	}

	allIDs := make(map[uint64]*search.Result)
	for _, res := range results {
		for _, r := range res {
			allIDs[*(r.DocID)] = &r
		}
	}
	missingIDs := make(map[uint64]targetVectorData)

	if params.TargetVectorJoin.ScoreFusion {
		weights := make([]float64, len(results))
		weightsMap := make(map[string]float32, len(results))
		for i, target := range targetVectors {
			weights[i] = 1. / float64(len(results))
			weightsMap[target] = float32(weights[i])
		}
		ress := make([][]*search.Result, len(results))
		for i, res := range results {
			localIDs := make(map[uint64]*search.Result, len(allIDs))
			for key, result := range allIDs {
				localIDs[key] = result
			}
			ress[i] = make([]*search.Result, len(res))
			for j := range res {
				delete(localIDs, *(res[j].DocID))

				ress[i][j] = &res[j]
				ress[i][j].SecondarySortValue = res[j].Dist
			}
			e.collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i)

			if err := e.getScoresOfMissingResults(ctx, missingIDs, ResultContainerHybrid{ResultsIn: ress[i], allIDs: allIDs}, params, weightsMap); err != nil {
				return nil, err
			}
			clear(missingIDs) // each target vector is handles separately for hybrid
		}
		joined := hybrid.FusionRelativeScore(weights, ress, targetVectors, false)
		joinedResults := make([]search.Result, len(joined))
		for i := range joined {
			joinedResults[i] = *joined[i]
			joinedResults[i].Dist = joined[i].Score
		}
		return joinedResults, nil
	}

	combinedResults := make(map[uint64]search.Result, len(results[0]))
	for i, res := range results {
		var localIDs map[uint64]*search.Result
		for _, r := range res {
			id := *(r.DocID)

			if params.TargetVectorJoin.Min {
				tmp := r
				if _, ok := combinedResults[id]; ok {
					tmp = combinedResults[id]
				}

				tmp.Dist = min(r.Dist, tmp.Dist)
				tmp.Certainty = min(r.Certainty, tmp.Certainty)
				combinedResults[id] = tmp
			} else {
				if len(localIDs) == 0 { // this is only needed if the join method is not Min
					localIDs = make(map[uint64]*search.Result, len(allIDs))
					for val := range allIDs {
						localIDs[val] = &search.Result{} // content does not matter here - the entry is only needed to combine hybrid search
					}

				}
				delete(localIDs, id)
				if len(params.TargetVectorJoin.Weights) != len(results) {
					return nil, fmt.Errorf("number of weights in join does not match number of results")
				}
				weight := params.TargetVectorJoin.Weights[targetVectors[i]]
				r.Score *= weight
				r.SecondarySortValue *= weight
				r.Dist *= weight
				if _, ok := combinedResults[id]; ok {
					r.Dist += combinedResults[id].Dist
				}
				combinedResults[id] = r
			}
		}
		e.collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i)
	}
	if !params.TargetVectorJoin.Min {
		if err := e.getScoresOfMissingResults(ctx, missingIDs, ResultContainerStandard{combinedResults}, params, params.TargetVectorJoin.Weights); err != nil {
			return nil, err
		}
	}

	limit := params.Pagination.Limit
	if limit > len(combinedResults) {
		limit = len(combinedResults)
	}

	queue := priorityqueue.NewMin[float32](limit)

	for id, res := range combinedResults {
		queue.Insert(id, res.Dist)
	}
	returnResults := make([]search.Result, 0, queue.Len())
	for i := 0; i < limit; i++ {
		item := queue.Pop()
		returnResults = append(returnResults, combinedResults[item.ID])
	}

	return returnResults, nil
}

func (e *Explorer) collectMissingIds(localIDs map[uint64]*search.Result, missingIDs map[uint64]targetVectorData, targetVectors []string, searchVectors [][]float32, i int) {
	for id := range localIDs {
		val, ok := missingIDs[id]
		if !ok {
			val = targetVectorData{target: []string{targetVectors[i]}, searchVector: [][]float32{searchVectors[i]}}
		} else {
			val.target = append(val.target, targetVectors[i])
			val.searchVector = append(val.searchVector, searchVectors[i])
		}
		missingIDs[id] = val
	}
}

func (e *Explorer) getScoresOfMissingResults(ctx context.Context, missingIDs map[uint64]targetVectorData, combinedResults ResultContainer, params dto.GetParams, weights map[string]float32) error {
	if len(missingIDs) == 0 {
		return nil
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(e.logger, ctx)
	eg.SetLimit(_NUMCPU * 2)
	lock := sync.Mutex{}
	for id, targets := range missingIDs {
		distances, err := e.searcher.VectorDistanceForQuery(ctx, params.ClassName, id, targets.target, targets.searchVector, params.Tenant)
		if err != nil {
			return errors.Wrap(err, "multi target search")
		}
		lock.Lock()
		combinedResults.AddScores(id, targets.target, distances, weights)
		lock.Unlock()
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
