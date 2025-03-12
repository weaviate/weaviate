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

package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

type DistanceForVector interface {
	VectorDistanceForQuery(ctx context.Context, id uint64, searchVectors []models.Vector, targets []string) ([]float32, error)
}

type idAndDistance struct {
	docId    uint64
	distance float32
}

type ResultContainer interface {
	AddScores(id uint64, targets []string, distances []float32, weights []float32)
	RemoveIdFromResult(id uint64)
}

type ResultContainerHybrid struct {
	ResultsIn []*search.Result

	IDsToRemove map[uint64]struct{}
	allIDs      map[uint64]struct{}
}

func (r *ResultContainerHybrid) AddScores(id uint64, targets []string, distances []float32, weights []float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	newResult := &search.Result{SecondarySortValue: distances[0], DocID: &id, ID: uuidFromUint64(id)}
	r.ResultsIn = append(r.ResultsIn, newResult)
}

func (r *ResultContainerHybrid) RemoveIdFromResult(id uint64) {
	r.IDsToRemove[id] = struct{}{}
}

type ResultContainerStandard struct {
	ResultsIn map[uint64]idAndDistance
}

func (r *ResultContainerStandard) AddScores(id uint64, targets []string, distances []float32, weights []float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	tmp := r.ResultsIn[id]
	for i := 0; i < len(targets); i++ {
		tmp.distance += distances[i] * weights[i]
	}
	r.ResultsIn[id] = tmp
}

func (r *ResultContainerStandard) RemoveIdFromResult(id uint64) {
	delete(r.ResultsIn, id)
}

type targetVectorData struct {
	target       []string
	searchVector []models.Vector
	weights      []float32
}

func uuidFromUint64(id uint64) strfmt.UUID {
	return strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("%d", id))).String())
}

func CombineMultiTargetResults(ctx context.Context, shard DistanceForVector, logger logrus.FieldLogger, results [][]uint64, dists [][]float32, targetVectors []string, searchVectors []models.Vector, targetCombination *dto.TargetCombination, limit int, targetDist float32) ([]uint64, []float32, error) {
	if len(results) == 0 {
		return []uint64{}, []float32{}, nil
	}

	if len(results) == 1 {
		return results[0], dists[0], nil
	}

	allNil := true
	for i := range results {
		if len(results[i]) > 0 {
			allNil = false
			break
		}
	}
	if allNil {
		return []uint64{}, []float32{}, nil
	}

	if targetCombination == nil {
		return nil, nil, fmt.Errorf("multi target combination is nil")
	}

	allIDs := make(map[uint64]struct{})
	for i := range results {
		for j := range results[i] {
			allIDs[results[i][j]] = struct{}{}
		}
	}
	missingIDs := make(map[uint64]targetVectorData)

	if targetCombination.Type == dto.RelativeScore {
		weights := make([]float64, len(results))
		for i := range targetVectors {
			weights[i] = float64(targetCombination.Weights[i])
		}

		scoresToRemove := make(map[uint64]struct{})

		fusionInput := make([][]*search.Result, len(results))
		for i := range results {
			localIDs := make(map[uint64]struct{}, len(allIDs))
			for key, result := range allIDs {
				localIDs[key] = result
			}
			fusionInput[i] = make([]*search.Result, len(results[i]))
			for j := range results[i] {
				delete(localIDs, results[i][j])
				docId := &results[i][j]
				// ID needs to be set because the fusion algorithm uses it to identify the objects - value doesn't matter as long as they are unique
				fusionInput[i][j] = &search.Result{SecondarySortValue: dists[i][j], DocID: docId, ID: uuidFromUint64(*docId)}
			}
			collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i, targetCombination.Weights)
			resultContainer := ResultContainerHybrid{ResultsIn: fusionInput[i], allIDs: allIDs, IDsToRemove: make(map[uint64]struct{})}
			if err := getScoresOfMissingResults(ctx, shard, logger, missingIDs, &resultContainer, targetCombination.Weights); err != nil {
				return nil, nil, err
			}
			for key := range resultContainer.IDsToRemove {
				scoresToRemove[key] = struct{}{}
			}
			fusionInput[i] = resultContainer.ResultsIn
			clear(missingIDs) // each target vector is handled separately for hybrid
		}

		// remove objects that have missing target vectors
		if len(scoresToRemove) > 0 {
			for i := range fusionInput {
				for j := len(fusionInput[i]) - 1; j >= 0; j-- {
					if _, ok := scoresToRemove[*fusionInput[i][j].DocID]; ok {
						if j < len(fusionInput[i])-1 {
							fusionInput[i] = append(fusionInput[i][:j], fusionInput[i][j+1:]...)
						} else {
							fusionInput[i] = fusionInput[i][:j]
						}
					}
				}
			}
		}

		joined := hybrid.FusionRelativeScore(weights, fusionInput, targetVectors, false)
		joinedResults := make([]uint64, len(joined))
		joinedDists := make([]float32, len(joined))
		for i := range joined {
			joinedResults[i] = *(joined[i].DocID)
			joinedDists[i] = joined[i].Score
		}

		if limit > len(joinedResults) {
			limit = len(joinedResults)
		}

		return joinedResults[:limit], joinedDists[:limit], nil
	}

	combinedResults := make(map[uint64]idAndDistance, len(results[0]))
	for i := range results {
		if len(results[i]) > len(dists[i]) {
			return nil, nil, fmt.Errorf("number of results does not match number of distances")
		}
		var localIDs map[uint64]struct{}

		if targetCombination.Type != dto.Minimum {
			localIDs = make(map[uint64]struct{}, len(allIDs))
			for val := range allIDs {
				localIDs[val] = struct{}{}
			}
		}

		for j := range results[i] {
			uid := results[i][j]

			tmp, ok := combinedResults[uid]

			if targetCombination.Type == dto.Minimum {
				if !ok {
					tmp = idAndDistance{docId: results[i][j], distance: dists[i][j]}
				}
				tmp.distance = min(tmp.distance, dists[i][j])
			} else {
				delete(localIDs, uid)
				if len(targetCombination.Weights) != len(results) {
					return nil, nil, fmt.Errorf("number of weights in join does not match number of results")
				}
				if !ok {
					tmp = idAndDistance{docId: results[i][j], distance: 0}
				}

				weight := targetCombination.Weights[i]
				tmp.distance += weight * dists[i][j]
			}
			combinedResults[uid] = tmp

		}
		collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i, targetCombination.Weights)
	}
	if targetCombination.Type != dto.Minimum {
		if err := getScoresOfMissingResults(ctx, shard, logger, missingIDs, &ResultContainerStandard{combinedResults}, targetCombination.Weights); err != nil {
			return nil, nil, err
		}
	}

	// unlimited results
	if limit < 0 {
		limit = len(combinedResults)
	}

	queue := priorityqueue.NewMin[float32](limit)
	uuidCounter := make([]uint64, len(combinedResults))
	count := uint64(0)
	for id, res := range combinedResults {
		if targetDist > 0 && res.distance > targetDist { // targetDist == 0 means no target distance is set
			continue
		}
		uuidCounter[count] = id
		queue.Insert(count, res.distance)
		count++

	}
	returnResults := make([]uint64, 0, queue.Len())
	returnDists := make([]float32, 0, queue.Len())
	lim := queue.Len()
	for i := 0; i < lim; i++ {
		item := queue.Pop()
		returnResults = append(returnResults, combinedResults[uuidCounter[item.ID]].docId)
		returnDists = append(returnDists, combinedResults[uuidCounter[item.ID]].distance)
	}

	if limit > len(returnResults) {
		limit = len(returnResults)
	}

	return returnResults[:limit], returnDists[:limit], nil
}

func collectMissingIds(localIDs map[uint64]struct{}, missingIDs map[uint64]targetVectorData, targetVectors []string, searchVectors []models.Vector, i int, weights []float32) {
	for id := range localIDs {
		val, ok := missingIDs[id]
		if !ok {
			val = targetVectorData{target: []string{targetVectors[i]}, searchVector: []models.Vector{searchVectors[i]}, weights: []float32{weights[i]}}
		} else {
			val.target = append(val.target, targetVectors[i])
			val.searchVector = append(val.searchVector, searchVectors[i])
			val.weights = append(val.weights, weights[i])
		}
		missingIDs[id] = val
	}
}

func getScoresOfMissingResults(ctx context.Context, shard DistanceForVector, logger logrus.FieldLogger, missingIDs map[uint64]targetVectorData, combinedResults ResultContainer, weights []float32) error {
	if len(missingIDs) == 0 {
		return nil
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(_NUMCPU * 2)
	mutex := sync.Mutex{}
	for id, targets := range missingIDs {
		id := id
		targets := targets
		f := func() error {
			distances, err := shard.VectorDistanceForQuery(ctx, id, targets.searchVector, targets.target)
			mutex.Lock()
			defer mutex.Unlock()
			if err != nil {
				// when we cannot look up missing distances for an object, it will be removed from the result list
				combinedResults.RemoveIdFromResult(id)
			} else {
				combinedResults.AddScores(id, targets.target, distances, targets.weights)
			}
			return nil
		}
		eg.Go(f)
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
