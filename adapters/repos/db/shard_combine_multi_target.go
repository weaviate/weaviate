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

	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"

	"github.com/go-openapi/strfmt"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type DistanceForVector interface {
	VectorDistanceForQuery(ctx context.Context, id strfmt.UUID, searchVectors [][]float32, targets []string) ([]float32, error)
}

type objAndDistance struct {
	obj      *storobj.Object
	distance float32
}

type ResultContainer interface {
	AddScores(id strfmt.UUID, targets []string, distances []float32, weights map[string]float32)
	RemoveIdFromResult(id strfmt.UUID)
}

type ResultContainerHybrid struct {
	ResultsIn []*search.Result

	IDsToRemove map[strfmt.UUID]struct{}
	allIDs      map[strfmt.UUID]*storobj.Object
}

func (r *ResultContainerHybrid) AddScores(id strfmt.UUID, targets []string, distances []float32, weights map[string]float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	newResult := &search.Result{SecondarySortValue: distances[0], ID: id}
	r.ResultsIn = append(r.ResultsIn, newResult)
}

func (r *ResultContainerHybrid) RemoveIdFromResult(id strfmt.UUID) {
	r.IDsToRemove[id] = struct{}{}
}

type ResultContainerStandard struct {
	ResultsIn map[strfmt.UUID]objAndDistance
}

func (r *ResultContainerStandard) AddScores(id strfmt.UUID, targets []string, distances []float32, weights map[string]float32) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	tmp := r.ResultsIn[id]
	for i := 0; i < len(targets); i++ {
		tmp.distance += distances[i] * weights[targets[i]]
	}
	r.ResultsIn[id] = tmp
}

func (r *ResultContainerStandard) RemoveIdFromResult(id strfmt.UUID) {
	delete(r.ResultsIn, id)
}

type targetVectorData struct {
	target       []string
	searchVector [][]float32
}

func CombineMultiTargetResults(ctx context.Context, shard DistanceForVector, logger logrus.FieldLogger, results [][]*storobj.Object, dists [][]float32, targetVectors []string, searchVectors [][]float32, multiTargetCombination *dto.TargetCombination, limit int, targetDist float32) ([]*storobj.Object, []float32, error) {
	if len(results) == 0 {
		return []*storobj.Object{}, []float32{}, nil
	}

	if len(results) == 1 {
		return results[0], dists[0], nil
	}

	allIDs := make(map[strfmt.UUID]*storobj.Object)
	for i := range results {
		for j := range results[i] {
			allIDs[results[i][j].ID()] = results[i][j]
		}
	}
	missingIDs := make(map[strfmt.UUID]targetVectorData)

	if multiTargetCombination.Type == dto.RelativeScore {
		weights := make([]float64, len(results))
		for i, target := range targetVectors {
			weights[i] = float64(multiTargetCombination.Weights[target])
		}

		scoresToRemove := make(map[strfmt.UUID]struct{})

		fusionInput := make([][]*search.Result, len(results))
		for i := range results {
			localIDs := make(map[strfmt.UUID]*storobj.Object, len(allIDs))
			for key, result := range allIDs {
				localIDs[key] = result
			}
			fusionInput[i] = make([]*search.Result, len(results[i]))
			for j := range results[i] {
				delete(localIDs, results[i][j].ID())
				fusionInput[i][j] = &search.Result{SecondarySortValue: dists[i][j], ID: results[i][j].ID()}
			}
			collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i)
			resultContainer := ResultContainerHybrid{ResultsIn: fusionInput[i], allIDs: allIDs, IDsToRemove: make(map[strfmt.UUID]struct{})}
			if err := getScoresOfMissingResults(ctx, shard, logger, missingIDs, &resultContainer, multiTargetCombination.Weights, allIDs); err != nil {
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
				for j := range fusionInput[i] {
					if _, ok := scoresToRemove[fusionInput[i][j].ID]; ok {
						fusionInput[i] = append(fusionInput[i][:j], fusionInput[i][j+1:]...)
					}
				}
			}
		}

		joined := hybrid.FusionRelativeScore(weights, fusionInput, targetVectors, false)
		joinedResults := make([]*storobj.Object, len(joined))
		joinedDists := make([]float32, len(joined))
		for i := range joined {
			joinedResults[i] = allIDs[joined[i].ID]
			joinedDists[i] = joined[i].Score
		}
		if len(joinedResults) > limit {
			joinedResults = joinedResults[:limit]
			joinedDists = joinedDists[:limit]
		}
		return joinedResults[:limit], joinedDists[:limit], nil
	}

	combinedResults := make(map[strfmt.UUID]objAndDistance, len(results[0]))
	for i := range results {
		if len(results[i]) > len(dists[i]) {
			return nil, nil, fmt.Errorf("number of results does not match number of distances")
		}
		var localIDs map[strfmt.UUID]*storobj.Object
		for j := range results[i] {
			uid := results[i][j].ID()

			tmp, ok := combinedResults[uid]

			if multiTargetCombination.Type == dto.Minimum {
				if !ok {
					tmp = objAndDistance{obj: results[i][j], distance: dists[i][j]}
				}
				tmp.distance = min(tmp.distance, dists[i][j])
			} else {
				if len(localIDs) == 0 { // this is only needed if the join method is not Min
					localIDs = make(map[strfmt.UUID]*storobj.Object, len(allIDs))
					for val := range allIDs {
						localIDs[val] = &storobj.Object{} // content does not matter here - the entry is only needed to combine hybrid search
					}

				}
				delete(localIDs, uid)
				if len(multiTargetCombination.Weights) != len(results) {
					return nil, nil, fmt.Errorf("number of weights in join does not match number of results")
				}
				if !ok {
					tmp = objAndDistance{obj: results[i][j], distance: 0}
				}

				weight := multiTargetCombination.Weights[targetVectors[i]]
				tmp.distance += weight * dists[i][j]
			}
			combinedResults[uid] = tmp

		}
		collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i)
	}
	if multiTargetCombination.Type != dto.Minimum {
		if err := getScoresOfMissingResults(ctx, shard, logger, missingIDs, &ResultContainerStandard{combinedResults}, multiTargetCombination.Weights, allIDs); err != nil {
			return nil, nil, err
		}
	}

	queue := priorityqueue.NewMin[float32](limit)
	uuidCounter := make([]strfmt.UUID, len(combinedResults))
	count := uint64(0)
	for id, res := range combinedResults {
		if targetDist > 0 && res.distance > targetDist { // targetDist == 0 means no target distance is set
			continue
		}
		uuidCounter[count] = id
		queue.Insert(count, res.distance)
		count++

	}
	returnResults := make([]*storobj.Object, 0, queue.Len())
	returnDists := make([]float32, 0, queue.Len())
	lim := queue.Len()
	for i := 0; i < lim; i++ {
		item := queue.Pop()
		returnResults = append(returnResults, combinedResults[uuidCounter[item.ID]].obj)
		returnDists = append(returnDists, combinedResults[uuidCounter[item.ID]].distance)
	}

	if limit > len(returnResults) {
		limit = len(returnResults)
	}

	return returnResults[:limit], returnDists[:limit], nil
}

func collectMissingIds(localIDs map[strfmt.UUID]*storobj.Object, missingIDs map[strfmt.UUID]targetVectorData, targetVectors []string, searchVectors [][]float32, i int) {
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

func getScoresOfMissingResults(ctx context.Context, shard DistanceForVector, logger logrus.FieldLogger, missingIDs map[strfmt.UUID]targetVectorData, combinedResults ResultContainer, weights map[string]float32, allIDs map[strfmt.UUID]*storobj.Object) error {
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
			distances, err := shard.VectorDistanceForQuery(ctx, allIDs[id].ID(), targets.searchVector, targets.target)
			mutex.Lock()
			if err != nil {
				// when we cannot look up missing distances for an object, it will be removed from the result list
				combinedResults.RemoveIdFromResult(id)
			} else {
				combinedResults.AddScores(id, targets.target, distances, weights)
			}
			mutex.Unlock()
			return nil
		}
		eg.Go(f)
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
