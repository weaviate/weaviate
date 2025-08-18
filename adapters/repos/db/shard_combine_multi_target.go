//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	docId     uint64
	distance  float32
	distances []float32
}

type ResultContainer interface {
	AddScores(id uint64, targets []string, distances []float32, weights []float32, originalIndices []int)
	RemoveIdFromResult(id uint64)
}

type ResultContainerHybrid struct {
	ResultsIn     []*search.Result
	distancesById map[uint64][]float32

	IDsToRemove      map[uint64]struct{}
	allIDs           map[uint64]struct{}
	numTargetVectors int
}

func (r *ResultContainerHybrid) AddScores(id uint64, targets []string, distances []float32, weights []float32, originalIndices []int) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	newResult := &search.Result{SecondarySortValue: distances[0], DocID: &id, ID: uuidFromUint64(id)}
	distancesById, ok := r.distancesById[id]
	if !ok {
		distancesById = make([]float32, r.numTargetVectors)
	}
	distancesById[originalIndices[0]] = distances[0]
	r.distancesById[id] = distancesById
	r.ResultsIn = append(r.ResultsIn, newResult)
}

func (r *ResultContainerHybrid) RemoveIdFromResult(id uint64) {
	r.IDsToRemove[id] = struct{}{}
}

type ResultContainerStandard struct {
	ResultsIn map[uint64]idAndDistance
}

func (r *ResultContainerStandard) AddScores(id uint64, targets []string, distances []float32, weights []float32, originalIndices []int) {
	// we need to add a copy of the properties etc to make sure that the correct object is returned
	tmp := r.ResultsIn[id]
	for i := 0; i < len(targets); i++ {
		tmp.distance += distances[i] * weights[i]
		tmp.distances[originalIndices[i]] = distances[i]
	}
	r.ResultsIn[id] = tmp
}

func (r *ResultContainerStandard) RemoveIdFromResult(id uint64) {
	delete(r.ResultsIn, id)
}

type targetVectorData struct {
	target          []string
	searchVector    []models.Vector
	weights         []float32
	originalIndices []int // indices of the target vectors in the original searchVectors slice
}

func uuidFromUint64(id uint64) strfmt.UUID {
	return strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("%d", id))).String())
}

func CombineMultiTargetResults(ctx context.Context, shard DistanceForVector, logger logrus.FieldLogger, idsPerTarget [][]uint64, distsPerTarget [][]float32, targetVectors []string, searchVectors []models.Vector, targetCombination *dto.TargetCombination, limit int, targetDist float32) ([]uint64, search.Distances, error) {
	if len(idsPerTarget) == 0 {
		return []uint64{}, search.Distances{}, nil
	}

	if len(idsPerTarget) == 1 {
		distances := make(search.Distances, len(distsPerTarget[0]))
		for i := range distances {
			distances[i] = &search.Distance{
				Distance:             distsPerTarget[0][i],
				MultiTargetDistances: []float32{distsPerTarget[0][i]},
			}
		}
		return idsPerTarget[0], distances, nil
	}

	allNil := true
	for i := range idsPerTarget {
		if len(idsPerTarget[i]) > 0 {
			allNil = false
			break
		}
	}
	if allNil {
		return []uint64{}, search.Distances{}, nil
	}

	if targetCombination == nil {
		return nil, nil, fmt.Errorf("multi target combination is nil")
	}

	allIDs := make(map[uint64]struct{})
	for i := range idsPerTarget {
		for j := range idsPerTarget[i] {
			allIDs[idsPerTarget[i][j]] = struct{}{}
		}
	}
	missingIDs := make(map[uint64]targetVectorData)

	if targetCombination.Type == dto.RelativeScore {
		weights := make([]float64, len(idsPerTarget))
		for i := range targetVectors {
			weights[i] = float64(targetCombination.Weights[i])
		}

		scoresToRemove := make(map[uint64]struct{})

		fusionInput := make([][]*search.Result, len(idsPerTarget))
		distancesPerID := make(map[uint64][]float32, len(allIDs))
		for i := range idsPerTarget {
			localIDs := make(map[uint64]struct{}, len(allIDs))
			for key, result := range allIDs {
				localIDs[key] = result
			}
			fusionInput[i] = make([]*search.Result, len(idsPerTarget[i]))
			for j := range idsPerTarget[i] {
				delete(localIDs, idsPerTarget[i][j])
				docId := &idsPerTarget[i][j]
				// ID needs to be set because the fusion algorithm uses it to identify the objects - value doesn't matter as long as they are unique
				fusionInput[i][j] = &search.Result{SecondarySortValue: distsPerTarget[i][j], DocID: docId, ID: uuidFromUint64(*docId)}
				distances, ok := distancesPerID[idsPerTarget[i][j]]
				if !ok {
					distances = make([]float32, len(targetVectors))
				}
				distances[i] = distsPerTarget[i][j]
				distancesPerID[idsPerTarget[i][j]] = distances
			}
			collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i, targetCombination.Weights)
			resultContainer := ResultContainerHybrid{ResultsIn: fusionInput[i], allIDs: allIDs, IDsToRemove: make(map[uint64]struct{}), distancesById: distancesPerID, numTargetVectors: len(targetVectors)}
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
		if limit > len(joined) {
			limit = len(joined)
		}
		joined = joined[:limit]

		joinedResults := make([]uint64, len(joined))
		joinedDists := make(search.Distances, len(joined))
		for i := range joined {
			joinedResults[i] = *(joined[i].DocID)
			joinedDists[i] = &search.Distance{Distance: joined[i].Score, MultiTargetDistances: distancesPerID[*(joined[i].DocID)]}
		}

		return joinedResults, joinedDists, nil
	}

	combinedResults := make(map[uint64]idAndDistance, len(idsPerTarget[0]))
	for i := range idsPerTarget {
		if len(idsPerTarget[i]) > len(distsPerTarget[i]) {
			return nil, nil, fmt.Errorf("number of idsPerTarget does not match number of distances")
		}
		var localIDs map[uint64]struct{}

		if targetCombination.Type != dto.Minimum {
			localIDs = make(map[uint64]struct{}, len(allIDs))
			for val := range allIDs {
				localIDs[val] = struct{}{}
			}
		}

		for j := range idsPerTarget[i] {
			uid := idsPerTarget[i][j]

			tmp, ok := combinedResults[uid]
			if !ok {
				tmp = idAndDistance{docId: idsPerTarget[i][j], distance: 0, distances: make([]float32, len(targetVectors))}
			}
			if targetCombination.Type == dto.Minimum {
				if !ok {
					tmp.distance = distsPerTarget[i][j]
				}
				tmp.distance = min(tmp.distance, distsPerTarget[i][j])
			} else {
				delete(localIDs, uid)
				if len(targetCombination.Weights) != len(idsPerTarget) {
					return nil, nil, fmt.Errorf("number of weights in join does not match number of idsPerTarget")
				}

				weight := targetCombination.Weights[i]
				tmp.distance += weight * distsPerTarget[i][j]
			}
			tmp.distances[i] = distsPerTarget[i][j]
			combinedResults[uid] = tmp

		}
		collectMissingIds(localIDs, missingIDs, targetVectors, searchVectors, i, targetCombination.Weights)
	}
	if targetCombination.Type != dto.Minimum {
		if err := getScoresOfMissingResults(ctx, shard, logger, missingIDs, &ResultContainerStandard{combinedResults}, targetCombination.Weights); err != nil {
			return nil, nil, err
		}
	}

	// unlimited idsPerTarget
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
	returnDists := make(search.Distances, 0, queue.Len())
	lim := queue.Len()
	for i := 0; i < lim; i++ {
		item := queue.Pop()
		returnResults = append(returnResults, combinedResults[uuidCounter[item.ID]].docId)
		returnDists = append(returnDists, &search.Distance{Distance: combinedResults[uuidCounter[item.ID]].distance, MultiTargetDistances: combinedResults[uuidCounter[item.ID]].distances})
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
			val = targetVectorData{target: []string{targetVectors[i]}, searchVector: []models.Vector{searchVectors[i]}, weights: []float32{weights[i]}, originalIndices: []int{i}}
		} else {
			val.target = append(val.target, targetVectors[i])
			val.searchVector = append(val.searchVector, searchVectors[i])
			val.weights = append(val.weights, weights[i])
			val.originalIndices = append(val.originalIndices, i)
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
				combinedResults.AddScores(id, targets.target, distances, targets.weights, targets.originalIndices)
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
