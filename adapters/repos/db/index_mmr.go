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

package db

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	selector "github.com/weaviate/weaviate/adapters/repos/db/vector/selection"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

// applyGlobalMMR applies MMR post-processing to a merged pool of objects
// collected from all shards. Unlike per-shard MMR, this enforces diversity
// globally across the full candidate pool rather than independently per shard.
//
// objects and dists need not be pre-sorted; this function sorts them
// internally before running the greedy MMR selection.
//
// objects must carry vector payloads for the relevant targetVector (the caller
// is responsible for setting additionalProps.Vector/Vectors before issuing
// shard queries). originalAdditionalProps is used only to decide whether to
// strip the vector from returned objects after selection.
func (i *Index) applyGlobalMMR(
	ctx context.Context,
	selection *searchparams.Selection,
	targetVector string,
	objects []*storobj.Object,
	dists []float32,
	originalAdditionalProps additional.Properties,
) ([]*storobj.Object, []float32, error) {
	// Sort all candidates by ascending distance so that the best item is
	// first — the MMR greedy loop seeds from the nearest candidate.
	objects, dists = newDistancesSorter().sort(objects, dists)

	n := len(objects)
	if n == 0 || selection == nil || selection.MMR == nil {
		return objects, dists, nil
	}

	// Build the distance provider from the target vector's index config.
	var distProv distancer.Provider
	cfg := i.GetVectorIndexConfig(targetVector)
	switch cfg.DistanceName() {
	case "", vectorIndexCommon.DistanceCosine:
		distProv = distancer.NewCosineDistanceProvider()
	case vectorIndexCommon.DistanceDot:
		distProv = distancer.NewDotProductProvider()
	case vectorIndexCommon.DistanceL2Squared:
		distProv = distancer.NewL2SquaredProvider()
	case vectorIndexCommon.DistanceManhattan:
		distProv = distancer.NewManhattanProvider()
	case vectorIndexCommon.DistanceHamming:
		distProv = distancer.NewHammingProvider()
	default:
		distProv = distancer.NewCosineDistanceProvider()
	}

	distFn := func(a, b []float32) (float32, error) {
		return distProv.SingleDist(a, b)
	}

	// Assign sequential fake uint64 IDs and pre-build a vector lookup slice.
	// Using sequential indices lets us map selected IDs back to objects without
	// a separate hash-map lookup.
	ids := make([]uint64, n)
	vecMap := make([][]float32, n)
	for idx, obj := range objects {
		ids[idx] = uint64(idx)
		if targetVector == "" {
			vecMap[idx] = obj.Vector
		} else if obj.Vectors != nil {
			vecMap[idx] = obj.Vectors[targetVector]
		}
	}

	vecForID := func(_ context.Context, id uint64) ([]float32, error) {
		if int(id) >= len(vecMap) {
			return nil, fmt.Errorf("global MMR: vecForID out of range: %d", id)
		}
		return vecMap[id], nil
	}

	k := int(selection.MMR.Limit)
	sel, err := selector.New(selection, distFn, vecForID, k)
	if err != nil {
		return nil, nil, fmt.Errorf("global MMR selector: %w", err)
	}
	if sel == nil {
		// No selection algorithm active — simple truncation.
		if k > 0 && len(objects) > k {
			objects = objects[:k]
			dists = dists[:k]
		}
		return objects, dists, nil
	}

	selectedIDs, selectedDists, err := sel.Select(ctx, ids, dists)
	if err != nil {
		return nil, nil, fmt.Errorf("global MMR select: %w", err)
	}

	// Map sequential IDs back to original objects.
	result := make([]*storobj.Object, len(selectedIDs))
	resultDists := make([]float32, len(selectedIDs))
	for j, id := range selectedIDs {
		result[j] = objects[id] // id == sequential index into objects slice
		resultDists[j] = selectedDists[j]
	}

	// Strip vectors from results if the user did not originally request them.
	// They were only fetched internally to enable coordinator-level MMR.
	if targetVector == "" {
		if !originalAdditionalProps.Vector {
			for _, obj := range result {
				obj.Vector = nil
			}
		}
	} else {
		tvRequested := originalAdditionalProps.IncludeAllTargetVectors
		if !tvRequested {
			for _, v := range originalAdditionalProps.Vectors {
				if v == targetVector {
					tvRequested = true
					break
				}
			}
		}
		if !tvRequested {
			for _, obj := range result {
				if obj.Vectors != nil {
					delete(obj.Vectors, targetVector)
					if len(obj.Vectors) == 0 {
						obj.Vectors = nil
					}
				}
			}
		}
	}

	return result, resultDists, nil
}
