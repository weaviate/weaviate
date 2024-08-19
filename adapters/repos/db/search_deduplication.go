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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/storobj"
)

func searchResultDedup(out []*storobj.Object, dists []float32) ([]*storobj.Object, []float32, error) {
	type indexAndScore struct {
		i     int
		score float32
	}
	allKeys := make(map[strfmt.UUID]indexAndScore)
	filteredObjects := make([]*storobj.Object, 0, len(out))
	filteredScores := make([]float32, 0, len(dists))

	i := 0
	// Iterate over all the objects, the corresponding score is always dists[j] for object at index j
	for j, obj := range out {
		// If we have encountered the object before lookup the score of the current object vs the previous one. If
		// the score is better then we keep this one by replacing it in filtered arrays in place, if not we ignore
		// it and move on
		val, ok := allKeys[obj.ID()]
		if ok {
			// If the store distance is bigger than the current object distance we want to replace the object we
			// have in the filtered array
			if val.score > dists[j] {
				// Update in place in the filtered arrays
				filteredObjects[val.i] = obj
				filteredScores[val.i] = dists[j]
				// Update the score stored in the map tracking what we have seen so far
				allKeys[obj.ID()] = indexAndScore{val.i, dists[j]}
			}
		} else {
			// We have never seen that object before, append to the filtered arrays and add the tracking map
			filteredObjects = append(filteredObjects, obj)
			filteredScores = append(filteredScores, dists[j])
			allKeys[obj.ID()] = indexAndScore{i: i, score: dists[j]}
			i++
		}
	}
	if len(filteredObjects) != len(filteredScores) {
		return []*storobj.Object{}, []float32{}, fmt.Errorf("length of object and scores should be equal obj=%d vs dists=%d", len(filteredObjects), len(filteredScores))
	}
	return filteredObjects, filteredScores, nil
}
