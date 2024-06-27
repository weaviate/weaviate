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

package common_filters

import (
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// ExtractNearVector arguments, such as "vector" and "distance"
func ExtractNearVector(source map[string]interface{}) (searchparams.NearVector, *dto.TargetCombination, error) {
	var args searchparams.NearVector

	vectorGQL, okVec := source["vector"].([]interface{})
	vectorPerTarget, okVecPerTarget := source["vectorPerTarget"].(map[string][]float32)
	if (!okVec && !okVecPerTarget) || (okVec && okVecPerTarget) {
		return searchparams.NearVector{}, nil,
			fmt.Errorf("vector or vectorPerTarget is required field")
	}

	certainty, certaintyOK := source["certainty"]
	if certaintyOK {
		args.Certainty = certainty.(float64)
	}

	distance, distanceOK := source["distance"]
	if distanceOK {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	if certaintyOK && distanceOK {
		return searchparams.NearVector{}, nil,
			fmt.Errorf("cannot provide distance and certainty")
	}

	targetVectors, combination, err := ExtractTargets(source)
	if err != nil {
		return searchparams.NearVector{}, nil, err
	}
	args.TargetVectors = targetVectors

	if okVec {
		vector := make([]float32, len(vectorGQL))
		for i, value := range vectorGQL {
			vector[i] = float32(value.(float64))
		}
		if len(targetVectors) == 0 {
			args.VectorPerTarget = map[string][]float32{"": vector}
		} else {
			args.VectorPerTarget = make(map[string][]float32, len(targetVectors))
			for _, target := range targetVectors {
				args.VectorPerTarget[target] = vector
			}
		}
	}

	if okVecPerTarget {
		targets := make([]string, 0, len(vectorPerTarget))
		for target := range vectorPerTarget {
			targets = append(targets, target)
		}

		if len(targetVectors) == 0 {
			args.TargetVectors = targets
		} else {
			if len(targetVectors) != len(targets) {
				return searchparams.NearVector{}, nil,
					fmt.Errorf("number of targets (%d) does not match number of vectors (%d)", len(targetVectors), len(targets))
			}
			sort.Strings(targetVectors)
			sort.Strings(targets)
			for i, target := range targetVectors {
				if target != targets[i] {
					return searchparams.NearVector{}, nil,
						fmt.Errorf("targets do not match: %s != %s", target, targets[i])
				}
			}

		}
		args.VectorPerTarget = vectorPerTarget
	}

	return args, combination, nil
}
