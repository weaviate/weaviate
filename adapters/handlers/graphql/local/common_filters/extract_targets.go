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

	"github.com/weaviate/weaviate/entities/dto"
)

func ExtractTargets(source map[string]interface{}) ([]string, *dto.TargetCombination, error) {
	targets, ok := source["targets"]
	if ok {
		targetsGql, ok := targets.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("targets is not a map, got %v", targets)
		}
		targetVectorsGQL, ok := targetsGql["targetVectors"]
		if !ok {
			return nil, nil, fmt.Errorf("targetVectors is required field, got %v", targets)
		}
		targetVectorsArray, ok := targetVectorsGQL.([]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("targetVectors is not an array, got %v", targetVectorsGQL)
		}
		targetVectors := make([]string, len(targetVectorsArray))
		for i, value := range targetVectorsArray {
			targetVectors[i], ok = value.(string)
			if !ok {
				return nil, nil, fmt.Errorf("target vector is not a string, got %v", value)
			}
		}

		combinationType, ok := targetsGql["combinationMethod"]
		targetCombinationType := dto.DefaultTargetCombinationType
		if ok {
			targetCombinationType, ok = combinationType.(dto.TargetCombinationType)
			if !ok {
				return nil, nil, fmt.Errorf("combinationMethod is not a TargetCombinationType, got %v", combinationType)
			}
		}

		weightsGQL, ok := targetsGql["weights"]
		var weightsIn map[string]float64
		if ok {
			weightsIn = weightsGQL.(map[string]float64)
		}

		weights := make(map[string]float32, len(targetVectors))
		switch targetCombinationType {
		case dto.Average:
			for _, target := range targetVectors {
				weights[target] = 1.0 / float32(len(targetVectors))
			}
		case dto.Sum:
			for _, target := range targetVectors {
				weights[target] = 1.0
			}
		case dto.Minimum:
		case dto.ManualWeights:
			if len(weightsIn) != len(targetVectors) {
				return nil, nil, fmt.Errorf("number of weights (%d) does not match number of targets (%d)", len(weightsIn), len(targetVectors))
			}
			for k, v := range weightsIn {
				weights[k] = float32(v)
			}
		case dto.RelativeScore:
			if len(weightsIn) != len(targetVectors) {
				return nil, nil, fmt.Errorf("number of weights (%d) does not match number of targets (%d)", len(weightsIn), len(targetVectors))
			}
			for k, v := range weightsIn {
				weights[k] = float32(v)
			}
		default:
			return nil, nil, fmt.Errorf("unknown combination method %v", targetCombinationType)
		}
		return targetVectors, &dto.TargetCombination{Weights: weights, Type: targetCombinationType}, nil

	} else {
		targetVectorsGQL, ok := source["targetVectors"]
		if ok {
			targetVectorsArray := targetVectorsGQL.([]interface{})
			targetVectors := make([]string, len(targetVectorsArray))
			for i, value := range targetVectorsArray {
				targetVectors[i] = value.(string)
			}
			return targetVectors, &dto.TargetCombination{Type: dto.Minimum}, nil
		}
	}
	return nil, nil, nil
}
