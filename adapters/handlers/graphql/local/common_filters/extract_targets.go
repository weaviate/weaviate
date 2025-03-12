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
		var weightsIn map[string]interface{}
		if ok {
			weightsIn = weightsGQL.(map[string]interface{})
		}

		extractWeights := func(weightsIn map[string]interface{}, weightsOut []float32) error {
			handled := make(map[string]struct{})
			for i, target := range targetVectors {
				if _, ok := handled[target]; ok {
					continue
				} else {
					handled[target] = struct{}{}
				}
				weightForTarget, ok := weightsIn[target]
				if !ok {
					return fmt.Errorf("weight for target %s is not provided", target)
				}
				if weightIn, ok := weightForTarget.(float64); ok {
					weightsOut[i] = float32(weightIn)
				} else if weightsIn, ok := weightForTarget.([]float64); ok {
					for j, w := range weightsIn {
						weightsOut[i+j] = float32(w)
					}
				} else {
					return fmt.Errorf("weight for target %s is not a float or list of floats, got %v", target, weightForTarget)
				}
			}
			return nil
		}

		weights := make([]float32, len(targetVectors))
		switch targetCombinationType {
		case dto.Average:
			for i := range targetVectors {
				weights[i] = 1.0 / float32(len(targetVectors))
			}
		case dto.Sum:
			for i := range targetVectors {
				weights[i] = 1.0
			}
		case dto.Minimum:
		case dto.ManualWeights:
			if err := extractWeights(weightsIn, weights); err != nil {
				return nil, nil, err
			}
		case dto.RelativeScore:
			if err := extractWeights(weightsIn, weights); err != nil {
				return nil, nil, err
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
