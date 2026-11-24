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

package dto

import (
	"errors"
	"fmt"
	"math"
)

// Target combination method names in the REST API.
const (
	TargetCombinationMethodSum           = "sum"
	TargetCombinationMethodAverage       = "average"
	TargetCombinationMethodMinimum       = "minimum"
	TargetCombinationMethodManualWeights = "manualWeights"
	TargetCombinationMethodRelativeScore = "relativeScore"
)

// NewTargetCombination builds a multi-target combination from a method name
// and weights keyed by target name, converted to the engine's positional
// weights. An empty method gives nil for at most one target and the default
// otherwise. Only manualWeights and relativeScore take weights, one per
// target. Errors are client errors.
func NewTargetCombination(method string, targets []string, weightsByName map[string]float64) (*TargetCombination, error) {
	var combinationType TargetCombinationType
	switch method {
	case "":
		if len(weightsByName) > 0 {
			return nil, errors.New("weights are only allowed with manualWeights or relativeScore")
		}
		if len(targets) <= 1 {
			return nil, nil
		}
		return &TargetCombination{Type: DefaultTargetCombinationType}, nil
	case TargetCombinationMethodSum:
		combinationType = Sum
	case TargetCombinationMethodAverage:
		combinationType = Average
	case TargetCombinationMethodMinimum:
		combinationType = Minimum
	case TargetCombinationMethodManualWeights:
		combinationType = ManualWeights
	case TargetCombinationMethodRelativeScore:
		combinationType = RelativeScore
	default:
		return nil, fmt.Errorf("unknown target combination method %q, expected one of sum, average, minimum, manualWeights, relativeScore", method)
	}

	if combinationType != ManualWeights && combinationType != RelativeScore {
		if len(weightsByName) > 0 {
			return nil, errors.New("weights are only allowed with manualWeights or relativeScore")
		}
		// the shard join needs weights for sum and average too, as gRPC sets them
		return &TargetCombination{Type: combinationType, Weights: implicitWeights(combinationType, len(targets))}, nil
	}

	targetSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		targetSet[target] = struct{}{}
	}
	for name := range weightsByName {
		if _, ok := targetSet[name]; !ok {
			return nil, fmt.Errorf("weight given for %q, which is not a target vector of this search", name)
		}
	}

	weights := make([]float32, len(targets))
	for i, target := range targets {
		weight, ok := weightsByName[target]
		if !ok {
			return nil, fmt.Errorf("weight for target %q not provided", target)
		}
		weights[i] = float32(weight)
		if math.IsNaN(float64(weights[i])) || math.IsInf(float64(weights[i]), 0) {
			return nil, fmt.Errorf("weight for target %q does not fit a 32-bit float", target)
		}
	}
	return &TargetCombination{Type: combinationType, Weights: weights}, nil
}

// implicitWeights returns 1 per target for sum, 1/n for average, nil otherwise.
func implicitWeights(t TargetCombinationType, n int) []float32 {
	switch t {
	case Sum:
		weights := make([]float32, n)
		for i := range weights {
			weights[i] = 1
		}
		return weights
	case Average:
		weights := make([]float32, n)
		for i := range weights {
			weights[i] = 1 / float32(n)
		}
		return weights
	default:
		return nil
	}
}
