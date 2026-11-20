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

// Target combination method names as they appear in the REST API. They are
// the camelCase spelling of the TargetCombinationType values.
const (
	TargetCombinationMethodSum           = "sum"
	TargetCombinationMethodAverage       = "average"
	TargetCombinationMethodMinimum       = "minimum"
	TargetCombinationMethodManualWeights = "manualWeights"
	TargetCombinationMethodRelativeScore = "relativeScore"
)

// NewTargetCombination builds the combination of a multi-target vector search
// from a method name and weights keyed by target vector name.
//
// An empty method yields nil for zero or one target (the engine needs no
// combination there) and the DefaultTargetCombinationType otherwise. The
// weights are converted to the positional form the engine expects, aligned
// with targets: manualWeights and relativeScore need exactly one weight per
// target, the other methods take none.
//
// Errors are caller (validation) errors; the message is safe to return to an
// API client.
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
		return &TargetCombination{Type: combinationType}, nil
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
