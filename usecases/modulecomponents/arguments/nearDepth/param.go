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

package nearDepth

import (
	"errors"
)

type NearDepthParams struct {
	Depth         string
	Certainty     float64
	Distance      float64
	WithDistance  bool
	TargetVectors []string
}

func (n NearDepthParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearDepthParams) GetDistance() float64 {
	return n.Distance
}

func (n NearDepthParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n NearDepthParams) GetTargetVectors() []string {
	return n.TargetVectors
}

func validateNearDepthFn(param interface{}) error {
	nearDepth, ok := param.(*NearDepthParams)
	if !ok {
		return errors.New("'nearDepth' invalid parameter")
	}

	if len(nearDepth.Depth) == 0 {
		return errors.New("'nearDepth.depth' needs to be defined")
	}

	if nearDepth.Certainty != 0 && nearDepth.WithDistance {
		return errors.New(
			"nearDepth cannot provide both distance and certainty")
	}

	if len(nearDepth.TargetVectors) > 1 {
		return errors.New(
			"nearText.targetVectors cannot provide more than 1 target vector value")
	}

	return nil
}
