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

package nearThermal

import (
	"errors"
)

type NearThermalParams struct {
	Thermal       string
	Certainty     float64
	Distance      float64
	WithDistance  bool
	TargetVectors []string
}

func (n NearThermalParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearThermalParams) GetDistance() float64 {
	return n.Distance
}

func (n NearThermalParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n NearThermalParams) GetTargetVectors() []string {
	return n.TargetVectors
}

func validateNearThermalFn(param interface{}) error {
	nearThermal, ok := param.(*NearThermalParams)
	if !ok {
		return errors.New("'nearThermal' invalid parameter")
	}

	if len(nearThermal.Thermal) == 0 {
		return errors.New("'nearThermal.thermal' needs to be defined")
	}

	if nearThermal.Certainty != 0 && nearThermal.WithDistance {
		return errors.New(
			"nearThermal cannot provide both distance and certainty")
	}

	if len(nearThermal.TargetVectors) > 1 {
		return errors.New(
			"nearThermal.targetVectors cannot provide more than 1 target vector value")
	}

	return nil
}
