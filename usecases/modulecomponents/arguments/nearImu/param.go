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

package nearImu

import (
	"errors"
)

type NearIMUParams struct {
	IMU           string
	Certainty     float64
	Distance      float64
	WithDistance  bool
	TargetVectors []string
}

func (n NearIMUParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearIMUParams) GetDistance() float64 {
	return n.Distance
}

func (n NearIMUParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n NearIMUParams) GetTargetVectors() []string {
	return n.TargetVectors
}

func validateNearIMUFn(param interface{}) error {
	nearIMU, ok := param.(*NearIMUParams)
	if !ok {
		return errors.New("'nearIMU' invalid parameter")
	}

	if len(nearIMU.IMU) == 0 {
		return errors.New("'nearIMU.imu' needs to be defined")
	}

	if nearIMU.Certainty != 0 && nearIMU.WithDistance {
		return errors.New(
			"nearIMU cannot provide both distance and certainty")
	}

	if len(nearIMU.TargetVectors) > 1 {
		return errors.New(
			"nearIMU.targetVectors cannot provide more than 1 target vector value")
	}

	return nil
}
