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

package nearAudio

import (
	"errors"
)

type NearAudioParams struct {
	Audio         string
	Certainty     float64
	Distance      float64
	WithDistance  bool
	TargetVectors []string
}

func (n NearAudioParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearAudioParams) GetDistance() float64 {
	return n.Distance
}

func (n NearAudioParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func (n NearAudioParams) GetTargetVectors() []string {
	return n.TargetVectors
}

func validateNearAudioFn(param interface{}) error {
	nearAudio, ok := param.(*NearAudioParams)
	if !ok {
		return errors.New("'nearAudio' invalid parameter")
	}

	if len(nearAudio.Audio) == 0 {
		return errors.New("'nearAudio.audio' needs to be defined")
	}

	if nearAudio.Certainty != 0 && nearAudio.WithDistance {
		return errors.New(
			"nearAudio cannot provide both distance and certainty")
	}

	if len(nearAudio.TargetVectors) > 1 {
		return errors.New(
			"nearAudio.targetVectors cannot provide more than 1 target vector value")
	}

	return nil
}
