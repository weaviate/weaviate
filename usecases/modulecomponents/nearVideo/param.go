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

package nearVideo

import (
	"errors"
)

type NearVideoParams struct {
	Video        string
	Certainty    float64
	Distance     float64
	WithDistance bool
}

func (n NearVideoParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearVideoParams) GetDistance() float64 {
	return n.Distance
}

func (n NearVideoParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func ValidateNearVideoFn(param interface{}) error {
	nearVideo, ok := param.(*NearVideoParams)
	if !ok {
		return errors.New("'nearVideo' invalid parameter")
	}

	if len(nearVideo.Video) == 0 {
		return errors.New("'nearVideo.video' needs to be defined")
	}

	if nearVideo.Certainty != 0 && nearVideo.WithDistance {
		return errors.New(
			"nearVideo cannot provide both distance and certainty")
	}

	return nil
}
