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

package nearImage

import (
	"github.com/pkg/errors"
)

type NearImageParams struct {
	Image        string
	Certainty    float64
	Distance     float64
	WithDistance bool
}

func (n NearImageParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearImageParams) GetDistance() float64 {
	return n.Distance
}

func (n NearImageParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
}

func ValidateNearImageFn(param interface{}) error {
	nearImage, ok := param.(*NearImageParams)
	if !ok {
		return errors.New("'nearImage' invalid parameter")
	}

	if len(nearImage.Image) == 0 {
		return errors.Errorf("'nearImage.image' needs to be defined")
	}

	if nearImage.Certainty != 0 && nearImage.WithDistance {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	return nil
}
