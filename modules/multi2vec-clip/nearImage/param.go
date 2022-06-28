//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package nearImage

import (
	"github.com/pkg/errors"
)

type NearImageParams struct {
	Image     string
	Certainty float64
	Distance  float64
}

func (n NearImageParams) GetCertainty() float64 {
	return n.Certainty
}

func (n NearImageParams) GetDistance() float64 {
	return n.Distance
}

func (n NearImageParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.Distance != 0
}

func validateNearImageFn(param interface{}) error {
	nearImage, ok := param.(*NearImageParams)
	if !ok {
		return errors.New("'nearImage' invalid parameter")
	}

	if len(nearImage.Image) == 0 {
		return errors.Errorf("'nearImage.image' needs to be defined")
	}

	if nearImage.Certainty != 0 && nearImage.Distance != 0 {
		return errors.Errorf(
			"nearText cannot provide both distance and certainty")
	}

	// because the modules all still accept certainty as
	// the only similarity metric input, me must make the
	// conversion to certainty if distance is provided
	if nearImage.Distance != 0 {
		nearImage.Certainty = 1 - nearImage.Distance
	}

	return nil
}
