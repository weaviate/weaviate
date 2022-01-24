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
}

func (n NearImageParams) GetCertainty() float64 {
	return n.Certainty
}

func validateNearImageFn(param interface{}) error {
	nearImage, ok := param.(*NearImageParams)
	if !ok {
		return errors.New("'nearImage' invalid parameter")
	}

	if len(nearImage.Image) == 0 {
		return errors.Errorf("'nearImage.image' needs to be defined")
	}

	return nil
}
