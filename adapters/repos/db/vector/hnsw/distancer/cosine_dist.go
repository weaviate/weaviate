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

package distancer

import (
	"github.com/pkg/errors"
)

type CosineDistance struct {
	a []float32
}

func (d *CosineDistance) Distance(b []float32) (float32, bool, error) {
	if len(d.a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(d.a), len(b))
	}

	dist := 1 - dotProductImplementation(d.a, b)
	return dist, true, nil
}

type CosineDistanceProvider struct{}

func NewCosineDistanceProvider() CosineDistanceProvider {
	return CosineDistanceProvider{}
}

func (d CosineDistanceProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	prod := 1 - dotProductImplementation(a, b)

	return prod, true, nil
}

func (d CosineDistanceProvider) Type() string {
	return "cosine-dot"
}

func (d CosineDistanceProvider) New(a []float32) Distancer {
	return &CosineDistance{a: a}
}
