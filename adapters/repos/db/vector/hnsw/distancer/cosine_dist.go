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

package distancer

import (
	"github.com/pkg/errors"
)

type CosineDistance struct {
	a []float32
}

func (d *CosineDistance) Distance(b []float32) (float32, error) {
	if len(d.a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(d.a), len(b))
	}

	dist := 1 - dotProductPrecise(d.a, b)

	if dist < 0 {
		return 0, nil
	}
	return dist, nil
}

type CosineDistanceProvider struct{}

func NewCosineDistanceProvider() CosineDistanceProvider {
	return CosineDistanceProvider{}
}

func (d CosineDistanceProvider) SingleDist(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(a), len(b))
	}

	prod := 1 - dotProductPrecise(a, b)

	if prod < 0 {
		return 0, nil
	}
	return prod, nil
}

func (d CosineDistanceProvider) Type() string {
	return "cosine-dot"
}

func (d CosineDistanceProvider) New(a []float32) Distancer {
	return &CosineDistance{a: a}
}

func (d CosineDistanceProvider) Step(x, y []float32) float32 {
	var sum float64
	for i := range x {
		sum += float64(x[i]) * float64(y[i])
	}

	return float32(sum)
}

func (d CosineDistanceProvider) Wrap(x float32) float32 {
	w := 1 - x
	if w < 0 {
		return 0
	}
	return w
}
