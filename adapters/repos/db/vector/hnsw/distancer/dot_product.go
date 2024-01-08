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

package distancer

import (
	"github.com/pkg/errors"
)

// can be set depending on architecture, e.g. pure go, AVX-enabled assembly, etc.
// Warning: This is not the dot product distance, but the pure product.
//
// This default will always work, regardless of architecture. An init function
// will overwrite it on amd64 if AVX is present.
var dotProductImplementation func(a, b []float32) float32 = func(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}

	return sum
}

type DotProduct struct {
	a []float32
}

func (d *DotProduct) Distance(b []float32) (float32, bool, error) {
	if len(d.a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(d.a), len(b))
	}

	dist := -dotProductImplementation(d.a, b)
	return dist, true, nil
}

type DotProductProvider struct{}

func NewDotProductProvider() DotProductProvider {
	return DotProductProvider{}
}

func DotProductGo(a, b []float32) float32 {
	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}

	return -sum
}

func (d DotProductProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	prod := -dotProductImplementation(a, b)

	return prod, true, nil
}

func (d DotProductProvider) Type() string {
	return "dot"
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}

func (d DotProductProvider) Step(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}

	return sum
}

func (d DotProductProvider) Wrap(x float32) float32 {
	return -x
}
