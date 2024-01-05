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
	"math"

	"github.com/pkg/errors"
)

var manhattanImpl func(a, b []float32) float32 = func(a, b []float32) float32 {
	var sum float32

	for i := range a {
		// take absolute difference, converted to float64 because math.Abs needs that
		// convert back to float32 as sum is float32
		sum += float32(math.Abs(float64(a[i] - b[i])))
	}

	return sum
}

type Manhattan struct {
	a []float32
}

func (l Manhattan) Distance(b []float32) (float32, bool, error) {
	if len(l.a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(l.a), len(b))
	}

	return manhattanImpl(l.a, b), true, nil
}

type ManhattanProvider struct{}

func NewManhattanProvider() ManhattanProvider {
	return ManhattanProvider{}
}

func (l ManhattanProvider) SingleDist(a, b []float32) (float32, bool, error) {
	if len(a) != len(b) {
		return 0, false, errors.Errorf("vector lengths don't match: %d vs %d",
			len(a), len(b))
	}

	return manhattanImpl(a, b), true, nil
}

func (l ManhattanProvider) Type() string {
	return "manhattan"
}

func (l ManhattanProvider) New(a []float32) Distancer {
	return &Manhattan{a: a}
}

func (l ManhattanProvider) Step(x, y []float32) float32 {
	var sum float32

	for i := range x {
		// take absolute difference, converted to float64 because math.Abs needs that
		// convert back to float32 as sum is float32
		sum += float32(math.Abs(float64(x[i] - y[i])))
	}

	return sum
}

func (l ManhattanProvider) Wrap(x float32) float32 {
	return x
}
