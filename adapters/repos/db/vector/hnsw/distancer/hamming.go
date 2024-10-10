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
	"math/bits"

	"github.com/pkg/errors"
)

var hammingImpl func(a, b []float32) float32 = func(a, b []float32) float32 {
	var sum float32 // default value of float in golang is 0

	for i := range a {
		if a[i] != b[i] {
			sum += 1
		}
	}

	return sum
}

var hammingBitwiseImpl func(a, b []uint64) float32 = func(a, b []uint64) float32 {
	total := float32(0)
	for segment := range a {
		total += float32(bits.OnesCount64(a[segment] ^ b[segment]))
	}
	return total
}

type Hamming struct {
	a []float32
}

func (l Hamming) Distance(b []float32) (float32, error) {
	if len(l.a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(l.a), len(b))
	}

	return hammingImpl(l.a, b), nil
}

func HammingDistanceGo(a, b []float32) float32 {
	var sum float32
	for i := range a {
		if a[i] != b[i] {
			sum += float32(1)
		}
	}
	return sum
}

func HammingBitwise(x []uint64, y []uint64) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.New("both vectors should have the same len")
	}
	return hammingBitwiseImpl(x, y), nil
}

type HammingProvider struct{}

func NewHammingProvider() HammingProvider {
	return HammingProvider{}
}

func (l HammingProvider) SingleDist(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(a), len(b))
	}

	return hammingImpl(a, b), nil
}

func (l HammingProvider) Type() string {
	return "hamming"
}

func (l HammingProvider) New(a []float32) Distancer {
	return &Hamming{a: a}
}

func (l HammingProvider) Step(x, y []float32) float32 {
	var sum float32 // default value of float in golang is 0

	for i := range x {
		if x[i] != y[i] {
			sum += float32(1)
		}
	}

	return sum
}

func (l HammingProvider) Wrap(x float32) float32 {
	return x
}
