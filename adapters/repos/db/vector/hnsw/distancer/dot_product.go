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
	"math"

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

func DotProductFloatGo(a, b []float32) float32 {
	return clampDotProduct(-dotProductFloat64(a, b))
}

// dotProductFloat64 accumulates in float64 so that finite float32 inputs cannot
// overflow the intermediate sum. Dot products of float32 vectors fit in
// float64 even when the float32 result does not.
func dotProductFloat64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}

	return sum
}

// safeDotProduct uses the optimized float32 implementation and only falls back
// to float64 accumulation when the fast path overflows or loses the result to
// float32 cancellation.
func safeDotProduct(a, b []float32) float64 {
	sum := dotProductImplementation(a, b)
	if math.IsInf(float64(sum), 0) || math.IsNaN(float64(sum)) {
		return dotProductFloat64(a, b)
	}

	return float64(sum)
}

// clampDotProduct saturates values that are outside the float32 range. The
// distancer and all API distance fields are float32, and returning +/-Inf for
// finite inputs makes JSON serialization fail. Saturation keeps the value
// representable while preserving the ordering of finite vectors.
func clampDotProduct(v float64) float32 {
	if v > math.MaxFloat32 {
		return math.MaxFloat32
	}
	if v < -math.MaxFloat32 {
		return -math.MaxFloat32
	}

	return float32(v)
}

func DotProductByteGo(a, b []uint8) uint32 {
	return dotProductGo[uint8, uint32](a, b)
}

func dotProductGo[C uint8 | float32, T uint32 | float32](a, b []C) T {
	var sum T
	for i := range a {
		sum += T(a[i]) * T(b[i])
	}
	return sum
}

type DotProduct struct {
	a []float32
}

func (d *DotProduct) Distance(b []float32) (float32, error) {
	if len(d.a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(d.a), len(b))
	}

	dist := clampDotProduct(-safeDotProduct(d.a, b))
	return dist, nil
}

type DotProductProvider struct{}

func NewDotProductProvider() DotProductProvider {
	return DotProductProvider{}
}

func (d DotProductProvider) SingleDist(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, errors.Wrapf(ErrVectorLength, "%d vs %d",
			len(a), len(b))
	}

	prod := clampDotProduct(-safeDotProduct(a, b))

	return prod, nil
}

func (d DotProductProvider) Type() string {
	return "dot"
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}

func (d DotProductProvider) Step(x, y []float32) float32 {
	return clampDotProduct(dotProductFloat64(x, y))
}

func (d DotProductProvider) Wrap(x float32) float32 {
	return clampDotProduct(-float64(x))
}
