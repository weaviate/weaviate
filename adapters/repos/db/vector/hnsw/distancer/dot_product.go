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
	return -dotProductGo[float32, float32](a, b)
}

// dotProductFloat64 accumulates the dot product in float64. A dot product of
// finite float32 vectors always fits in float64, even when the float32
// intermediate overflows (e.g. vectors with 1e20-scale components, where a
// single product is 1e40 and already exceeds math.MaxFloat32).
func dotProductFloat64(a, b []float32) float64 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}

	return sum
}

// dotProduct returns the dot product of a and b. It uses the fast float32
// implementation (SIMD where available) whenever it produces a finite result
// and only falls back to float64 accumulation when the float32 sum overflows
// to +/-Inf or NaN, so the hot path keeps its current performance and
// bit-identical results for ordinary vectors. The result is float64 so the
// caller can clamp it to the float32 range exactly once, at the end.
func dotProduct(a, b []float32) float64 {
	if sum := dotProductImplementation(a, b); !math.IsInf(float64(sum), 0) &&
		!math.IsNaN(float64(sum)) {
		return float64(sum)
	}

	return dotProductFloat64(a, b)
}

// clampDotProduct saturates a float64 dot product (or negated distance) to
// the float32 range. Distances are float32 throughout the API, and returning
// +/-Inf for finite input vectors breaks JSON serialization of query
// responses. Saturation is monotonic, so it preserves HNSW neighbor ordering:
// the closest vectors stay the closest, they just saturate at the bound.
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

	dist := clampDotProduct(-dotProduct(d.a, b))
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

	prod := clampDotProduct(-dotProduct(a, b))

	return prod, nil
}

func (d DotProductProvider) Type() string {
	return "dot"
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}

func (d DotProductProvider) Step(x, y []float32) float32 {
	return clampDotProduct(dotProduct(x, y))
}

func (d DotProductProvider) Wrap(x float32) float32 {
	return clampDotProduct(-float64(x))
}
