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

// can be set depending on architecture, e.g. pure go, AVX-enabled assembly, etc.
// Warning: This is not the dot product distance, but the pure product.
//
// This default will always work, regardless of architecture. An init function
// will overwrite it on amd64 if AVX is present.
var dotProductImplementation func(a, b []float32) float32 = func(a, b []float32) float32 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}

	return float32(sum)
}

// dotProductPrecise always uses Go float64 accumulation, regardless of
// architecture. It is never overridden by AVX/NEON/SVE init functions.
// Use this when numerical precision matters (e.g., exact/flat search)
// rather than raw throughput (e.g., HNSW traversal).
var dotProductPrecise = func(a, b []float32) float32 {
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum)
}

func DotProductFloatGo(a, b []float32) float32 {
	return -dotProductGo[float32, float32](a, b)
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

	dist := -dotProductPrecise(d.a, b)
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

	prod := -dotProductPrecise(a, b)

	return prod, nil
}

func (d DotProductProvider) Type() string {
	return "dot"
}

func (d DotProductProvider) New(a []float32) Distancer {
	return &DotProduct{a: a}
}

func (d DotProductProvider) Step(x, y []float32) float32 {
	var sum float64
	for i := range x {
		sum += float64(x[i]) * float64(y[i])
	}

	return float32(sum)
}

func (d DotProductProvider) Wrap(x float32) float32 {
	return -x
}
