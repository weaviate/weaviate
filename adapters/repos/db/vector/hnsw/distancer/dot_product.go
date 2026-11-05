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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/simd"
)

// dotProductImplementation is the float32 dot product kernel (the pure
// product, not the distance). The simd package dispatches to archsimd
// NEON/AVX2/AVX-512 kernels when built with GOEXPERIMENT=simd and to portable
// Go otherwise. The amd64 init overrides it where the hand-written AVX2 and
// AVX-512 assembly still measures faster, for lengths above the small-vector
// paths; on arm64 the archsimd NEON kernels beat both the retired NEON and SVE
// assembly (SVE Dot by 2x or more on Neoverse V2), so the default stands.
var dotProductImplementation func(a, b []float32) float32 = simd.DotProduct

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

	dist := -dotProductImplementation(d.a, b)
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

	prod := -dotProductImplementation(a, b)

	return prod, nil
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
