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

	"github.com/tphakala/simd/f32"
)

// minNormalFloat32 is the smallest positive normal float32 (2^-126).
const minNormalFloat32 = 0x1p-126

func squaredNorm(v []float32) float32 {
	if len(v) == 0 {
		return 0
	}
	return dotProductImplementation(v, v)
}

// squaredNorm64 sums the squares of v in float64. The square of any finite
// float32 is representable in float64, so unlike squaredNorm this can
// neither overflow to +Inf nor underflow to zero for a nonzero v.
func squaredNorm64(v []float32) float64 {
	var sum float64
	for _, x := range v {
		fx := float64(x)
		sum += fx * fx
	}
	return sum
}

// scaleToUnit scales v into dst (dst may alias v) to unit L2 norm. A zero
// vector scales to a zero vector (dst is cleared).
//
// The fast path multiplies by the reciprocal of the norm, which is
// substantially faster than per-element division and differs from it by at
// most one ulp per element. It is only valid while the float32 sum of
// squares lands in the normal range: squares of components below ~3.7e-23
// underflow to zero and squares of components above ~1.8e19 overflow to
// +Inf, either of which would silently drop nonzero components or collapse
// the whole vector to zero (issues #13751, #11671). In those cases the norm
// is recomputed in float64 and applied as a float64 reciprocal, which cannot
// overflow or underflow because the square of any finite float32 is a
// finite float64.
func scaleToUnit(dst, v []float32) {
	norm2 := squaredNorm(v)
	if norm2 >= minNormalFloat32 && norm2 <= math.MaxFloat32 {
		f32.Scale(dst, v, float32(1/math.Sqrt(float64(norm2))))
		return
	}

	sum := squaredNorm64(v)
	if sum == 0 {
		clear(dst)
		return
	}
	recip := 1 / math.Sqrt(sum)
	for i, x := range v {
		dst[i] = float32(float64(x) * recip)
	}
}

// Normalize returns v scaled to unit length as a new slice. A zero vector
// normalizes to the zero vector.
func Normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	scaleToUnit(out, v)
	return out
}

// NormalizeInPlace normalizes a vector in-place without allocating.
// Use this when you own the vector and don't need to preserve the original.
// A zero vector is left unchanged.
func NormalizeInPlace(v []float32) {
	scaleToUnit(v, v)
}

// NormalizeInto normalizes v into the caller-provided buffer dst, growing it
// if needed, and returns the buffer sliced to len(v). dst must not alias v.
// Use this on hot paths that pool their allocations; the zero-vector case
// fills dst with zeros to match Normalize.
func NormalizeInto(dst, v []float32) []float32 {
	if cap(dst) < len(v) {
		dst = make([]float32, len(v))
	}
	dst = dst[:len(v)]
	scaleToUnit(dst, v)
	return dst
}
