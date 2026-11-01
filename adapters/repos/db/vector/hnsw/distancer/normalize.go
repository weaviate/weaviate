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

func squaredNorm(v []float32) float32 {
	if len(v) == 0 {
		return 0
	}
	return dotProductImplementation(v, v)
}

// scalable reports whether the float32 sum of squares can be turned into a
// scaling factor. It is false for the zero vector, but also for vectors whose
// components are valid float32 values yet square outside the float32 range:
// 1e20*1e20 overflows to +Inf and 1e-25*1e-25 underflows to 0. Both cases
// scale the vector to zeros, which reads as a valid vector downstream and
// makes every cosine distance 1.
func scalable(norm2 float32) bool {
	return norm2 > 0 && !math.IsInf(float64(norm2), 1)
}

// normalizeFloat64 scales v to unit length into dst, accumulating the norm in
// float64 so that squares beyond the float32 range still resolve. It reports
// false and leaves dst untouched when v is the zero vector. It divides rather
// than multiplying by the reciprocal because the reciprocal itself is not
// always a finite float32. dst and v may be the same slice.
func normalizeFloat64(dst, v []float32) bool {
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		return false
	}
	norm = math.Sqrt(norm)
	for i, x := range v {
		dst[i] = float32(float64(x) / norm)
	}
	return true
}

// Normalize returns v scaled to unit length as a new slice. A zero vector
// normalizes to the zero vector. The norm is applied as a multiplication by
// the reciprocal, which is substantially faster than per-element division
// and differs from it by at most one ulp per element.
func Normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	norm2 := squaredNorm(v)
	if !scalable(norm2) {
		normalizeFloat64(out, v)
		return out
	}
	f32.Scale(out, v, float32(1/math.Sqrt(float64(norm2))))
	return out
}

// NormalizeInPlace normalizes a vector in-place without allocating.
// Use this when you own the vector and don't need to preserve the original.
// A zero vector is left unchanged.
func NormalizeInPlace(v []float32) {
	norm2 := squaredNorm(v)
	if !scalable(norm2) {
		normalizeFloat64(v, v)
		return
	}
	f32.Scale(v, v, float32(1/math.Sqrt(float64(norm2))))
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
	norm2 := squaredNorm(v)
	if !scalable(norm2) {
		if !normalizeFloat64(dst, v) {
			clear(dst)
		}
		return dst
	}
	f32.Scale(dst, v, float32(1/math.Sqrt(float64(norm2))))
	return dst
}
