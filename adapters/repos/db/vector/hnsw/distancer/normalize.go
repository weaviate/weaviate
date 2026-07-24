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

// Normalize returns v scaled to unit length as a new slice. A zero vector
// normalizes to the zero vector. The norm is applied as a multiplication by
// the reciprocal, which is substantially faster than per-element division
// and differs from it by at most one ulp per element.
func Normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	norm2 := f32.SumOfSquares(v)
	if norm2 == 0 {
		return out
	}
	f32.Scale(out, v, float32(1/math.Sqrt(float64(norm2))))
	return out
}

// NormalizeInPlace normalizes a vector in-place without allocating.
// Use this when you own the vector and don't need to preserve the original.
// A zero vector is left unchanged.
func NormalizeInPlace(v []float32) {
	norm2 := f32.SumOfSquares(v)
	if norm2 == 0 {
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
	norm2 := f32.SumOfSquares(v)
	if norm2 == 0 {
		clear(dst)
		return dst
	}
	f32.Scale(dst, v, float32(1/math.Sqrt(float64(norm2))))
	return dst
}
