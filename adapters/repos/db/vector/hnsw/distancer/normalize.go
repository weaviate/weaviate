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

import "math"

func Normalize(v []float32) []float32 {
	var norm float32
	out := make([]float32, len(v))
	for i := range v {
		norm += v[i] * v[i]
	}
	if norm == 0 {
		return out
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		out[i] = v[i] / norm
	}

	return out
}

// NormalizeOut writes the normalized form of src into dst without
// allocating, returning dst trimmed to len(src). dst must hold at least
// len(src) elements and must either be exactly src (full in-place overlap)
// or not overlap src at all — a partial overlap corrupts the result. Use it
// when src must not be mutated, e.g. when it aliases a read-only segment
// mmap (the columnar zero-copy rescore path).
func NormalizeOut(dst, src []float32) []float32 {
	dst = dst[:len(src)]
	var norm float32
	for i := range src {
		norm += src[i] * src[i]
	}
	if norm == 0 {
		copy(dst, src) // all-zero input normalizes to itself
		return dst
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range src {
		dst[i] = src[i] / norm
	}
	return dst
}

// NormalizeInPlace normalizes a vector in-place without allocating.
// Use this when you own the vector and don't need to preserve the original.
func NormalizeInPlace(v []float32) {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}
	if norm == 0 {
		return
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}
}
