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
	// Accumulate the sum of squares in float64. A float32 accumulator overflows
	// to +Inf for large but valid components (e.g. 1e20*1e20 = 1e40 exceeds the
	// float32 max), which would collapse the result to a zero vector.
	var norm float64
	out := make([]float32, len(v))
	for i := range v {
		val := float64(v[i])
		norm += val * val
	}
	if norm == 0 {
		return out
	}

	norm = math.Sqrt(norm)
	for i := range v {
		out[i] = float32(float64(v[i]) / norm)
	}

	return out
}

// NormalizeInPlace normalizes a vector in-place without allocating.
// Use this when you own the vector and don't need to preserve the original.
func NormalizeInPlace(v []float32) {
	// See Normalize: float64 accumulation avoids a float32 overflow to +Inf that
	// would otherwise collapse large-magnitude vectors to zero.
	var norm float64
	for i := range v {
		val := float64(v[i])
		norm += val * val
	}
	if norm == 0 {
		return
	}

	norm = math.Sqrt(norm)
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
}
