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

package asm

// Experiment with inlining and flattening the L2Squared distancer.
// Theoretically, this should be faster than the loop version for small vectors
// - it avoids the loop overhead
// - it eliminates the bounds check by reversing the iteration
// - it allows dot2, dot4 and dot6 to be inlined (the other ones are too large)
// See go tool compile -d=ssa/check_bce/debug=1 -m dot_inline.go
func hamming2(x []float32, y []float32) float32 {
	sum := float32(0)

	if x[1] != y[1] {
		sum = sum + 1
	}
	if x[0] != y[0] {
		sum = sum + 1
	}

	return sum
}

func hamming4(x []float32, y []float32) float32 {
	sum := float32(0)

	if x[3] != y[3] {
		sum = sum + 1
	}
	if x[2] != y[2] {
		sum = sum + 1
	}

	return hamming2(x, y) + sum
}

func hamming6(x []float32, y []float32) float32 {
	sum := float32(0)

	if x[5] != y[5] {
		sum = sum + 1.0
	}
	if x[4] != y[4] {
		sum = sum + 1.0
	}

	return hamming4(x, y) + sum
}
