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

func dot2(x []float32, y []float32) float32 {
	sum := x[1]*y[1] + x[0]*y[0]

	return sum
}

func dot4(x []float32, y []float32) float32 {
	sum := x[3]*y[3] + x[2]*y[2]

	return dot2(x, y) + sum
}

func dot6(x []float32, y []float32) float32 {
	sum := x[5]*y[5] + x[4]*y[4]

	return dot4(x, y) + sum
}

func dot8(x []float32, y []float32) float32 {
	sum := x[7]*y[7] + x[6]*y[6]

	return dot6(x, y) + sum
}

func dot10(x []float32, y []float32) float32 {
	sum := x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]

	return dot6(x, y) + sum
}

func dot12(x []float32, y []float32) float32 {
	sum := x[11]*y[11] + x[10]*y[10] + x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]

	return dot6(x, y) + sum
}
