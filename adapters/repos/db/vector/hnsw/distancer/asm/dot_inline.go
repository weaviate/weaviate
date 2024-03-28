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

type number interface {
	~uint8 | ~uint32 | ~float32
}

//gcassert:inline
func dot2[T number, U number](x []T, y []T) U {
	sum := x[1]*y[1] + x[0]*y[0]

	return U(sum)
}

//gcassert:inline
func dot3[T, U number](x []T, y []T) U {
	sum := x[2] * y[2]

	return dot2[T, U](x, y) + U(sum)
}

//gcassert:inline
func dot4[T, U number](x []T, y []T) U {
	sum := x[3]*y[3] + x[2]*y[2]

	return dot2[T, U](x, y) + U(sum)
}

//gcassert:inline
func dot5[T, U number](x []T, y []T) U {
	sum := x[4] * y[4]

	return dot4[T, U](x, y) + U(sum)
}

//gcassert:inline
func dot6[T, U number](x []T, y []T) U {
	sum := x[5]*y[5] + x[4]*y[4]

	return dot4[T, U](x, y) + U(sum)
}

//gcassert:inline
func dot7[T, U number](x []T, y []T) U {
	sum := x[6] * y[6]

	return dot6[T, U](x, y) + U(sum)
}
