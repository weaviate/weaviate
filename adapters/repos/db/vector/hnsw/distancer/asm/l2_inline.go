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
// - it allows l22 and l24 to be inlined (the other ones are too large)
// See go tool compile -d=ssa/check_bce/debug=1 -m l2_inline.go

func l22(x []float32, y []float32) float32 {
	diff := x[1] - y[1]
	sum := diff * diff

	diff = x[0] - y[0]
	sum += diff * diff

	return sum
}

func l24(x []float32, y []float32) float32 {
	diff := x[3] - y[3]
	sum := diff * diff

	diff = x[2] - y[2]
	sum += diff * diff

	return l22(x, y) + sum
}

func l26(x []float32, y []float32) float32 {
	diff := x[5] - y[5]
	sum := diff * diff

	diff = x[4] - y[4]
	sum += diff * diff

	return l24(x, y) + sum
}

func l28(x []float32, y []float32) float32 {
	diff := x[7] - y[7]
	sum := diff * diff

	diff = x[6] - y[6]
	sum += diff * diff

	diff = x[5] - y[5]
	sum += diff * diff

	diff = x[4] - y[4]
	sum += diff * diff

	return l24(x, y) + sum
}

func l210(x []float32, y []float32) float32 {
	diff := x[9] - y[9]
	sum := diff * diff

	diff = x[8] - y[8]
	sum += diff * diff

	diff = x[7] - y[7]
	sum += diff * diff

	diff = x[6] - y[6]
	sum += diff * diff

	diff = x[5] - y[5]
	sum += diff * diff

	diff = x[4] - y[4]
	sum += diff * diff

	return l24(x, y) + sum
}

func l212(x []float32, y []float32) float32 {
	diff := x[11] - y[11]
	sum := diff * diff

	diff = x[10] - y[10]
	sum += diff * diff

	diff = x[9] - y[9]
	sum += diff * diff

	diff = x[8] - y[8]
	sum += diff * diff

	diff = x[7] - y[7]
	sum += diff * diff

	diff = x[6] - y[6]
	sum += diff * diff

	diff = x[5] - y[5]
	sum += diff * diff

	diff = x[4] - y[4]
	sum += diff * diff

	return l24(x, y) + sum
}
