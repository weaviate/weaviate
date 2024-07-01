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

func l22[T number, U number](x []T, y []T) U {
	diff := U(x[1]) - U(y[1])
	sum := diff * diff

	diff = U(x[0]) - U(y[0])
	sum += diff * diff

	return sum
}

func l23[T number, U number](x []T, y []T) U {
	diff := U(x[2]) - U(y[2])
	sum := diff * diff

	return l22[T, U](x, y) + sum
}

func l24[T number, U number](x []T, y []T) U {
	diff := U(x[3]) - U(y[3])
	sum := diff * diff

	diff = U(x[2]) - U(y[2])
	sum += diff * diff

	return l22[T, U](x, y) + sum
}

func l25[T number, U number](x []T, y []T) U {
	diff := U(x[4]) - U(y[4])
	sum := diff * diff

	return l24[T, U](x, y) + sum
}

func l26[T number, U number](x []T, y []T) U {
	diff := U(x[5]) - U(y[5])
	sum := diff * diff

	diff = U(x[4]) - U(y[4])
	sum += diff * diff

	return l24[T, U](x, y) + sum
}

func l28[T number, U number](x []T, y []T) U {
	diff := U(x[7]) - U(y[7])
	sum := diff * diff

	diff = U(x[6]) - U(y[6])
	sum += diff * diff

	diff = U(x[5]) - U(y[5])
	sum += diff * diff

	diff = U(x[4]) - U(y[4])
	sum += diff * diff

	return l24[T, U](x, y) + sum
}

func l210[T number, U number](x []T, y []T) U {
	diff := U(x[9]) - U(y[9])
	sum := diff * diff

	diff = U(x[8]) - U(y[8])
	sum += diff * diff

	diff = U(x[7]) - U(y[7])
	sum += diff * diff

	diff = U(x[6]) - U(y[6])
	sum += diff * diff

	diff = U(x[5]) - U(y[5])
	sum += diff * diff

	diff = U(x[4]) - U(y[4])
	sum += diff * diff

	return l24[T, U](x, y) + sum
}

func l212[T number, U number](x []T, y []T) U {
	diff := U(x[11]) - U(y[11])
	sum := diff * diff

	diff = U(x[10]) - U(y[10])
	sum += diff * diff

	diff = U(x[9]) - U(y[9])
	sum += diff * diff

	diff = U(x[8]) - U(y[8])
	sum += diff * diff

	diff = U(x[7]) - U(y[7])
	sum += diff * diff

	diff = U(x[6]) - U(y[6])
	sum += diff * diff

	diff = U(x[5]) - U(y[5])
	sum += diff * diff

	diff = U(x[4]) - U(y[4])
	sum += diff * diff

	return l24[T, U](x, y) + sum
}

func l22FloatByte(x []float32, y []byte) float32 {
	diff := x[1] - float32(y[1])
	sum := diff * diff

	diff = x[0] - float32(y[0])
	sum += diff * diff

	return sum
}

func l23FloatByte(x []float32, y []byte) float32 {
	diff := x[2] - float32(y[2])
	sum := diff * diff

	return l22FloatByte(x, y) + sum
}

func l24FloatByte(x []float32, y []byte) float32 {
	diff := x[3] - float32(y[3])
	sum := diff * diff

	diff = x[2] - float32(y[2])
	sum += diff * diff

	return l22FloatByte(x, y) + sum
}

func l25FloatByte(x []float32, y []byte) float32 {
	diff := x[4] - float32(y[4])
	sum := diff * diff

	return l24FloatByte(x, y) + sum
}

func l210FloatByte(x []float32, y []byte) float32 {
	diff := x[9] - float32(y[9])
	sum := diff * diff

	diff = x[8] - float32(y[8])
	sum += diff * diff

	diff = x[7] - float32(y[7])
	sum += diff * diff

	diff = x[6] - float32(y[6])
	sum += diff * diff

	diff = x[5] - float32(y[5])
	sum += diff * diff

	diff = x[4] - float32(y[4])
	sum += diff * diff

	return l24FloatByte(x, y) + sum
}

func l212FloatByte(x []float32, y []byte) float32 {
	diff := x[11] - float32(y[11])
	sum := diff * diff

	diff = x[10] - float32(y[10])
	sum += diff * diff

	diff = x[9] - float32(y[9])
	sum += diff * diff

	diff = x[8] - float32(y[8])
	sum += diff * diff

	diff = x[7] - float32(y[7])
	sum += diff * diff

	diff = x[6] - float32(y[6])
	sum += diff * diff

	diff = x[5] - float32(y[5])
	sum += diff * diff

	diff = x[4] - float32(y[4])
	sum += diff * diff

	return l24FloatByte(x, y) + sum
}
