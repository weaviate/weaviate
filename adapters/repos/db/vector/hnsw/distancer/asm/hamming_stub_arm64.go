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

// To generate the asm code, run:
//   go install github.com/gorse-io/goat@v0.1.0
//   go generate

//go:generate goat ../c/hamming_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"
//go:generate goat ../c/hamming_bitwise_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"

import (
	"unsafe"
)

// Hamming calculates the hamming distance between two vectors
// using SIMD instructions.
func Hamming(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return hamming2(x, y)
	case 4:
		return hamming4(x, y)
	case 6:
		return hamming6(x, y)
	case 8:
		// manually inlined hamming8(x, y)
		sum := float32(0)

		if x[7] != y[7] {
			sum = sum + 1.0
		}
		if x[6] != y[6] {
			sum = sum + 1.0
		}
		return hamming6(x, y) + sum
	case 10:
		// manually inlined hamming10(x, y)
		sum := float32(0)

		if x[9] != y[9] {
			sum = sum + 1.0
		}
		if x[8] != y[8] {
			sum = sum + 1.0
		}

		if x[7] != y[7] {
			sum = sum + 1.0
		}

		if x[6] != y[6] {
			sum = sum + 1.0
		}
		return hamming6(x, y) + sum
	case 12:
		// manually inlined hamming12(x, y)
		sum := float32(0)

		if x[11] != y[11] {
			sum = sum + 1.0
		}
		if x[10] != y[10] {
			sum = sum + 1.0
		}
		if x[9] != y[9] {
			sum = sum + 1.0
		}
		if x[8] != y[8] {
			sum = sum + 1.0
		}
		if x[7] != y[7] {
			sum = sum + 1.0
		}
		if x[6] != y[6] {
			sum = sum + 1.0
		}
		return hamming6(x, y) + sum
	}

	var res float32

	l := len(x)
	hamming(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func HammingBitwise(x []uint64, y []uint64) float32 {
	l := len(x)

	var res uint64
	hamming_bitwise(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return float32(res)
}
