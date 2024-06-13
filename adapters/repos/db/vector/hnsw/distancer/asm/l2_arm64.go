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

import (
	"unsafe"
)

// To generate the asm code, run:
//   go install github.com/gorse-io/goat@v0.1.0
//   go generate

//go:generate goat ../c/l2_neon_arm64.c -O3 -e="--target=arm64" -e="-march=armv8-a+simd+fp"
//go:generate goat ../c/l2_sve_arm64.c -O3 -e="-mcpu=neoverse-v1" -e="--target=arm64" -e="-march=armv8-a+sve"
//go:generate goat ../c/l2_neon_byte_arm64.c -O3 -e="--target=arm64" -e="-march=armv8-a+simd+fp"

// L2 calculates the L2 distance between two vectors
// using SIMD instructions when possible.
// Vector lengths < 16 are handled by the Go implementation
// because the overhead of using reflection is too high.

func L2_Neon(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return l22[float32, float32](x, y)
	case 4:
		return l24[float32, float32](x, y)
	case 6:
		// manually inlined l26(x, y)
		diff := x[5] - y[5]
		sum := diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[float32, float32](x, y) + sum
	case 8:
		// manually inlined l28(x, y)
		diff := x[7] - y[7]
		sum := diff * diff

		diff = x[6] - y[6]
		sum += diff * diff

		diff = x[5] - y[5]
		sum += diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[float32, float32](x, y) + sum
	case 10:
		return l210[float32, float32](x, y)
	case 12:
		return l212[float32, float32](x, y)
	}

	// deal with odd lengths and lengths 13, 14, 15
	if len(x) < 16 {
		var sum float32

		for i := range x {
			diff := x[i] - y[i]
			sum += diff * diff
		}

		return sum
	}

	var res float32

	l := len(x)

	l2_neon(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func L2_SVE(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return l22[float32, float32](x, y)
	case 4:
		return l24[float32, float32](x, y)
	case 6:
		// manually inlined l26(x, y)
		diff := x[5] - y[5]
		sum := diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[float32, float32](x, y) + sum
	case 8:
		// manually inlined l28(x, y)
		diff := x[7] - y[7]
		sum := diff * diff

		diff = x[6] - y[6]
		sum += diff * diff

		diff = x[5] - y[5]
		sum += diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[float32, float32](x, y) + sum
	case 10:
		return l210[float32, float32](x, y)
	case 12:
		return l212[float32, float32](x, y)
	}

	// deal with odd lengths and lengths 13, 14, 15
	if len(x) < 16 {
		var sum float32

		for i := range x {
			diff := x[i] - y[i]
			sum += diff * diff
		}

		return sum
	}

	var res float32

	l := len(x)
	l2_sve(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func L2ByteARM64(x []uint8, y []uint8) uint32 {
	switch len(x) {
	case 2:
		return l22[uint8, uint32](x, y)
	case 4:
		return l24[uint8, uint32](x, y)
	case 6:
		// manually inlined l26(x, y)
		diff := x[5] - y[5]
		sum := diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[uint8, uint32](x, y) + uint32(sum)
	case 8:
		// manually inlined l28(x, y)
		diff := x[7] - y[7]
		sum := diff * diff

		diff = x[6] - y[6]
		sum += diff * diff

		diff = x[5] - y[5]
		sum += diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24[uint8, uint32](x, y) + uint32(sum)
	case 10:
		return l210[uint8, uint32](x, y)
	case 12:
		return l212[uint8, uint32](x, y)
	}

	// deal with odd lengths and lengths 13, 14, 15
	if len(x) < 16 {
		var sum uint32

		for i := range x {
			diff := int32(x[i]) - int32(y[i])
			sum += uint32(diff * diff)
		}

		return sum
	}

	var res uint32

	l := len(x)

	l2_neon_byte_256(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}
