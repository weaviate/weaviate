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

//// go:generate goat ../c/dot_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"
//go:generate goat ../c/dot_neon_arm64.c -O3 -e="--target=arm64" -e="-march=armv8-a+simd+fp"
//go:generate goat ../c/dot_sve_arm64.c -O3 -e="-mcpu=neoverse-v1" -e="--target=arm64" -e="-march=armv8-a+sve"
//go:generate goat ../c/dot_byte_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"

import (
	"unsafe"
)

// Dot calculates the dot product between two vectors
// using SIMD instructions.
func Dot_Neon(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return dot2[float32, float32](x, y)
	case 4:
		return dot4[float32, float32](x, y)
	case 6:
		return dot6[float32, float32](x, y)
	case 8:
		// manually inlined dot8(x, y)
		sum := x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	case 10:
		// manually inlined dot10(x, y)
		sum := x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	case 12:
		// manually inlined dot12(x, y)
		sum := x[11]*y[11] + x[10]*y[10] + x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	}

	var res float32

	// The C function expects pointers to the underlying array, not slices.
	hdrx := unsafe.SliceData(x)
	hdry := unsafe.SliceData(y)

	l := len(x)
	dot_neon(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(hdrx),
		unsafe.Pointer(hdry),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func Dot_SVE(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return dot2[float32, float32](x, y)
	case 4:
		return dot4[float32, float32](x, y)
	case 6:
		return dot6[float32, float32](x, y)
	case 8:
		// manually inlined dot8(x, y)
		sum := x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	case 10:
		// manually inlined dot10(x, y)
		sum := x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	case 12:
		// manually inlined dot12(x, y)
		sum := x[11]*y[11] + x[10]*y[10] + x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6[float32, float32](x, y) + sum
	}

	var res float32

	l := len(x)
	dot_sve(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func DotByteARM64(x []uint8, y []uint8) uint32 {
	switch len(x) {
	case 2:
		return dot2[uint8, uint32](x, y)
	case 3:
		return dot3[uint8, uint32](x, y)
	case 4:
		return dot4[uint8, uint32](x, y)
	case 5:
		return dot5[uint8, uint32](x, y)
	case 6:
		return dot6[uint8, uint32](x, y)
	case 7:
		return dot7[uint8, uint32](x, y)
	case 8:
		// manually inlined dot8(x, y)
		sum := uint32(x[7])*uint32(y[7]) + uint32(x[6])*uint32(y[6])
		return dot6[uint8, uint32](x, y) + uint32(sum)
	case 10:
		// manually inlined dot10(x, y)
		sum := uint32(x[9])*uint32(y[9]) + uint32(x[8])*uint32(y[8]) + uint32(x[7])*uint32(y[7]) + uint32(x[6])*uint32(y[6])
		return dot6[uint8, uint32](x, y) + uint32(sum)
	case 12:
		// manually inlined dot12(x, y)
		return dot6[uint8, uint32](x, y) + dot6[uint8, uint32](x[6:12], y[6:12])
	}

	var res uint32

	l := len(x)

	dot_byte_256(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func DotFloatByte_Neon(x []float32, y []uint8) float32 {
	var res float32

	l := len(x)

	if l < 16 {
		for i := 0; i < l; i++ {
			res += x[i] * float32(y[i])
		}
		return res
	}

	dot_float_byte_neon(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	if l > 16 && l%16 != 0 {
		start := l - l%16
		for i := start; i < l; i++ {
			res += x[i] * float32(y[i])
		}
	}

	return res
}
