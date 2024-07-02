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

//go:generate goat ../c/dot_avx256_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
//go:generate goat ../c/dot_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"

import (
	"unsafe"
)

func DotAVX256(x []float32, y []float32) float32 {
	var res float32

	l := len(x)
	dot_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func DotAVX512(x []float32, y []float32) float32 {
	var res float32

	l := len(x)
	dot_512(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func DotByteAVX256(x []uint8, y []uint8) uint32 {
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
		sum := uint32(x[11])*uint32(y[11]) + uint32(x[10])*uint32(y[10]) + uint32(x[9])*uint32(y[9]) + uint32(x[8])*uint32(y[8]) + uint32(x[7])*uint32(y[7]) + uint32(x[6])*uint32(y[6])
		return dot6[uint8, uint32](x, y) + uint32(sum)
	}

	var res uint32

	l := len(x)
	dot_byte_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func DotFloatByteAVX256(x []float32, y []uint8) float32 {
	var res float32

	l := len(x)
	dot_float_byte_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}
