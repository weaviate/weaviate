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

//go:generate goat ../c/hamming_avx256_amd64.c -O3 -mavx2  -mno-avx512f  -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
//go:generate goat ../c/hamming_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -mavx512vl -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
//go:generate goat ../c/hamming_bitwise_avx256_amd64.c -O3 -mavx2   -mno-avx512f  -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
//go:generate goat ../c/hamming_bitwise_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -mavx512bw -mavx512vl -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"

import "unsafe"

func HammingAVX256(x []float32, y []float32) float32 {
	var res float32

	l := len(x)
	hamming_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func HammingAVX512(x []float32, y []float32) float32 {
	var res float32

	l := len(x)
	hamming_512(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

// lookup_avx and popcnt_constants are being passed in through go
// to keep clang from creating a .rodata section for the constants
// (which goat cannot handle)
var lookup_avx = []uint8{
	/* 0 */ 0 /* 1 */, 1 /* 2 */, 1 /* 3 */, 2,
	/* 4 */ 1 /* 5 */, 2 /* 6 */, 2 /* 7 */, 3,
	/* 8 */ 1 /* 9 */, 2 /* a */, 2 /* b */, 3,
	/* c */ 2 /* d */, 3 /* e */, 3 /* f */, 4,
	/* 0 */ 0 /* 1 */, 1 /* 2 */, 1 /* 3 */, 2,
	/* 4 */ 1 /* 5 */, 2 /* 6 */, 2 /* 7 */, 3,
	/* 8 */ 1 /* 9 */, 2 /* a */, 2 /* b */, 3,
	/* c */ 2 /* d */, 3 /* e */, 3 /* f */, 4,
}

var popcnt_constants = []uint64{
	0x5555555555555555, // MASK_01010101
	0x3333333333333333, // MASK_00110011
	0x0F0F0F0F0F0F0F0F, // MASK_00001111
	0x0101010101010101, // MULT_01010101
	0x0f0f0f0f0f0f0f0f, // MASK_00001111
}

func HammingBitwiseAVX256(x []uint64, y []uint64) float32 {
	var res uint64

	l := len(x)
	hamming_bitwise_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l),
		unsafe.Pointer(unsafe.SliceData(lookup_avx)),
		unsafe.Pointer(unsafe.SliceData(popcnt_constants)),
	)

	return float32(res)
}

func HammingBitwiseAVX512(x []uint64, y []uint64) float32 {
	var res uint64

	l := len(x)
	hamming_bitwise_512(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l),

		unsafe.Pointer(unsafe.SliceData(popcnt_constants)))

	return float32(res)
}
