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

////go:generate goat ../c/hamming_avx256_amd64.c -O3 -mavx2  -mno-avx512f  -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
////go:generate goat ../c/hamming_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -mavx512vl -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
////go:generate goat ../c/hamming_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -mavx512vl -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
//go:generate goat ../c/hamming_bitwise_avx256_amd64.c -O3 -mavx2   -mno-avx512f  -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"
////go:generate goat ../c/hamming_bitwise_avx512_amd64.c -O3 -mavx2 -mfma -mavx512f -mavx512dq -mavx512vl -e="-mfloat-abi=hard" -e="-Rpass-analysis=loop-vectorize" -e="-Rpass=loop-vectorize" -e="-Rpass-missed=loop-vectorize"

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

var lookup64bit = []uint64{
	/* 0 */ 0 /* 1 */, 1 /* 2 */, 1 /* 3 */, 2,
	/* 4 */ 1 /* 5 */, 2 /* 6 */, 2 /* 7 */, 3,
	/* 8 */ 1 /* 9 */, 2 /* a */, 2 /* b */, 3,
	/* c */ 2 /* d */, 3 /* e */, 3 /* f */, 4,
	/* 10 */ 1 /* 11 */, 2 /* 12 */, 2 /* 13 */, 3,
	/* 14 */ 2 /* 15 */, 3 /* 16 */, 3 /* 17 */, 4,
	/* 18 */ 2 /* 19 */, 3 /* 1a */, 3 /* 1b */, 4,
	/* 1c */ 3 /* 1d */, 4 /* 1e */, 4 /* 1f */, 5,
	/* 20 */ 1 /* 21 */, 2 /* 22 */, 2 /* 23 */, 3,
	/* 24 */ 2 /* 25 */, 3 /* 26 */, 3 /* 27 */, 4,
	/* 28 */ 2 /* 29 */, 3 /* 2a */, 3 /* 2b */, 4,
	/* 2c */ 3 /* 2d */, 4 /* 2e */, 4 /* 2f */, 5,
	/* 30 */ 2 /* 31 */, 3 /* 32 */, 3 /* 33 */, 4,
	/* 34 */ 3 /* 35 */, 4 /* 36 */, 4 /* 37 */, 5,
	/* 38 */ 3 /* 39 */, 4 /* 3a */, 4 /* 3b */, 5,
	/* 3c */ 4 /* 3d */, 5 /* 3e */, 5 /* 3f */, 6,
	/* 40 */ 1 /* 41 */, 2 /* 42 */, 2 /* 43 */, 3,
	/* 44 */ 2 /* 45 */, 3 /* 46 */, 3 /* 47 */, 4,
	/* 48 */ 2 /* 49 */, 3 /* 4a */, 3 /* 4b */, 4,
	/* 4c */ 3 /* 4d */, 4 /* 4e */, 4 /* 4f */, 5,
	/* 50 */ 2 /* 51 */, 3 /* 52 */, 3 /* 53 */, 4,
	/* 54 */ 3 /* 55 */, 4 /* 56 */, 4 /* 57 */, 5,
	/* 58 */ 3 /* 59 */, 4 /* 5a */, 4 /* 5b */, 5,
	/* 5c */ 4 /* 5d */, 5 /* 5e */, 5 /* 5f */, 6,
	/* 60 */ 2 /* 61 */, 3 /* 62 */, 3 /* 63 */, 4,
	/* 64 */ 3 /* 65 */, 4 /* 66 */, 4 /* 67 */, 5,
	/* 68 */ 3 /* 69 */, 4 /* 6a */, 4 /* 6b */, 5,
	/* 6c */ 4 /* 6d */, 5 /* 6e */, 5 /* 6f */, 6,
	/* 70 */ 3 /* 71 */, 4 /* 72 */, 4 /* 73 */, 5,
	/* 74 */ 4 /* 75 */, 5 /* 76 */, 5 /* 77 */, 6,
	/* 78 */ 4 /* 79 */, 5 /* 7a */, 5 /* 7b */, 6,
	/* 7c */ 5 /* 7d */, 6 /* 7e */, 6 /* 7f */, 7,
	/* 80 */ 1 /* 81 */, 2 /* 82 */, 2 /* 83 */, 3,
	/* 84 */ 2 /* 85 */, 3 /* 86 */, 3 /* 87 */, 4,
	/* 88 */ 2 /* 89 */, 3 /* 8a */, 3 /* 8b */, 4,
	/* 8c */ 3 /* 8d */, 4 /* 8e */, 4 /* 8f */, 5,
	/* 90 */ 2 /* 91 */, 3 /* 92 */, 3 /* 93 */, 4,
	/* 94 */ 3 /* 95 */, 4 /* 96 */, 4 /* 97 */, 5,
	/* 98 */ 3 /* 99 */, 4 /* 9a */, 4 /* 9b */, 5,
	/* 9c */ 4 /* 9d */, 5 /* 9e */, 5 /* 9f */, 6,
	/* a0 */ 2 /* a1 */, 3 /* a2 */, 3 /* a3 */, 4,
	/* a4 */ 3 /* a5 */, 4 /* a6 */, 4 /* a7 */, 5,
	/* a8 */ 3 /* a9 */, 4 /* aa */, 4 /* ab */, 5,
	/* ac */ 4 /* ad */, 5 /* ae */, 5 /* af */, 6,
	/* b0 */ 3 /* b1 */, 4 /* b2 */, 4 /* b3 */, 5,
	/* b4 */ 4 /* b5 */, 5 /* b6 */, 5 /* b7 */, 6,
	/* b8 */ 4 /* b9 */, 5 /* ba */, 5 /* bb */, 6,
	/* bc */ 5 /* bd */, 6 /* be */, 6 /* bf */, 7,
	/* c0 */ 2 /* c1 */, 3 /* c2 */, 3 /* c3 */, 4,
	/* c4 */ 3 /* c5 */, 4 /* c6 */, 4 /* c7 */, 5,
	/* c8 */ 3 /* c9 */, 4 /* ca */, 4 /* cb */, 5,
	/* cc */ 4 /* cd */, 5 /* ce */, 5 /* cf */, 6,
	/* d0 */ 3 /* d1 */, 4 /* d2 */, 4 /* d3 */, 5,
	/* d4 */ 4 /* d5 */, 5 /* d6 */, 5 /* d7 */, 6,
	/* d8 */ 4 /* d9 */, 5 /* da */, 5 /* db */, 6,
	/* dc */ 5 /* dd */, 6 /* de */, 6 /* df */, 7,
	/* e0 */ 3 /* e1 */, 4 /* e2 */, 4 /* e3 */, 5,
	/* e4 */ 4 /* e5 */, 5 /* e6 */, 5 /* e7 */, 6,
	/* e8 */ 4 /* e9 */, 5 /* ea */, 5 /* eb */, 6,
	/* ec */ 5 /* ed */, 6 /* ee */, 6 /* ef */, 7,
	/* f0 */ 4 /* f1 */, 5 /* f2 */, 5 /* f3 */, 6,
	/* f4 */ 5 /* f5 */, 6 /* f6 */, 6 /* f7 */, 7,
	/* f8 */ 5 /* f9 */, 6 /* fa */, 6 /* fb */, 7,
	/* fc */ 6 /* fd */, 7 /* fe */, 7 /* ff */, 8}

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
	0x0f0f0f0f0f0f0f0f,
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
		unsafe.Pointer(&l))

	print(res)

	return float32(res)
}
