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

import "unsafe"

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
