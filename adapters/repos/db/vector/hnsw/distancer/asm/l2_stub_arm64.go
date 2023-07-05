//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package asm

// To generate the asm code, run:
//   go install github.com/gorse-io/goat@v0.1.0
//   go generate

//go:generate goat ../c/l2_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"

import (
	"reflect"
	"unsafe"
)

// L2 calculates the L2 distance between two vectors
// using SIMD instructions.
func L2(x []float32, y []float32) float32 {
	var res float32

	// The C function expects pointers to the underlying array, not slices.
	hdrx := (*reflect.SliceHeader)(unsafe.Pointer(&x))
	hdry := (*reflect.SliceHeader)(unsafe.Pointer(&y))

	l := len(x)
	l2(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(hdrx.Data),
		unsafe.Pointer(hdry.Data),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}
