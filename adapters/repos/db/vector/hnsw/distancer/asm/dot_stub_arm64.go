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

//go:generate goat ../c/dot_arm64.c -O3 -e="--target=arm64-arm-none-eabi"

import (
	"reflect"
	"unsafe"
)

// Dot calculates the dot product between two vectors
// using SIMD instructions.
func Dot(x []float32, y []float32) float32 {
	var res float32

	// The C function expects pointers to the underlying array, not slices.
	hdrx := (*reflect.SliceHeader)(unsafe.Pointer(&x))
	hdry := (*reflect.SliceHeader)(unsafe.Pointer(&y))

	l := len(x)
	dot(
		// The slice header contains the address of the underlying array.
		// We only need to cast it to a pointer.
		unsafe.Pointer(hdrx.Data),
		unsafe.Pointer(hdry.Data),
		// The C function expects pointers to the result and the length of the arrays.
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}
