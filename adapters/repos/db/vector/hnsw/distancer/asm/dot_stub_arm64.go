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

//go:generate goat ../c/dot_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"

import (
	"reflect"
	"unsafe"
)

// Dot calculates the dot product between two vectors
// using SIMD instructions.
func Dot(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return dot2(x, y)
	case 4:
		return dot4(x, y)
	case 6:
		return dot6(x, y)
	case 8:
		// manually inlined dot8(x, y)
		sum := x[7]*y[7] + x[6]*y[6]
		return dot6(x, y) + sum
	case 10:
		// manually inlined dot10(x, y)
		sum := x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6(x, y) + sum
	case 12:
		// manually inlined dot12(x, y)
		sum := x[11]*y[11] + x[10]*y[10] + x[9]*y[9] + x[8]*y[8] + x[7]*y[7] + x[6]*y[6]
		return dot6(x, y) + sum
	}

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
