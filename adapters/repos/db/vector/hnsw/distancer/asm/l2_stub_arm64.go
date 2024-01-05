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
	"reflect"
	"unsafe"
)

// To generate the asm code, run:
//   go install github.com/gorse-io/goat@v0.1.0
//   go generate

//go:generate goat ../c/l2_arm64.c -O3 -e="-mfpu=neon-fp-armv8" -e="-mfloat-abi=hard" -e="--target=arm64" -e="-march=armv8-a+simd+fp"

// L2 calculates the L2 distance between two vectors
// using SIMD instructions when possible.
// Vector lengths < 16 are handled by the Go implementation
// because the overhead of using reflection is too high.
func L2(x []float32, y []float32) float32 {
	switch len(x) {
	case 2:
		return l22(x, y)
	case 4:
		return l24(x, y)
	case 6:
		// manually inlined l26(x, y)
		diff := x[5] - y[5]
		sum := diff * diff

		diff = x[4] - y[4]
		sum += diff * diff

		return l24(x, y) + sum
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

		return l24(x, y) + sum
	case 10:
		return l210(x, y)
	case 12:
		return l212(x, y)
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
