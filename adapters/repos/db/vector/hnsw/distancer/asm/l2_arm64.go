//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

//go:generate goat ../c/l2_neon_byte_arm64.c -O3 -e="--target=arm64" -e="-march=armv8-a+simd+fp"

// L2_Neon is the float32 squared Euclidean distance on arm64: inlined Go up
// to SmallFloatMax elements, the hand-written NEON kernel above that. y must
// be at least as long as x.
func L2_Neon(x []float32, y []float32) float32 {
	if len(x) <= 2 { // geo coordinates (n=2): keep the hottest small cases inline
		if len(x) == 2 {
			d0, d1 := x[0]-y[0], x[1]-y[1]
			return d0*d0 + d1*d1
		}
		if len(x) == 1 {
			d := x[0] - y[0]
			return d * d
		}
		return 0
	}
	if len(x) <= SmallFloatMax {
		return l2Small(x, y)
	}
	return l2NEON(x, y)
}

func L2ByteARM64(x []uint8, y []uint8) uint32 {
	switch len(x) {
	case 2:
		return l22[uint8, uint32](x, y)
	case 4:
		return l24[uint8, uint32](x, y)
	case 6:
		// manually inlined l26(x, y). The differences must be widened before
		// squaring: uint8 arithmetic would truncate the squares mod 256.
		diff := uint32(x[5]) - uint32(y[5])
		sum := diff * diff

		diff = uint32(x[4]) - uint32(y[4])
		sum += diff * diff

		return l24[uint8, uint32](x, y) + sum
	case 8:
		// manually inlined l28(x, y)
		diff := uint32(x[7]) - uint32(y[7])
		sum := diff * diff

		diff = uint32(x[6]) - uint32(y[6])
		sum += diff * diff

		diff = uint32(x[5]) - uint32(y[5])
		sum += diff * diff

		diff = uint32(x[4]) - uint32(y[4])
		sum += diff * diff

		return l24[uint8, uint32](x, y) + sum
	case 10:
		return l210[uint8, uint32](x, y)
	case 12:
		return l212[uint8, uint32](x, y)
	}

	// deal with odd lengths and lengths 13, 14, 15
	if len(x) < 16 { // the AVX-512 kernel needs one full 16-lane vector
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
