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

import "unsafe"

// L2AVX256 is the float32 squared Euclidean distance for AVX2+FMA hosts:
// inlined Go up to SmallFloatMax elements, the hand-written kernel above
// that. y must be at least as long as x.
func L2AVX256(x []float32, y []float32) float32 {
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
	return l2AVX2(x, y)
}

// L2AVX512 is L2AVX256 for AVX-512 hosts; see DotAVX512 for the length
// routing.
func L2AVX512(x []float32, y []float32) float32 {
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
	if len(x) < 16 { // the AVX-512 kernel needs one full 16-lane vector
		return l2AVX2(x, y)
	}
	return l2AVX512(x, y)
}

func L2ByteAVX256(x []uint8, y []uint8) uint32 {
	switch len(x) {
	case 1:
		diff := uint32(x[0]) - uint32(y[0])
		return diff * diff
	case 2:
		return l22[uint8, uint32](x, y)
	case 3:
		return l23[uint8, uint32](x, y)
	case 4:
		return l24[uint8, uint32](x, y)
	case 5:
		return l25[uint8, uint32](x, y)
	case 6:
		// manually inlined l26(x, y)
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

	var res uint32

	l := len(x)
	l2_byte_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}

func L2FloatByteAVX256(x []float32, y []uint8) float32 {
	var res float32

	switch len(x) {
	case 1:
		diff := x[0] - float32(y[0])
		return diff * diff
	case 2:
		return l22FloatByte(x, y)
	case 3:
		return l23FloatByte(x, y)
	case 4:
		return l24FloatByte(x, y)
	case 5:
		return l25FloatByte(x, y)
	case 6:
		// manually inlined l26(x, y)
		diff := x[5] - float32(y[5])
		sum := diff * diff

		diff = x[4] - float32(y[4])
		sum += diff * diff

		return l24FloatByte(x, y) + sum
	case 8:
		// manually inlined l28(x, y)
		diff := x[7] - float32(y[7])
		sum := diff * diff

		diff = x[6] - float32(y[6])
		sum += diff * diff

		diff = x[5] - float32(y[5])
		sum += diff * diff

		diff = x[4] - float32(y[4])
		sum += diff * diff

		return l24FloatByte(x, y) + sum
	case 10:
		return l210FloatByte(x, y)
	case 12:
		return l212FloatByte(x, y)
	}

	l := len(x)
	l2_float_byte_256(
		unsafe.Pointer(unsafe.SliceData(x)),
		unsafe.Pointer(unsafe.SliceData(y)),
		unsafe.Pointer(&res),
		unsafe.Pointer(&l))

	return res
}
