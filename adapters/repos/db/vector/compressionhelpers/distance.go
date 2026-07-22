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

package compressionhelpers

import "math/bits"

var l2SquaredByteImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}

	return sum
}

var dotByteImpl func(a, b []uint8) uint32 = func(a, b []byte) uint32 {
	var sum uint32

	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}

	return sum
}

var hammingBitwiseImpl func(a, b []uint64) float32 = func(a, b []uint64) float32 {
	total := float32(0)
	for segment := range a {
		total += float32(bits.OnesCount64(a[segment] ^ b[segment]))
	}
	return total
}

// Dot product between an unpacked 8-bit code (one byte per dimension) and a
// packed 4-bit code in plane layout (byte j holds dimension j in the low
// nibble and dimension j+D/2 in the high nibble). Used by the scalar fallback
// of the asymmetric query-to-data distance computation of the 4-bit
// rotational quantizer; the hot path unpacks the nibbles and uses an int8
// SIMD dot product instead.
var dotByteNibbleImpl func(q, packed []byte) uint32 = func(q, packed []byte) uint32 {
	var sum uint32
	half := len(packed)
	for i, b := range packed {
		sum += uint32(q[i])*uint32(b&0x0F) + uint32(q[half+i])*uint32(b>>4)
	}
	return sum
}

// Dot product between two packed 4-bit codes in plane layout. The pairing of
// dimensions is position-wise, so the layout does not affect the result as
// long as both codes use the same one. Used for compressed-to-compressed
// distances of the 4-bit rotational quantizer. No SIMD implementation exists
// yet, so all architectures use this pure Go version.
var dotNibbleNibbleImpl func(a, b []byte) uint32 = func(a, b []byte) uint32 {
	var sum uint32
	for i := range a {
		x, y := a[i], b[i]
		sum += uint32(x&0x0F)*uint32(y&0x0F) + uint32(x>>4)*uint32(y>>4)
	}
	return sum
}
