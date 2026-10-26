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

// dotByteNibbleGo is the pure Go dot product between an unpacked 8-bit code
// (one byte per dimension) and a packed 4-bit code in plane layout (byte j
// holds dimension j in the low nibble and dimension j+D/2 in the high
// nibble). It is the reference implementation for the fused SIMD kernels and
// the fallback on CPUs without them.
func dotByteNibbleGo(q, packed []byte) uint32 {
	var sum uint32
	half := len(packed)
	for i, b := range packed {
		sum += uint32(q[i])*uint32(b&0x0F) + uint32(q[half+i])*uint32(b>>4)
	}
	return sum
}

// Asymmetric query-to-data dot product of the 4-bit rotational quantizer.
// Arch-specific init replaces this with a fused SIMD kernel that unpacks the
// nibbles in registers (UDOT/UADALP on arm64, VPMADDUBSW on amd64 with AVX2).
var dotByteNibbleImpl func(q, packed []byte) uint32 = dotByteNibbleGo

// dotNibbleNibbleGo is the pure Go dot product between two packed 4-bit
// codes in plane layout. The pairing of dimensions is position-wise, so the
// layout does not affect the result as long as both codes use the same one.
func dotNibbleNibbleGo(a, b []byte) uint32 {
	var sum uint32
	for i := range a {
		x, y := a[i], b[i]
		sum += uint32(x&0x0F)*uint32(y&0x0F) + uint32(x>>4)*uint32(y>>4)
	}
	return sum
}

// Compressed-to-compressed dot product of the 4-bit rotational quantizer,
// hot during HNSW inserts (neighbor re-ranking works on stored codes).
// Arch-specific init replaces this with a fused SIMD kernel.
var dotNibbleNibbleImpl func(a, b []byte) uint32 = dotNibbleNibbleGo
