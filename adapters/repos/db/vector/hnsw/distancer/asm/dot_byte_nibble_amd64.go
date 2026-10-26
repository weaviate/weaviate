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

//go:noescape
func dotByteNibbleAVX2Asm(q, packed *byte, half int) uint32

//go:noescape
func dotNibbleNibbleAVX2Asm(a, b *byte, n int) uint32

//go:noescape
func dotByteNibbleVNNI512Asm(q, packed *byte, half int) uint32

//go:noescape
func dotNibbleNibbleVNNI512Asm(a, b *byte, n int) uint32

// DotByteNibbleAVX2 computes the dot product between an unpacked 8-bit code
// and a packed 4-bit code in plane layout (byte j holds dimension j in the
// low nibble and dimension j+D/2 in the high nibble), unpacking nibbles in
// registers. Callers must gate on cpu.X86.HasAVX2.
func DotByteNibbleAVX2(q, packed []byte) uint32 {
	half := len(packed)
	if half == 0 {
		return 0
	}
	_ = q[2*half-1] // the kernel reads both nibble planes of q
	return dotByteNibbleAVX2Asm(&q[0], &packed[0], half)
}

// DotNibbleNibbleAVX2 computes the dot product between two packed 4-bit
// codes in plane layout. Callers must gate on cpu.X86.HasAVX2.
func DotNibbleNibbleAVX2(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both codes
	return dotNibbleNibbleAVX2Asm(&a[0], &b[0], len(a))
}

// DotByteNibbleVNNI512 is the AVX-512 VPDPBUSD variant of DotByteNibbleAVX2.
// Callers must gate on cpu.X86.HasAVX512VNNI && cpu.X86.HasAVX512BW.
func DotByteNibbleVNNI512(q, packed []byte) uint32 {
	half := len(packed)
	if half == 0 {
		return 0
	}
	_ = q[2*half-1] // the kernel reads both nibble planes of q
	return dotByteNibbleVNNI512Asm(&q[0], &packed[0], half)
}

// DotNibbleNibbleVNNI512 is the AVX-512 VPDPBUSD variant of
// DotNibbleNibbleAVX2. Callers must gate on cpu.X86.HasAVX512VNNI &&
// cpu.X86.HasAVX512BW.
func DotNibbleNibbleVNNI512(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both codes
	return dotNibbleNibbleVNNI512Asm(&a[0], &b[0], len(a))
}
