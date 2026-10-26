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
func dotByteVNNI512Asm(a, b *byte, n int) uint32

//go:noescape
func l2ByteVNNI512Asm(a, b *byte, n int) uint32

//go:noescape
func dotByteAVXVNNIAsm(a, b *byte, n int) uint32

//go:noescape
func l2ByteAVXVNNIAsm(a, b *byte, n int) uint32

// DotByteVNNI512 computes the uint8 dot product with AVX-512 VPDPBUSD.
// Callers must gate on cpu.X86.HasAVX512VNNI && cpu.X86.HasAVX512BW.
func DotByteVNNI512(x []uint8, y []uint8) uint32 {
	if len(x) == 0 {
		return 0
	}
	_ = y[len(x)-1] // the kernel iterates over len(x) bytes of both slices
	return dotByteVNNI512Asm(&x[0], &y[0], len(x))
}

// L2ByteVNNI512 computes the uint8 squared L2 distance with AVX-512
// VPDPBUSD. Callers must gate on cpu.X86.HasAVX512VNNI &&
// cpu.X86.HasAVX512BW.
func L2ByteVNNI512(x []uint8, y []uint8) uint32 {
	if len(x) == 0 {
		return 0
	}
	_ = y[len(x)-1] // the kernel iterates over len(x) bytes of both slices
	return l2ByteVNNI512Asm(&x[0], &y[0], len(x))
}

// DotByteAVXVNNI computes the uint8 dot product with the 256-bit AVX-VNNI
// VPDPBUSD (client CPUs without AVX-512). Callers must gate on
// cpu.X86.HasAVXVNNI.
func DotByteAVXVNNI(x []uint8, y []uint8) uint32 {
	if len(x) == 0 {
		return 0
	}
	_ = y[len(x)-1] // the kernel iterates over len(x) bytes of both slices
	return dotByteAVXVNNIAsm(&x[0], &y[0], len(x))
}

// L2ByteAVXVNNI computes the uint8 squared L2 distance with the 256-bit
// AVX-VNNI VPDPBUSD. Callers must gate on cpu.X86.HasAVXVNNI.
func L2ByteAVXVNNI(x []uint8, y []uint8) uint32 {
	if len(x) == 0 {
		return 0
	}
	_ = y[len(x)-1] // the kernel iterates over len(x) bytes of both slices
	return l2ByteAVXVNNIAsm(&x[0], &y[0], len(x))
}
