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

//go:build arm64

package compressionhelpers

//go:noescape
func dotByteNibbleUDOTAsm(q, packed *byte, half int) uint32

//go:noescape
func dotByteNibbleUADALPAsm(q, packed *byte, half int) uint32

//go:noescape
func dotNibbleNibbleUDOTAsm(a, b *byte, n int) uint32

//go:noescape
func dotNibbleNibbleUADALPAsm(a, b *byte, n int) uint32

func dotByteNibbleUDOT(q, packed []byte) uint32 {
	half := len(packed)
	if half == 0 {
		return 0
	}
	_ = q[2*half-1] // the kernel reads both nibble planes of q
	return dotByteNibbleUDOTAsm(&q[0], &packed[0], half)
}

func dotByteNibbleUADALP(q, packed []byte) uint32 {
	half := len(packed)
	if half == 0 {
		return 0
	}
	_ = q[2*half-1] // the kernel reads both nibble planes of q
	return dotByteNibbleUADALPAsm(&q[0], &packed[0], half)
}

func dotNibbleNibbleUDOT(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both codes
	return dotNibbleNibbleUDOTAsm(&a[0], &b[0], len(a))
}

func dotNibbleNibbleUADALP(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both codes
	return dotNibbleNibbleUADALPAsm(&a[0], &b[0], len(a))
}
