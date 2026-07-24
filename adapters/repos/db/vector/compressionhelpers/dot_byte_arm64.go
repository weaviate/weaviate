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
func dotByteUDOTAsm(a, b *byte, n int) uint32

//go:noescape
func l2ByteUDOTAsm(a, b *byte, n int) uint32

func dotByteUDOT(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both slices
	return dotByteUDOTAsm(&a[0], &b[0], len(a))
}

func l2ByteUDOT(a, b []byte) uint32 {
	if len(a) == 0 {
		return 0
	}
	_ = b[len(a)-1] // the kernel iterates over len(a) bytes of both slices
	return l2ByteUDOTAsm(&a[0], &b[0], len(a))
}
