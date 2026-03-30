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

//go:build amd64 || arm64 || arm || 386 || mips64le || mipsle || ppc64le || riscv64 || wasm

package byteops

import "unsafe"

// CopyBytesToSlice bulk-copies src bytes into dst typed slice using a single memmove.
// This is safe on little-endian architectures where the in-memory representation
// of float32/uint64 matches the little-endian byte layout used on disk.
func CopyBytesToSlice[T float32 | uint64](dst []T, src []byte) {
	if len(dst) == 0 {
		return
	}
	// Reinterpret the typed slice's backing array as []byte so we can bulk-copy
	// the raw bytes directly, avoiding per-element decoding.
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), len(dst)*int(unsafe.Sizeof(dst[0])))
	copy(dstBytes, src)
}

// CopySliceToBytes bulk-copies a typed slice into dst bytes using a single memmove.
// This is safe on little-endian architectures where the in-memory representation
// of float32/uint64 matches the little-endian byte layout used on disk.
func CopySliceToBytes[T float32 | uint64](dst []byte, src []T) {
	if len(src) == 0 {
		return
	}
	// Reinterpret the typed slice's backing array as []byte so we can bulk-copy
	// the raw bytes directly, avoiding per-element encoding.
	srcBytes := unsafe.Slice((*byte)(unsafe.Pointer(&src[0])), len(src)*int(unsafe.Sizeof(src[0])))
	copy(dst, srcBytes)
}
