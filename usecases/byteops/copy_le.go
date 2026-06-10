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

// Float32sFromBytesZeroCopy reinterprets a little-endian float32 payload as a
// []float32 WITHOUT copying. The returned slice ALIASES b: it is only valid as
// long as b's backing memory is (e.g. an mmap'd segment pinned by a refcounted
// view) and must never be written to (the backing memory may be a read-only
// mapping, where a write faults).
//
// Returns (nil, false) when the reinterpretation is not possible: empty input,
// length not a multiple of 4, or &b[0] not 4-byte aligned (Go's unsafe.Pointer
// alignment rule). Callers must fall back to a copy (CopyBytesToSlice) in that
// case. The big-endian build of this function always returns (nil, false),
// because the on-disk little-endian layout does not match the in-memory
// representation there.
func Float32sFromBytesZeroCopy(b []byte) ([]float32, bool) {
	if len(b) == 0 || len(b)%4 != 0 {
		return nil, false
	}
	if uintptr(unsafe.Pointer(&b[0]))%unsafe.Alignof(float32(0)) != 0 {
		return nil, false
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), len(b)/4), true
}
