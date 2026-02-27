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

//go:build !amd64 && !arm64 && !arm && !386 && !mips64le && !mipsle && !ppc64le && !riscv64 && !wasm

package byteops

import (
	"encoding/binary"
	"math"
)

// CopyBytesToSlice copies little-endian encoded bytes into a typed slice
// using per-element decoding. This fallback is used on big-endian architectures
// where the in-memory representation differs from the little-endian byte layout on disk.
func CopyBytesToSlice[T float32 | uint64](dst []T, src []byte) {
	switch any(dst).(type) {
	case []float32:
		for i := range dst {
			bits := binary.LittleEndian.Uint32(src[i*Uint32Len : (i+1)*Uint32Len])
			*(*float32)(any(&dst[i]).(*float32)) = math.Float32frombits(bits)
		}
	case []uint64:
		for i := range dst {
			*(*uint64)(any(&dst[i]).(*uint64)) = binary.LittleEndian.Uint64(src[i*Uint64Len : (i+1)*Uint64Len])
		}
	}
}

// CopySliceToBytes copies a typed slice into little-endian encoded bytes
// using per-element encoding. This fallback is used on big-endian architectures
// where the in-memory representation differs from the little-endian byte layout on disk.
func CopySliceToBytes[T float32 | uint64](dst []byte, src []T) {
	switch any(src).(type) {
	case []float32:
		for i := range src {
			binary.LittleEndian.PutUint32(dst[i*Uint32Len:(i+1)*Uint32Len], math.Float32bits(*(*float32)(any(&src[i]).(*float32))))
		}
	case []uint64:
		for i := range src {
			binary.LittleEndian.PutUint64(dst[i*Uint64Len:(i+1)*Uint64Len], *(*uint64)(any(&src[i]).(*uint64)))
		}
	}
}
