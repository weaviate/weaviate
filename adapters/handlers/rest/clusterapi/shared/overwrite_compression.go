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

package shared

import (
	"fmt"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

// Shared because EncodeAll/DecodeAll are safe for concurrent use.
var (
	overwriteRawZstdEncoder *zstd.Encoder
	overwriteRawZstdDecoder *zstd.Decoder
)

// maxDecodedOverwriteRaw is a decompression-bomb backstop (klauspost defaults to
// 64GiB). ~20x the default gRPC max message size, so it never rejects real
// batches (which must fit that size serialized), only pathological payloads.
const maxDecodedOverwriteRaw = 2 << 30 // 2 GiB

func init() {
	var err error
	// Sub-encoder state is retained for the process lifetime, so concurrency and
	// window are capped: defaults hold GOMAXPROCS×~1.5MiB permanently and grow by
	// 16MiB per sub-encoder on any payload over one block (128KiB).
	if overwriteRawZstdEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(min(4, runtime.GOMAXPROCS(0))),
		zstd.WithWindowSize(1<<20)); err != nil {
		panic(fmt.Sprintf("init overwrite raw zstd encoder: %v", err))
	}
	if overwriteRawZstdDecoder, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecodedOverwriteRaw)); err != nil {
		panic(fmt.Sprintf("init overwrite raw zstd decoder: %v", err))
	}
}

// CompressOverwriteRaw zstd-compresses a raw-encoded overwrite payload.
func CompressOverwriteRaw(in []byte) []byte {
	return overwriteRawZstdEncoder.EncodeAll(in, make([]byte, 0, len(in)))
}

// DecompressOverwriteRaw reverses CompressOverwriteRaw. It errors if the
// decompressed payload would exceed maxDecodedOverwriteRaw.
func DecompressOverwriteRaw(in []byte) ([]byte, error) {
	return overwriteRawZstdDecoder.DecodeAll(in, nil)
}
