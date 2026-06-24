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

	"github.com/klauspost/compress/zstd"
)

// Raw overwrite payloads are zstd-compressed for the gRPC transport (which,
// unlike REST, does not compress the request body itself). EncodeAll/DecodeAll
// are safe for concurrent use, so a single encoder/decoder is shared.
var (
	overwriteRawZstdEncoder *zstd.Encoder
	overwriteRawZstdDecoder *zstd.Decoder
)

// maxDecodedOverwriteRaw bounds the decompressed size of a raw overwrite payload
// as a decompression-bomb backstop (klauspost defaults to 64GiB). It is ~20x the
// default gRPC max message size — far above any legitimate propagation batch,
// which must already fit the gRPC message size once serialized — so it cannot
// reject real traffic, only refuse a pathological peer payload.
const maxDecodedOverwriteRaw = 2 << 30 // 2 GiB

func init() {
	var err error
	if overwriteRawZstdEncoder, err = zstd.NewWriter(nil); err != nil {
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
