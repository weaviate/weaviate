//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type BinaryQuantizer struct {
	distancer distancer.Provider
}

func NewBinaryQuantizer(distancer distancer.Provider) BinaryQuantizer {
	return BinaryQuantizer{
		distancer: distancer,
	}
}

func (bq BinaryQuantizer) Encode(vec []float32) []uint64 {
	len := len(vec)
	blocks := (len + 63) >> 6 // ceil(len / 64)
	code := make([]uint64, blocks)
	i := 0
	for b := range blocks {
		var bits uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			if vec[i] < 0 {
				bits |= bit
			}
			i++
			if i == len {
				break
			}
		}
		code[b] = bits
	}
	return code
}

func (bq BinaryQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	return distancer.HammingBitwise(x, y)
}

type BQStats struct{}

func (b BQStats) CompressionType() string {
	return "bq"
}

func (bq *BinaryQuantizer) Stats() CompressionStats {
	return BQStats{}
}
