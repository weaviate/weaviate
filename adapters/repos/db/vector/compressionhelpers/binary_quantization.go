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
	total := (len + 63) >> 6
	fullBlocks := len >> 6
	code := make([]uint64, total)
	i := 0
	// Process vec in blocks of 64 elements
	for block := 0; block < fullBlocks; block++ {
		var bits uint64
		var bit uint64 = 1
		for bit != 0 {
			if vec[i] < 0 {
				bits |= bit
			}
			i++
			bit <<= 1
		}
		code[block] = bits
	}
	// Process the tail block (fewer than 64 elements)
	if tail := len & 63; tail != 0 {
		var bits uint64
		var bit uint64 = 1
		for tail != 0 {
			if vec[i] < 0 {
				bits |= bit
			}
			bit <<= 1
			i++
			tail--
		}
		code[fullBlocks] = bits
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
