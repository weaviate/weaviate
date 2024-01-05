//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compressionhelpers

import (
	"errors"
	"math"
	"math/bits"

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
	total := len(vec) / 64
	if len(vec)%64 != 0 {
		total++
	}
	code := make([]uint64, total)
	for j := 0; j < len(vec); j++ {
		if vec[j] < 0 {
			segment := j / 64
			code[segment] += uint64(math.Pow(2, float64(j%64)))
		}
	}
	return code
}

func (bq BinaryQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.New("BinaryQuantizer.DistanceBetweenCompressedVectors: Both vectors should have the same len")
	}
	total := float32(0)
	for segment := range x {
		total += float32(bits.OnesCount64(x[segment] ^ y[segment]))
	}
	return total, nil
}
