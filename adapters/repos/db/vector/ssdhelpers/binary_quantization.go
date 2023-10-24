//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io
package ssdhelpers

import (
	"errors"
	"math"
	"math/bits"
)

type BinaryQuantizer struct {
	dimensions int
}

func NewBinaryQuantizer() *BinaryQuantizer {
	return &BinaryQuantizer{
		dimensions: 256,
	}
}

func (bq *BinaryQuantizer) Fit(data [][]float32) {
	bq.dimensions = len(data[0])

}

func (bq *BinaryQuantizer) Encode(vec []float32) ([]uint64, error) {
	total := bq.dimensions / 64
	if bq.dimensions%64 != 0 {
		total++
	}
	if total*64 < len(vec) {
		return nil, errors.New("BinaryQuantizer.Encode: The vector to encode is longer than those used for training")
	}
	code := make([]uint64, total)
	for j := 0; j < bq.dimensions; j++ {
		if vec[j] < 0 {
			segment := j / 64
			code[segment] += uint64(math.Pow(2, float64(j%64)))
		}
	}
	return code, nil
}

func (bq *BinaryQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.New("BinaryQuantizer.DistanceBetweenCompressedVectors: Both vectors should have the same len")
	}
	total := float32(0)
	for segment := range x {
		total += float32(bits.OnesCount64(x[segment] ^ y[segment]))
	}
	return total, nil
}
