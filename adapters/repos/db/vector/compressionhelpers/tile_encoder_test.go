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

//go:build !race

package compressionhelpers_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

func Test_NoRaceTileEncoderEncode(t *testing.T) {
	encoder := compressionhelpers.NewTileEncoder(4, 0, compressionhelpers.LogNormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64() + 100)})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, encoder.Encode([]float32{0.1}), byte(0))
	assert.Equal(t, encoder.Encode([]float32{100}), byte(8))
	assert.Equal(t, encoder.Encode([]float32{1000}), byte(16))
}

func Test_NoRaceTileEncoderCentroids(t *testing.T) {
	encoder := compressionhelpers.NewTileEncoder(4, 0, compressionhelpers.LogNormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64() + 100)})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, math.Round(float64(encoder.Centroid(0)[0])), 98.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(2)[0])), 99.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(14)[0])), 101.0)
}

func Test_NoRaceNormalTileEncoderEncode(t *testing.T) {
	encoder := compressionhelpers.NewTileEncoder(4, 0, compressionhelpers.NormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64())})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, encoder.Encode([]float32{0.1}), byte(8))
	assert.Equal(t, encoder.Encode([]float32{100}), byte(16))
	assert.Equal(t, encoder.Encode([]float32{1000}), byte(16))
}

func Test_NoRaceNormalTileEncoderCentroids(t *testing.T) {
	encoder := compressionhelpers.NewTileEncoder(4, 0, compressionhelpers.NormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64())})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, math.Round(float64(encoder.Centroid(0)[0])), -2.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(8)[0])), 0.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(15)[0])), 2.0)
}
