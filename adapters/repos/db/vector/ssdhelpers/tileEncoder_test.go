//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package ssdhelpers_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

func TestTileEncoderEncode(t *testing.T) {
	encoder := ssdhelpers.NewTileEncoder(4, 0, ssdhelpers.LogNormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64() + 100)})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, encoder.Encode([]float32{0.1}), byte(0))
	assert.Equal(t, encoder.Encode([]float32{100}), byte(8))
	assert.Equal(t, encoder.Encode([]float32{1000}), byte(16))
}

func TestTileEncoderCentroids(t *testing.T) {
	encoder := ssdhelpers.NewTileEncoder(4, 0, ssdhelpers.LogNormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64() + 100)})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, math.Round(float64(encoder.Centroid(0)[0])), 98.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(2)[0])), 99.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(14)[0])), 101.0)
}

func TestNormalTileEncoderEncode(t *testing.T) {
	encoder := ssdhelpers.NewTileEncoder(4, 0, ssdhelpers.NormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64())})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, encoder.Encode([]float32{0.1}), byte(8))
	assert.Equal(t, encoder.Encode([]float32{100}), byte(16))
	assert.Equal(t, encoder.Encode([]float32{1000}), byte(16))
}

func TestNormalTileEncoderCentroids(t *testing.T) {
	encoder := ssdhelpers.NewTileEncoder(4, 0, ssdhelpers.NormalEncoderDistribution)
	for i := 0; i < 1000000; i++ {
		encoder.Add([]float32{float32(rand.NormFloat64())})
	}
	encoder.Fit([][]float32{})
	assert.Equal(t, math.Round(float64(encoder.Centroid(0)[0])), -2.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(8)[0])), 0.0)
	assert.Equal(t, math.Round(float64(encoder.Centroid(16)[0])), 2.0)
}
