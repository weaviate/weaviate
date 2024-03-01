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

package compressionhelpers

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func Test_NoRaceSQEncode(t *testing.T) {
	sq := NewScalarQuantizer([][]float32{
		{0, 0, 0, 0},
		{1, 1, 1, 1},
	}, distancer.NewCosineDistanceProvider())
	vec := []float32{0.5, 1, 0, 2}
	code := sq.Encode(vec)
	assert.NotNil(t, code)
	assert.Equal(t, byte(127), code[0])
	assert.Equal(t, byte(255), code[1])
	assert.Equal(t, byte(0), code[2])
	assert.Equal(t, byte(255), code[3])
	assert.Equal(t, uint16(255+255+127), sq.norm(code))
}

func Test_NoRaceSQDistance(t *testing.T) {
	distancers := []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewCosineDistanceProvider(), distancer.NewDotProductProvider()}
	for _, distancer := range distancers {
		sq := NewScalarQuantizer([][]float32{
			{-1, 0, 0, 0},
			{1, 1, 1, 1},
		}, distancer)
		vec1 := []float32{0.217, 0.435, 0, 0.348}
		vec2 := []float32{0.241, 0.202, 0.257, 0.300}

		dist, err := sq.DistanceBetweenCompressedVectors(sq.Encode(vec1), sq.Encode(vec2))
		expectedDist, _, _ := distancer.SingleDist(vec1, vec2)
		assert.Nil(t, err)
		if err == nil {
			assert.True(t, math.Abs(float64(expectedDist-dist)) < 0.01)
			fmt.Println(expectedDist-dist, expectedDist, dist)
		}
	}
}
