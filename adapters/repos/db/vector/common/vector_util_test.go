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

package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVectorUtil_Equal(t *testing.T) {
	type testCase struct {
		vecA          []float32
		vecB          []float32
		expectedEqual bool
	}

	testCases := []testCase{
		{
			vecA:          nil,
			vecB:          nil,
			expectedEqual: true,
		},
		{
			vecA:          nil,
			vecB:          []float32{},
			expectedEqual: true,
		},
		{
			vecA:          []float32{},
			vecB:          nil,
			expectedEqual: true,
		},
		{
			vecA:          []float32{},
			vecB:          []float32{},
			expectedEqual: true,
		},
		{
			vecA:          []float32{1, 2, 3},
			vecB:          []float32{1., 2., 3.},
			expectedEqual: true,
		},
		{
			vecA:          []float32{1, 2, 3, 4},
			vecB:          []float32{1., 2., 3.},
			expectedEqual: false,
		},
		{
			vecA:          []float32{1, 2, 3},
			vecB:          []float32{1., 2., 3., 4.},
			expectedEqual: false,
		},
		{
			vecA:          []float32{},
			vecB:          []float32{1., 2., 3.},
			expectedEqual: false,
		},
		{
			vecA:          []float32{1, 2, 3},
			vecB:          []float32{},
			expectedEqual: false,
		},
		{
			vecA:          []float32{1, 2, 3},
			vecB:          []float32{2, 3, 4},
			expectedEqual: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) {
			assert.Equal(t, tc.expectedEqual, VectorsEqual(tc.vecA, tc.vecB))
		})
	}
}

func TestMultiVectorUtil_Equal(t *testing.T) {
	type testCase struct {
		vecA          [][]float32
		vecB          [][]float32
		expectedEqual bool
	}

	testCases := []testCase{
		{
			vecA:          nil,
			vecB:          nil,
			expectedEqual: true,
		},
		{
			vecA:          nil,
			vecB:          [][]float32{},
			expectedEqual: true,
		},
		{
			vecA:          [][]float32{},
			vecB:          nil,
			expectedEqual: true,
		},
		{
			vecA:          [][]float32{},
			vecB:          [][]float32{},
			expectedEqual: true,
		},
		{
			vecA:          [][]float32{{1, 2, 3}},
			vecB:          [][]float32{{1., 2., 3.}},
			expectedEqual: true,
		},
		{
			vecA:          [][]float32{{1, 2, 3, 4}},
			vecB:          [][]float32{{1., 2., 3.}},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{{1, 2, 3}},
			vecB:          [][]float32{{1., 2., 3., 4.}},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{},
			vecB:          [][]float32{{1., 2., 3.}},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{{1, 2, 3}},
			vecB:          [][]float32{},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{{1, 2, 3}, {11, 22, 33}, {111, 222, 333}},
			vecB:          [][]float32{{1, 2, 3}, {11, 22, 33}},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{{1, 2, 3}, {11, 22, 33}},
			vecB:          [][]float32{{1, 2, 3}, {11, 22, 33}, {111, 222, 333}},
			expectedEqual: false,
		},
		{
			vecA:          [][]float32{{1, 2, 3}, {11, 22, 33}, {111, 222, 333}},
			vecB:          [][]float32{{1, 2, 3}, {11, 22, 33}, {111, 222, 333}},
			expectedEqual: true,
		},
		{
			vecA:          [][]float32{{1, 2, 3}, {11, 22, 33}, {111, 222, 333}},
			vecB:          [][]float32{{11, 22, 33}, {111, 222, 333}, {1, 2, 3}},
			expectedEqual: false,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) {
			assert.Equal(t, tc.expectedEqual, MultiVectorsEqual(tc.vecA, tc.vecB))
		})
	}
}

func Test_CalculateOptimalSegments(t *testing.T) {
	type testCase struct {
		dimensions       int
		expectedSegments int
	}

	for _, tc := range []testCase{
		{
			dimensions:       2048,
			expectedSegments: 256,
		},
		{
			dimensions:       1536,
			expectedSegments: 256,
		},
		{
			dimensions:       768,
			expectedSegments: 128,
		},
		{
			dimensions:       512,
			expectedSegments: 128,
		},
		{
			dimensions:       256,
			expectedSegments: 64,
		},
		{
			dimensions:       125,
			expectedSegments: 125,
		},
		{
			dimensions:       64,
			expectedSegments: 32,
		},
		{
			dimensions:       27,
			expectedSegments: 27,
		},
		{
			dimensions:       19,
			expectedSegments: 19,
		},
		{
			dimensions:       2,
			expectedSegments: 1,
		},
	} {
		segments := CalculateOptimalSegments(tc.dimensions)
		assert.Equal(t, tc.expectedSegments, segments)
	}
}
