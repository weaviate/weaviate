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
			expectedEqual: false,
		},
		{
			vecA:          []float32{},
			vecB:          nil,
			expectedEqual: false,
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
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) {
			assert.Equal(t, tc.expectedEqual, VectorsEqual(tc.vecA, tc.vecB))
		})
	}
}
