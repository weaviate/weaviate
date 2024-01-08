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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompression_CalculateOptimalSegments(t *testing.T) {
	h := &hnsw{}

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
		segments := h.calculateOptimalSegments(tc.dimensions)
		assert.Equal(t, tc.expectedSegments, segments)
	}
}
