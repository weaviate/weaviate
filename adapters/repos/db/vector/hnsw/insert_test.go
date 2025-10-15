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

package hnsw

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHnswCappedLevel(t *testing.T) {
	type testCase struct {
		name           string
		maxConnections int
		randomValue    float64
		expectedLevel  uint8
	}

	testCases := []testCase{
		{
			name:           "test max function with normal value",
			maxConnections: 64,
			randomValue:    0.3,
			expectedLevel:  0,
		},
		{
			name:           "randFunc returning 1-eps (very close to 1)",
			maxConnections: 16,
			randomValue:    1.0 - 1e-15,
			expectedLevel:  0,
		},
		{
			name:           "randFunc returning 1",
			maxConnections: 16,
			randomValue:    1.0,
			expectedLevel:  0,
		},
		{
			name:           "randFunc returning 0.5",
			maxConnections: 32,
			randomValue:    0.5,
			expectedLevel:  0,
		},
		{
			name:           "randFunc returning 0.1",
			maxConnections: 16,
			randomValue:    0.1,
			expectedLevel:  0,
		},
		{
			name:           "randFunc returning 0.01",
			maxConnections: 16,
			randomValue:    0.01,
			expectedLevel:  1,
		},
		{
			name:           "randFunc returning 1e-10",
			maxConnections: 16,
			randomValue:    1e-10,
			expectedLevel:  8,
		},
		{
			name:           "randFunc returning exactly 1e-20",
			maxConnections: 16,
			randomValue:    1e-20,
			expectedLevel:  15,
		},
		{
			name:           "randFunc returning value less than 1e-20",
			maxConnections: 16,
			randomValue:    1e-25,
			expectedLevel:  15,
		},
		{
			name:           "different maxConnections: 8 with 0.1",
			maxConnections: 8,
			randomValue:    0.1,
			expectedLevel:  1,
		},
		{
			name:           "different maxConnections: 32 with 0.1",
			maxConnections: 32,
			randomValue:    0.1,
			expectedLevel:  0,
		},
		{
			name:           "different maxConnections: 8 with 0.01",
			maxConnections: 8,
			randomValue:    0.01,
			expectedLevel:  2,
		},
		{
			name:           "different maxConnections: 32 with 0.01",
			maxConnections: 32,
			randomValue:    0.01,
			expectedLevel:  1,
		},
		{
			name:           "test max function with very small value",
			maxConnections: 2,
			randomValue:    1e-50,
			expectedLevel:  63, // Should use 1e-20 due to max function, same as 1e-20 case
		},
		{
			name:           "test max function with very small value",
			maxConnections: 2,
			randomValue:    1e-100,
			expectedLevel:  63, // Should use 1e-20 due to max function, same as 1e-20 case
		},
		{
			name:           "test max function with zero small value",
			maxConnections: 2,
			randomValue:    0.0,
			expectedLevel:  63,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create minimal HNSW struct with only the fields needed for level() function
			h := &hnsw{
				randFunc:        func() float64 { return tc.randomValue },
				levelNormalizer: 1.0 / math.Log(float64(tc.maxConnections)),
			}

			level := h.generateLevel()

			oldLevel := 0
			if tc.randomValue != 0 {
				oldLevel = int(math.Floor(-math.Log(tc.randomValue) * h.levelNormalizer))
			}

			assert.Equal(t, tc.expectedLevel, level,
				"Test case: %s\nRandom value: %f\nMaxConnections: %d\nLevelNormalizer: %f\nExpected: %d, Got: %d, OldLevel: %d",
				tc.name, tc.randomValue, tc.maxConnections, h.levelNormalizer, tc.expectedLevel, level, oldLevel)
		})
	}
}
