//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package concurrency

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimesGomaxprocs(t *testing.T) {
	type testCase struct {
		gomaxprocs int
		factor     int
		expectedN  int
	}

	testCases := []testCase{
		{
			gomaxprocs: 10,
			factor:     -15,
			expectedN:  1,
		},
		{
			gomaxprocs: 10,
			factor:     -4,
			expectedN:  2,
		},
		{
			gomaxprocs: 10,
			factor:     -3,
			expectedN:  3,
		},
		{
			gomaxprocs: 10,
			factor:     -2,
			expectedN:  5,
		},
		{
			gomaxprocs: 10,
			factor:     -1,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     0,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     1,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     2,
			expectedN:  20,
		},
		{
			gomaxprocs: 10,
			factor:     3,
			expectedN:  30,
		},
		{
			gomaxprocs: 10,
			factor:     4,
			expectedN:  40,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("times gomaxprocs %d factor %d", tc.gomaxprocs, tc.factor), func(t *testing.T) {
			n := timesGOMAXPROCS(tc.factor, tc.gomaxprocs)
			assert.Equal(t, tc.expectedN, n)
		})
	}
}

func TestTimesFloatGomaxprocs(t *testing.T) {
	type testCase struct {
		gomaxprocs int
		factor     float64
		expectedN  int
	}

	testCases := []testCase{
		{
			gomaxprocs: 10,
			factor:     -1,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     0,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     0.01,
			expectedN:  1,
		},
		{
			gomaxprocs: 10,
			factor:     0.04,
			expectedN:  1,
		},
		{
			gomaxprocs: 10,
			factor:     0.1,
			expectedN:  1,
		},
		{
			gomaxprocs: 10,
			factor:     0.14,
			expectedN:  1,
		},
		{
			gomaxprocs: 10,
			factor:     0.15,
			expectedN:  2,
		},

		{
			gomaxprocs: 10,
			factor:     0.16,
			expectedN:  2,
		},
		{
			gomaxprocs: 10,
			factor:     0.5,
			expectedN:  5,
		},
		{
			gomaxprocs: 10,
			factor:     1,
			expectedN:  10,
		},
		{
			gomaxprocs: 10,
			factor:     2,
			expectedN:  20,
		},
		{
			gomaxprocs: 10,
			factor:     3,
			expectedN:  30,
		},
		{
			gomaxprocs: 10,
			factor:     4,
			expectedN:  40,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("times gomaxprocs %d factor %f", tc.gomaxprocs, tc.factor), func(t *testing.T) {
			n := timesFloatGOMAXPROCS(tc.factor, tc.gomaxprocs)
			assert.Equal(t, tc.expectedN, n)
		})
	}
}

func TestFractionOf(t *testing.T) {
	type testCase struct {
		original int
		factor   int
		expected int
	}

	testCases := []testCase{
		{
			original: 10,
			factor:   -1,
			expected: 10,
		},
		{
			original: 10,
			factor:   0,
			expected: 10,
		},
		{
			original: 10,
			factor:   1,
			expected: 10,
		},
		{
			original: 10,
			factor:   2,
			expected: 5,
		},
		{
			original: 10,
			factor:   3,
			expected: 3,
		},
		{
			original: 10,
			factor:   24,
			expected: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("fraction of %d factor %d", tc.original, tc.factor), func(t *testing.T) {
			n := FractionOf(tc.original, tc.factor)
			assert.Equal(t, tc.expected, n)
		})
	}
}
