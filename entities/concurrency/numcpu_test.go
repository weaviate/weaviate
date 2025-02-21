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

package concurrency

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTimesNumcpu(t *testing.T) {
	type testCase struct {
		numcpu    int
		factor    int
		expectedN int
	}

	testCases := []testCase{
		{
			numcpu:    10,
			factor:    -15,
			expectedN: 1,
		},
		{
			numcpu:    10,
			factor:    -4,
			expectedN: 2,
		},
		{
			numcpu:    10,
			factor:    -3,
			expectedN: 3,
		},
		{
			numcpu:    10,
			factor:    -2,
			expectedN: 5,
		},
		{
			numcpu:    10,
			factor:    -1,
			expectedN: 10,
		},
		{
			numcpu:    10,
			factor:    0,
			expectedN: 10,
		},
		{
			numcpu:    10,
			factor:    1,
			expectedN: 10,
		},
		{
			numcpu:    10,
			factor:    2,
			expectedN: 20,
		},
		{
			numcpu:    10,
			factor:    3,
			expectedN: 30,
		},
		{
			numcpu:    10,
			factor:    4,
			expectedN: 40,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("times numcpu %d factor %d", tc.numcpu, tc.factor), func(t *testing.T) {
			n := timesNUMCPU(tc.factor, tc.numcpu)
			assert.Equal(t, tc.expectedN, n)
		})
	}
}
