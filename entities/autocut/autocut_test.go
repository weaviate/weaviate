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

package autocut

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAutoCut(t *testing.T) {
	cases := []struct {
		values          []float32
		cutOff          int
		expectedResults int
	}{
		{values: []float32{}, cutOff: 1, expectedResults: 0},
		{values: []float32{2}, cutOff: 1, expectedResults: 1},
		{values: []float32{2, 1.95, 1.9, 0.2, 0.1, 0.1, -1}, cutOff: 1, expectedResults: 3},
		{values: []float32{2, 1.95, 1.9, 0.2, 0.1, 0.1, -2}, cutOff: 2, expectedResults: 6},
		{values: []float32{5, 1, 1, 1, 1, 0, 0}, cutOff: 1, expectedResults: 1},
		{values: []float32{5, 1, 1, 1, 1, 0, 0}, cutOff: 2, expectedResults: 5},
		{values: []float32{0.298, 0.260, 0.169, 0.108, 0.108, 0.104, 0.093}, cutOff: 1, expectedResults: 3},
		{values: []float32{0.5, 0.32, 0.31, 0.30, 0.29, 0.15}, cutOff: 1, expectedResults: 1},
		{values: []float32{0.5, 0.32, 0.31, 0.30, 0.29, 0.15, 0.15, 0.15}, cutOff: 2, expectedResults: 5},
		{values: []float32{1.0, 0.98, 0.95, 0.9, 0.88, 0.87, 0.80, 0.79}, cutOff: 1, expectedResults: 3},
		{values: []float32{1.0, 0.98, 0.95, 0.9, 0.88, 0.87, 0.80, 0.79}, cutOff: 2, expectedResults: 6},
		{values: []float32{1.0, 0.98, 0.95, 0.9, 0.88, 0.87, 0.80, 0.79}, cutOff: 3, expectedResults: 8}, // all values
		{values: []float32{0.586835, 0.5450372, 0.34137487, 0.30482167, 0.2753393}, cutOff: 1, expectedResults: 2},
		{values: []float32{0.36663342, 0.33818772, 0.045160502, 0.045160501}, cutOff: 1, expectedResults: 2},
	}
	for _, tt := range cases {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tt.expectedResults, Autocut(tt.values, tt.cutOff))
		})
	}
}
