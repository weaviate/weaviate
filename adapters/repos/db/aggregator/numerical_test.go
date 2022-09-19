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

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNumericalAggregator_MedianCalculation(t *testing.T) {
	tests := []struct {
		name           string
		numbers        []float64
		expectedMedian float64
		expectedMode   float64
	}{
		{
			name:           "Uneven number of elements",
			numbers:        []float64{1, 2, 3, 4, 5, 6, 7},
			expectedMedian: 4,
		},
		{
			name:           "Uneven number of elements with double elements",
			numbers:        []float64{2, 4, 4, 4, 5, 5, 7},
			expectedMedian: 4,
			expectedMode:   4,
		},
		{
			name:           "Even number of elements",
			numbers:        []float64{1, 2, 3, 5, 7, 7},
			expectedMedian: 4,
			expectedMode:   7,
		},
		{
			name:           "Even number of elements with double elements, median is within double entries",
			numbers:        []float64{1, 2, 3, 3, 6, 7},
			expectedMedian: 3,
			expectedMode:   3,
		},
		{
			name:           "Even number of elements with double elements, median is after double entries",
			numbers:        []float64{3, 3, 3, 5, 5, 7},
			expectedMedian: 4,
			expectedMode:   3,
		},
		{
			name:           "Single value",
			numbers:        []float64{42},
			expectedMedian: 42,
			expectedMode:   42,
		},
	}
	for _, tt := range tests {
		for i := 0; i < 2; i++ { // test two ways of adding the value to the aggregator
			t.Run(tt.name, func(t *testing.T) {
				agg := newNumericalAggregator()
				for _, num := range tt.numbers {
					if i == 0 {
						agg.AddFloat64(num)
					} else {
						agg.AddNumberRow(num, 1)
					}
				}
				agg.buildPairsFromCounts() // needed to populate all required info
				assert.Equal(t, tt.expectedMedian, agg.Median())
				if tt.expectedMode > 0 { // if there is no value that appears more often than other values
					assert.Equal(t, tt.expectedMode, agg.Mode())
				}
			})
		}
	}
}
