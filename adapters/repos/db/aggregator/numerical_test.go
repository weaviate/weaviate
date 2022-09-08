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
	}{
		{
			name:           "Uneven number of elements",
			numbers:        []float64{1, 2, 3, 4, 5, 6, 7},
			expectedMedian: 4,
		},
		{
			name:           "Uneven number of elements with double elements",
			numbers:        []float64{2, 2, 4, 4, 5, 5, 7},
			expectedMedian: 4,
		},
		{
			name:           "Even number of elements",
			numbers:        []float64{1, 2, 3, 5, 6, 7},
			expectedMedian: 4,
		},
		{
			name:           "Even number of elements with double elements, median is within double entries",
			numbers:        []float64{1, 2, 3, 3, 6, 7},
			expectedMedian: 3,
		},
		{
			name:           "Even number of elements with double elements, median is after double entries",
			numbers:        []float64{1, 3, 3, 5, 5, 7},
			expectedMedian: 4,
		},
		{
			name:           "Single value",
			numbers:        []float64{42},
			expectedMedian: 42,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := newNumericalAggregator()
			for _, num := range tt.numbers {
				agg.AddFloat64(num)
			}
			agg.buildPairsFromCounts() // needed to populate all required info
			assert.Equal(t, tt.expectedMedian, agg.Median())
		})
	}
}
