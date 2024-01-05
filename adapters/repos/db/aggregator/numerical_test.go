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
			numbers:        []float64{2, 4, 4, 4, 5, 5, 7},
			expectedMedian: 4,
		},
		{
			name:           "Even number of elements",
			numbers:        []float64{1, 2, 3, 5, 7, 7},
			expectedMedian: 4,
		},
		{
			name:           "Even number of elements with double elements, median is within double entries",
			numbers:        []float64{1, 2, 3, 3, 6, 7},
			expectedMedian: 3,
		},
		{
			name:           "Even number of elements with double elements, median is after double entries",
			numbers:        []float64{3, 3, 3, 5, 5, 7},
			expectedMedian: 4,
		},
		{
			name:           "Single value",
			numbers:        []float64{42},
			expectedMedian: 42,
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
			})
		}
	}
}

func TestNumericalAggregator_ModeCalculation(t *testing.T) {
	tests := []struct {
		name         string
		numbers      []float64
		expectedMode float64
	}{
		{
			name:         "Different elements (asc)",
			numbers:      []float64{1, 2, 3, 4, 5, 6, 7},
			expectedMode: 1,
		},
		{
			name:         "Different elements (desc)",
			numbers:      []float64{7, 6, 5, 4, 3, 2, 1},
			expectedMode: 1,
		},
		{
			name:         "Elements with different number of duplicates",
			numbers:      []float64{2, 4, 4, 5, 5, 5, 7},
			expectedMode: 5,
		},
		{
			name:         "Elements with same number of duplicates (asc)",
			numbers:      []float64{1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7},
			expectedMode: 1,
		},
		{
			name:         "Elements with same number of duplicates (desc)",
			numbers:      []float64{7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1},
			expectedMode: 1,
		},
		{
			name:         "Elements with same number of duplicates (mixed oreder)",
			numbers:      []float64{5, 2, 6, 2, 1, 4, 7, 5, 7, 4, 3, 1, 6, 3},
			expectedMode: 1,
		},
		{
			name:         "Single element",
			numbers:      []float64{42},
			expectedMode: 42,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := newNumericalAggregator()
			for _, num := range tt.numbers {
				agg.AddFloat64(num)
			}
			agg.buildPairsFromCounts() // needed to populate all required info

			assert.Equal(t, tt.expectedMode, agg.Mode())
		})

		t.Run(tt.name, func(t *testing.T) {
			agg := newNumericalAggregator()
			for _, num := range tt.numbers {
				agg.AddNumberRow(num, 1)
			}
			agg.buildPairsFromCounts() // needed to populate all required info

			assert.Equal(t, tt.expectedMode, agg.Mode())
		})
	}
}

func TestNumericalAggregator_MinMaxCalculation(t *testing.T) {
	tests := []struct {
		name        string
		numbers     []float64
		expectedMin float64
		expectedMax float64
	}{
		{
			name:        "Different positive elements (asc)",
			numbers:     []float64{0, 1, 2, 3, 4, 5, 6, 7},
			expectedMin: 0,
			expectedMax: 7,
		},
		{
			name:        "Different positive elements (desc)",
			numbers:     []float64{7, 6, 5, 4, 3, 2, 1, 0},
			expectedMin: 0,
			expectedMax: 7,
		},
		{
			name:        "Different negative elements (desc)",
			numbers:     []float64{-1, -2, -3, -4, -5, -6, -7},
			expectedMin: -7,
			expectedMax: -1,
		},
		{
			name:        "Different negative elements (asc)",
			numbers:     []float64{-7, -6, -5, -4, -3, -2, -1},
			expectedMin: -7,
			expectedMax: -1,
		},
		{
			name:        "Different elements (mixed order)",
			numbers:     []float64{-7, 6, -5, 4, -3, 2, -1, 0, 1, -2, 3, -4, 5, -6, 7},
			expectedMin: -7,
			expectedMax: 7,
		},
		{
			name:        "Single element",
			numbers:     []float64{-42},
			expectedMin: -42,
			expectedMax: -42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := newNumericalAggregator()
			for _, num := range tt.numbers {
				agg.AddFloat64(num)
			}
			assert.Equal(t, tt.expectedMin, agg.Min())
			assert.Equal(t, tt.expectedMax, agg.Max())
		})

		t.Run(tt.name, func(t *testing.T) {
			agg := newNumericalAggregator()
			for _, num := range tt.numbers {
				agg.AddNumberRow(num, 1)
			}
			assert.Equal(t, tt.expectedMin, agg.Min())
			assert.Equal(t, tt.expectedMax, agg.Max())
		})
	}
}
