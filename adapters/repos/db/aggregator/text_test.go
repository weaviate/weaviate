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
	"github.com/weaviate/weaviate/entities/aggregation"
)

func TestTextAggregator_TopOccurrencesCalculation(t *testing.T) {
	testCases := []struct {
		name                   string
		texts                  []string
		expectedCount          int
		expectedTopOccurrences []aggregation.TextOccurrence
	}{
		{
			name:          "All texts occurring once",
			texts:         []string{"b_occurs1", "c_occurs1", "g_occurs1", "f_occurs1", "a_occurs1", "d_occurs1", "e_occurs1"},
			expectedCount: 7,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "a_occurs1", Occurs: 1},
				{Value: "b_occurs1", Occurs: 1},
				{Value: "c_occurs1", Occurs: 1},
				{Value: "d_occurs1", Occurs: 1},
				{Value: "e_occurs1", Occurs: 1},
			},
		},
		{
			name: "All texts occurring different number of times",
			texts: []string{
				"b_occurs2", "e_occurs5", "d_occurs4", "c_occurs3", "g_occurs7", "e_occurs5", "d_occurs4",
				"f_occurs6", "g_occurs7", "c_occurs3", "b_occurs2", "g_occurs7", "f_occurs6", "g_occurs7", "d_occurs4",
				"a_occurs1", "f_occurs6", "g_occurs7", "g_occurs7", "f_occurs6", "d_occurs4", "e_occurs5", "g_occurs7",
				"c_occurs3", "f_occurs6", "e_occurs5", "f_occurs6", "e_occurs5",
			},
			expectedCount: 28,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "g_occurs7", Occurs: 7},
				{Value: "f_occurs6", Occurs: 6},
				{Value: "e_occurs5", Occurs: 5},
				{Value: "d_occurs4", Occurs: 4},
				{Value: "c_occurs3", Occurs: 3},
			},
		},
		{
			name: "Some texts occurring same number of times",
			texts: []string{
				"a_occurs4", "b_occurs3", "g_occurs4", "f_occurs3", "a_occurs4", "e_occurs2", "a_occurs4",
				"c_occurs2", "g_occurs4", "f_occurs3", "b_occurs3", "c_occurs2", "a_occurs4", "f_occurs3", "g_occurs4",
				"b_occurs3", "d_occurs1", "e_occurs2", "g_occurs4",
			},
			expectedCount: 19,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "a_occurs4", Occurs: 4},
				{Value: "g_occurs4", Occurs: 4},
				{Value: "b_occurs3", Occurs: 3},
				{Value: "f_occurs3", Occurs: 3},
				{Value: "c_occurs2", Occurs: 2},
			},
		},
		{
			name:          "Fewer texts than limit",
			texts:         []string{"b_occurs3", "d_occurs3", "c_occurs1", "d_occurs3", "b_occurs3", "b_occurs3", "a_occurs1", "d_occurs3"},
			expectedCount: 8,
			expectedTopOccurrences: []aggregation.TextOccurrence{
				{Value: "b_occurs3", Occurs: 3},
				{Value: "d_occurs3", Occurs: 3},
				{Value: "a_occurs1", Occurs: 1},
				{Value: "c_occurs1", Occurs: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			agg := newTextAggregator(5)
			for _, text := range tc.texts {
				agg.AddText(text)
			}

			res := agg.Res()
			assert.Equal(t, tc.expectedCount, res.Count)
			assert.Equal(t, tc.expectedTopOccurrences, res.Items)
		})
	}
}
