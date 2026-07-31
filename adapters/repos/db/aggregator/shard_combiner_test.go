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

package aggregator

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
)

const (
	YearMonthDayHourMinute = "2022-06-16T18:30:"
	NanoSecondsTimeZone    = ".451235Z"
)

type TestStructDates struct {
	name            string
	dates1          []string
	dates2          []string
	expectedMedian  string
	expectedMaximum string
	expectedMode    string
	expectedMinimum string
}

func TestShardCombinerMergeDates(t *testing.T) {
	tests := []TestStructDates{
		{
			name:            "Many values",
			dates1:          []string{"55", "26", "10"},
			dates2:          []string{"15", "26", "45", "26"},
			expectedMaximum: "55",
			expectedMinimum: "10",
			expectedMedian:  "26",
			expectedMode:    "26",
		},
		{
			name:            "Struct with single element",
			dates1:          []string{"45"},
			dates2:          []string{"00", "26", "45", "27"},
			expectedMaximum: "45",
			expectedMinimum: "00",
			expectedMedian:  "27",
			expectedMode:    "45",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDates(t, tt.dates1, tt.dates2, tt)
			testDates(t, tt.dates2, tt.dates1, tt)
		})
	}
}

func testDates(t *testing.T, dates1, dates2 []string, tt TestStructDates) {
	sc := NewShardCombiner(aggregation.Params{})
	dateMap1 := createDateAgg(dates1)
	dateMap2 := createDateAgg(dates2)

	sc.mergeDateProp(dateMap1, dateMap2)
	sc.finalizeDateProp(dateMap1)
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMinimum+NanoSecondsTimeZone, dateMap1["minimum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMaximum+NanoSecondsTimeZone, dateMap1["maximum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMedian+NanoSecondsTimeZone, dateMap1["median"])
	assert.Equal(t, int64(len(tt.dates1)+len(tt.dates2)), dateMap1["count"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMode+NanoSecondsTimeZone, dateMap1["mode"])
}

func createDateAgg(dates []string) map[string]interface{} {
	agg := newDateAggregator()
	for _, date := range dates {
		agg.AddTimestamp(YearMonthDayHourMinute + date + NanoSecondsTimeZone)
	}
	agg.buildPairsFromCounts() // needed to populate all required info

	prop := aggregation.Property{}
	aggs := []aggregation.Aggregator{aggregation.MedianAggregator, aggregation.MinimumAggregator, aggregation.MaximumAggregator, aggregation.CountAggregator, aggregation.ModeAggregator}
	addDateAggregations(&prop, aggs, agg)
	return prop.DateAggregations
}

type TestStructNumbers struct {
	name     string
	numbers1 []float64
	numbers2 []float64
	testMode bool
}

func TestShardCombinerMergeNumerical(t *testing.T) {
	tests := []TestStructNumbers{
		{
			name:     "Uneven number of elements for both",
			numbers1: []float64{0, 9, 9},
			numbers2: []float64{2},
			testMode: true,
		},
		{
			name:     "Even number of elements for both",
			numbers1: []float64{0, 5, 10, 15},
			numbers2: []float64{15, 15},
			testMode: true,
		},
		{
			name:     "Mode is affected by merge",
			numbers1: []float64{2.5, 2.5, 10, 15},
			numbers2: []float64{15, 15},
			testMode: true,
		},
		{
			name:     "random",
			numbers1: createRandomSlice(),
			numbers2: createRandomSlice(),
			testMode: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testNumbers(t, tt.numbers1, tt.numbers2, tt.testMode)
			testNumbers(t, tt.numbers2, tt.numbers1, tt.testMode)
		})
	}
}

func TestShardCombinerMergeNil(t *testing.T) {
	tests := []struct {
		name         string
		results      []*aggregation.Result
		totalResults int
	}{
		{
			name: "First is nil",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{},
				},
				{
					Groups: []aggregation.Group{{GroupedBy: &aggregation.GroupedBy{Value: 10, Path: []string{"something"}}}},
				},
			},
			totalResults: 1,
		},
		{
			name: "Second is nil",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{{GroupedBy: &aggregation.GroupedBy{Value: 10, Path: []string{"something"}}}},
				},
				{
					Groups: []aggregation.Group{},
				},
			},
			totalResults: 1,
		},
		{
			name: "Both are nil",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{},
				},
				{
					Groups: []aggregation.Group{},
				},
			},
			totalResults: 0,
		},
		{
			name: "Non are nil",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{{GroupedBy: &aggregation.GroupedBy{Value: 9, Path: []string{"other thing"}}}},
				},
				{
					Groups: []aggregation.Group{{GroupedBy: &aggregation.GroupedBy{Value: 10, Path: []string{"something"}}}},
				},
			},
			totalResults: 2,
		},
		{
			name: "Ungrouped with nil",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{{Count: 1}},
				},
				{
					Groups: []aggregation.Group{},
				},
			},
			totalResults: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			combinedResults := NewShardCombiner(aggregation.Params{}).Do(tt.results)
			assert.Equal(t, len(combinedResults.Groups), tt.totalResults)
		})
	}
}

const cutoffTestProp = "someProp"

// textShard is one shard's ungrouped result for a single text property.
func textShard(text aggregation.Text) *aggregation.Result {
	return &aggregation.Result{
		Groups: []aggregation.Group{{
			Count: text.Count,
			Properties: map[string]aggregation.Property{
				cutoffTestProp: {
					Type:            aggregation.PropertyTypeText,
					SchemaType:      "text",
					TextAggregation: text,
				},
			},
		}},
	}
}

func occ(value string, occurs int) aggregation.TextOccurrence {
	return aggregation.TextOccurrence{Value: value, Occurs: occurs}
}

func TestShardCombinerMergeTextCutoff(t *testing.T) {
	tests := []struct {
		name     string
		results  []*aggregation.Result
		expected aggregation.Text
	}{
		{
			name: "single shard, not exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{Count: 7, Items: []aggregation.TextOccurrence{occ("a", 5), occ("b", 2)}}),
			},
			expected: aggregation.Text{Count: 7, Items: []aggregation.TextOccurrence{occ("a", 5), occ("b", 2)}},
		},
		{
			name: "single shard, exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{CutoffExceeded: true}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name: "neither shard exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{Count: 7, Items: []aggregation.TextOccurrence{occ("a", 5), occ("b", 2)}}),
				textShard(aggregation.Text{Count: 5, Items: []aggregation.TextOccurrence{occ("b", 4), occ("c", 1)}}),
			},
			expected: aggregation.Text{
				Count: 12,
				Items: []aggregation.TextOccurrence{occ("b", 6), occ("a", 5), occ("c", 1)},
			},
		},
		{
			name: "first shard exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{CutoffExceeded: true}),
				textShard(aggregation.Text{Count: 5, Items: []aggregation.TextOccurrence{occ("b", 4), occ("c", 1)}}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name: "second shard exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{Count: 7, Items: []aggregation.TextOccurrence{occ("a", 5), occ("b", 2)}}),
				textShard(aggregation.Text{CutoffExceeded: true}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name: "both shards exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{CutoffExceeded: true}),
				textShard(aggregation.Text{CutoffExceeded: true}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name: "three shards, middle one exceeded",
			results: []*aggregation.Result{
				textShard(aggregation.Text{Count: 7, Items: []aggregation.TextOccurrence{occ("a", 5), occ("b", 2)}}),
				textShard(aggregation.Text{CutoffExceeded: true}),
				textShard(aggregation.Text{Count: 5, Items: []aggregation.TextOccurrence{occ("b", 4), occ("c", 1)}}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			combined := NewShardCombiner(aggregation.Params{}).Do(tt.results)

			require.Len(t, combined.Groups, 1)
			prop, ok := combined.Groups[0].Properties[cutoffTestProp]
			require.True(t, ok)

			text := prop.TextAggregation
			assert.Equal(t, tt.expected.CutoffExceeded, text.CutoffExceeded)
			assert.Equal(t, tt.expected.Count, text.Count)
			assert.Equal(t, tt.expected.Items, text.Items)
			if tt.expected.CutoffExceeded {
				assert.Empty(t, text.Items)
				assert.Zero(t, text.Count)
			}
		})
	}
}

// A shard only counts the values it holds, so the cutoff can only be decided
// once every shard's list is merged.
func TestShardCombinerTopOccurrencesCutoffAcrossShards(t *testing.T) {
	complete := func(items ...aggregation.TextOccurrence) aggregation.Text {
		text := aggregation.Text{Items: items, ValuesComplete: true}
		for _, item := range items {
			text.Count += item.Occurs
		}
		return text
	}

	params := func(cutoff uint32, limit int) aggregation.Params {
		return aggregation.Params{Properties: []aggregation.ParamProperty{{
			Name:                 cutoffTestProp,
			Aggregators:          []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&limit)},
			TopOccurrencesCutoff: cutoff,
		}}}
	}

	tests := []struct {
		name     string
		params   aggregation.Params
		results  []*aggregation.Result
		expected aggregation.Text
	}{
		{
			name:   "shards under the cutoff, union over it",
			params: params(3, 10),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 2), occ("b", 1))),
				textShard(complete(occ("c", 2), occ("d", 1))),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name:   "shards sharing values stay under the cutoff",
			params: params(3, 10),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 2), occ("b", 1))),
				textShard(complete(occ("a", 3), occ("c", 1))),
			},
			expected: aggregation.Text{
				Count:          7,
				Items:          []aggregation.TextOccurrence{occ("a", 5), occ("b", 1), occ("c", 1)},
				ValuesComplete: true,
			},
		},
		{
			name:   "union exactly at the cutoff passes",
			params: params(3, 10),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 1), occ("b", 1))),
				textShard(complete(occ("c", 1))),
			},
			expected: aggregation.Text{
				Count:          3,
				Items:          []aggregation.TextOccurrence{occ("a", 1), occ("b", 1), occ("c", 1)},
				ValuesComplete: true,
			},
		},
		{
			name:   "merged list is cut to the requested limit",
			params: params(10, 2),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 5), occ("b", 1))),
				textShard(complete(occ("c", 3), occ("d", 1))),
			},
			expected: aggregation.Text{
				Count:          10,
				Items:          []aggregation.TextOccurrence{occ("a", 5), occ("c", 3)},
				ValuesComplete: true,
			},
		},
		{
			// the object-scan fallback lists top values, not all of them, so
			// the union proves nothing about the collection's cardinality
			name:   "a shard that could not evaluate the cutoff drops it",
			params: params(2, 10),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 2))),
				textShard(aggregation.Text{Count: 4, Items: []aggregation.TextOccurrence{occ("b", 3), occ("c", 1)}}),
			},
			expected: aggregation.Text{
				Count: 6,
				Items: []aggregation.TextOccurrence{occ("b", 3), occ("a", 2), occ("c", 1)},
			},
		},
		{
			name:   "one shard over the cutoff keeps the sentinel",
			params: params(3, 10),
			results: []*aggregation.Result{
				textShard(complete(occ("a", 2))),
				textShard(aggregation.Text{CutoffExceeded: true}),
			},
			expected: aggregation.Text{CutoffExceeded: true},
		},
		{
			name:   "no cutoff requested leaves the merge alone",
			params: aggregation.Params{},
			results: []*aggregation.Result{
				textShard(complete(occ("a", 2), occ("b", 1))),
				textShard(complete(occ("c", 2), occ("d", 1))),
			},
			expected: aggregation.Text{
				Count: 6,
				Items: []aggregation.TextOccurrence{occ("a", 2), occ("c", 2), occ("b", 1), occ("d", 1)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			combined := NewShardCombiner(tt.params).Do(tt.results)

			require.Len(t, combined.Groups, 1)
			prop, ok := combined.Groups[0].Properties[cutoffTestProp]
			require.True(t, ok)

			text := prop.TextAggregation
			assert.Equal(t, tt.expected.CutoffExceeded, text.CutoffExceeded)
			assert.Equal(t, tt.expected.Count, text.Count)
			assert.Equal(t, tt.expected.Items, text.Items)
			assert.Equal(t, tt.expected.ValuesComplete, text.ValuesComplete)
		})
	}
}

func testNumbers(t *testing.T, numbers1, numbers2 []float64, testMode bool) {
	sc := NewShardCombiner(aggregation.Params{})
	numberMap1 := createNumericalAgg(numbers1)
	numberMap2 := createNumericalAgg(numbers2)

	combinedMap := createNumericalAgg(append(numbers1, numbers2...))

	sc.mergeNumericalProp(numberMap1, numberMap2)
	sc.finalizeNumerical(numberMap1)

	assert.Equal(t, len(numbers1)+len(numbers2), int(numberMap1["count"].(float64)))
	assert.InDelta(t, combinedMap["mean"], numberMap1["mean"], 0.0001)
	assert.InDelta(t, combinedMap["median"], numberMap1["median"], 0.0001)
	if testMode { // for random numbers the mode is flaky as there is no guaranteed order if several values have the same count
		assert.Equal(t, combinedMap["mode"], numberMap1["mode"])
	}
}

func createNumericalAgg(numbers []float64) map[string]interface{} {
	agg := newNumericalAggregator()
	for _, num := range numbers {
		agg.AddFloat64(num)
	}
	agg.buildPairsFromCounts() // needed to populate all required info

	prop := aggregation.Property{}
	aggs := []aggregation.Aggregator{aggregation.MedianAggregator, aggregation.MeanAggregator, aggregation.ModeAggregator, aggregation.CountAggregator}
	addNumericalAggregations(&prop, aggs, agg)
	return prop.NumericalAggregations
}

func createRandomSlice() []float64 {
	size := rand.Intn(100) + 1 // at least one entry
	array := make([]float64, size)
	for i := 0; i < size; i++ {
		array[i] = rand.Float64() * 1000
	}
	return array
}
