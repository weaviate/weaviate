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
	"encoding/json"
	"fmt"
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
	sc := NewShardCombiner()
	dateMap1 := createDateAgg(dates1)
	dateMap2 := createDateAgg(dates2)

	require.NoError(t, sc.mergeDateProp(dateMap1, dateMap2))
	sc.finalizeDateProp(dateMap1)
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMinimum+NanoSecondsTimeZone, dateMap1["minimum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMaximum+NanoSecondsTimeZone, dateMap1["maximum"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMedian+NanoSecondsTimeZone, dateMap1["median"])
	assert.Equal(t, int64(len(tt.dates1)+len(tt.dates2)), dateMap1["count"])
	assert.Equal(t, YearMonthDayHourMinute+tt.expectedMode+NanoSecondsTimeZone, dateMap1["mode"])
}

func createDateAgg(dates []string) map[string]interface{} {
	return createDateAggWith(dates,
		[]aggregation.Aggregator{aggregation.MedianAggregator, aggregation.MinimumAggregator, aggregation.MaximumAggregator, aggregation.CountAggregator, aggregation.ModeAggregator})
}

func createDateAggWith(dates []string, aggs []aggregation.Aggregator) map[string]interface{} {
	agg := newDateAggregator()
	for _, date := range dates {
		agg.AddTimestamp(YearMonthDayHourMinute + date + NanoSecondsTimeZone)
	}
	agg.buildPairsFromCounts() // needed to populate all required info

	prop := aggregation.Property{}
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
			combinedResults, err := NewShardCombiner().Do(tt.results)
			require.NoError(t, err)
			assert.Equal(t, len(combinedResults.Groups), tt.totalResults)
		})
	}
}

func testNumbers(t *testing.T, numbers1, numbers2 []float64, testMode bool) {
	sc := NewShardCombiner()
	numberMap1 := createNumericalAgg(numbers1)
	numberMap2 := createNumericalAgg(numbers2)

	combinedMap := createNumericalAgg(append(numbers1, numbers2...))

	require.NoError(t, sc.mergeNumericalProp(numberMap1, numberMap2))
	sc.finalizeNumerical(numberMap1)

	assert.Equal(t, len(numbers1)+len(numbers2), int(numberMap1["count"].(float64)))
	assert.InDelta(t, combinedMap["mean"], numberMap1["mean"], 0.0001)
	assert.InDelta(t, combinedMap["median"], numberMap1["median"], 0.0001)
	if testMode { // for random numbers the mode is flaky as there is no guaranteed order if several values have the same count
		assert.Equal(t, combinedMap["mode"], numberMap1["mode"])
	}
}

func createNumericalAgg(numbers []float64) map[string]interface{} {
	return createNumericalAggWith(numbers,
		[]aggregation.Aggregator{aggregation.MedianAggregator, aggregation.MeanAggregator, aggregation.ModeAggregator, aggregation.CountAggregator})
}

func createNumericalAggWith(numbers []float64, aggs []aggregation.Aggregator) map[string]interface{} {
	agg := newNumericalAggregator()
	for _, num := range numbers {
		agg.AddFloat64(num)
	}
	agg.buildPairsFromCounts() // needed to populate all required info

	prop := aggregation.Property{}
	addNumericalAggregations(&prop, aggs, agg)
	return prop.NumericalAggregations
}

func roundTrip(t *testing.T, res *aggregation.Result) *aggregation.Result {
	t.Helper()
	b, err := json.Marshal(res)
	require.NoError(t, err)
	var out aggregation.Result
	require.NoError(t, json.Unmarshal(b, &out))
	return &out
}

func makeResult(numbers []float64, dates []string, groupedBy *aggregation.GroupedBy) *aggregation.Result {
	return &aggregation.Result{
		Groups: []aggregation.Group{{
			GroupedBy: groupedBy,
			Count:     len(numbers),
			Properties: map[string]aggregation.Property{
				"number": {
					Type:                  aggregation.PropertyTypeNumerical,
					NumericalAggregations: createNumericalAgg(numbers),
				},
				"date": {
					Type:             aggregation.PropertyTypeDate,
					DateAggregations: createDateAgg(dates),
				},
			},
		}},
	}
}

func makeNumberResult(numbers []float64, aggs ...aggregation.Aggregator) *aggregation.Result {
	return &aggregation.Result{
		Groups: []aggregation.Group{{
			Count: len(numbers),
			Properties: map[string]aggregation.Property{
				"number": {
					Type:                  aggregation.PropertyTypeNumerical,
					NumericalAggregations: createNumericalAggWith(numbers, aggs),
				},
			},
		}},
	}
}

func makeDateResult(dates []string, aggs ...aggregation.Aggregator) *aggregation.Result {
	return &aggregation.Result{
		Groups: []aggregation.Group{{
			Count: len(dates),
			Properties: map[string]aggregation.Property{
				"date": {
					Type:             aggregation.PropertyTypeDate,
					DateAggregations: createDateAggWith(dates, aggs),
				},
			},
		}},
	}
}

// Remote shard results cross the cluster-internal REST API as plain JSON
// (https://github.com/weaviate/weaviate/issues/11687).
func TestShardCombinerRemoteShardResults(t *testing.T) {
	numbers1 := []float64{0, 5, 10, 15}
	numbers2 := []float64{15, 15, 2.5}
	dates1 := []string{"55", "26", "10"}
	dates2 := []string{"15", "26", "45", "26"}

	assertCombined := func(t *testing.T, combined *aggregation.Result) {
		require.Len(t, combined.Groups, 1)

		expectedNum := createNumericalAgg(append(append([]float64{}, numbers1...), numbers2...))
		num := combined.Groups[0].Properties["number"].NumericalAggregations
		assert.Equal(t, float64(len(numbers1)+len(numbers2)), num["count"])
		assert.InDelta(t, expectedNum["mean"], num["mean"], 0.0001)
		assert.InDelta(t, expectedNum["median"], num["median"], 0.0001)
		assert.Equal(t, expectedNum["mode"], num["mode"])
		assert.NotContains(t, num, "_numericalAggregator")

		expectedDate := createDateAgg(append(append([]string{}, dates1...), dates2...))
		date := combined.Groups[0].Properties["date"].DateAggregations
		assert.Equal(t, int64(len(dates1)+len(dates2)), date["count"])
		for _, key := range []string{"minimum", "maximum", "median", "mode"} {
			assert.Equal(t, expectedDate[key], date[key], key)
		}
		assert.NotContains(t, date, "_dateAggregator")
	}

	tests := []struct {
		name    string
		remote1 bool
		remote2 bool
	}{
		{name: "first shard remote", remote1: true},
		{name: "second shard remote", remote2: true},
		{name: "both shards remote", remote1: true, remote2: true},
	}

	for _, tt := range tests {
		t.Run("ungrouped, "+tt.name, func(t *testing.T) {
			res1 := makeResult(numbers1, dates1, nil)
			res2 := makeResult(numbers2, dates2, nil)
			if tt.remote1 {
				res1 = roundTrip(t, res1)
			}
			if tt.remote2 {
				res2 = roundTrip(t, res2)
			}
			combined, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
			require.NoError(t, err)
			assertCombined(t, combined)
		})
	}

	t.Run("grouped, remote group appended before merge", func(t *testing.T) {
		groupedBy := func() *aggregation.GroupedBy {
			return &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}
		}
		res1 := roundTrip(t, makeResult(numbers1, dates1, groupedBy()))
		res2 := makeResult(numbers2, dates2, groupedBy())
		combined, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
		require.NoError(t, err)
		assertCombined(t, combined)
	})

	t.Run("mean-only query ships count and sum instead of pairs", func(t *testing.T) {
		meanOnly := func(numbers []float64) *aggregation.Result {
			return makeNumberResult(numbers, aggregation.MeanAggregator, aggregation.CountAggregator)
		}

		res2 := roundTrip(t, meanOnly(numbers2))
		marker, ok := res2.Groups[0].Properties["number"].NumericalAggregations["_numericalAggregator"].(map[string]interface{})
		require.True(t, ok)
		assert.NotContains(t, marker, "pairs")

		combined, err := NewShardCombiner().Do([]*aggregation.Result{meanOnly(numbers1), res2})
		require.NoError(t, err)
		require.Len(t, combined.Groups, 1)

		all := append(append([]float64{}, numbers1...), numbers2...)
		var sum float64
		for _, v := range all {
			sum += v
		}
		num := combined.Groups[0].Properties["number"].NumericalAggregations
		assert.InDelta(t, sum/float64(len(all)), num["mean"], 0.0001)
		assert.Equal(t, float64(len(all)), num["count"])
	})

	t.Run("payload from an older node is rejected, not merged wrong", func(t *testing.T) {
		for _, propName := range []string{"number", "date"} {
			res1 := makeResult(numbers1, dates1, nil)
			res2 := roundTrip(t, makeResult(numbers2, dates2, nil))
			// an older node serializes the aggregator's unexported fields as {}
			props := res2.Groups[0].Properties[propName]
			switch propName {
			case "number":
				props.NumericalAggregations["_numericalAggregator"] = map[string]interface{}{}
			case "date":
				props.DateAggregations["_dateAggregator"] = map[string]interface{}{}
			}
			_, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
			require.ErrorContains(t, err, "older version")
		}
	})

	t.Run("malformed pairs are rejected", func(t *testing.T) {
		res2 := roundTrip(t, makeResult(numbers2, dates2, nil))
		res2.Groups[0].Properties["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
			"count": float64(1), "sum": float64(1), "pairs": []interface{}{"garbage"},
		}
		_, err := NewShardCombiner().Do([]*aggregation.Result{makeResult(numbers1, dates1, nil), res2})
		require.ErrorContains(t, err, "malformed")
	})

	t.Run("selector-specific wire round trips", func(t *testing.T) {
		unionNumbers := append(append([]float64{}, numbers1...), numbers2...)
		unionDates := append(append([]string{}, dates1...), dates2...)

		tests := []struct {
			name   string
			isDate bool
			aggs   []aggregation.Aggregator
			keys   []string
		}{
			{
				name: "numerical median-only",
				aggs: []aggregation.Aggregator{aggregation.MedianAggregator, aggregation.CountAggregator},
				keys: []string{"median", "count"},
			},
			{
				name: "numerical mode-only",
				aggs: []aggregation.Aggregator{aggregation.ModeAggregator, aggregation.CountAggregator},
				keys: []string{"mode", "count"},
			},
			{
				name:   "date median-only",
				isDate: true,
				aggs:   []aggregation.Aggregator{aggregation.MedianAggregator, aggregation.CountAggregator},
				keys:   []string{"median", "count"},
			},
			{
				name:   "date mode-only",
				isDate: true,
				aggs:   []aggregation.Aggregator{aggregation.ModeAggregator, aggregation.CountAggregator},
				keys:   []string{"mode", "count"},
			},
			{
				name:   "date count-only",
				isDate: true,
				aggs:   []aggregation.Aggregator{aggregation.CountAggregator},
				keys:   []string{"count"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var res1, res2 *aggregation.Result
				var expected map[string]interface{}
				if tt.isDate {
					res1 = makeDateResult(dates1, tt.aggs...)
					res2 = roundTrip(t, makeDateResult(dates2, tt.aggs...))
					expected = createDateAggWith(unionDates, tt.aggs)
				} else {
					res1 = makeNumberResult(numbers1, tt.aggs...)
					res2 = roundTrip(t, makeNumberResult(numbers2, tt.aggs...))
					expected = createNumericalAggWith(unionNumbers, tt.aggs)
				}

				combined, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
				require.NoError(t, err)
				require.Len(t, combined.Groups, 1)

				var got map[string]interface{}
				if tt.isDate {
					got = combined.Groups[0].Properties["date"].DateAggregations
					assert.NotContains(t, got, "_dateAggregator")
					assert.IsType(t, int64(0), got["count"])
				} else {
					got = combined.Groups[0].Properties["number"].NumericalAggregations
					assert.NotContains(t, got, "_numericalAggregator")
				}
				for _, key := range tt.keys {
					assert.Equal(t, expected[key], got[key], key)
				}
			})
		}
	})

	t.Run("single remote result is restored and finalized", func(t *testing.T) {
		combined, err := NewShardCombiner().Do([]*aggregation.Result{roundTrip(t, makeResult(numbers2, dates2, nil))})
		require.NoError(t, err)
		require.Len(t, combined.Groups, 1)

		expectedNum := createNumericalAgg(numbers2)
		num := combined.Groups[0].Properties["number"].NumericalAggregations
		assert.Equal(t, float64(len(numbers2)), num["count"])
		assert.InDelta(t, expectedNum["mean"], num["mean"], 0.0001)
		assert.InDelta(t, expectedNum["median"], num["median"], 0.0001)
		assert.Equal(t, expectedNum["mode"], num["mode"])
		assert.NotContains(t, num, "_numericalAggregator")

		expectedDate := createDateAgg(dates2)
		date := combined.Groups[0].Properties["date"].DateAggregations
		assert.Equal(t, int64(len(dates2)), date["count"])
		for _, key := range []string{"minimum", "maximum", "median", "mode"} {
			assert.Equal(t, expectedDate[key], date[key], key)
		}
		assert.NotContains(t, date, "_dateAggregator")
	})

	t.Run("grouped, remote-only group is restored", func(t *testing.T) {
		groupA := &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}
		groupB := &aggregation.GroupedBy{Value: "b", Path: []string{"prop"}}
		numbersB := []float64{1, 2, 2}
		datesB := []string{"15", "45"}

		res1 := makeResult(numbers1, dates1, groupA)
		res2 := roundTrip(t, &aggregation.Result{Groups: []aggregation.Group{
			makeResult(numbers2, dates2, groupA).Groups[0],
			makeResult(numbersB, datesB, groupB).Groups[0],
		}})

		combined, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
		require.NoError(t, err)
		require.Len(t, combined.Groups, 2)

		byValue := map[interface{}]aggregation.Group{}
		for _, group := range combined.Groups {
			byValue[group.GroupedBy.Value] = group
		}

		unionNum := createNumericalAgg(append(append([]float64{}, numbers1...), numbers2...))
		numA := byValue["a"].Properties["number"].NumericalAggregations
		assert.Equal(t, len(numbers1)+len(numbers2), byValue["a"].Count)
		assert.Equal(t, float64(len(numbers1)+len(numbers2)), numA["count"])
		assert.Equal(t, unionNum["mode"], numA["mode"])
		assert.NotContains(t, numA, "_numericalAggregator")

		expectedB := createNumericalAgg(numbersB)
		numB := byValue["b"].Properties["number"].NumericalAggregations
		assert.Equal(t, len(numbersB), byValue["b"].Count)
		assert.Equal(t, float64(len(numbersB)), numB["count"])
		assert.Equal(t, expectedB["mode"], numB["mode"])
		assert.InDelta(t, expectedB["median"], numB["median"], 0.0001)
		assert.NotContains(t, numB, "_numericalAggregator")

		dateB := byValue["b"].Properties["date"].DateAggregations
		assert.Equal(t, int64(len(datesB)), dateB["count"])
		assert.NotContains(t, dateB, "_dateAggregator")
	})

	t.Run("later group failure surfaces after earlier group merged", func(t *testing.T) {
		groupA := &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}
		groupB := &aggregation.GroupedBy{Value: "b", Path: []string{"prop"}}
		twoGroups := func() *aggregation.Result {
			return &aggregation.Result{Groups: []aggregation.Group{
				makeResult(numbers1, dates1, groupA).Groups[0],
				makeResult(numbers2, dates2, groupB).Groups[0],
			}}
		}

		res2 := roundTrip(t, twoGroups())
		res2.Groups[1].Properties["number"].NumericalAggregations["percentile"] = float64(5)

		_, err := NewShardCombiner().Do([]*aggregation.Result{twoGroups(), res2})
		require.ErrorContains(t, err, `unknown aggregation "percentile"`)
	})
}

func TestShardCombinerRejectsInvalidWireCounts(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}

	targets := []struct {
		name    string
		results func(t *testing.T, bad float64) []*aggregation.Result
	}{
		{
			name: "numerical mean-only top-level count",
			results: func(t *testing.T, bad float64) []*aggregation.Result {
				meanOnly := func() *aggregation.Result {
					return makeNumberResult(numbers, aggregation.MeanAggregator, aggregation.CountAggregator)
				}
				remote := roundTrip(t, meanOnly())
				remote.Groups[0].Properties["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": bad, "sum": float64(1),
				}
				return []*aggregation.Result{meanOnly(), remote}
			},
		},
		{
			name: "numerical pair count",
			results: func(t *testing.T, bad float64) []*aggregation.Result {
				remote := roundTrip(t, makeResult(numbers, dates, nil))
				remote.Groups[0].Properties["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(1), "sum": float64(1),
					"pairs": []interface{}{map[string]interface{}{"value": float64(1), "count": bad}},
				}
				return []*aggregation.Result{makeResult(numbers, dates, nil), remote}
			},
		},
		{
			name: "date pair count",
			results: func(t *testing.T, bad float64) []*aggregation.Result {
				remote := roundTrip(t, makeResult(numbers, dates, nil))
				remote.Groups[0].Properties["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{
					"pairs": []interface{}{map[string]interface{}{
						"value": YearMonthDayHourMinute + "26" + NanoSecondsTimeZone, "count": bad,
					}},
				}
				return []*aggregation.Result{makeResult(numbers, dates, nil), remote}
			},
		},
		{
			name: "date count scalar",
			results: func(t *testing.T, bad float64) []*aggregation.Result {
				remote := roundTrip(t, makeResult(numbers, dates, nil))
				remote.Groups[0].Properties["date"].DateAggregations["count"] = bad
				return []*aggregation.Result{makeResult(numbers, dates, nil), remote}
			},
		},
	}

	badCounts := []float64{-1, 1.5, 1 << 53, 1e19}
	for _, target := range targets {
		for _, bad := range badCounts {
			t.Run(fmt.Sprintf("%s with %v", target.name, bad), func(t *testing.T) {
				_, err := NewShardCombiner().Do(target.results(t, bad))
				require.ErrorContains(t, err, "invalid count")
			})
		}
	}
}

func TestShardCombinerRejectsMalformedAggregatorState(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}
	badStates := []interface{}{nil, "garbage", float64(42), []interface{}{}}

	for _, marker := range []string{"_numericalAggregator", "_dateAggregator"} {
		for _, state := range badStates {
			t.Run(fmt.Sprintf("%s set to %v", marker, state), func(t *testing.T) {
				remote := roundTrip(t, makeResult(numbers, dates, nil))
				props := remote.Groups[0].Properties
				switch marker {
				case "_numericalAggregator":
					props["number"].NumericalAggregations[marker] = state
				case "_dateAggregator":
					props["date"].DateAggregations[marker] = state
				}
				_, err := NewShardCombiner().Do([]*aggregation.Result{makeResult(numbers, dates, nil), remote})
				require.ErrorContains(t, err, "malformed")
				require.ErrorContains(t, err, "aggregator state")
			})
		}
	}
}

func TestShardCombinerRejectsPairlessModeMedian(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}

	tests := []struct {
		name   string
		mutate func(props map[string]aggregation.Property)
	}{
		{
			name: "numerical state without pairs",
			mutate: func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(3), "sum": float64(6),
				}
			},
		},
		{
			name: "date state with empty pairs",
			mutate: func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{
					"pairs": []interface{}{},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remote := roundTrip(t, makeResult(numbers, dates, nil))
			tt.mutate(remote.Groups[0].Properties)
			_, err := NewShardCombiner().Do([]*aggregation.Result{makeResult(numbers, dates, nil), remote})
			require.ErrorContains(t, err, "without value pairs")
		})
	}
}

func TestShardCombinerRejectsUnknownAggregationKey(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}

	remote := roundTrip(t, makeResult(numbers, dates, nil))
	remote.Groups[0].Properties["number"].NumericalAggregations["percentile"] = float64(5)

	_, err := NewShardCombiner().Do([]*aggregation.Result{makeResult(numbers, dates, nil), remote})
	require.ErrorContains(t, err, `unknown aggregation "percentile"`)
}

func TestShardCombinerRejectsMissingDistributionState(t *testing.T) {
	t.Run("mode/median without marker is rejected at restore", func(t *testing.T) {
		remote := roundTrip(t, makeResult([]float64{0, 5, 10, 15}, []string{"55", "26", "10"}, nil))
		delete(remote.Groups[0].Properties["number"].NumericalAggregations, "_numericalAggregator")

		_, err := NewShardCombiner().Do([]*aggregation.Result{remote})
		require.ErrorContains(t, err, "without value pairs")
	})

	t.Run("mean without marker is rejected at merge", func(t *testing.T) {
		remote := roundTrip(t, makeNumberResult([]float64{0, 5},
			aggregation.MeanAggregator, aggregation.CountAggregator))
		delete(remote.Groups[0].Properties["number"].NumericalAggregations, "_numericalAggregator")

		_, err := NewShardCombiner().Do([]*aggregation.Result{remote})
		require.ErrorContains(t, err, "without distribution state")
	})
}

func TestShardCombinerRejectsMeanOnlyModeMedianMixture(t *testing.T) {
	meanOnly := func() *aggregation.Result {
		return makeNumberResult([]float64{0, 5}, aggregation.MeanAggregator, aggregation.CountAggregator)
	}
	full := func() *aggregation.Result {
		return makeNumberResult([]float64{10, 15, 15},
			aggregation.MedianAggregator, aggregation.MeanAggregator, aggregation.ModeAggregator, aggregation.CountAggregator)
	}

	tests := []struct {
		name    string
		results func(t *testing.T) []*aggregation.Result
	}{
		{
			name: "mean-only shard first",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{roundTrip(t, meanOnly()), roundTrip(t, full())}
			},
		},
		{
			name: "mode/median shard first",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{roundTrip(t, full()), roundTrip(t, meanOnly())}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewShardCombiner().Do(tt.results(t))
			require.ErrorContains(t, err, "incomplete distribution state")
		})
	}
}

func TestShardCombinerRejectsBareScalarFromSecondShard(t *testing.T) {
	meanOnly := func() *aggregation.Result {
		return makeNumberResult([]float64{0, 5}, aggregation.MeanAggregator, aggregation.CountAggregator)
	}

	remote := roundTrip(t, meanOnly())
	delete(remote.Groups[0].Properties["number"].NumericalAggregations, "_numericalAggregator")

	_, err := NewShardCombiner().Do([]*aggregation.Result{roundTrip(t, meanOnly()), remote})
	require.ErrorContains(t, err, "without distribution state")
}

func TestShardCombinerRejectsMixedGroupedness(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}
	groupedBy := &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}

	tests := []struct {
		name         string
		groupedFirst bool
	}{
		{name: "ungrouped first"},
		{name: "grouped first", groupedFirst: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ungrouped := roundTrip(t, makeResult(numbers, dates, nil))
			grouped := roundTrip(t, makeResult(numbers, dates, groupedBy))
			results := []*aggregation.Result{ungrouped, grouped}
			if tt.groupedFirst {
				results = []*aggregation.Result{grouped, ungrouped}
			}

			_, err := NewShardCombiner().Do(results)
			require.ErrorContains(t, err, "mixed grouped and ungrouped")
		})
	}
}

func TestShardCombinerRejectsMalformedWireEntries(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}

	tests := []struct {
		name    string
		mutate  func(props map[string]aggregation.Property)
		errText string
	}{
		{
			name: "date count as string",
			mutate: func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["count"] = "garbage"
			},
			errText: "malformed count",
		},
		{
			name: "numerical pairs as string",
			mutate: func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(2), "sum": float64(3), "pairs": "garbage",
				}
			},
			errText: "malformed numerical aggregator pairs",
		},
		{
			name: "date pairs as string",
			mutate: func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{
					"pairs": "garbage",
				}
			},
			errText: "malformed date aggregator pairs",
		},
		{
			name: "date minimum as number",
			mutate: func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["minimum"] = float64(5)
			},
			errText: `malformed "minimum" entry`,
		},
		{
			name: "numerical sum as string",
			mutate: func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["sum"] = "garbage"
			},
			errText: `malformed "sum" entry`,
		},
		{
			name: "date minimum as unparseable string",
			mutate: func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["minimum"] = "not-a-date"
			},
			errText: `malformed "minimum" entry`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remote := roundTrip(t, makeResult(numbers, dates, nil))
			tt.mutate(remote.Groups[0].Properties)
			_, err := NewShardCombiner().Do([]*aggregation.Result{makeResult(numbers, dates, nil), remote})
			require.ErrorContains(t, err, tt.errText)
		})
	}
}

func TestShardCombinerRejectsUnknownPropertyType(t *testing.T) {
	bogusProps := func() map[string]aggregation.Property {
		return map[string]aggregation.Property{
			"weird": {Type: aggregation.PropertyType("bogus")},
		}
	}

	t.Run("ungrouped merge path", func(t *testing.T) {
		bogus := &aggregation.Result{Groups: []aggregation.Group{{Count: 1, Properties: bogusProps()}}}
		_, err := NewShardCombiner().Do([]*aggregation.Result{
			makeResult([]float64{0, 5}, []string{"26"}, nil), bogus,
		})
		require.ErrorContains(t, err, "unknown property type")
	})

	t.Run("grouped finalize path", func(t *testing.T) {
		bogus := &aggregation.Result{Groups: []aggregation.Group{{
			GroupedBy:  &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}},
			Count:      1,
			Properties: bogusProps(),
		}}}
		_, err := NewShardCombiner().Do([]*aggregation.Result{bogus})
		require.ErrorContains(t, err, "unknown property type")
	})
}

func TestShardCombinerMergesEmptyShardForModeMedian(t *testing.T) {
	numbers := []float64{15, 15, 2.5}
	dates := []string{"15", "26", "45", "26"}

	tests := []struct {
		name       string
		emptyFirst bool
	}{
		{name: "empty shard last"},
		{name: "empty shard first", emptyFirst: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := roundTrip(t, makeResult(numbers, dates, nil))
			empty := roundTrip(t, makeResult([]float64{}, []string{}, nil))
			results := []*aggregation.Result{data, empty}
			if tt.emptyFirst {
				results = []*aggregation.Result{empty, data}
			}

			combined, err := NewShardCombiner().Do(results)
			require.NoError(t, err)
			require.Len(t, combined.Groups, 1)
			assert.Equal(t, len(numbers), combined.Groups[0].Count)

			expectedNum := createNumericalAgg(numbers)
			num := combined.Groups[0].Properties["number"].NumericalAggregations
			assert.Equal(t, float64(len(numbers)), num["count"])
			assert.InDelta(t, expectedNum["mean"], num["mean"], 0.0001)
			assert.InDelta(t, expectedNum["median"], num["median"], 0.0001)
			assert.Equal(t, expectedNum["mode"], num["mode"])
			assert.NotContains(t, num, "_numericalAggregator")

			expectedDate := createDateAgg(dates)
			date := combined.Groups[0].Properties["date"].DateAggregations
			assert.Equal(t, int64(len(dates)), date["count"])
			for _, key := range []string{"minimum", "maximum", "median", "mode"} {
				assert.Equal(t, expectedDate[key], date[key], key)
			}
			assert.NotContains(t, date, "_dateAggregator")
		})
	}
}

func TestShardCombinerMergeLiteralNil(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}

	tests := []struct {
		name      string
		groupedBy *aggregation.GroupedBy
		nilFirst  bool
	}{
		{name: "ungrouped, nil first", nilFirst: true},
		{name: "ungrouped, nil last"},
		{name: "grouped, nil first", groupedBy: &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}, nilFirst: true},
		{name: "grouped, nil last", groupedBy: &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := makeResult(numbers, dates, tt.groupedBy)
			results := []*aggregation.Result{valid, nil}
			if tt.nilFirst {
				results = []*aggregation.Result{nil, valid}
			}

			combined, err := NewShardCombiner().Do(results)
			require.NoError(t, err)
			require.Len(t, combined.Groups, 1)
			assert.Equal(t, len(numbers), combined.Groups[0].Count)

			expected := createNumericalAgg(numbers)
			num := combined.Groups[0].Properties["number"].NumericalAggregations
			assert.Equal(t, float64(len(numbers)), num["count"])
			assert.Equal(t, expected["mode"], num["mode"])
		})
	}
}

// oldNodeNumericalWireJSON is the exact body a pre-fix node produces for an
// ungrouped numerical mean/mode/median query: the aggregator's unexported
// fields marshal as {}.
const oldNodeNumericalWireJSON = `{
	"groups": [
		{
			"properties": {
				"number": {
					"type": "numerical",
					"numericalAggregations": {
						"_numericalAggregator": {},
						"count": 4,
						"mean": 7.5,
						"median": 7.5,
						"mode": 0
					},
					"textAggregation": {"items": null, "count": 0},
					"booleanAggregation": {"count": 0, "totalTrue": 0, "totalFalse": 0, "percentageTrue": 0, "percentageFalse": 0},
					"schemaType": "int",
					"referenceAggregation": {"pointingTo": null},
					"dateAggregation": null
				}
			},
			"groupedBy": null,
			"count": 4
		}
	]
}`

func TestShardCombinerRejectsOldNodeRawPayload(t *testing.T) {
	var remote aggregation.Result
	require.NoError(t, json.Unmarshal([]byte(oldNodeNumericalWireJSON), &remote))

	local := makeNumberResult([]float64{0, 5, 10, 15},
		aggregation.MeanAggregator, aggregation.ModeAggregator, aggregation.MedianAggregator, aggregation.CountAggregator)
	_, err := NewShardCombiner().Do([]*aggregation.Result{local, &remote})
	require.ErrorContains(t, err, "older version")
}

func createRandomSlice() []float64 {
	size := rand.Intn(100) + 1 // at least one entry
	array := make([]float64, size)
	for i := 0; i < size; i++ {
		array[i] = rand.Float64() * 1000
	}
	return array
}
