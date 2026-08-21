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
		{
			name: "Literal nil first",
			results: []*aggregation.Result{
				nil,
				{
					Groups: []aggregation.Group{{Count: 1}},
				},
			},
			totalResults: 1,
		},
		{
			name: "Literal nil last, grouped",
			results: []*aggregation.Result{
				{
					Groups: []aggregation.Group{{GroupedBy: &aggregation.GroupedBy{Value: 10, Path: []string{"something"}}}},
				},
				nil,
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
	unionNumbers := append(append([]float64{}, numbers1...), numbers2...)
	unionDates := append(append([]string{}, dates1...), dates2...)

	for _, tt := range []struct {
		name    string
		remote1 bool
		remote2 bool
	}{
		{name: "first shard remote", remote1: true},
		{name: "second shard remote", remote2: true},
	} {
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
			assertCombinedResult(t, combined, unionNumbers, unionDates)
		})
	}

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

	t.Run("date count-only query has no merge state", func(t *testing.T) {
		res1 := makeDateResult(dates1, aggregation.CountAggregator)
		res2 := roundTrip(t, makeDateResult(dates2, aggregation.CountAggregator))

		combined, err := NewShardCombiner().Do([]*aggregation.Result{res1, res2})
		require.NoError(t, err)
		require.Len(t, combined.Groups, 1)
		date := combined.Groups[0].Properties["date"].DateAggregations
		assert.Equal(t, int64(len(dates1)+len(dates2)), date["count"])
		assert.NotContains(t, date, "_dateAggregator")
	})

	t.Run("zero-row shard merges cleanly", func(t *testing.T) {
		for _, emptyFirst := range []bool{false, true} {
			data := roundTrip(t, makeResult(numbers2, dates2, nil))
			empty := roundTrip(t, makeResult([]float64{}, []string{}, nil))
			results := []*aggregation.Result{data, empty}
			if emptyFirst {
				results = []*aggregation.Result{empty, data}
			}
			combined, err := NewShardCombiner().Do(results)
			require.NoError(t, err)
			assertCombinedResult(t, combined, numbers2, dates2)
		}
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
}

func assertCombinedResult(t *testing.T, combined *aggregation.Result, numbers []float64, dates []string) {
	t.Helper()
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
}

// TestShardCombinerRejectsMalformedPayloads pins every payload-rejection
// branch: one row per branch, each expecting an error instead of the panic or
// silent mis-merge the branch replaces.
func TestShardCombinerRejectsMalformedPayloads(t *testing.T) {
	numbers := []float64{0, 5, 10, 15}
	dates := []string{"55", "26", "10"}
	groupedBy := &aggregation.GroupedBy{Value: "a", Path: []string{"prop"}}
	ts26 := YearMonthDayHourMinute + "26" + NanoSecondsTimeZone

	meanOnly := func() *aggregation.Result {
		return makeNumberResult(numbers, aggregation.MeanAggregator, aggregation.CountAggregator)
	}
	full := func() *aggregation.Result {
		return makeNumberResult(numbers,
			aggregation.MedianAggregator, aggregation.MeanAggregator, aggregation.ModeAggregator, aggregation.CountAggregator)
	}
	mutated := func(mutate func(props map[string]aggregation.Property)) func(t *testing.T) []*aggregation.Result {
		return func(t *testing.T) []*aggregation.Result {
			remote := roundTrip(t, makeResult(numbers, dates, nil))
			mutate(remote.Groups[0].Properties)
			return []*aggregation.Result{makeResult(numbers, dates, nil), remote}
		}
	}

	tests := []struct {
		name    string
		results func(t *testing.T) []*aggregation.Result
		errText string
	}{
		{
			name: "old-node numerical payload",
			results: func(t *testing.T) []*aggregation.Result {
				var remote aggregation.Result
				require.NoError(t, json.Unmarshal([]byte(oldNodeNumericalWireJSON), &remote))
				return []*aggregation.Result{full(), &remote}
			},
			errText: "older version",
		},
		{
			name: "old-node date marker",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{}
			}),
			errText: "older version",
		},
		{
			name: "numerical marker as null",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = nil
			}),
			errText: "malformed numerical aggregator state",
		},
		{
			name: "date marker as string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = "garbage"
			}),
			errText: "malformed date aggregator state",
		},
		{
			name: "garbage numerical pair element",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(1), "sum": float64(1), "pairs": []interface{}{"garbage"},
				}
			}),
			errText: "malformed numerical aggregator pair",
		},
		{
			name: "numerical pairs as string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(2), "sum": float64(3), "pairs": "garbage",
				}
			}),
			errText: "malformed numerical aggregator pairs",
		},
		{
			name: "date pairs as string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{"pairs": "garbage"}
			}),
			errText: "malformed date aggregator pairs",
		},
		{
			name: "negative numerical pair count",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(1), "sum": float64(1),
					"pairs": []interface{}{map[string]interface{}{"value": float64(1), "count": float64(-1)}},
				}
			}),
			errText: "invalid count",
		},
		{
			name: "fractional mean-only count",
			results: func(t *testing.T) []*aggregation.Result {
				remote := roundTrip(t, meanOnly())
				remote.Groups[0].Properties["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": 1.5, "sum": float64(3),
				}
				return []*aggregation.Result{meanOnly(), remote}
			},
			errText: "invalid count",
		},
		{
			name: "float64-inexact date pair count",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{
					"pairs": []interface{}{map[string]interface{}{"value": ts26, "count": float64(1 << 53)}},
				}
			}),
			errText: "invalid count",
		},
		{
			name: "out-of-range date count scalar",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["count"] = 1e19
			}),
			errText: "invalid count",
		},
		{
			name: "numerical mode/median state without pairs",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["_numericalAggregator"] = map[string]interface{}{
					"count": float64(3), "sum": float64(6),
				}
			}),
			errText: "without value pairs",
		},
		{
			name: "date state with empty pairs",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["_dateAggregator"] = map[string]interface{}{"pairs": []interface{}{}}
			}),
			errText: "without value pairs",
		},
		{
			name: "mode/median without marker",
			results: mutated(func(props map[string]aggregation.Property) {
				delete(props["number"].NumericalAggregations, "_numericalAggregator")
			}),
			errText: "without value pairs",
		},
		{
			name: "mean scalar without marker",
			results: func(t *testing.T) []*aggregation.Result {
				remote := roundTrip(t, meanOnly())
				delete(remote.Groups[0].Properties["number"].NumericalAggregations, "_numericalAggregator")
				return []*aggregation.Result{roundTrip(t, meanOnly()), remote}
			},
			errText: "without distribution state",
		},
		{
			name: "mean-only shard mixed with mode/median shard",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{roundTrip(t, meanOnly()), roundTrip(t, full())}
			},
			errText: "incomplete distribution state",
		},
		{
			name: "mode/median shard mixed with mean-only shard",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{roundTrip(t, full()), roundTrip(t, meanOnly())}
			},
			errText: "incomplete distribution state",
		},
		{
			name: "unknown aggregation key",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["percentile"] = float64(5)
			}),
			errText: `unknown aggregation "percentile"`,
		},
		{
			name: "unknown aggregation key in a later group",
			results: func(t *testing.T) []*aggregation.Result {
				groupB := &aggregation.GroupedBy{Value: "b", Path: []string{"prop"}}
				twoGroups := func() *aggregation.Result {
					return &aggregation.Result{Groups: []aggregation.Group{
						makeResult(numbers, dates, groupedBy).Groups[0],
						makeResult(numbers, dates, groupB).Groups[0],
					}}
				}
				remote := roundTrip(t, twoGroups())
				remote.Groups[1].Properties["number"].NumericalAggregations["percentile"] = float64(5)
				return []*aggregation.Result{twoGroups(), remote}
			},
			errText: `unknown aggregation "percentile"`,
		},
		{
			name: "mixed groupedness, ungrouped first",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{
					roundTrip(t, makeResult(numbers, dates, nil)),
					roundTrip(t, makeResult(numbers, dates, groupedBy)),
				}
			},
			errText: "mixed grouped and ungrouped",
		},
		{
			name: "mixed groupedness, grouped first",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{
					roundTrip(t, makeResult(numbers, dates, groupedBy)),
					roundTrip(t, makeResult(numbers, dates, nil)),
				}
			},
			errText: "mixed grouped and ungrouped",
		},
		{
			name: "date count as string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["count"] = "garbage"
			}),
			errText: "malformed count",
		},
		{
			name: "date minimum as number",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["minimum"] = float64(5)
			}),
			errText: `malformed "minimum" entry`,
		},
		{
			name: "date minimum as unparseable string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["date"].DateAggregations["minimum"] = "not-a-date"
			}),
			errText: `malformed "minimum" entry`,
		},
		{
			name: "numerical sum as string",
			results: mutated(func(props map[string]aggregation.Property) {
				props["number"].NumericalAggregations["sum"] = "garbage"
			}),
			errText: `malformed "sum" entry`,
		},
		{
			name: "unknown property type",
			results: func(t *testing.T) []*aggregation.Result {
				bogus := &aggregation.Result{Groups: []aggregation.Group{{Count: 1, Properties: map[string]aggregation.Property{
					"weird": {Type: aggregation.PropertyType("bogus")},
				}}}}
				return []*aggregation.Result{makeResult(numbers, dates, nil), bogus}
			},
			errText: "unknown property type",
		},
		{
			name: "unknown property type in appended group",
			results: func(t *testing.T) []*aggregation.Result {
				return []*aggregation.Result{{Groups: []aggregation.Group{{
					GroupedBy:  groupedBy,
					Count:      1,
					Properties: map[string]aggregation.Property{"weird": {Type: aggregation.PropertyType("bogus")}},
				}}}}
			},
			errText: "unknown property type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewShardCombiner().Do(tt.results(t))
			require.ErrorContains(t, err, tt.errText)
		})
	}
}

// oldNodeNumericalWireJSON is the body a pre-fix node produces: the
// aggregator's unexported fields marshal as {}.
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

func createRandomSlice() []float64 {
	size := rand.Intn(100) + 1 // at least one entry
	array := make([]float64, size)
	for i := 0; i < size; i++ {
		array[i] = rand.Float64() * 1000
	}
	return array
}
