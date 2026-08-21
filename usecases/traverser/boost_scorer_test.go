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

package traverser

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func makeResult(id string, score float32, props map[string]any) search.Result {
	return search.Result{
		ID:     strfmt.UUID(id),
		Score:  score,
		Schema: props,
	}
}

func filterCondition(path string, op filters.Operator, val any, dt schema.DataType) filters.BoostCondition {
	return filters.BoostCondition{
		Filter: &filters.LocalFilter{
			Root: &filters.Clause{
				On: &filters.Path{Property: schema.PropertyName(path)},
				Value: &filters.Value{
					Value: val,
					Type:  dt,
				},
				Operator: op,
			},
		},
		Weight: 1.0,
	}
}

func decayCondition(path, origin, scale string, curve filters.DecayCurveType, decayValue float32) filters.BoostCondition {
	return filters.BoostCondition{
		Decay: &filters.Decay{
			Path:       &filters.Path{Property: schema.PropertyName(path)},
			Origin:     origin,
			Scale:      scale,
			Curve:      curve,
			DecayValue: decayValue,
		},
		Weight: 1.0,
	}
}

// withOriginalLimit returns a shallow copy of the boost with OriginalOffset and OriginalLimit set.
func withOriginalLimit(b *filters.Boost, limit int) *filters.Boost {
	cp := *b
	cp.OriginalLimit = limit
	return &cp
}

func withOriginalPagination(b *filters.Boost, offset, limit int) *filters.Boost {
	cp := *b
	cp.OriginalOffset = offset
	cp.OriginalLimit = limit
	return &cp
}

// --- applyBoostScoring tests ---

func TestApplyBoostScoring_NilRank(t *testing.T) {
	results := []search.Result{makeResult("a", 1.0, nil)}
	got := applyBoostScoring(results, nil)
	assert.Equal(t, results, got)
}

func TestApplyBoostScoring_EmptyConditions(t *testing.T) {
	results := []search.Result{makeResult("a", 1.0, nil)}
	got := applyBoostScoring(results, &filters.Boost{Conditions: nil, Weight: 0.5, OriginalLimit: 10})
	assert.Equal(t, results, got)
}

func TestApplyBoostScoring_WeightZero(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]any{"inStock": true}),
		makeResult("b", 0.5, map[string]any{"inStock": false}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// weight=0 → no change, original order preserved
	assert.Equal(t, strfmt.UUID("a"), got[0].ID)
	assert.Equal(t, strfmt.UUID("b"), got[1].ID)
}

func TestApplyBoostScoring_FilterPromotesMatchingResults(t *testing.T) {
	results := []search.Result{
		makeResult("no-stock", 1.0, map[string]any{"inStock": false}),
		makeResult("in-stock", 0.5, map[string]any{"inStock": true}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// With weight=1.0, only boost score matters. in-stock should be first.
	assert.Equal(t, strfmt.UUID("in-stock"), got[0].ID)
	assert.Equal(t, strfmt.UUID("no-stock"), got[1].ID)
}

func TestApplyBoostScoring_Truncation(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]any{"x": true}),
		makeResult("b", 0.9, map[string]any{"x": true}),
		makeResult("c", 0.8, map[string]any{"x": true}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("x", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 2))
	assert.Len(t, got, 2)
}

func TestApplyBoostScoring_DepthPromotesDeepResult(t *testing.T) {
	// Simulates depth overfetch: 10 results fetched (depth=10), original limit=3.
	// The best boost match is at position 9 (last). With boost weight=1.0,
	// it should be promoted to the top and the output truncated to 3.
	results := make([]search.Result, 10)
	for i := range results {
		results[i] = makeResult(
			fmt.Sprintf("item-%d", i),
			float32(10-i)*0.1, // decreasing primary scores: 1.0, 0.9, ..., 0.1
			map[string]any{"promoted": i == 9},
		)
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("promoted", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 3))
	require.Len(t, got, 3)
	// item-9 (promoted=true, boost=1.0) should be first despite worst primary score.
	assert.Equal(t, strfmt.UUID("item-9"), got[0].ID)
}

func TestApplyBoostScoring_SmallDepthMissesDeepResult(t *testing.T) {
	// When depth is small (limit=3, only 3 results fetched), the promoted item
	// at position 9 is never seen. Only the top-3 primary results are rescored.
	results := make([]search.Result, 3)
	for i := range results {
		results[i] = makeResult(
			fmt.Sprintf("item-%d", i),
			float32(10-i)*0.1,
			map[string]any{"promoted": false},
		)
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("promoted", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 3))
	require.Len(t, got, 3)
	// No promoted items in the candidate pool, so order is unchanged.
	assert.Equal(t, strfmt.UUID("item-0"), got[0].ID)
}

func TestApplyBoostScoring_DepthTruncatesToOriginalLimit(t *testing.T) {
	// 20 results fetched (depth=20), original limit=5.
	// Verify output is exactly 5 results.
	results := make([]search.Result, 20)
	for i := range results {
		results[i] = makeResult(
			fmt.Sprintf("item-%02d", i),
			float32(20-i)*0.05,
			map[string]any{"inStock": i%3 == 0},
		)
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.8,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 5))
	require.Len(t, got, 5)
	// With weight=0.8, inStock items should dominate the top-5.
	inStockCount := 0
	for _, r := range got {
		props := r.Schema.(map[string]any)
		if props["inStock"] == true {
			inStockCount++
		}
	}
	assert.GreaterOrEqual(t, inStockCount, 3, "most top-5 results should be in-stock after boost")
}

func TestApplyBoostScoring_AllSamePrimaryScore(t *testing.T) {
	// When all primary scores are equal, they normalize to 1.0
	// and boost becomes the tiebreaker.
	results := []search.Result{
		makeResult("no-match", 0.5, map[string]any{"inStock": false}),
		makeResult("match", 0.5, map[string]any{"inStock": true}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// match has boost=1.0, no-match has boost=0.0
	// final(match) = 0.5*1.0 + 0.5*1.0 = 1.0
	// final(no-match) = 0.5*1.0 + 0.5*0.0 = 0.5
	assert.Equal(t, strfmt.UUID("match"), got[0].ID)
}

func TestDistToScore(t *testing.T) {
	// distToScore converts distance-based results to score-based by inverting.
	results := []search.Result{
		{ID: strfmt.UUID("close"), Dist: 0.01},
		{ID: strfmt.UUID("medium"), Dist: 0.10},
		{ID: strfmt.UUID("far"), Dist: 0.90},
	}
	distToScore(results)
	assert.InDelta(t, -0.01, float64(results[0].Score), 0.001)
	assert.InDelta(t, -0.10, float64(results[1].Score), 0.001)
	assert.InDelta(t, -0.90, float64(results[2].Score), 0.001)
}

func TestApplyBoostScoring_WithDistConvertedScores(t *testing.T) {
	// Simulates vector search: explorer calls distToScore before applyBoostScoring.
	results := []search.Result{
		{ID: strfmt.UUID("close"), Dist: 0.01, Schema: map[string]any{"streaming": false}},
		{ID: strfmt.UUID("medium"), Dist: 0.10, Schema: map[string]any{"streaming": true}},
		{ID: strfmt.UUID("far"), Dist: 0.90, Schema: map[string]any{"streaming": true}},
	}
	distToScore(results)
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("streaming", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// "medium": decent distance (normalized ~0.9) + streaming (1.0) → best combined
	// "close": best distance (normalized 1.0) but no streaming (0.0) → 0.5
	// "far": worst distance (normalized 0.0) but streaming (1.0) → 0.5
	assert.Equal(t, strfmt.UUID("medium"), got[0].ID)
}

func TestApplyBoostScoring_EmptyResults(t *testing.T) {
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("x", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(nil, withOriginalLimit(boost, 10))
	assert.Nil(t, got)
}

// --- scoreResult tests ---

func TestScoreResult_FilterMatch(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{"color": "red"})
	conds := []filters.BoostCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

func TestScoreResult_FilterNoMatch(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{"color": "blue"})
	conds := []filters.BoostCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_FilterMissingProperty(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{})
	conds := []filters.BoostCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_NilSchema(t *testing.T) {
	r := makeResult("a", 1.0, nil)
	conds := []filters.BoostCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_WeightedAverage(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{
		"inStock":  true,
		"category": "electronics",
	})
	conds := []filters.BoostCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "inStock"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: 2.0, // matches → score 1.0
		},
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "category"},
				Value:    &filters.Value{Value: "sports", Type: schema.DataTypeText},
				Operator: filters.OperatorEqual,
			}},
			Weight: 1.0, // doesn't match → score 0.0
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	// (2*1.0 + 1*0.0) / (2+1) = 0.6667
	assert.InDelta(t, 0.6667, float64(score), 0.01)
}

func TestScoreResult_ZeroWeight(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{"x": true})
	conds := []filters.BoostCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "x"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: 0, // zero weight defaults to 1.0
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

// --- matchesFilter / matchesClause tests ---

func TestMatchesFilter_And(t *testing.T) {
	props := map[string]any{"a": true, "b": true}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorAnd,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
			{On: &filters.Path{Property: "b"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props))

	props["b"] = false
	assert.False(t, matchesFilter(filter, props))
}

func TestMatchesFilter_Or(t *testing.T) {
	props := map[string]any{"a": false, "b": true}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorOr,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
			{On: &filters.Path{Property: "b"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props))

	props["b"] = false
	assert.False(t, matchesFilter(filter, props))
}

func TestMatchesFilter_Not(t *testing.T) {
	props := map[string]any{"a": false}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorNot,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props))
}

func TestMatchesFilter_NilInputs(t *testing.T) {
	assert.False(t, matchesFilter(nil, map[string]any{}))
	assert.False(t, matchesFilter(&filters.LocalFilter{}, map[string]any{}))
	assert.False(t, matchesFilter(&filters.LocalFilter{Root: &filters.Clause{
		On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual,
	}}, nil))
}

// --- compareValues tests ---

func TestCompareValues_Numeric(t *testing.T) {
	tests := []struct {
		name     string
		op       filters.Operator
		propVal  any
		filterV  any
		expected bool
	}{
		{"equal float64", filters.OperatorEqual, float64(10), float64(10), true},
		{"not equal", filters.OperatorNotEqual, float64(10), float64(5), true},
		{"greater than", filters.OperatorGreaterThan, float64(10), float64(5), true},
		{"greater than false", filters.OperatorGreaterThan, float64(5), float64(10), false},
		{"greater or equal", filters.OperatorGreaterThanEqual, float64(10), float64(10), true},
		{"less than", filters.OperatorLessThan, float64(5), float64(10), true},
		{"less or equal", filters.OperatorLessThanEqual, float64(10), float64(10), true},
		{"int prop", filters.OperatorEqual, int(42), float64(42), true},
		{"int64 prop", filters.OperatorEqual, int64(42), float64(42), true},
		{"float32 prop", filters.OperatorEqual, float32(42), float64(42), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.propVal, tt.filterV))
		})
	}
}

func TestCompareValues_String(t *testing.T) {
	tests := []struct {
		name     string
		op       filters.Operator
		prop     string
		filter   string
		expected bool
	}{
		{"equal", filters.OperatorEqual, "abc", "abc", true},
		{"not equal", filters.OperatorNotEqual, "abc", "def", true},
		{"gt", filters.OperatorGreaterThan, "b", "a", true},
		{"lt", filters.OperatorLessThan, "a", "b", true},
		{"gte", filters.OperatorGreaterThanEqual, "a", "a", true},
		{"lte", filters.OperatorLessThanEqual, "a", "a", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.prop, tt.filter))
		})
	}
}

func TestCompareValues_Boolean(t *testing.T) {
	assert.True(t, compareValues(filters.OperatorEqual, true, true))
	assert.False(t, compareValues(filters.OperatorEqual, true, false))
	assert.True(t, compareValues(filters.OperatorNotEqual, true, false))
}

// --- compareValues with array-typed properties (text[], number[], boolean[]) ---

func TestBoostCompareValues_ArrayString(t *testing.T) {
	tests := []struct {
		name     string
		op       filters.Operator
		propVal  any
		filterV  any
		expected bool
	}{
		// Equal: match if any element matches
		{"equal match first", filters.OperatorEqual, []string{"red", "green"}, "red", true},
		{"equal match last", filters.OperatorEqual, []string{"red", "green"}, "green", true},
		{"equal no match", filters.OperatorEqual, []string{"red", "green"}, "blue", false},
		// NotEqual: true only when no element equals the filter
		{"not equal no match", filters.OperatorNotEqual, []string{"red", "green"}, "blue", true},
		{"not equal match", filters.OperatorNotEqual, []string{"red", "green"}, "red", false},
		// Empty slice
		{"empty slice equal", filters.OperatorEqual, []string{}, "red", false},
		{"empty slice not equal", filters.OperatorNotEqual, []string{}, "red", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.propVal, tt.filterV))
		})
	}
}

func TestBoostCompareValues_ArrayFloat64(t *testing.T) {
	tests := []struct {
		name     string
		op       filters.Operator
		propVal  any
		filterV  any
		expected bool
	}{
		{"equal match", filters.OperatorEqual, []float64{10, 20}, float64(10), true},
		{"equal no match", filters.OperatorEqual, []float64{10, 20}, float64(30), false},
		{"not equal", filters.OperatorNotEqual, []float64{10, 20}, float64(30), true},
		{"not equal match", filters.OperatorNotEqual, []float64{10, 20}, float64(10), false},
		{"greater than match", filters.OperatorGreaterThan, []float64{5, 20}, float64(10), true},
		{"greater than no match", filters.OperatorGreaterThan, []float64{5, 8}, float64(10), false},
		{"less than match", filters.OperatorLessThan, []float64{5, 20}, float64(10), true},
		{"less than no match", filters.OperatorLessThan, []float64{15, 20}, float64(10), false},
		{"greater or equal", filters.OperatorGreaterThanEqual, []float64{10, 20}, float64(10), true},
		{"less or equal", filters.OperatorLessThanEqual, []float64{10, 20}, float64(10), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.propVal, tt.filterV))
		})
	}
}

func TestBoostCompareValues_ArrayBool(t *testing.T) {
	// []bool array with Equal
	assert.True(t, compareValues(filters.OperatorEqual, []bool{false, true}, true))
	assert.False(t, compareValues(filters.OperatorEqual, []bool{false, false}, true))
	// []bool array with NotEqual
	assert.True(t, compareValues(filters.OperatorNotEqual, []bool{false, false}, true))
	assert.False(t, compareValues(filters.OperatorNotEqual, []bool{false, true}, true))
}

func TestBoostCompareValues_AnySlice(t *testing.T) {
	// []any containing mixed types
	assert.True(t, compareValues(filters.OperatorEqual, []any{"a", "b"}, "b"))
	assert.False(t, compareValues(filters.OperatorEqual, []any{"a", "b"}, "c"))
	assert.True(t, compareValues(filters.OperatorNotEqual, []any{"a", "b"}, "c"))
	assert.False(t, compareValues(filters.OperatorNotEqual, []any{"a", "b"}, "a"))
	assert.True(t, compareValues(filters.OperatorEqual, []any{"a", int(10), nil, true}, float64(10)))
	assert.True(t, compareValues(filters.OperatorEqual, []any{"a", int(10), nil, true}, true))
	assert.False(t, compareValues(filters.OperatorEqual, []any{"a", int(10), nil, true}, nil))
	assert.True(t, compareValues(filters.OperatorNotEqual, []any{"a", int(10), nil, true}, nil))
	assert.False(t, compareValues(filters.OperatorGreaterThan, []any{"a", nil, true}, float64(10)))
	assert.False(t, compareValues(filters.OperatorEqual, nil, nil))
	assert.False(t, compareValues(filters.OperatorNotEqual, nil, "a"))
}

func TestBoostCompareValues_NonSliceUnchanged(t *testing.T) {
	// Scalar values should still work as before (not affected by asSlice logic)
	assert.True(t, compareValues(filters.OperatorEqual, "red", "red"))
	assert.False(t, compareValues(filters.OperatorEqual, "red", "blue"))
	assert.True(t, compareValues(filters.OperatorEqual, float64(42), float64(42)))
}

// --- Decay function tests ---

func TestComputeDecayFunction_Exp(t *testing.T) {
	// decay=0.5, dist=scale → score=0.5
	score := computeDecayFunction(filters.DecayCurveExp, 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → score=1.0
	score = computeDecayFunction(filters.DecayCurveExp, 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist=2*scale → score=0.25
	score = computeDecayFunction(filters.DecayCurveExp, 200, 0, 100, 0.5)
	assert.InDelta(t, 0.25, float64(score), 0.001)
}

func TestComputeDecayFunction_Gauss(t *testing.T) {
	// dist=scale → score=decayValue
	score := computeDecayFunction(filters.DecayCurveGauss, 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → 1.0
	score = computeDecayFunction(filters.DecayCurveGauss, 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

func TestComputeDecayFunction_Linear(t *testing.T) {
	// dist=scale → decayValue
	score := computeDecayFunction(filters.DecayCurveLinear, 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → 1.0
	score = computeDecayFunction(filters.DecayCurveLinear, 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist > scale/(1-decay) → 0
	score = computeDecayFunction(filters.DecayCurveLinear, 300, 0, 100, 0.5)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestComputeDecayFunction_WithOffset(t *testing.T) {
	// dist within offset → score=1.0
	score := computeDecayFunction(filters.DecayCurveExp, 50, 50, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist=offset+scale → decayValue
	score = computeDecayFunction(filters.DecayCurveExp, 150, 50, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)
}

func TestComputeDecayFunction_DefaultCurve(t *testing.T) {
	// Unknown curve falls back to exp behavior
	score := computeDecayFunction("unknown", 100, 0, 100, 0.5)
	scoreExp := computeDecayFunction(filters.DecayCurveExp, 100, 0, 100, 0.5)
	assert.InDelta(t, float64(scoreExp), float64(score), 0.001)
}

// --- Decay on search result ---

func TestComputeDecayForResult_NumericProperty(t *testing.T) {
	decay := &filters.Decay{
		Path:       &filters.Path{Property: "price"},
		Origin:     "100",
		Scale:      "200",
		Curve:      filters.DecayCurveExp,
		DecayValue: 0.5,
	}
	parsed := parseDecayParams(decay, time.Now())
	props := map[string]any{"price": float64(300)} // dist=200=scale → 0.5
	score := computeDecayForResult(decay, parsed, props)
	assert.InDelta(t, 0.5, float64(score), 0.001)
}

func TestComputeDecayForResult_MissingProperty(t *testing.T) {
	decay := &filters.Decay{
		Path:   &filters.Path{Property: "price"},
		Origin: "100",
		Scale:  "200",
	}
	parsed := parseDecayParams(decay, time.Now())
	props := map[string]any{}
	score := computeDecayForResult(decay, parsed, props)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestComputeDecayForResult_DateProperty(t *testing.T) {
	now := time.Now()
	origin := "now"
	scale := "7d"
	decay := &filters.Decay{
		Path:       &filters.Path{Property: "createdAt"},
		Origin:     origin,
		Scale:      scale,
		Curve:      filters.DecayCurveExp,
		DecayValue: 0.5,
	}
	parsed := parseDecayParams(decay, now)

	// 7 days ago → dist=scale → 0.5
	t7dAgo := now.Add(-7 * 24 * time.Hour).Format(time.RFC3339)
	props := map[string]any{"createdAt": t7dAgo}
	score := computeDecayForResult(decay, parsed, props)
	assert.InDelta(t, 0.5, float64(score), 0.05)

	// Now → dist=0 → 1.0
	props = map[string]any{"createdAt": now.Format(time.RFC3339)}
	score = computeDecayForResult(decay, parsed, props)
	assert.InDelta(t, 1.0, float64(score), 0.05)
}

// --- parseNumericOrDuration ---

func TestParseNumericOrDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"", 0},
		{"42", 42},
		{"3.14", 3.14},
		{"7d", 7 * float64(24*time.Hour)},
		{"24h", 24 * float64(time.Hour)},
		{"30m", 30 * float64(time.Minute)},
		{"10s", 10 * float64(time.Second)},
		{"500ms", 500 * float64(time.Millisecond)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseNumericOrDuration(tt.input)
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, got, 0.001)
		})
	}
}

// --- parseDecayParams ---

func TestParseDecayParams_Valid(t *testing.T) {
	d := &filters.Decay{
		Origin:     "100",
		Scale:      "200",
		Offset:     "10",
		Curve:      filters.DecayCurveGauss,
		DecayValue: 0.3,
	}
	p := parseDecayParams(d, time.Now())
	assert.True(t, p.valid)
	assert.InDelta(t, 10, p.offset, 0.001)
	assert.InDelta(t, 200, p.scale, 0.001)
	assert.InDelta(t, 0.3, p.decayValue, 0.001)
	assert.Equal(t, filters.DecayCurveGauss, p.curve)
}

func TestParseDecayParams_Defaults(t *testing.T) {
	d := &filters.Decay{
		Scale: "100",
	}
	p := parseDecayParams(d, time.Now())
	assert.True(t, p.valid)
	assert.InDelta(t, 0.5, p.decayValue, 0.001)     // default
	assert.Equal(t, filters.DecayCurveExp, p.curve) // default
}

func TestParseDecayParams_InvalidScale(t *testing.T) {
	d := &filters.Decay{Scale: ""}
	p := parseDecayParams(d, time.Now())
	assert.False(t, p.valid)

	d = &filters.Decay{Scale: "0"}
	p = parseDecayParams(d, time.Now())
	assert.False(t, p.valid)

	d = &filters.Decay{Scale: "-10"}
	p = parseDecayParams(d, time.Now())
	assert.False(t, p.valid, "negative scale should produce invalid params")
}

// --- toFloat64 ---

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    any
		expected float64
		hasErr   bool
	}{
		{float64(1.5), 1.5, false},
		{float32(2.5), 2.5, false},
		{int(10), 10, false},
		{int64(20), 20, false},
		{"3.14", 3.14, false},
		{[]int{1}, 0, true},
	}
	for _, tt := range tests {
		got, err := toFloat64(tt.input)
		if tt.hasErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.InDelta(t, tt.expected, got, 0.001)
		}
	}
}

// --- tryParseDate ---

func TestTryParseDate(t *testing.T) {
	now := time.Now()

	// time.Time passthrough
	parsed, ok := tryParseDate(now)
	assert.True(t, ok)
	assert.Equal(t, now, parsed)

	// RFC3339
	s := "2024-06-15T10:30:00Z"
	parsed, ok = tryParseDate(s)
	assert.True(t, ok)
	assert.Equal(t, 2024, parsed.Year())

	// Date only
	parsed, ok = tryParseDate("2024-06-15")
	assert.True(t, ok)
	assert.Equal(t, 15, parsed.Day())

	// Invalid
	_, ok = tryParseDate("not-a-date")
	assert.False(t, ok)

	// Non-string, non-time
	_, ok = tryParseDate(42)
	assert.False(t, ok)
}

// --- extractProps ---

func TestExtractProps(t *testing.T) {
	r := makeResult("a", 1.0, map[string]any{"key": "val"})
	props := extractProps(&r)
	assert.Equal(t, "val", props["key"])

	r2 := makeResult("b", 1.0, nil)
	assert.Nil(t, extractProps(&r2))

	r3 := search.Result{Schema: "not-a-map"}
	assert.Nil(t, extractProps(&r3))
}

// --- Integration: applyBoostScoring with decay ---

func TestApplyBoostScoring_DecayReorders(t *testing.T) {
	results := []search.Result{
		makeResult("far", 0.5, map[string]any{"price": float64(1000)}),
		makeResult("close", 0.5, map[string]any{"price": float64(110)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			decayCondition("price", "100", "200", filters.DecayCurveExp, 0.5),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// close (dist=10) should rank first, far (dist=900) last
	assert.Equal(t, strfmt.UUID("close"), got[0].ID)
	assert.Equal(t, strfmt.UUID("far"), got[1].ID)
}

func TestApplyBoostScoring_MixedConditions(t *testing.T) {
	results := []search.Result{
		makeResult("a", 0.5, map[string]any{"inStock": true, "price": float64(500)}),
		makeResult("b", 0.5, map[string]any{"inStock": false, "price": float64(100)}),
		makeResult("c", 0.5, map[string]any{"inStock": true, "price": float64(100)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			{
				Filter: &filters.LocalFilter{Root: &filters.Clause{
					On:       &filters.Path{Property: "inStock"},
					Value:    &filters.Value{Value: true},
					Operator: filters.OperatorEqual,
				}},
				Weight: 2.0,
			},
			decayCondition("price", "100", "500", filters.DecayCurveLinear, 0.5),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// c: inStock=true(w=2,s=1) + price=100(w=1,s=1.0) → (2*1+1*1)/3 = 1.0
	// a: inStock=true(w=2,s=1) + price=500(w=1,s=0.5) → (2*1+1*0.5)/3 = 0.833
	// b: inStock=false(w=2,s=0) + price=100(w=1,s=1.0) → (2*0+1*1.0)/3 = 0.333
	assert.Equal(t, strfmt.UUID("c"), got[0].ID)
	assert.Equal(t, strfmt.UUID("a"), got[1].ID)
	assert.Equal(t, strfmt.UUID("b"), got[2].ID)
}

func TestScoreResult_NegativeWeightDemotes(t *testing.T) {
	// A single negative weight: matching result gets demoted.
	r := makeResult("a", 1.0, map[string]any{"inStock": true})
	conds := []filters.BoostCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "inStock"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: -1.0,
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	// weight=-1, condScore=1 → weightedSum = -1*1 = -1, weightSum = 1 → -1/1 = -1
	assert.InDelta(t, -1.0, float64(score), 0.001)
}

func TestScoreResult_MixedPositiveNegativeWeights(t *testing.T) {
	// Positive weight=2 matches + negative weight=-1 matches → (2*1 + -1*1) / (2+1) = 1/3
	r := makeResult("a", 1.0, map[string]any{
		"inStock":  true,
		"category": "electronics",
	})
	conds := []filters.BoostCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "inStock"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: 2.0,
		},
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "category"},
				Value:    &filters.Value{Value: "electronics", Type: schema.DataTypeText},
				Operator: filters.OperatorEqual,
			}},
			Weight: -1.0,
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	// (2*1 + -1*1) / (2+1) = 1/3 ≈ 0.333
	assert.InDelta(t, 0.333, float64(score), 0.01)
}

func TestApplyBoostScoring_NegativeWeightDemotesMatchingResult(t *testing.T) {
	results := []search.Result{
		makeResult("match", 0.5, map[string]any{"banned": true}),
		makeResult("no-match", 0.5, map[string]any{"banned": false}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			{
				Filter: &filters.LocalFilter{Root: &filters.Clause{
					On:       &filters.Path{Property: "banned"},
					Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
					Operator: filters.OperatorEqual,
				}},
				Weight: -1.0,
			},
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// no-match has boost=0 (doesn't match, so condScore=0, score=0)
	// match has boost=-1 (matches negative weight)
	// After blending with weight=0.5: no-match ranks higher
	assert.Equal(t, strfmt.UUID("no-match"), got[0].ID)
	assert.Equal(t, strfmt.UUID("match"), got[1].ID)
}

// --- asFloat64 / asBool ---

func TestAsFloat64(t *testing.T) {
	v, ok := asFloat64(float64(1.5))
	assert.True(t, ok)
	assert.InDelta(t, 1.5, v, 0.001)

	v, ok = asFloat64(float32(2.5))
	assert.True(t, ok)
	assert.InDelta(t, 2.5, v, 0.001)

	v, ok = asFloat64(int(10))
	assert.True(t, ok)
	assert.InDelta(t, 10, v, 0.001)

	_, ok = asFloat64("string")
	assert.False(t, ok)
}

func TestAsBool(t *testing.T) {
	v, ok := asBool(true)
	assert.True(t, ok)
	assert.True(t, v)

	_, ok = asBool("string")
	assert.False(t, ok)
}

// --- parseOriginAsTime ---

func TestParseOriginAsTime(t *testing.T) {
	now := time.Now()
	parsed, err := parseOriginAsTime("now", now)
	require.NoError(t, err)
	assert.Equal(t, now, parsed)

	parsed, err = parseOriginAsTime("2024-06-15T10:00:00Z", now)
	require.NoError(t, err)
	assert.Equal(t, 2024, parsed.Year())

	_, err = parseOriginAsTime("invalid", now)
	assert.Error(t, err)
}

// --- computeDistance ---

func TestComputeDistance_Numeric(t *testing.T) {
	decay := &filters.Decay{Origin: "100", Scale: "200"}
	parsed := parseDecayParams(decay, time.Now())
	dist, err := computeDistance(parsed, float64(150))
	require.NoError(t, err)
	assert.InDelta(t, 50, dist, 0.001)
}

func TestComputeDistance_Date(t *testing.T) {
	now := time.Now()
	decay := &filters.Decay{Origin: "now", Scale: "7d"}
	parsed := parseDecayParams(decay, now)
	dateVal := now.Add(-48 * time.Hour).Format(time.RFC3339)
	dist, err := computeDistance(parsed, dateVal)
	require.NoError(t, err)
	expected := float64(48 * time.Hour)
	assert.InDelta(t, expected, dist, float64(2*time.Second))
}

// Ensure decay curves score correctly at extreme distances.
func TestComputeDecayFunction_ExtremeDistances(t *testing.T) {
	// Very large distance → exp approaches 0
	score := computeDecayFunction(filters.DecayCurveExp, 1e9, 0, 100, 0.5)
	assert.True(t, float64(score) < 1e-10, "exp at huge distance should be near 0")

	// Gauss at huge distance → 0
	score = computeDecayFunction(filters.DecayCurveGauss, 1e6, 0, 100, 0.5)
	assert.InDelta(t, 0, float64(score), 1e-10)

	// Linear caps at 0
	score = computeDecayFunction(filters.DecayCurveLinear, 1e6, 0, 100, 0.5)
	assert.InDelta(t, 0, float64(score), 1e-10)
}

// Verify that NaN/Inf don't leak through decay calculations.
func TestComputeDecayFunction_NoNaN(t *testing.T) {
	score := computeDecayFunction(filters.DecayCurveExp, 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))
	assert.False(t, math.IsInf(float64(score), 0))

	score = computeDecayFunction(filters.DecayCurveGauss, 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))

	score = computeDecayFunction(filters.DecayCurveLinear, 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))
}

// --- PropertyValue tests ---

func propertyValueCondition(path string, modifier filters.PropertyValueModifierType) filters.BoostCondition {
	return filters.BoostCondition{
		PropertyValue: &filters.PropertyValue{
			Path:     &filters.Path{Property: schema.PropertyName(path)},
			Modifier: modifier,
		},
		Weight: 1.0,
	}
}

func TestApplyBoostScoring_PropertyValuePromotesHighValues(t *testing.T) {
	results := []search.Result{
		makeResult("low-likes", 1.0, map[string]any{"likes": float64(10)}),
		makeResult("high-likes", 0.5, map[string]any{"likes": float64(1000)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("likes", filters.PropertyValueModifierNone)},
		Weight:     1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// With weight=1.0, only boost score matters. high-likes (normalized to 1.0) should be first.
	assert.Equal(t, strfmt.UUID("high-likes"), got[0].ID)
	assert.Equal(t, strfmt.UUID("low-likes"), got[1].ID)
}

func TestApplyBoostScoring_PropertyValueLog1p(t *testing.T) {
	results := []search.Result{
		makeResult("a", 0.5, map[string]any{"likes": float64(0)}),
		makeResult("b", 0.5, map[string]any{"likes": float64(100)}),
		makeResult("c", 0.5, map[string]any{"likes": float64(10000)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("likes", filters.PropertyValueModifierLog1p)},
		Weight:     1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// log1p compresses the range: log1p(10000) >> log1p(100) > log1p(0)
	assert.Equal(t, strfmt.UUID("c"), got[0].ID)
	assert.Equal(t, strfmt.UUID("b"), got[1].ID)
	assert.Equal(t, strfmt.UUID("a"), got[2].ID)
	// With log1p, middle value should be closer to top than with "none"
	assert.True(t, got[1].Score > 0.4, "log1p should compress range, middle score should be > 0.4")
}

func TestApplyBoostScoring_PropertyValueSqrt(t *testing.T) {
	results := []search.Result{
		makeResult("a", 0.5, map[string]any{"likes": float64(0)}),
		makeResult("b", 0.5, map[string]any{"likes": float64(100)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("likes", filters.PropertyValueModifierSqrt)},
		Weight:     1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	assert.Equal(t, strfmt.UUID("b"), got[0].ID)
	assert.Equal(t, strfmt.UUID("a"), got[1].ID)
}

func TestApplyBoostScoring_PropertyValueAllSameValue(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]any{"likes": float64(50)}),
		makeResult("b", 0.5, map[string]any{"likes": float64(50)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("likes", filters.PropertyValueModifierNone)},
		Weight:     0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// All same property value → all normalize to 1.0
	// Primary score breaks the tie: a (1.0) > b (0.5)
	assert.Equal(t, strfmt.UUID("a"), got[0].ID)
}

func TestApplyBoostScoring_PropertyValueMissingProperty(t *testing.T) {
	results := []search.Result{
		makeResult("has-likes", 0.5, map[string]any{"likes": float64(100)}),
		makeResult("no-likes", 0.5, map[string]any{"title": "hello"}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("likes", filters.PropertyValueModifierNone)},
		Weight:     1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// Missing property → raw value 0. has-likes should rank first.
	assert.Equal(t, strfmt.UUID("has-likes"), got[0].ID)
}

func TestApplyPropertyValueModifier(t *testing.T) {
	tests := []struct {
		name     string
		val      float64
		modifier filters.PropertyValueModifierType
		expected float64
	}{
		{"none passes through", 100, filters.PropertyValueModifierNone, 100},
		{"log1p of 0", 0, filters.PropertyValueModifierLog1p, 0},
		{"log1p of 100", 100, filters.PropertyValueModifierLog1p, math.Log1p(100)},
		{"log1p of negative clamps to 0", -5, filters.PropertyValueModifierLog1p, 0},
		{"sqrt of 100", 100, filters.PropertyValueModifierSqrt, 10},
		{"sqrt of 0", 0, filters.PropertyValueModifierSqrt, 0},
		{"sqrt of negative clamps to 0", -5, filters.PropertyValueModifierSqrt, 0},
		{"empty modifier is none", 42, "", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyPropertyValueModifier(tt.val, tt.modifier)
			assert.InDelta(t, tt.expected, got, 0.001)
		})
	}
}

func TestScoreResult_NegativeWeightNonMatch(t *testing.T) {
	// Negative weight on a non-matching result should score 0 (not negative).
	r := makeResult("a", 1.0, map[string]any{"inStock": false})
	conds := []filters.BoostCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "inStock"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: -1.0,
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), nil, 0)
	// condScore=0 (no match), weight=-1 → weightedSum=-1*0=0, weightSum=1 → 0/1=0
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestComputeDecayForResult_NonExistingField(t *testing.T) {
	decay := &filters.Decay{
		Path:   &filters.Path{Property: "nonExistent"},
		Origin: "100",
		Scale:  "200",
	}
	parsed := parseDecayParams(decay, time.Now())
	props := map[string]any{"price": float64(50)}
	score := computeDecayForResult(decay, parsed, props)
	assert.InDelta(t, 0.0, float64(score), 0.001, "decay on non-existing field should score 0")
}

func TestComputeDecayForResult_NilProps(t *testing.T) {
	decay := &filters.Decay{
		Path:   &filters.Path{Property: "price"},
		Origin: "100",
		Scale:  "200",
	}
	parsed := parseDecayParams(decay, time.Now())
	score := computeDecayForResult(decay, parsed, nil)
	assert.InDelta(t, 0.0, float64(score), 0.001, "decay on nil props should score 0")
}

func TestApplyBoostScoring_PropertyValueNonExistingField(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]any{"title": "hello"}),
		makeResult("b", 0.5, map[string]any{"title": "world"}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{{
			PropertyValue: &filters.PropertyValue{
				Path:     &filters.Path{Property: "nonExistent"},
				Modifier: filters.PropertyValueModifierNone,
			},
			Weight: 1.0,
		}},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// All property values are 0 → all normalize to 1.0 (same value).
	// Primary scores break the tie: a (1.0) > b (0.5) after normalization.
	require.Len(t, got, 2)
	assert.Equal(t, strfmt.UUID("a"), got[0].ID)
}

func TestApplyBoostScoring_PropertyValueNilSchema(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, nil),
		makeResult("b", 0.5, map[string]any{"likes": float64(100)}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{{
			PropertyValue: &filters.PropertyValue{
				Path:     &filters.Path{Property: "likes"},
				Modifier: filters.PropertyValueModifierNone,
			},
			Weight: 1.0,
		}},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// a has nil schema → likes=0, b has likes=100.
	// With weight=1.0, only boost matters. b should rank first.
	require.Len(t, got, 2)
	assert.Equal(t, strfmt.UUID("b"), got[0].ID)
}

// --- Offset pagination tests ---

func TestApplyBoostScoring_OffsetSkipsTopResults(t *testing.T) {
	results := make([]search.Result, 10)
	for i := range results {
		results[i] = makeResult(
			fmt.Sprintf("item-%d", i),
			float32(10-i)*0.1,
			map[string]any{"promoted": i == 9},
		)
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("promoted", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 1.0,
	}
	// offset=0, limit=3: should get [item-9, item-0, item-1]
	page1 := applyBoostScoring(cloneResults(results), withOriginalPagination(boost, 0, 3))
	require.Len(t, page1, 3)
	assert.Equal(t, strfmt.UUID("item-9"), page1[0].ID)

	// offset=3, limit=3: should skip the first 3 and return the next 3
	page2 := applyBoostScoring(cloneResults(results), withOriginalPagination(boost, 3, 3))
	require.Len(t, page2, 3)

	// Pages should not overlap
	page1IDs := make(map[strfmt.UUID]bool)
	for _, r := range page1 {
		page1IDs[r.ID] = true
	}
	for _, r := range page2 {
		assert.False(t, page1IDs[r.ID], "page2 result %s should not appear in page1", r.ID)
	}
}

func TestApplyBoostScoring_OffsetPlusLimitMatchesFullResults(t *testing.T) {
	results := make([]search.Result, 10)
	for i := range results {
		results[i] = makeResult(
			fmt.Sprintf("item-%d", i),
			float32(10-i)*0.1,
			map[string]any{"likes": float64(i * 100)},
		)
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{{
			PropertyValue: &filters.PropertyValue{
				Path:     &filters.Path{Property: "likes"},
				Modifier: filters.PropertyValueModifierNone,
			},
			Weight: 1.0,
		}},
		Weight: 0.7,
	}
	// Get all 10 results in one go
	all := applyBoostScoring(cloneResults(results), withOriginalPagination(boost, 0, 10))
	require.Len(t, all, 10)

	// Get page 1 (offset=0, limit=5) and page 2 (offset=5, limit=5)
	p1 := applyBoostScoring(cloneResults(results), withOriginalPagination(boost, 0, 5))
	p2 := applyBoostScoring(cloneResults(results), withOriginalPagination(boost, 5, 5))
	require.Len(t, p1, 5)
	require.Len(t, p2, 5)

	// Concatenation of pages should match full result
	combined := append(p1, p2...)
	for i := range all {
		assert.Equal(t, all[i].ID, combined[i].ID, "position %d mismatch", i)
	}
}

func TestApplyBoostScoring_OffsetBeyondResults(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]any{"x": true}),
		makeResult("b", 0.5, map[string]any{"x": false}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("x", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Weight: 0.5,
	}
	got := applyBoostScoring(results, withOriginalPagination(boost, 10, 5))
	assert.Nil(t, got, "offset beyond result count should return nil")
}

// Reproduces https://github.com/weaviate/weaviate/issues/11592: a missing
// numeric property must not be treated as raw 0, which would outrank objects
// whose actual values are negative.
func TestApplyBoostScoring_PropertyValueMissingPropertyWithNegativeValues(t *testing.T) {
	results := []search.Result{
		makeResult("11111111-1111-1111-1111-111111111111", 0.5, map[string]any{"rating": float64(-1.0)}),
		makeResult("22222222-2222-2222-2222-222222222222", 0.5, map[string]any{"rating": float64(-2.0)}),
		makeResult("33333333-3333-3333-3333-333333333333", 0.5, map[string]any{"title": "missing rating"}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("rating", filters.PropertyValueModifierNone)},
		Weight:     1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	require.Len(t, got, 3)
	assert.Equal(t, strfmt.UUID("11111111-1111-1111-1111-111111111111"), got[0].ID,
		"object with highest present rating (-1.0) must rank first")
}

// log1p clamps negatives to 0, collapsing all present values to the same raw
// score (range 0). Present results must still rank above the missing one.
func TestApplyBoostScoring_PropertyValueModifierNegativeAndMissing(t *testing.T) {
	for _, modifier := range []filters.PropertyValueModifierType{
		filters.PropertyValueModifierLog1p, filters.PropertyValueModifierSqrt,
	} {
		t.Run(string(modifier), func(t *testing.T) {
			results := []search.Result{
				makeResult("aaaaaaaa-0000-0000-0000-000000000001", 0.5, map[string]any{"rating": float64(-1.0)}),
				makeResult("aaaaaaaa-0000-0000-0000-000000000002", 0.5, map[string]any{"rating": float64(-2.0)}),
				makeResult("aaaaaaaa-0000-0000-0000-000000000003", 0.5, map[string]any{"title": "missing rating"}),
			}
			boost := &filters.Boost{
				Conditions: []filters.BoostCondition{propertyValueCondition("rating", modifier)},
				Weight:     1.0,
			}
			got := applyBoostScoring(results, withOriginalLimit(boost, 10))
			require.Len(t, got, 3)
			assert.Equal(t, strfmt.UUID("aaaaaaaa-0000-0000-0000-000000000003"), got[2].ID,
				"missing property must rank last")
		})
	}
}

func TestApplyBoostScoring_PropertyValueAllMissing(t *testing.T) {
	results := []search.Result{
		makeResult("low-primary", 0.2, map[string]any{"title": "x"}),
		makeResult("high-primary", 0.9, map[string]any{"title": "y"}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{propertyValueCondition("rating", filters.PropertyValueModifierNone)},
		Weight:     0.5,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	require.Len(t, got, 2)
	// No result has the property: the condition is neutral and the primary
	// score decides the order.
	assert.Equal(t, strfmt.UUID("high-primary"), got[0].ID)
	assert.Equal(t, strfmt.UUID("low-primary"), got[1].ID)
}

// --- asSlice ---

func TestBoostAsSlice(t *testing.T) {
	t.Run("[]string", func(t *testing.T) {
		out, ok := asSlice([]string{"a", "b"})
		assert.True(t, ok)
		assert.Equal(t, []any{"a", "b"}, out)
	})

	t.Run("[]float64", func(t *testing.T) {
		out, ok := asSlice([]float64{1, 2})
		assert.True(t, ok)
		assert.Equal(t, []any{float64(1), float64(2)}, out)
	})

	t.Run("[]bool", func(t *testing.T) {
		out, ok := asSlice([]bool{true, false})
		assert.True(t, ok)
		assert.Equal(t, []any{true, false}, out)
	})

	t.Run("[]any", func(t *testing.T) {
		in := []any{"x", 42}
		out, ok := asSlice(in)
		assert.True(t, ok)
		assert.Equal(t, in, out)
	})

	t.Run("[]int", func(t *testing.T) {
		out, ok := asSlice([]int{1, 2})
		assert.True(t, ok)
		assert.Equal(t, []any{1, 2}, out)
	})

	t.Run("[]int64", func(t *testing.T) {
		out, ok := asSlice([]int64{1, 2})
		assert.True(t, ok)
		assert.Equal(t, []any{int64(1), int64(2)}, out)
	})

	t.Run("non-slice returns false", func(t *testing.T) {
		_, ok := asSlice("not a slice")
		assert.False(t, ok)

		_, ok = asSlice(float64(42))
		assert.False(t, ok)

		_, ok = asSlice(true)
		assert.False(t, ok)

		_, ok = asSlice(nil)
		assert.False(t, ok)
	})

	t.Run("empty slice", func(t *testing.T) {
		out, ok := asSlice([]string{})
		assert.True(t, ok)
		assert.Empty(t, out)
	})

	t.Run("typed nil slice", func(t *testing.T) {
		var in []int
		out, ok := asSlice(in)
		assert.True(t, ok)
		assert.Empty(t, out)
	})
}

// Integration: applyBoostScoring with array-typed properties (text[], number[]).
// Reproduces https://github.com/weaviate/weaviate/issues/11814.
func TestApplyBoostScoring_TextArrayFilter(t *testing.T) {
	results := []search.Result{
		makeResult("no-tag", 1.0, map[string]any{"tags": []string{"sports", "news"}}),
		makeResult("has-tag", 0.5, map[string]any{"tags": []string{"tech", "science"}}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("tags", filters.OperatorEqual, "tech", schema.DataTypeText),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// has-tag contains "tech" → boost=1.0; no-tag does not → boost=0.0
	require.Len(t, got, 2)
	assert.Equal(t, strfmt.UUID("has-tag"), got[0].ID)
	assert.Equal(t, strfmt.UUID("no-tag"), got[1].ID)
}

func TestApplyBoostScoring_NumberArrayFilter(t *testing.T) {
	results := []search.Result{
		makeResult("match", 0.5, map[string]any{"scores": []float64{100, 200, 300}}),
		makeResult("no-match", 1.0, map[string]any{"scores": []float64{10, 20, 30}}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("scores", filters.OperatorEqual, float64(200), schema.DataTypeNumber),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	require.Len(t, got, 2)
	assert.Equal(t, strfmt.UUID("match"), got[0].ID)
	assert.Equal(t, strfmt.UUID("no-match"), got[1].ID)
}

func TestApplyBoostScoring_TextArrayNotEqual(t *testing.T) {
	results := []search.Result{
		makeResult("has-banned", 1.0, map[string]any{"tags": []string{"spam", "news"}}),
		makeResult("clean", 0.5, map[string]any{"tags": []string{"sports", "tech"}}),
	}
	boost := &filters.Boost{
		Conditions: []filters.BoostCondition{
			filterCondition("tags", filters.OperatorNotEqual, "spam", schema.DataTypeText),
		},
		Weight: 1.0,
	}
	got := applyBoostScoring(results, withOriginalLimit(boost, 10))
	// clean: no element == "spam" → NotEqual=true → boost=1.0
	// has-banned: contains "spam" → NotEqual=false → boost=0.0
	require.Len(t, got, 2)
	assert.Equal(t, strfmt.UUID("clean"), got[0].ID)
	assert.Equal(t, strfmt.UUID("has-banned"), got[1].ID)
}

func cloneResults(results []search.Result) []search.Result {
	out := make([]search.Result, len(results))
	copy(out, results)
	return out
}
