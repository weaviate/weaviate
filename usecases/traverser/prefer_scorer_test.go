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
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
)

func makeResult(id string, score float32, props map[string]interface{}) search.Result {
	return search.Result{
		ID:     strfmt.UUID(id),
		Score:  score,
		Schema: props,
	}
}

func filterCondition(path string, op filters.Operator, val interface{}, dt schema.DataType) filters.PreferCondition {
	return filters.PreferCondition{
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

func decayCondition(path, origin, scale, curve string, decayValue float32) filters.PreferCondition {
	return filters.PreferCondition{
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

// --- applyPreferScoring tests ---

func TestApplyPreferScoring_NilPrefer(t *testing.T) {
	results := []search.Result{makeResult("a", 1.0, nil)}
	got := applyPreferScoring(results, nil, 10)
	assert.Equal(t, results, got)
}

func TestApplyPreferScoring_EmptyConditions(t *testing.T) {
	results := []search.Result{makeResult("a", 1.0, nil)}
	got := applyPreferScoring(results, &filters.Prefer{Conditions: nil, Strength: 0.5}, 10)
	assert.Equal(t, results, got)
}

func TestApplyPreferScoring_StrengthZero(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]interface{}{"inStock": true}),
		makeResult("b", 0.5, map[string]interface{}{"inStock": false}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 0,
	}
	got := applyPreferScoring(results, prefer, 10)
	// strength=0 → no change, original order preserved
	assert.Equal(t, strfmt.UUID("a"), got[0].ID)
	assert.Equal(t, strfmt.UUID("b"), got[1].ID)
}

func TestApplyPreferScoring_FilterPromotesMatchingResults(t *testing.T) {
	results := []search.Result{
		makeResult("no-stock", 1.0, map[string]interface{}{"inStock": false}),
		makeResult("in-stock", 0.5, map[string]interface{}{"inStock": true}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 1.0,
	}
	got := applyPreferScoring(results, prefer, 10)
	// With strength=1.0, only prefer score matters. in-stock should be first.
	assert.Equal(t, strfmt.UUID("in-stock"), got[0].ID)
	assert.Equal(t, strfmt.UUID("no-stock"), got[1].ID)
}

func TestApplyPreferScoring_Truncation(t *testing.T) {
	results := []search.Result{
		makeResult("a", 1.0, map[string]interface{}{"x": true}),
		makeResult("b", 0.9, map[string]interface{}{"x": true}),
		makeResult("c", 0.8, map[string]interface{}{"x": true}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("x", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 0.5,
	}
	got := applyPreferScoring(results, prefer, 2)
	assert.Len(t, got, 2)
}

func TestApplyPreferScoring_AllSamePrimaryScore(t *testing.T) {
	// When all primary scores are equal, they normalize to 1.0
	// and prefer becomes the tiebreaker.
	results := []search.Result{
		makeResult("no-match", 0.5, map[string]interface{}{"inStock": false}),
		makeResult("match", 0.5, map[string]interface{}{"inStock": true}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("inStock", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 0.5,
	}
	got := applyPreferScoring(results, prefer, 10)
	// match has prefer=1.0, no-match has prefer=0.0
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

func TestApplyPreferScoring_WithDistConvertedScores(t *testing.T) {
	// Simulates vector search: explorer calls distToScore before applyPreferScoring.
	results := []search.Result{
		{ID: strfmt.UUID("close"), Dist: 0.01, Schema: map[string]interface{}{"streaming": false}},
		{ID: strfmt.UUID("medium"), Dist: 0.10, Schema: map[string]interface{}{"streaming": true}},
		{ID: strfmt.UUID("far"), Dist: 0.90, Schema: map[string]interface{}{"streaming": true}},
	}
	distToScore(results)
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("streaming", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 0.5,
	}
	got := applyPreferScoring(results, prefer, 10)
	// "medium": decent distance (normalized ~0.9) + streaming (1.0) → best combined
	// "close": best distance (normalized 1.0) but no streaming (0.0) → 0.5
	// "far": worst distance (normalized 0.0) but streaming (1.0) → 0.5
	assert.Equal(t, strfmt.UUID("medium"), got[0].ID)
}

func TestApplyPreferScoring_EmptyResults(t *testing.T) {
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			filterCondition("x", filters.OperatorEqual, true, schema.DataTypeBoolean),
		},
		Strength: 0.5,
	}
	got := applyPreferScoring(nil, prefer, 10)
	assert.Nil(t, got)
}

// --- scoreResult tests ---

func TestScoreResult_FilterMatch(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{"color": "red"})
	conds := []filters.PreferCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

func TestScoreResult_FilterNoMatch(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{"color": "blue"})
	conds := []filters.PreferCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_FilterMissingProperty(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{})
	conds := []filters.PreferCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_NilSchema(t *testing.T) {
	r := makeResult("a", 1.0, nil)
	conds := []filters.PreferCondition{
		filterCondition("color", filters.OperatorEqual, "red", schema.DataTypeText),
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestScoreResult_WeightedAverage(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{
		"inStock":  true,
		"category": "electronics",
	})
	conds := []filters.PreferCondition{
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
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	// (2*1.0 + 1*0.0) / (2+1) = 0.6667
	assert.InDelta(t, 0.6667, float64(score), 0.01)
}

func TestScoreResult_ZeroWeight(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{"x": true})
	conds := []filters.PreferCondition{
		{
			Filter: &filters.LocalFilter{Root: &filters.Clause{
				On:       &filters.Path{Property: "x"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
				Operator: filters.OperatorEqual,
			}},
			Weight: 0, // zero weight defaults to 1.0
		},
	}
	score := scoreResult(&r, conds, make([]parsedDecay, len(conds)), time.Now(), nil)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

// --- matchesFilter / matchesClause tests ---

func TestMatchesFilter_And(t *testing.T) {
	props := map[string]interface{}{"a": true, "b": true}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorAnd,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
			{On: &filters.Path{Property: "b"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props, nil))

	props["b"] = false
	assert.False(t, matchesFilter(filter, props, nil))
}

func TestMatchesFilter_Or(t *testing.T) {
	props := map[string]interface{}{"a": false, "b": true}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorOr,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
			{On: &filters.Path{Property: "b"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props, nil))

	props["b"] = false
	assert.False(t, matchesFilter(filter, props, nil))
}

func TestMatchesFilter_Not(t *testing.T) {
	props := map[string]interface{}{"a": false}
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorNot,
		Operands: []filters.Clause{
			{On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual},
		},
	}}
	assert.True(t, matchesFilter(filter, props, nil))
}

func TestMatchesFilter_IsNull(t *testing.T) {
	// Property exists and is non-nil → isNull=false
	props := map[string]interface{}{"a": "hello"}
	assert.False(t, matchesFilter(&filters.LocalFilter{Root: &filters.Clause{
		On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorIsNull,
	}}, props, nil))

	// Property missing → isNull=true
	assert.True(t, matchesFilter(&filters.LocalFilter{Root: &filters.Clause{
		On: &filters.Path{Property: "missing"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorIsNull,
	}}, props, nil))
}

func TestMatchesFilter_NilInputs(t *testing.T) {
	assert.False(t, matchesFilter(nil, map[string]interface{}{}, nil))
	assert.False(t, matchesFilter(&filters.LocalFilter{}, map[string]interface{}{}, nil))
	assert.False(t, matchesFilter(&filters.LocalFilter{Root: &filters.Clause{
		On: &filters.Path{Property: "a"}, Value: &filters.Value{Value: true}, Operator: filters.OperatorEqual,
	}}, nil, nil))
}

// --- compareValues tests ---

func TestCompareValues_Numeric(t *testing.T) {
	tests := []struct {
		name     string
		op       filters.Operator
		propVal  interface{}
		filterV  interface{}
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
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.propVal, tt.filterV, nil))
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
			assert.Equal(t, tt.expected, compareValues(tt.op, tt.prop, tt.filter, nil))
		})
	}
}

func TestCompareValues_Boolean(t *testing.T) {
	assert.True(t, compareValues(filters.OperatorEqual, true, true, nil))
	assert.False(t, compareValues(filters.OperatorEqual, true, false, nil))
	assert.True(t, compareValues(filters.OperatorNotEqual, true, false, nil))
}

func TestCompareValues_Like(t *testing.T) {
	cache := make(map[string]*regexp.Regexp)
	precompileLikePatterns(&filters.Clause{
		Operator: filters.OperatorLike,
		Value:    &filters.Value{Value: "Lap*"},
	}, cache)
	assert.True(t, compareValues(filters.OperatorLike, "Laptop Pro", "Lap*", cache))
	assert.False(t, compareValues(filters.OperatorLike, "Desktop", "Lap*", cache))
}

// --- Decay function tests ---

func TestComputeDecayFunction_Exp(t *testing.T) {
	// decay=0.5, dist=scale → score=0.5
	score := computeDecayFunction("exp", 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → score=1.0
	score = computeDecayFunction("exp", 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist=2*scale → score=0.25
	score = computeDecayFunction("exp", 200, 0, 100, 0.5)
	assert.InDelta(t, 0.25, float64(score), 0.001)
}

func TestComputeDecayFunction_Gauss(t *testing.T) {
	// dist=scale → score=decayValue
	score := computeDecayFunction("gauss", 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → 1.0
	score = computeDecayFunction("gauss", 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)
}

func TestComputeDecayFunction_Linear(t *testing.T) {
	// dist=scale → decayValue
	score := computeDecayFunction("linear", 100, 0, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)

	// dist=0 → 1.0
	score = computeDecayFunction("linear", 0, 0, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist > scale/(1-decay) → 0
	score = computeDecayFunction("linear", 300, 0, 100, 0.5)
	assert.InDelta(t, 0.0, float64(score), 0.001)
}

func TestComputeDecayFunction_WithOffset(t *testing.T) {
	// dist within offset → score=1.0
	score := computeDecayFunction("exp", 50, 50, 100, 0.5)
	assert.InDelta(t, 1.0, float64(score), 0.001)

	// dist=offset+scale → decayValue
	score = computeDecayFunction("exp", 150, 50, 100, 0.5)
	assert.InDelta(t, 0.5, float64(score), 0.001)
}

func TestComputeDecayFunction_DefaultCurve(t *testing.T) {
	// Unknown curve falls back to exp behavior
	score := computeDecayFunction("unknown", 100, 0, 100, 0.5)
	scoreExp := computeDecayFunction("exp", 100, 0, 100, 0.5)
	assert.InDelta(t, float64(scoreExp), float64(score), 0.001)
}

// --- Decay on search result ---

func TestComputeDecayForResult_NumericProperty(t *testing.T) {
	decay := &filters.Decay{
		Path:       &filters.Path{Property: "price"},
		Origin:     "100",
		Scale:      "200",
		Curve:      "exp",
		DecayValue: 0.5,
	}
	parsed := parseDecayParams(decay)
	props := map[string]interface{}{"price": float64(300)} // dist=200=scale → 0.5
	score := computeDecayForResult(decay, parsed, props, time.Now())
	assert.InDelta(t, 0.5, float64(score), 0.001)
}

func TestComputeDecayForResult_MissingProperty(t *testing.T) {
	decay := &filters.Decay{
		Path:   &filters.Path{Property: "price"},
		Origin: "100",
		Scale:  "200",
	}
	parsed := parseDecayParams(decay)
	props := map[string]interface{}{}
	score := computeDecayForResult(decay, parsed, props, time.Now())
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
		Curve:      "exp",
		DecayValue: 0.5,
	}
	parsed := parseDecayParams(decay)

	// 7 days ago → dist=scale → 0.5
	t7dAgo := now.Add(-7 * 24 * time.Hour).Format(time.RFC3339)
	props := map[string]interface{}{"createdAt": t7dAgo}
	score := computeDecayForResult(decay, parsed, props, now)
	assert.InDelta(t, 0.5, float64(score), 0.05)

	// Now → dist=0 → 1.0
	props = map[string]interface{}{"createdAt": now.Format(time.RFC3339)}
	score = computeDecayForResult(decay, parsed, props, now)
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
		Curve:      "gauss",
		DecayValue: 0.3,
	}
	p := parseDecayParams(d)
	assert.True(t, p.valid)
	assert.InDelta(t, 10, p.offset, 0.001)
	assert.InDelta(t, 200, p.scale, 0.001)
	assert.InDelta(t, 0.3, p.decayValue, 0.001)
	assert.Equal(t, "gauss", p.curve)
}

func TestParseDecayParams_Defaults(t *testing.T) {
	d := &filters.Decay{
		Scale: "100",
	}
	p := parseDecayParams(d)
	assert.True(t, p.valid)
	assert.InDelta(t, 0.5, p.decayValue, 0.001) // default
	assert.Equal(t, "exp", p.curve)              // default
}

func TestParseDecayParams_InvalidScale(t *testing.T) {
	d := &filters.Decay{Scale: ""}
	p := parseDecayParams(d)
	assert.False(t, p.valid)

	d = &filters.Decay{Scale: "0"}
	p = parseDecayParams(d)
	assert.False(t, p.valid)
}

// --- toFloat64 ---

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
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

// --- precompileLikePatterns ---

func TestPrecompileLikePatterns(t *testing.T) {
	cache := make(map[string]*regexp.Regexp)
	clause := &filters.Clause{
		Operator: filters.OperatorLike,
		Value:    &filters.Value{Value: "hello*"},
	}
	precompileLikePatterns(clause, cache)
	require.Contains(t, cache, "hello*")
	assert.True(t, cache["hello*"].MatchString("hello world"))
	assert.False(t, cache["hello*"].MatchString("world hello"))
}

func TestPrecompileLikePatterns_QuestionMark(t *testing.T) {
	cache := make(map[string]*regexp.Regexp)
	clause := &filters.Clause{
		Operator: filters.OperatorLike,
		Value:    &filters.Value{Value: "h?llo"},
	}
	precompileLikePatterns(clause, cache)
	assert.True(t, cache["h?llo"].MatchString("hello"))
	assert.True(t, cache["h?llo"].MatchString("hallo"))
	assert.False(t, cache["h?llo"].MatchString("heello"))
}

func TestPrecompileLikePatterns_Nested(t *testing.T) {
	cache := make(map[string]*regexp.Regexp)
	clause := &filters.Clause{
		Operator: filters.OperatorAnd,
		Operands: []filters.Clause{
			{Operator: filters.OperatorLike, Value: &filters.Value{Value: "a*"}},
			{Operator: filters.OperatorLike, Value: &filters.Value{Value: "b*"}},
		},
	}
	precompileLikePatterns(clause, cache)
	assert.Len(t, cache, 2)
	assert.Contains(t, cache, "a*")
	assert.Contains(t, cache, "b*")
}

func TestPrecompileLikePatterns_NilClause(t *testing.T) {
	cache := make(map[string]*regexp.Regexp)
	precompileLikePatterns(nil, cache) // should not panic
	assert.Empty(t, cache)
}

// --- extractProps ---

func TestExtractProps(t *testing.T) {
	r := makeResult("a", 1.0, map[string]interface{}{"key": "val"})
	props := extractProps(&r)
	assert.Equal(t, "val", props["key"])

	r2 := makeResult("b", 1.0, nil)
	assert.Nil(t, extractProps(&r2))

	r3 := search.Result{Schema: "not-a-map"}
	assert.Nil(t, extractProps(&r3))
}

// --- Integration: applyPreferScoring with decay ---

func TestApplyPreferScoring_DecayReorders(t *testing.T) {
	results := []search.Result{
		makeResult("far", 0.5, map[string]interface{}{"price": float64(1000)}),
		makeResult("close", 0.5, map[string]interface{}{"price": float64(110)}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			decayCondition("price", "100", "200", "exp", 0.5),
		},
		Strength: 1.0,
	}
	got := applyPreferScoring(results, prefer, 10)
	// close (dist=10) should rank first, far (dist=900) last
	assert.Equal(t, strfmt.UUID("close"), got[0].ID)
	assert.Equal(t, strfmt.UUID("far"), got[1].ID)
}

func TestApplyPreferScoring_MixedConditions(t *testing.T) {
	results := []search.Result{
		makeResult("a", 0.5, map[string]interface{}{"inStock": true, "price": float64(500)}),
		makeResult("b", 0.5, map[string]interface{}{"inStock": false, "price": float64(100)}),
		makeResult("c", 0.5, map[string]interface{}{"inStock": true, "price": float64(100)}),
	}
	prefer := &filters.Prefer{
		Conditions: []filters.PreferCondition{
			{
				Filter: &filters.LocalFilter{Root: &filters.Clause{
					On:       &filters.Path{Property: "inStock"},
					Value:    &filters.Value{Value: true},
					Operator: filters.OperatorEqual,
				}},
				Weight: 2.0,
			},
			decayCondition("price", "100", "500", "linear", 0.5),
		},
		Strength: 1.0,
	}
	got := applyPreferScoring(results, prefer, 10)
	// c: inStock=true(w=2,s=1) + price=100(w=1,s=1.0) → (2*1+1*1)/3 = 1.0
	// a: inStock=true(w=2,s=1) + price=500(w=1,s=0.5) → (2*1+1*0.5)/3 = 0.833
	// b: inStock=false(w=2,s=0) + price=100(w=1,s=1.0) → (2*0+1*1.0)/3 = 0.333
	assert.Equal(t, strfmt.UUID("c"), got[0].ID)
	assert.Equal(t, strfmt.UUID("a"), got[1].ID)
	assert.Equal(t, strfmt.UUID("b"), got[2].ID)
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
	decay := &filters.Decay{Origin: "100"}
	dist, err := computeDistance(decay, float64(150), time.Now())
	require.NoError(t, err)
	assert.InDelta(t, 50, dist, 0.001)
}

func TestComputeDistance_Date(t *testing.T) {
	now := time.Now()
	decay := &filters.Decay{Origin: "now"}
	dateVal := now.Add(-48 * time.Hour).Format(time.RFC3339)
	dist, err := computeDistance(decay, dateVal, now)
	require.NoError(t, err)
	expected := float64(48 * time.Hour)
	assert.InDelta(t, expected, dist, float64(2*time.Second))
}

// Ensure decay curves score correctly at extreme distances.
func TestComputeDecayFunction_ExtremeDistances(t *testing.T) {
	// Very large distance → exp approaches 0
	score := computeDecayFunction("exp", 1e9, 0, 100, 0.5)
	assert.True(t, float64(score) < 1e-10, "exp at huge distance should be near 0")

	// Gauss at huge distance → 0
	score = computeDecayFunction("gauss", 1e6, 0, 100, 0.5)
	assert.InDelta(t, 0, float64(score), 1e-10)

	// Linear caps at 0
	score = computeDecayFunction("linear", 1e6, 0, 100, 0.5)
	assert.InDelta(t, 0, float64(score), 1e-10)
}

// Verify that NaN/Inf don't leak through decay calculations.
func TestComputeDecayFunction_NoNaN(t *testing.T) {
	score := computeDecayFunction("exp", 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))
	assert.False(t, math.IsInf(float64(score), 0))

	score = computeDecayFunction("gauss", 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))

	score = computeDecayFunction("linear", 0, 0, 100, 0.5)
	assert.False(t, math.IsNaN(float64(score)))
}
