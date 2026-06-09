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

// boost_scorer.go implements the boost post-scoring pipeline. It rescores
// search results by evaluating each result against a set of boost
// conditions (filter-based binary scoring and/or decay-based continuous
// scoring), then blending the boost score with the normalized primary
// search score using a configurable weight parameter.
//
// This file is intentionally self-contained: all scoring, filter matching,
// and decay computation happens in-memory on already-fetched search results.
// No index queries are issued from this code.

package traverser

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
)

// dateLayouts lists the time formats tried when parsing date property values
// and origin strings. Declared at package scope to avoid per-call allocation.
var dateLayouts = [...]string{
	time.RFC3339Nano, time.RFC3339,
	"2006-01-02T15:04:05", "2006-01-02",
}

// applyBoostScoring rescores results. typedValues, when non-nil, is aligned
// with results and carries property values served from columnar buckets
// (float64 for int/number, time.Time for dates); it takes precedence over
// the result's materialized properties, which remain the fallback.
func applyBoostScoring(results []search.Result, boost *filters.Boost,
	typedValues []map[string]any,
) []search.Result {
	if boost == nil || len(boost.Conditions) == 0 || len(results) == 0 {
		return results
	}
	if typedValues != nil && len(typedValues) != len(results) {
		typedValues = nil // misaligned input — fall back to props everywhere
	}

	weight := boost.Weight
	if weight <= 0 {
		return results
	}

	nowTime := time.Now()

	// Pre-parse decay parameters once.
	decayParams := make([]parsedDecay, len(boost.Conditions))
	for i, cond := range boost.Conditions {
		if cond.Decay != nil {
			decayParams[i] = parseDecayParams(cond.Decay, nowTime)
		}
	}

	// Per-result value getters: typed columnar values first, materialized
	// props as fallback.
	getters := make([]func(string) any, len(results))
	for i := range results {
		getters[i] = boostValueGetter(typedValuesAt(typedValues, i), extractProps(&results[i]))
	}

	// Pre-compute normalized property value scores for each property_value condition.
	// These need the full result set for min-max normalization.
	propertyValueScores := precomputePropertyValueScores(getters, boost.Conditions)

	// Compute boost score for each result.
	boostScores := make([]float32, len(results))
	for i := range results {
		boostScores[i] = scoreResult(getters[i], boost.Conditions, decayParams, propertyValueScores, i)
	}

	// Normalize primary scores to [0,1] using min-max.
	primaryScores := make([]float32, len(results))
	var minPrimary, maxPrimary float32
	minPrimary = math.MaxFloat32
	maxPrimary = -math.MaxFloat32
	for i := range results {
		s := results[i].Score
		primaryScores[i] = s
		if s < minPrimary {
			minPrimary = s
		}
		if s > maxPrimary {
			maxPrimary = s
		}
	}
	rangePrimary := maxPrimary - minPrimary
	if rangePrimary > 0 {
		for i := range primaryScores {
			primaryScores[i] = (primaryScores[i] - minPrimary) / rangePrimary
		}
	} else {
		// All same score — normalize to 1.0 so boost is the tiebreaker.
		for i := range primaryScores {
			primaryScores[i] = 1.0
		}
	}

	// Combine scores.
	for i := range results {
		results[i].Score = (1-weight)*primaryScores[i] + weight*boostScores[i]
	}

	// Normalize combined scores to [0,1] for user-facing display.
	// Negative per-condition weights can produce combined scores below 0.
	var minCombined, maxCombined float32
	minCombined = results[0].Score
	maxCombined = results[0].Score
	for _, r := range results[1:] {
		if r.Score < minCombined {
			minCombined = r.Score
		}
		if r.Score > maxCombined {
			maxCombined = r.Score
		}
	}
	if rangeCombined := maxCombined - minCombined; rangeCombined > 0 {
		for i := range results {
			results[i].Score = (results[i].Score - minCombined) / rangeCombined
		}
	} else {
		for i := range results {
			results[i].Score = 1.0
		}
	}

	// Re-sort by combined score descending.
	slices.SortFunc(results, func(a, b search.Result) int {
		if a.Score != b.Score {
			if a.Score > b.Score {
				return -1
			}
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})

	// Apply offset, then truncate to limit.
	offset := boost.OriginalOffset
	limit := boost.OriginalLimit
	if offset > 0 {
		if offset >= len(results) {
			return nil
		}
		results = results[offset:]
	}
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results
}

// scoreResult computes the weighted boost score for a single search result.
// Negative per-condition weights demote matching documents. The denominator
// uses abs(weight) so the score range is [-1, 1].
func scoreResult(getValue func(string) any, conditions []filters.BoostCondition,
	decayParams []parsedDecay, propertyValueScores [][]float32, resultIdx int,
) float32 {
	var weightedSum, weightSum float32

	for i, cond := range conditions {
		weight := cond.Weight
		if weight == 0 {
			weight = 1.0
		}

		var condScore float32

		if cond.Filter != nil {
			if matchesFilter(cond.Filter, getValue) {
				condScore = 1.0
			}
		} else if cond.Decay != nil {
			condScore = computeDecayForResult(cond.Decay, decayParams[i], getValue)
		} else if cond.PropertyValue != nil {
			condScore = propertyValueScores[i][resultIdx]
		}

		weightedSum += weight * condScore
		absWeight := weight
		if absWeight < 0 {
			absWeight = -absWeight
		}
		weightSum += absWeight
	}

	if weightSum == 0 {
		return 0
	}
	return weightedSum / weightSum
}

// distToScore converts distance-based results (from vector search) to
// score-based results that applyBoostScoring can blend with. Distance is
// inverted so that closer objects get higher scores.
func distToScore(results []search.Result) {
	for i := range results {
		results[i].Score = -results[i].Dist
	}
}

// precomputePropertyValueScores computes normalized [0,1] scores for each
// property_value condition across all results. Min-max normalization is applied
// after the modifier so that the highest value in the result set scores 1.0.
// Returns a slice indexed by [conditionIdx][resultIdx].
func precomputePropertyValueScores(getters []func(string) any, conditions []filters.BoostCondition) [][]float32 {
	scores := make([][]float32, len(conditions))

	hasPropertyValue := false
	for i := range conditions {
		if conditions[i].PropertyValue != nil {
			hasPropertyValue = true
			break
		}
	}
	if !hasPropertyValue {
		return scores
	}

	for i, cond := range conditions {
		if cond.PropertyValue == nil {
			continue
		}

		fv := cond.PropertyValue
		propName := string(fv.Path.Property)
		raw := make([]float64, len(getters))

		for j := range getters {
			val, err := toFloat64(getters[j](propName))
			if err != nil {
				continue
			}
			raw[j] = applyPropertyValueModifier(val, fv.Modifier)
		}

		// Min-max normalize to [0,1].
		minVal, maxVal := raw[0], raw[0]
		for _, v := range raw[1:] {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}

		scores[i] = make([]float32, len(getters))
		rangeVal := maxVal - minVal
		if rangeVal > 0 {
			for j := range raw {
				scores[i][j] = float32((raw[j] - minVal) / rangeVal)
			}
		} else {
			// All same value — normalize to 1.0
			for j := range scores[i] {
				scores[i][j] = 1.0
			}
		}
	}
	return scores
}

func applyPropertyValueModifier(val float64, modifier filters.PropertyValueModifierType) float64 {
	switch modifier {
	case filters.PropertyValueModifierLog1p:
		return math.Log1p(math.Max(0, val))
	case filters.PropertyValueModifierSqrt:
		return math.Sqrt(math.Max(0, val))
	default:
		return val
	}
}

// extractProps gets the properties map from a search result.
func extractProps(r *search.Result) map[string]any {
	if r.Schema == nil {
		return nil
	}
	props, ok := r.Schema.(map[string]any)
	if !ok {
		return nil
	}
	return props
}

func typedValuesAt(typedValues []map[string]any, i int) map[string]any {
	if typedValues == nil {
		return nil
	}
	return typedValues[i]
}

// boostValueGetter resolves a property value with typed columnar values
// taking precedence over materialized props.
func boostValueGetter(typed, props map[string]any) func(string) any {
	return func(name string) any {
		if typed != nil {
			if v, ok := typed[name]; ok {
				return v
			}
		}
		if props == nil {
			return nil
		}
		return props[name]
	}
}

// matchesFilter evaluates a LocalFilter against an object's property values
// in-memory. This handles the common filter operators used in boost
// conditions.
func matchesFilter(filter *filters.LocalFilter, getValue func(string) any) bool {
	if filter == nil || filter.Root == nil || getValue == nil {
		return false
	}
	return matchesClause(filter.Root, getValue)
}

func matchesClause(clause *filters.Clause, getValue func(string) any) bool {
	switch clause.Operator {
	case filters.OperatorAnd:
		for i := range clause.Operands {
			if !matchesClause(&clause.Operands[i], getValue) {
				return false
			}
		}
		return true

	case filters.OperatorOr:
		for i := range clause.Operands {
			if matchesClause(&clause.Operands[i], getValue) {
				return true
			}
		}
		return false

	case filters.OperatorNot:
		if len(clause.Operands) > 0 {
			return !matchesClause(&clause.Operands[0], getValue)
		}
		return false

	default:
		return matchesValueClause(clause, getValue)
	}
}

func matchesValueClause(clause *filters.Clause, getValue func(string) any) bool {
	if clause.On == nil || clause.Value == nil {
		return false
	}

	propVal := getValue(string(clause.On.Property))
	if propVal == nil {
		return false
	}

	return compareValues(clause.Operator, propVal, clause.Value.Value)
}

func compareValues(op filters.Operator, propVal, filterVal any) bool {
	// Try boolean comparison.
	if boolVal, ok := asBool(propVal); ok {
		if filterBool, ok := asBool(filterVal); ok {
			switch op {
			case filters.OperatorEqual:
				return boolVal == filterBool
			case filters.OperatorNotEqual:
				return boolVal != filterBool
			default:
				return false
			}
		}
		return false
	}

	// Try numeric comparison.
	if numProp, ok := asFloat64(propVal); ok {
		if numFilter, ok := asFloat64(filterVal); ok {
			switch op {
			case filters.OperatorEqual:
				return numProp == numFilter
			case filters.OperatorNotEqual:
				return numProp != numFilter
			case filters.OperatorGreaterThan:
				return numProp > numFilter
			case filters.OperatorGreaterThanEqual:
				return numProp >= numFilter
			case filters.OperatorLessThan:
				return numProp < numFilter
			case filters.OperatorLessThanEqual:
				return numProp <= numFilter
			default:
				return false
			}
		}
		return false
	}

	// Try string comparison.
	if strProp, ok := propVal.(string); ok {
		if strFilter, ok := filterVal.(string); ok {
			switch op {
			case filters.OperatorEqual:
				return strProp == strFilter
			case filters.OperatorNotEqual:
				return strProp != strFilter
			case filters.OperatorGreaterThan:
				return strProp > strFilter
			case filters.OperatorGreaterThanEqual:
				return strProp >= strFilter
			case filters.OperatorLessThan:
				return strProp < strFilter
			case filters.OperatorLessThanEqual:
				return strProp <= strFilter
			default:
				return false
			}
		}
		return false
	}

	return false
}

func asBool(v any) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	default:
		return false, false
	}
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// --- Decay scoring on search results ---

type parsedDecay struct {
	offset     float64
	scale      float64
	decayValue float64
	curve      filters.DecayCurveType

	// Pre-parsed origin values so computeDistance avoids re-parsing per result.
	originTime      time.Time
	originTimeValid bool
	originNum       float64
	originNumValid  bool

	valid bool
}

func parseDecayParams(d *filters.Decay, nowTime time.Time) parsedDecay {
	var offset, scale float64
	if d.IsNumeric {
		offset = d.OffsetNumeric
		scale = d.ScaleNumeric
	} else {
		var err error
		offset, _ = parseNumericOrDuration(d.Offset)
		scale, err = parseNumericOrDuration(d.Scale)
		if err != nil {
			return parsedDecay{}
		}
	}
	if scale <= 0 {
		return parsedDecay{}
	}
	decayValue := float64(d.DecayValue)
	if decayValue == 0 {
		decayValue = 0.5
	}
	curve := d.Curve
	if curve == "" {
		curve = filters.DecayCurveExp
	}

	p := parsedDecay{
		offset:     offset,
		scale:      scale,
		decayValue: decayValue,
		curve:      curve,
		valid:      true,
	}

	// Pre-parse origin as time and as number so computeDistance doesn't
	// repeat the work for every result.
	origin := d.Origin
	if d.IsNumeric {
		p.originNum = d.OriginNumeric
		p.originNumValid = true
	} else {
		// Origin is optional for time-based decay: default to "now".
		if origin == "" {
			p.originTime = nowTime
			p.originTimeValid = true
		} else if t, err := parseOriginAsTime(origin, nowTime); err == nil {
			p.originTime = t
			p.originTimeValid = true
		}
		if n, err := strconv.ParseFloat(origin, 64); err == nil {
			p.originNum = n
			p.originNumValid = true
		}
	}

	return p
}

func computeDecayForResult(decay *filters.Decay, parsed parsedDecay, getValue func(string) any) float32 {
	if !parsed.valid || getValue == nil || decay.Path == nil {
		return 0
	}

	propVal := getValue(string(decay.Path.Property))
	if propVal == nil {
		return 0
	}

	dist, err := computeDistance(parsed, propVal)
	if err != nil {
		return 0
	}

	return computeDecayFunction(parsed.curve, dist, parsed.offset, parsed.scale, parsed.decayValue)
}

func computeDistance(parsed parsedDecay, propValue any) (float64, error) {
	if dateVal, ok := tryParseDate(propValue); ok {
		if !parsed.originTimeValid {
			return 0, fmt.Errorf("no valid time origin for date property")
		}
		return math.Abs(float64(dateVal.Sub(parsed.originTime))), nil
	}

	numVal, err := toFloat64(propValue)
	if err != nil {
		return 0, err
	}

	if !parsed.originNumValid {
		return 0, fmt.Errorf("no valid numeric origin")
	}

	return math.Abs(numVal - parsed.originNum), nil
}

func computeDecayFunction(curve filters.DecayCurveType, dist, offset, scale, decayValue float64) float32 {
	effectiveDist := math.Max(0, dist-offset)
	if effectiveDist == 0 {
		return 1.0
	}

	var score float64
	switch curve {
	case filters.DecayCurveExp:
		score = math.Pow(decayValue, effectiveDist/scale)
	case filters.DecayCurveGauss:
		factor := -math.Log(decayValue)
		ratio := effectiveDist / scale
		score = math.Exp(-factor * ratio * ratio)
	case filters.DecayCurveLinear:
		score = math.Max(0, 1.0-(1.0-decayValue)*effectiveDist/scale)
	default:
		score = math.Pow(decayValue, effectiveDist/scale)
	}

	return float32(score)
}

// --- Time/duration parsing helpers ---

func tryParseDate(val any) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case string:
		for _, layout := range dateLayouts {
			if t, err := time.Parse(layout, v); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func parseOriginAsTime(origin string, nowTime time.Time) (time.Time, error) {
	if origin == "now" {
		return nowTime, nil
	}
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, origin); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as time", origin)
}

var durationPattern = regexp.MustCompile(`^(\d+(?:\.\d+)?)(d|h|m|s|ms)$`)

func parseNumericOrDuration(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	matches := durationPattern.FindStringSubmatch(s)
	if matches != nil {
		num, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return 0, err
		}
		switch matches[2] {
		case "d":
			return num * float64(24*time.Hour), nil
		case "h":
			return num * float64(time.Hour), nil
		case "m":
			return num * float64(time.Minute), nil
		case "s":
			return num * float64(time.Second), nil
		case "ms":
			return num * float64(time.Millisecond), nil
		}
	}
	if d, err := time.ParseDuration(s); err == nil {
		return float64(d), nil
	}
	return strconv.ParseFloat(s, 64)
}

func toFloat64(val any) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}
