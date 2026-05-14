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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
)

func applyBoostScoring(results []search.Result, boost *filters.Boost) []search.Result {
	if boost == nil || len(boost.Conditions) == 0 || len(results) == 0 {
		return results
	}

	weight := boost.Weight
	if weight <= 0 {
		return results
	}
	if weight > 1 {
		weight = 1
	}

	nowTime := time.Now()

	// Pre-parse decay parameters and pre-compile Like patterns once.
	decayParams := make([]parsedDecay, len(boost.Conditions))
	likeCache := make(map[string]*regexp.Regexp)
	for i, cond := range boost.Conditions {
		if cond.Decay != nil {
			decayParams[i] = parseDecayParams(cond.Decay)
		}
		if cond.Filter != nil {
			precompileLikePatterns(cond.Filter.Root, likeCache)
		}
	}

	// Pre-compute normalized property value scores for each property_value condition.
	// These need the full result set for min-max normalization.
	propertyValueScores := precomputePropertyValueScores(results, boost.Conditions)

	// Compute boost score for each result.
	boostScores := make([]float32, len(results))
	for i := range results {
		boostScores[i] = scoreResult(&results[i], boost.Conditions, decayParams, propertyValueScores, i, nowTime, likeCache)
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

	// Re-sort by combined score descending.
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
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
func scoreResult(r *search.Result, conditions []filters.BoostCondition,
	decayParams []parsedDecay, propertyValueScores [][]float32, resultIdx int,
	nowTime time.Time, likeCache map[string]*regexp.Regexp,
) float32 {
	var weightedSum, weightSum float32

	props := extractProps(r)

	for i, cond := range conditions {
		weight := cond.Weight
		if weight == 0 {
			weight = 1.0
		}

		var condScore float32

		if cond.Filter != nil {
			if matchesFilter(cond.Filter, props, likeCache) {
				condScore = 1.0
			}
		} else if cond.Decay != nil {
			condScore = computeDecayForResult(cond.Decay, decayParams[i], props, nowTime)
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
func precomputePropertyValueScores(results []search.Result, conditions []filters.BoostCondition) [][]float32 {
	scores := make([][]float32, len(conditions))
	for i, cond := range conditions {
		if cond.PropertyValue == nil {
			continue
		}

		fv := cond.PropertyValue
		propName := string(fv.Path.Property)
		raw := make([]float64, len(results))

		for j := range results {
			props := extractProps(&results[j])
			if props == nil {
				continue
			}
			val, err := toFloat64(props[propName])
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

		scores[i] = make([]float32, len(results))
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
func extractProps(r *search.Result) map[string]interface{} {
	if r.Schema == nil {
		return nil
	}
	props, ok := r.Schema.(map[string]interface{})
	if !ok {
		return nil
	}
	return props
}

// matchesFilter evaluates a LocalFilter against an object's properties in-memory.
// This handles the common filter operators used in prefer conditions.
func matchesFilter(filter *filters.LocalFilter, props map[string]interface{}, likeCache map[string]*regexp.Regexp) bool {
	if filter == nil || filter.Root == nil || props == nil {
		return false
	}
	return matchesClause(filter.Root, props, likeCache)
}

func matchesClause(clause *filters.Clause, props map[string]interface{}, likeCache map[string]*regexp.Regexp) bool {
	switch clause.Operator {
	case filters.OperatorAnd:
		for i := range clause.Operands {
			if !matchesClause(&clause.Operands[i], props, likeCache) {
				return false
			}
		}
		return true

	case filters.OperatorOr:
		for i := range clause.Operands {
			if matchesClause(&clause.Operands[i], props, likeCache) {
				return true
			}
		}
		return false

	case filters.OperatorNot:
		if len(clause.Operands) > 0 {
			return !matchesClause(&clause.Operands[0], props, likeCache)
		}
		return false

	default:
		return matchesValueClause(clause, props, likeCache)
	}
}

func matchesValueClause(clause *filters.Clause, props map[string]interface{}, likeCache map[string]*regexp.Regexp) bool {
	if clause.On == nil || clause.Value == nil {
		return false
	}

	propName := string(clause.On.Property)
	propVal, exists := props[propName]
	if !exists {
		if clause.Operator == filters.OperatorIsNull {
			return clause.Value.Value == true
		}
		return false
	}

	if clause.Operator == filters.OperatorIsNull {
		isNull := propVal == nil
		return isNull == (clause.Value.Value == true)
	}

	return compareValues(clause.Operator, propVal, clause.Value.Value, likeCache)
}

func compareValues(op filters.Operator, propVal, filterVal interface{}, likeCache map[string]*regexp.Regexp) bool {
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
			case filters.OperatorLike:
				return matchLikeCached(strProp, strFilter, likeCache)
			default:
				return false
			}
		}
		return false
	}

	return false
}

func asBool(v interface{}) (bool, bool) {
	switch b := v.(type) {
	case bool:
		return b, true
	default:
		return false, false
	}
}

func asFloat64(v interface{}) (float64, bool) {
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

// precompileLikePatterns walks a filter clause tree and pre-compiles any Like
// operator patterns into the cache so they aren't recompiled per document.
func precompileLikePatterns(clause *filters.Clause, cache map[string]*regexp.Regexp) {
	if clause == nil {
		return
	}
	if clause.Operator == filters.OperatorLike && clause.Value != nil {
		if pattern, ok := clause.Value.Value.(string); ok {
			if _, exists := cache[pattern]; !exists {
				regexStr := "^" + regexp.QuoteMeta(pattern) + "$"
				regexStr = strings.ReplaceAll(regexStr, `\*`, ".*")
				regexStr = strings.ReplaceAll(regexStr, `\?`, ".")
				if re, err := regexp.Compile(regexStr); err == nil {
					cache[pattern] = re
				}
			}
		}
	}
	for i := range clause.Operands {
		precompileLikePatterns(&clause.Operands[i], cache)
	}
}

func matchLikeCached(value, pattern string, cache map[string]*regexp.Regexp) bool {
	if re, ok := cache[pattern]; ok {
		return re.MatchString(value)
	}
	return false
}

// --- Decay scoring on search results ---

type parsedDecay struct {
	offset     float64
	scale      float64
	decayValue float64
	curve      filters.DecayCurveType
	valid      bool
}

func parseDecayParams(d *filters.Decay) parsedDecay {
	offset, _ := parseNumericOrDuration(d.Offset)
	scale, err := parseNumericOrDuration(d.Scale)
	if err != nil || scale <= 0 {
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
	return parsedDecay{
		offset:     offset,
		scale:      scale,
		decayValue: decayValue,
		curve:      curve,
		valid:      true,
	}
}

func computeDecayForResult(decay *filters.Decay, parsed parsedDecay, props map[string]interface{}, nowTime time.Time) float32 {
	if !parsed.valid || props == nil || decay.Path == nil {
		return 0
	}

	propName := string(decay.Path.Property)
	propVal, exists := props[propName]
	if !exists || propVal == nil {
		return 0
	}

	dist, err := computeDistance(decay, propVal, nowTime)
	if err != nil {
		return 0
	}

	return computeDecayFunction(parsed.curve, dist, parsed.offset, parsed.scale, parsed.decayValue)
}

func computeDistance(decay *filters.Decay, propValue interface{}, nowTime time.Time) (float64, error) {
	if dateVal, ok := tryParseDate(propValue); ok {
		origin := decay.Origin
		if origin == "" {
			origin = "now"
		}
		originTime, err := parseOriginAsTime(origin, nowTime)
		if err != nil {
			return 0, err
		}
		return math.Abs(float64(dateVal.Sub(originTime))), nil
	}

	numVal, err := toFloat64(propValue)
	if err != nil {
		return 0, err
	}

	originNum, err := strconv.ParseFloat(decay.Origin, 64)
	if err != nil {
		return 0, err
	}

	return math.Abs(numVal - originNum), nil
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

func tryParseDate(val interface{}) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case string:
		for _, layout := range []string{
			time.RFC3339Nano, time.RFC3339,
			"2006-01-02T15:04:05", "2006-01-02",
		} {
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
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC3339,
		"2006-01-02T15:04:05", "2006-01-02",
	} {
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

func toFloat64(val interface{}) (float64, error) {
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
