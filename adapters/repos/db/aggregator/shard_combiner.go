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
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
)

type ShardCombiner struct{}

func NewShardCombiner() *ShardCombiner {
	return &ShardCombiner{}
}

func (sc *ShardCombiner) Do(results []*aggregation.Result) (*aggregation.Result, error) {
	allResultsAreNil := true
	firstNonNilRes := 0
	for i, res := range results {
		if res == nil || len(res.Groups) < 1 {
			continue
		}
		if err := sc.RestoreSerializedAggregators(res); err != nil {
			return nil, err
		}
		if allResultsAreNil {
			firstNonNilRes = i
		}
		allResultsAreNil = false
	}

	if allResultsAreNil {
		return &aggregation.Result{}, nil
	}

	if results[firstNonNilRes].Groups[0].GroupedBy == nil {
		return sc.combineUngrouped(results)
	}

	return sc.combineGrouped(results)
}

func (sc *ShardCombiner) combineUngrouped(results []*aggregation.Result) (*aggregation.Result, error) {
	combined := aggregation.Result{
		Groups: make([]aggregation.Group, 1),
	}

	for _, shard := range results {
		if shard == nil || len(shard.Groups) == 0 { // not every shard has results
			continue
		}
		if shard.Groups[0].GroupedBy != nil {
			return nil, errors.New("mixed grouped and ungrouped shard results")
		}
		if err := sc.mergeIntoCombinedGroupAtPos(combined.Groups, 0, shard.Groups[0]); err != nil {
			return nil, err
		}
	}

	if err := sc.finalizeGroup(&combined.Groups[0]); err != nil {
		return nil, err
	}
	return &combined, nil
}

func (sc *ShardCombiner) combineGrouped(results []*aggregation.Result) (*aggregation.Result, error) {
	combined := aggregation.Result{}

	for _, shard := range results {
		if shard == nil {
			continue
		}
		for _, shardGroup := range shard.Groups {
			if shardGroup.GroupedBy == nil {
				return nil, errors.New("mixed grouped and ungrouped shard results")
			}
			pos := getPosOfGroup(combined.Groups, shardGroup.GroupedBy.Value)
			if pos < 0 {
				combined.Groups = append(combined.Groups, shardGroup)
			} else if err := sc.mergeIntoCombinedGroupAtPos(combined.Groups, pos, shardGroup); err != nil {
				return nil, err
			}
		}
	}

	for i := range combined.Groups {
		if err := sc.finalizeGroup(&combined.Groups[i]); err != nil {
			return nil, err
		}
	}

	sort.Slice(combined.Groups, func(a, b int) bool {
		return combined.Groups[a].Count > combined.Groups[b].Count
	})
	return &combined, nil
}

// RestoreSerializedAggregators converts aggregator merge state that crossed
// the cluster-internal JSON boundary back into its concrete in-memory types.
// It is idempotent: already-typed local state is left untouched.
func (sc *ShardCombiner) RestoreSerializedAggregators(res *aggregation.Result) error {
	for gi := range res.Groups {
		for name, prop := range res.Groups[gi].Properties {
			switch prop.Type {
			case aggregation.PropertyTypeNumerical:
				if err := restoreNumericalAggregations(name, prop.NumericalAggregations); err != nil {
					return err
				}
			case aggregation.PropertyTypeDate:
				if err := restoreDateAggregations(name, prop.DateAggregations); err != nil {
					return err
				}
			default:
				// only numerical and date merge state crosses the wire untyped
			}
		}
	}
	return nil
}

func restoreNumericalAggregations(name string, aggs map[string]interface{}) error {
	state, ok := aggs["_numericalAggregator"]
	if !ok {
		return requirePairsForModeMedian(name, aggs, 0)
	}
	if _, typed := state.(*numericalAggregator); typed {
		return nil
	}
	raw, ok := state.(map[string]interface{})
	if !ok {
		return fmt.Errorf("prop %q: malformed numerical aggregator state in remote shard result", name)
	}
	agg, err := numericalAggregatorFromJSON(raw)
	if err != nil {
		return fmt.Errorf("prop %q: %w", name, err)
	}
	if err := requirePairsForModeMedian(name, aggs, len(agg.pairs)); err != nil {
		return err
	}
	aggs["_numericalAggregator"] = agg
	return nil
}

func restoreDateAggregations(name string, aggs map[string]interface{}) error {
	if state, ok := aggs["_dateAggregator"]; !ok {
		if err := requirePairsForModeMedian(name, aggs, 0); err != nil {
			return err
		}
	} else {
		if _, typed := state.(*dateAggregator); !typed {
			raw, ok := state.(map[string]interface{})
			if !ok {
				return fmt.Errorf("prop %q: malformed date aggregator state in remote shard result", name)
			}
			agg, err := dateAggregatorFromJSON(raw)
			if err != nil {
				return fmt.Errorf("prop %q: %w", name, err)
			}
			if err := requirePairsForModeMedian(name, aggs, len(agg.pairs)); err != nil {
				return err
			}
			aggs["_dateAggregator"] = agg
		}
	}

	switch count := aggs["count"].(type) {
	case nil, int64:
		// absent, or a local shard result: nothing to restore
	case float64:
		restored, err := wireCount(count)
		if err != nil {
			return fmt.Errorf("prop %q: %w", name, err)
		}
		aggs["count"] = int64(restored)
	default:
		return fmt.Errorf("prop %q: malformed count in remote shard result", name)
	}
	return nil
}

// requirePairsForModeMedian rejects wire state that carries a mode or median
// result but no distribution to recompute it from during the merge.
func requirePairsForModeMedian(name string, aggs map[string]interface{}, numPairs int) error {
	if numPairs > 0 {
		return nil
	}
	_, hasMode := aggs["mode"]
	_, hasMedian := aggs["median"]
	if hasMode || hasMedian {
		return fmt.Errorf("prop %q: mode/median aggregation without value pairs in remote shard result", name)
	}
	return nil
}

func (sc *ShardCombiner) mergeIntoCombinedGroupAtPos(combinedGroups []aggregation.Group,
	pos int, shardGroup aggregation.Group,
) error {
	combinedGroups[pos].Count += shardGroup.Count

	for propName, prop := range shardGroup.Properties {
		if combinedGroups[pos].Properties == nil {
			combinedGroups[pos].Properties = map[string]aggregation.Property{}
		}

		combinedProp := combinedGroups[pos].Properties[propName]

		combinedProp.Type = prop.Type

		switch prop.Type {
		case aggregation.PropertyTypeNumerical:
			if combinedProp.NumericalAggregations == nil {
				combinedProp.NumericalAggregations = map[string]interface{}{}
			}
			if err := sc.mergeNumericalProp(
				combinedProp.NumericalAggregations, prop.NumericalAggregations); err != nil {
				return fmt.Errorf("prop %q: %w", propName, err)
			}
		case aggregation.PropertyTypeDate:
			if combinedProp.DateAggregations == nil {
				combinedProp.DateAggregations = map[string]interface{}{}
			}
			if err := sc.mergeDateProp(
				combinedProp.DateAggregations, prop.DateAggregations); err != nil {
				return fmt.Errorf("prop %q: %w", propName, err)
			}
		case aggregation.PropertyTypeBoolean:
			sc.mergeBooleanProp(
				&combinedProp.BooleanAggregation, &prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			sc.mergeTextProp(
				&combinedProp.TextAggregation, &prop.TextAggregation)
		case aggregation.PropertyTypeReference:
			sc.mergeRefProp(
				&combinedProp.ReferenceAggregation, &prop.ReferenceAggregation)
		default:
			return fmt.Errorf("unknown property type %q in shard result", prop.Type)
		}
		combinedGroups[pos].Properties[propName] = combinedProp
	}
	return nil
}

func (sc *ShardCombiner) mergeDateProp(first, second map[string]interface{}) error {
	if len(second) == 0 {
		return nil
	}

	// merge the raw distributions first, so that mode/median recompute over both shards
	if source, ok := second["_dateAggregator"]; ok {
		sourceTyped, ok := source.(*dateAggregator)
		if !ok {
			return errors.New("malformed date aggregator state in shard result")
		}
		if combined, ok := first["_dateAggregator"]; ok {
			combinedTyped, ok := combined.(*dateAggregator)
			if !ok {
				return errors.New("malformed date aggregator state in shard result")
			}
			for _, pair := range sourceTyped.pairs {
				combinedTyped.addRow(pair.value, pair.count)
			}
			combinedTyped.buildPairsFromCounts()
		} else {
			first["_dateAggregator"] = source
		}
	}

	for propType, value := range second {
		switch propType {
		case "count":
			sourceCount, ok := value.(int64)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			if val, ok := first[propType]; ok {
				combinedCount, ok := val.(int64)
				if !ok {
					return fmt.Errorf("malformed %q entry in shard result", propType)
				}
				first[propType] = combinedCount + sourceCount
			} else {
				first[propType] = value
			}
		case "mode", "median":
			if _, ok := second["_dateAggregator"]; !ok {
				return fmt.Errorf("%s aggregation without distribution state in shard result", propType)
			}
			agg, ok := first["_dateAggregator"].(*dateAggregator)
			if !ok {
				return fmt.Errorf("%s aggregation without distribution state in shard result", propType)
			}
			if !agg.hasCompleteDistribution() {
				return fmt.Errorf("%s aggregation with incomplete distribution state in shard result", propType)
			}
			if propType == "mode" {
				first[propType] = agg.Mode()
			} else {
				first[propType] = agg.Median()
			}
		case "minimum", "maximum":
			sourceStr, ok := value.(string)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			val, ok := first[propType]
			if !ok {
				first[propType] = value
				continue
			}
			combinedStr, ok := val.(string)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			combinedTime, err := time.Parse(time.RFC3339, combinedStr)
			if err != nil {
				return fmt.Errorf("malformed %q entry in shard result: %w", propType, err)
			}
			sourceTime, err := time.Parse(time.RFC3339, sourceStr)
			if err != nil {
				return fmt.Errorf("malformed %q entry in shard result: %w", propType, err)
			}
			if (propType == "minimum" && sourceTime.Before(combinedTime)) ||
				(propType == "maximum" && sourceTime.After(combinedTime)) {
				first[propType] = value
			}
		case "_dateAggregator":
			continue
		default:
			return fmt.Errorf("unknown aggregation %q in shard result", propType)
		}
	}
	return nil
}

func (sc *ShardCombiner) mergeNumericalProp(first, second map[string]interface{}) error {
	if len(second) == 0 {
		return nil
	}

	// merge the raw distributions first, so that mode/mean/median recompute over both shards
	if source, ok := second["_numericalAggregator"]; ok {
		sourceTyped, ok := source.(*numericalAggregator)
		if !ok {
			return errors.New("malformed numerical aggregator state in shard result")
		}
		if combined, ok := first["_numericalAggregator"]; ok {
			combinedTyped, ok := combined.(*numericalAggregator)
			if !ok {
				return errors.New("malformed numerical aggregator state in shard result")
			}
			combinedTyped.absorb(sourceTyped)
		} else {
			first["_numericalAggregator"] = source
		}
	}

	for propType, value := range second {
		switch propType {
		case "count", "sum":
			sourceVal, ok := value.(float64)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			if val, ok := first[propType]; ok {
				combinedVal, ok := val.(float64)
				if !ok {
					return fmt.Errorf("malformed %q entry in shard result", propType)
				}
				first[propType] = combinedVal + sourceVal
			} else {
				first[propType] = value
			}
		case "mode", "mean", "median":
			if _, ok := second["_numericalAggregator"]; !ok {
				return fmt.Errorf("%s aggregation without distribution state in shard result", propType)
			}
			agg, ok := first["_numericalAggregator"].(*numericalAggregator)
			if !ok {
				return fmt.Errorf("%s aggregation without distribution state in shard result", propType)
			}
			if propType != "mean" && !agg.hasCompleteDistribution() {
				return fmt.Errorf("%s aggregation with incomplete distribution state in shard result", propType)
			}
			switch propType {
			case "mode":
				first[propType] = agg.Mode()
			case "mean":
				first[propType] = agg.Mean()
			case "median":
				first[propType] = agg.Median()
			}
		case "minimum", "maximum":
			sourceVal, ok := value.(float64)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			val, ok := first[propType]
			if !ok {
				first[propType] = value
				continue
			}
			combinedVal, ok := val.(float64)
			if !ok {
				return fmt.Errorf("malformed %q entry in shard result", propType)
			}
			if (propType == "minimum" && sourceVal < combinedVal) ||
				(propType == "maximum" && sourceVal > combinedVal) {
				first[propType] = value
			}
		case "_numericalAggregator":
			continue
		default:
			return fmt.Errorf("unknown aggregation %q in shard result", propType)
		}
	}
	return nil
}

// finalizeDateProp and finalizeNumerical re-check distribution completeness on
// the fully combined state: the per-merge checks only run for shards that carry
// the mode/median key, so a trailing shard can still leave the combined
// distribution incomplete.
func (sc *ShardCombiner) finalizeDateProp(combined map[string]interface{}) error {
	if agg, ok := combined["_dateAggregator"].(*dateAggregator); ok {
		if err := requireCompleteForModeMedian(combined, agg.hasCompleteDistribution()); err != nil {
			return err
		}
	}
	delete(combined, "_dateAggregator")
	return nil
}

func (sc *ShardCombiner) finalizeNumerical(combined map[string]interface{}) error {
	if agg, ok := combined["_numericalAggregator"].(*numericalAggregator); ok {
		if err := requireCompleteForModeMedian(combined, agg.hasCompleteDistribution()); err != nil {
			return err
		}
	}
	delete(combined, "_numericalAggregator")
	return nil
}

func requireCompleteForModeMedian(combined map[string]interface{}, complete bool) error {
	if complete {
		return nil
	}
	_, hasMode := combined["mode"]
	_, hasMedian := combined["median"]
	if hasMode || hasMedian {
		return errors.New("incomplete distribution state for a mode/median aggregation in shard result")
	}
	return nil
}

func (sc *ShardCombiner) mergeBooleanProp(combined, source *aggregation.Boolean) {
	combined.Count += source.Count
	combined.TotalFalse += source.TotalFalse
	combined.TotalTrue += source.TotalTrue
}

func (sc *ShardCombiner) finalizeBoolean(combined *aggregation.Boolean) {
	combined.PercentageFalse = float64(combined.TotalFalse) / float64(combined.Count)
	combined.PercentageTrue = float64(combined.TotalTrue) / float64(combined.Count)
}

func (sc *ShardCombiner) mergeTextProp(first, second *aggregation.Text) {
	first.Count += second.Count

	for _, textOcc := range second.Items {
		pos := getPosOfTextOcc(first.Items, textOcc.Value)
		if pos < 0 {
			first.Items = append(first.Items, textOcc)
		} else {
			first.Items[pos].Occurs += textOcc.Occurs
		}
	}
}

func (sc *ShardCombiner) mergeRefProp(first, second *aggregation.Reference) {
	first.PointingTo = append(first.PointingTo, second.PointingTo...)
}

func (sc *ShardCombiner) finalizeText(combined *aggregation.Text) {
	sort.Slice(combined.Items, func(a, b int) bool {
		return combined.Items[a].Occurs > combined.Items[b].Occurs
	})
}

func getPosOfTextOcc(haystack []aggregation.TextOccurrence, needle string) int {
	for i, elem := range haystack {
		if elem.Value == needle {
			return i
		}
	}

	return -1
}

func (sc *ShardCombiner) finalizeGroup(group *aggregation.Group) error {
	for propName, prop := range group.Properties {
		switch prop.Type {
		case aggregation.PropertyTypeNumerical:
			if err := sc.finalizeNumerical(prop.NumericalAggregations); err != nil {
				return fmt.Errorf("prop %q: %w", propName, err)
			}
		case aggregation.PropertyTypeBoolean:
			sc.finalizeBoolean(&prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			sc.finalizeText(&prop.TextAggregation)
		case aggregation.PropertyTypeDate:
			if err := sc.finalizeDateProp(prop.DateAggregations); err != nil {
				return fmt.Errorf("prop %q: %w", propName, err)
			}
		case aggregation.PropertyTypeReference:
			continue
		default:
			return fmt.Errorf("unknown property type %q in shard result", prop.Type)
		}
		group.Properties[propName] = prop
	}
	return nil
}

func getPosOfGroup(haystack []aggregation.Group, needle interface{}) int {
	for i, elem := range haystack {
		if elem.GroupedBy.Value == needle {
			return i
		}
	}

	return -1
}
