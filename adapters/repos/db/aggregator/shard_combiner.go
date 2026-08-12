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
	"sort"
	"time"

	"github.com/weaviate/weaviate/entities/aggregation"
)

type ShardCombiner struct {
	params aggregation.Params
	// cutoffs indexes params.Properties by name, since the merge consults it
	// once per property per shard
	cutoffs map[string]int
}

func NewShardCombiner(params aggregation.Params) *ShardCombiner {
	cutoffs := map[string]int{}
	for _, prop := range params.Properties {
		if prop.TopOccurrencesCutoff > 0 {
			cutoffs[prop.Name.String()] = int(prop.TopOccurrencesCutoff)
		}
	}
	return &ShardCombiner{params: params, cutoffs: cutoffs}
}

func (sc *ShardCombiner) Do(results []*aggregation.Result) *aggregation.Result {
	allResultsAreNil := true
	firstNonNilRes := 0
	for i, res := range results {
		if res == nil || len(res.Groups) < 1 {
			continue
		}
		allResultsAreNil = false
		firstNonNilRes = i
	}

	if allResultsAreNil {
		return &aggregation.Result{}
	}

	if results[firstNonNilRes].Groups[0].GroupedBy == nil {
		return sc.combineUngrouped(results)
	}

	return sc.combineGrouped(results)
}

func (sc *ShardCombiner) combineUngrouped(results []*aggregation.Result) *aggregation.Result {
	combined := aggregation.Result{
		Groups: make([]aggregation.Group, 1),
	}

	for _, shard := range results {
		if len(shard.Groups) == 0 { // not every shard has results
			continue
		}
		sc.mergeIntoCombinedGroupAtPos(combined.Groups, 0, shard.Groups[0], sc.cutoffs)
	}

	sc.finalizeGroup(&combined.Groups[0])
	sc.applyTopOccurrencesCutoffs(results, &combined.Groups[0])
	return &combined
}

func (sc *ShardCombiner) combineGrouped(results []*aggregation.Result) *aggregation.Result {
	combined := aggregation.Result{}

	for _, shard := range results {
		for _, shardGroup := range shard.Groups {
			pos := getPosOfGroup(combined.Groups, shardGroup.GroupedBy.Value)
			if pos < 0 {
				combined.Groups = append(combined.Groups, shardGroup)
			} else {
				sc.mergeIntoCombinedGroupAtPos(combined.Groups, pos, shardGroup, nil)
			}
		}
	}

	for i := range combined.Groups {
		sc.finalizeGroup(&combined.Groups[i])
	}

	sort.Slice(combined.Groups, func(a, b int) bool {
		return combined.Groups[a].Count > combined.Groups[b].Count
	})
	return &combined
}

// cutoffs is nil for grouped results, which ignore the cutoff entirely.
func (sc *ShardCombiner) mergeIntoCombinedGroupAtPos(combinedGroups []aggregation.Group,
	pos int, shardGroup aggregation.Group, cutoffs map[string]int,
) {
	combinedGroups[pos].Count += shardGroup.Count

	for propName, prop := range shardGroup.Properties {
		if combinedGroups[pos].Properties == nil {
			combinedGroups[pos].Properties = map[string]aggregation.Property{}
		}

		combinedProp := combinedGroups[pos].Properties[propName]

		// a cardinality-only property carries no type; it must not clobber the
		// type another shard set
		if prop.Type != "" {
			combinedProp.Type = prop.Type
		}

		// estimates cannot be unioned across shards, so take the largest: a
		// lower bound that ignores cross-shard overlap
		if prop.ApproximateCardinality != nil {
			if combinedProp.ApproximateCardinality == nil ||
				*prop.ApproximateCardinality > *combinedProp.ApproximateCardinality {
				v := *prop.ApproximateCardinality
				combinedProp.ApproximateCardinality = &v
			}
		}

		switch prop.Type {
		case aggregation.PropertyTypeNumerical:
			if combinedProp.NumericalAggregations == nil {
				combinedProp.NumericalAggregations = map[string]interface{}{}
			}
			sc.mergeNumericalProp(
				combinedProp.NumericalAggregations, prop.NumericalAggregations)
		case aggregation.PropertyTypeDate:
			if combinedProp.DateAggregations == nil {
				combinedProp.DateAggregations = map[string]interface{}{}
			}
			sc.mergeDateProp(
				combinedProp.DateAggregations, prop.DateAggregations)
		case aggregation.PropertyTypeBoolean:
			sc.mergeBooleanProp(
				&combinedProp.BooleanAggregation, &prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			sc.mergeTextProp(
				&combinedProp.TextAggregation, &prop.TextAggregation, cutoffs[propName])
		case aggregation.PropertyTypeReference:
			sc.mergeRefProp(
				&combinedProp.ReferenceAggregation, &prop.ReferenceAggregation)
		case "":
			// cardinality-only property; the default arm panics
		default:
			panic("unknown prop type: " + prop.Type)
		}
		combinedGroups[pos].Properties[propName] = combinedProp

	}
}

func (sc *ShardCombiner) mergeDateProp(first, second map[string]interface{}) {
	if len(second) == 0 {
		return
	}

	// add all values from the second map to the first one. This is needed to compute median and mode correctly
	for propType := range second {
		switch propType {
		case "_dateAggregator":
			dateAggSource := second[propType].(*dateAggregator)
			if dateAggCombined, ok := first[propType]; ok {
				dateAggCombinedTyped := dateAggCombined.(*dateAggregator)
				for _, pair := range dateAggSource.pairs {
					for i := uint64(0); i < pair.count; i++ {
						dateAggCombinedTyped.AddTimestamp(pair.value.rfc3339)
					}
				}
				dateAggCombinedTyped.buildPairsFromCounts()
				first[propType] = dateAggCombinedTyped

			} else {
				first[propType] = second[propType]
			}
		}
	}

	for propType, value := range second {
		switch propType {
		case "count":
			if val, ok := first[propType]; ok {
				first[propType] = val.(int64) + value.(int64)
			} else {
				first[propType] = value
			}
		case "mode":
			dateAggCombined := first["_dateAggregator"].(*dateAggregator)
			first[propType] = dateAggCombined.Mode()
		case "median":
			dateAggCombined := first["_dateAggregator"].(*dateAggregator)
			first[propType] = dateAggCombined.Median()
		case "minimum":
			val, ok := first["minimum"]
			if !ok {
				first["minimum"] = value
			} else {
				source1Time, _ := time.Parse(time.RFC3339, val.(string))
				source2Time, _ := time.Parse(time.RFC3339, value.(string))
				if source2Time.Before(source1Time) {
					first["minimum"] = value
				}
			}
		case "maximum":
			val, ok := first["maximum"]
			if !ok {
				first["maximum"] = value
			} else {
				source1Time, _ := time.Parse(time.RFC3339, val.(string))
				source2Time, _ := time.Parse(time.RFC3339, value.(string))
				if source2Time.After(source1Time) {
					first["maximum"] = value
				}
			}
		case "_dateAggregator":
			continue
		default:
			panic("unknown map entry: " + propType)
		}
	}
}

func (sc *ShardCombiner) mergeNumericalProp(first, second map[string]interface{}) {
	if len(second) == 0 {
		return
	}

	// add all values from the second map to the first one. This is needed to compute median, mean and mode correctly
	for propType := range second {
		switch propType {
		case "_numericalAggregator":
			numAggSecondTyped := second[propType].(*numericalAggregator)
			if numAggFirst, ok := first[propType]; ok {
				numAggFirstTyped := numAggFirst.(*numericalAggregator)
				for _, pair := range numAggSecondTyped.pairs {
					for i := uint64(0); i < pair.count; i++ {
						numAggFirstTyped.AddFloat64(pair.value)
					}
				}
				numAggFirstTyped.buildPairsFromCounts()
				first[propType] = numAggFirstTyped
			} else {
				first[propType] = second[propType]
			}
		}
	}

	for propType, value := range second {
		switch propType {
		case "count", "sum":
			if val, ok := first[propType]; ok {
				first[propType] = val.(float64) + value.(float64)
			} else {
				first[propType] = value
			}
		case "mode":
			numAggFirst := first["_numericalAggregator"].(*numericalAggregator)
			first[propType] = numAggFirst.Mode()
		case "mean":
			numAggFirst := first["_numericalAggregator"].(*numericalAggregator)
			first[propType] = numAggFirst.Mean()
		case "median":
			numAggFirst := first["_numericalAggregator"].(*numericalAggregator)
			first[propType] = numAggFirst.Median()
		case "minimum":
			if _, ok := first["minimum"]; !ok || value.(float64) < first["minimum"].(float64) {
				first["minimum"] = value
			}
		case "maximum":
			if _, ok := first["maximum"]; !ok || value.(float64) > first["maximum"].(float64) {
				first["maximum"] = value
			}
		case "_numericalAggregator":
			continue
		default:
			panic("unknown map entry: " + propType)
		}
	}
}

func (sc *ShardCombiner) finalizeDateProp(combined map[string]interface{}) {
	delete(combined, "_dateAggregator")
}

func (sc *ShardCombiner) finalizeNumerical(combined map[string]interface{}) {
	delete(combined, "_numericalAggregator")
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

func (sc *ShardCombiner) mergeTextProp(first, second *aggregation.Text, cutoff int) {
	// one shard over the cutoff makes the collection's value list incomplete;
	// keeping the other shards' values would hand out a silently truncated
	// vocabulary with per-shard counts
	if first.CutoffExceeded || second.CutoffExceeded {
		*first = aggregation.Text{CutoffExceeded: true}
		return
	}

	first.Count += second.Count
	if len(second.Items) == 0 {
		return
	}

	// a cutoff merge runs lists as long as the cutoff itself, where a scan per
	// incoming value is quadratic
	posOf := make(map[string]int, len(first.Items)+len(second.Items))
	for i, item := range first.Items {
		posOf[item.Value] = i
	}

	for _, textOcc := range second.Items {
		if i, ok := posOf[textOcc.Value]; ok {
			first.Items[i].Occurs += textOcc.Occurs
			continue
		}
		// every shard list is a subset of the values its shard holds, so a
		// union already past the cutoff stays past it however the rest merge —
		// whether or not the lists are complete
		if cutoff > 0 && len(first.Items) == cutoff {
			*first = aggregation.Text{CutoffExceeded: true}
			return
		}
		posOf[textOcc.Value] = len(first.Items)
		first.Items = append(first.Items, textOcc)
	}
}

func (sc *ShardCombiner) mergeRefProp(first, second *aggregation.Reference) {
	first.PointingTo = append(first.PointingTo, second.PointingTo...)
}

// applyTopOccurrencesCutoffs enforces the distinct-value cutoffs on the merged
// value lists, since a collection can hold more distinct values than the cutoff
// although no single shard did. A shard that could not evaluate the cutoff
// contributes top values rather than all of them, which proves nothing about
// the collection, so the cutoff is dropped instead of guessed at.
func (sc *ShardCombiner) applyTopOccurrencesCutoffs(results []*aggregation.Result,
	combined *aggregation.Group,
) {
	for _, paramProp := range sc.params.Properties {
		name := paramProp.Name.String()
		if sc.cutoffs[name] == 0 {
			continue
		}

		prop, ok := combined.Properties[name]
		if !ok || prop.Type != aggregation.PropertyTypeText ||
			prop.TextAggregation.CutoffExceeded {
			continue
		}

		// the shards hand over cutoff-many values rather than the top ones, so
		// the requested limit is applied here whether or not the cutoff held
		prop.TextAggregation = topOccurrences(prop.TextAggregation,
			extractLimitFromTopOccs(paramProp.Aggregators),
			shardValuesComplete(results, name))
		combined.Properties[name] = prop
	}
}

// shardValuesComplete reports whether every shard holding the property listed
// all of its values.
func shardValuesComplete(results []*aggregation.Result, propName string) bool {
	for _, shard := range results {
		if shard == nil || len(shard.Groups) == 0 {
			continue
		}
		prop, ok := shard.Groups[0].Properties[propName]
		if !ok || prop.Type != aggregation.PropertyTypeText {
			continue
		}
		if !prop.TextAggregation.ValuesComplete && !prop.TextAggregation.CutoffExceeded {
			return false
		}
	}
	return true
}

// topOccurrences cuts a merged value list down to the requested limit, through
// the aggregator the single-shard path uses so both order the same. complete
// reports whether the list it cuts held every value of the property.
func topOccurrences(text aggregation.Text, limit int, complete bool) aggregation.Text {
	agg := newTextAggregator(limit)
	for _, item := range text.Items {
		if err := agg.AddTextCount(item.Value, item.Occurs); err != nil {
			return text
		}
	}

	out := agg.Res()
	out.ValuesComplete = complete
	return out
}

func (sc *ShardCombiner) finalizeText(combined *aggregation.Text) {
	sort.Slice(combined.Items, func(a, b int) bool {
		return combined.Items[a].Occurs > combined.Items[b].Occurs
	})
}

func (sc *ShardCombiner) finalizeGroup(group *aggregation.Group) {
	for propName, prop := range group.Properties {
		switch prop.Type {
		case aggregation.PropertyTypeNumerical:
			sc.finalizeNumerical(prop.NumericalAggregations)
		case aggregation.PropertyTypeBoolean:
			sc.finalizeBoolean(&prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			sc.finalizeText(&prop.TextAggregation)
		case aggregation.PropertyTypeDate:
			sc.finalizeDateProp(prop.DateAggregations)
		case aggregation.PropertyTypeReference:
			continue
		case "":
			// cardinality-only property; the default arm panics
		default:
			panic("Unknown prop type: " + prop.Type)
		}
		group.Properties[propName] = prop
	}
}

func getPosOfGroup(haystack []aggregation.Group, needle interface{}) int {
	for i, elem := range haystack {
		if elem.GroupedBy.Value == needle {
			return i
		}
	}

	return -1
}
