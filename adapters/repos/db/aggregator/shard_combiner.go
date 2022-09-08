//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"sort"
	"time"

	"github.com/semi-technologies/weaviate/entities/aggregation"
)

type ShardCombiner struct{}

func NewShardCombiner() *ShardCombiner {
	return &ShardCombiner{}
}

func (sc *ShardCombiner) Do(results []*aggregation.Result) *aggregation.Result {
	if results[0] == nil || len(results[0].Groups) < 1 {
		return nil
	}

	if results[0].Groups[0].GroupedBy == nil {
		return sc.combineUngrouped(results)
	}

	return sc.combineGrouped(results)
}

func (sc *ShardCombiner) combineUngrouped(results []*aggregation.Result) *aggregation.Result {
	combined := aggregation.Result{
		Groups: make([]aggregation.Group, 1),
	}

	for _, shard := range results {
		sc.mergeIntoCombinedGroupAtPos(combined.Groups, 0, shard.Groups[0])
	}

	sc.finalizeGroup(&combined.Groups[0])
	return &combined
}

func (sc *ShardCombiner) combineGrouped(results []*aggregation.Result) *aggregation.Result {
	combined := aggregation.Result{}

	for _, shard := range results {
		for _, shardGroup := range shard.Groups {
			pos := getPosOfGroup(combined.Groups, shardGroup.GroupedBy.Value)
			if pos < 0 {
				for propName := range shardGroup.Properties {
					// set the dummy rounds prop which is used as a counter for means,
					// etc.
					if shardGroup.Properties[propName].NumericalAggregations != nil {
						shardGroup.Properties[propName].NumericalAggregations["_rounds"] = 1.
					}
				}
				combined.Groups = append(combined.Groups, shardGroup)
			} else {
				sc.mergeIntoCombinedGroupAtPos(combined.Groups, pos, shardGroup)
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

func (sc *ShardCombiner) mergeIntoCombinedGroupAtPos(combinedGroups []aggregation.Group,
	pos int, shardGroup aggregation.Group,
) {
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
			combinedProp.NumericalAggregations = sc.mergeNumericalProp(
				combinedProp.NumericalAggregations, prop.NumericalAggregations)
		case aggregation.PropertyTypeDate:
			if combinedProp.DateAggregations == nil {
				combinedProp.DateAggregations = map[string]interface{}{}
			}
			sc.mergeDateProp(
				combinedProp.DateAggregations, prop.DateAggregations)
		case aggregation.PropertyTypeBoolean:
			combinedProp.BooleanAggregation = sc.mergeBooleanProp(
				combinedProp.BooleanAggregation, prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			combinedProp.TextAggregation = sc.mergeTextProp(
				combinedProp.TextAggregation, prop.TextAggregation)
		}
		combinedGroups[pos].Properties[propName] = combinedProp

	}
}

func (sc ShardCombiner) mergeDateProp(first, second map[string]interface{}) {
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
			panic("unknown prop type: " + propType)
		}
	}
}

func (sc ShardCombiner) mergeNumericalProp(combined, source map[string]interface{}) map[string]interface{} {
	if len(source) == 0 {
		return combined
	}

	for propType, value := range source {
		switch propType {
		case "mean", "mode", "median", "count", "sum":
			if val, ok := combined[propType]; ok {
				combined[propType] = val.(float64) + value.(float64)
			} else {
				combined[propType] = value
			}
		case "minimum":
			if _, ok := combined["minimum"]; !ok || value.(float64) < combined["minimum"].(float64) {
				combined["minimum"] = value
			}
		case "maximum":
			if _, ok := combined["maximum"]; !ok || value.(float64) > combined["maximum"].(float64) {
				combined["maximum"] = value
			}
		default:
			panic("unkwnon prop type: " + propType)
		}
	}

	valMean, ok := combined["_rounds"]
	if ok {
		val := valMean.(float64)
		val++
		combined["_rounds"] = val
	} else {
		combined["_rounds"] = 1.
	}

	return combined
}

func (sc ShardCombiner) finalizeDateProp(combined map[string]interface{}) {
	delete(combined, "_dateAggregator")
}

func (sc ShardCombiner) finalizeNumerical(combined map[string]interface{}) map[string]interface{} {
	if _, ok := combined["mean"]; ok {
		combined["mean"] = combined["mean"].(float64) / combined["_rounds"].(float64)
	}
	if _, ok := combined["mode"]; ok {
		combined["mode"] = combined["mode"].(float64) / combined["_rounds"].(float64)
	}
	if _, ok := combined["median"]; ok {
		combined["median"] = combined["median"].(float64) / combined["_rounds"].(float64)
	}
	delete(combined, "_rounds")
	return combined
}

func (sc ShardCombiner) mergeBooleanProp(combined,
	source aggregation.Boolean,
) aggregation.Boolean {
	combined.Count += source.Count
	combined.TotalFalse += source.TotalFalse
	combined.TotalTrue += source.TotalTrue
	return combined
}

func (sc ShardCombiner) finalizeBoolean(combined aggregation.Boolean) aggregation.Boolean {
	combined.PercentageFalse = float64(combined.TotalFalse) / float64(combined.Count)
	combined.PercentageTrue = float64(combined.TotalTrue) / float64(combined.Count)
	return combined
}

func (sc ShardCombiner) mergeTextProp(combined,
	source aggregation.Text,
) aggregation.Text {
	combined.Count += source.Count

	for _, textOcc := range source.Items {
		pos := getPosOfTextOcc(combined.Items, textOcc.Value)
		if pos < 0 {
			combined.Items = append(combined.Items, textOcc)
		} else {
			combined.Items[pos].Occurs += textOcc.Occurs
		}
	}

	return combined
}

func (sc ShardCombiner) finalizeText(combined aggregation.Text) aggregation.Text {
	sort.Slice(combined.Items, func(a, b int) bool {
		return combined.Items[a].Occurs > combined.Items[b].Occurs
	})
	return combined
}

func getPosOfTextOcc(haystack []aggregation.TextOccurrence, needle string) int {
	for i, elem := range haystack {
		if elem.Value == needle {
			return i
		}
	}

	return -1
}

func (sc ShardCombiner) finalizeGroup(group *aggregation.Group) {
	for propName, prop := range group.Properties {
		switch prop.Type {
		case aggregation.PropertyTypeNumerical:
			prop.NumericalAggregations = sc.finalizeNumerical(prop.NumericalAggregations)
		case aggregation.PropertyTypeBoolean:
			prop.BooleanAggregation = sc.finalizeBoolean(prop.BooleanAggregation)
		case aggregation.PropertyTypeText:
			prop.TextAggregation = sc.finalizeText(prop.TextAggregation)
		case aggregation.PropertyTypeDate:
			sc.finalizeDateProp(prop.DateAggregations)

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
