package aggregator

import (
	"sort"

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
						shardGroup.Properties[propName].NumericalAggregations["_rounds"] = 1
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
	pos int, shardGroup aggregation.Group) {
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
				combinedProp.NumericalAggregations = map[string]float64{}
			}
			combinedProp.NumericalAggregations = sc.mergeNumericalProp(
				combinedProp.NumericalAggregations, prop.NumericalAggregations)
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

func (sc ShardCombiner) mergeNumericalProp(combined, source map[string]float64) map[string]float64 {
	if len(source) == 0 {
		return combined
	}

	for propType, value := range source {
		switch propType {
		case "mean":
			combined["mean"] = combined["mean"] + value
		case "mode":
			combined["mode"] = combined["mode"] + value
		case "median":
			combined["median"] = combined["median"] + value
		case "count":
			combined["count"] = combined["count"] + value
		case "minimum":
			if _, ok := combined["minimum"]; !ok || value < combined["minimum"] {
				combined["minimum"] = value
			}
		case "maximum":
			if value > combined["maximum"] {
				combined["maximum"] = value
			}
		case "sum":
			combined["sum"] = combined["sum"] + value

		default:
			panic("unkwnon prop type: " + propType)
		}
	}

	combined["_rounds"]++

	return combined
}

func (sc ShardCombiner) finalizeNumerical(combined map[string]float64) map[string]float64 {
	if _, ok := combined["mean"]; ok {
		combined["mean"] = combined["mean"] / combined["_rounds"]
	}
	if _, ok := combined["mode"]; ok {
		combined["mode"] = combined["mode"] / combined["_rounds"]
	}
	if _, ok := combined["median"]; ok {
		combined["median"] = combined["median"] / combined["_rounds"]
	}
	delete(combined, "_rounds")
	return combined
}

func (sc ShardCombiner) mergeBooleanProp(combined,
	source aggregation.Boolean) aggregation.Boolean {
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
	source aggregation.Text) aggregation.Text {
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
