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

package common_filters

import (
	"fmt"

	"github.com/tailor-platform/graphql"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	DefaultPreferStrength   = float32(0.5)
	DefaultConditionWeight  = float32(1.0)
	DefaultDecayCurve       = "exp"
	DefaultDecayValue       = float32(0.5)
	DefaultDecayOffset      = "0"
)

// ExtractPrefer extracts prefer parameters from GraphQL args.
func ExtractPrefer(source map[string]interface{}, rootClass string) (*filters.Prefer, error) {
	if source == nil {
		return nil, nil
	}

	prefer := &filters.Prefer{
		Strength: DefaultPreferStrength,
	}

	if strengthVal, ok := source["strength"]; ok {
		v, ok := strengthVal.(float64)
		if !ok {
			return nil, fmt.Errorf("prefer: strength must be a number, got %T", strengthVal)
		}
		prefer.Strength = float32(v)
	}

	conditionsRaw, ok := source["conditions"]
	if !ok {
		return nil, fmt.Errorf("prefer: conditions field is required")
	}

	conditionsList, ok := conditionsRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("prefer: conditions must be a list")
	}

	conditions, err := extractPreferConditions(conditionsList, rootClass)
	if err != nil {
		return nil, err
	}
	prefer.Conditions = conditions

	return prefer, nil
}

func extractPreferConditions(conditionsList []interface{}, rootClass string) ([]filters.PreferCondition, error) {
	conditions := make([]filters.PreferCondition, 0, len(conditionsList))

	for i, condRaw := range conditionsList {
		condMap, ok := condRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("prefer condition[%d]: expected object", i)
		}

		cond := filters.PreferCondition{
			Weight: DefaultConditionWeight,
		}

		if weightVal, ok := condMap["weight"]; ok {
			v, ok := weightVal.(float64)
			if !ok {
				return nil, fmt.Errorf("prefer condition[%d]: weight must be a number, got %T", i, weightVal)
			}
			cond.Weight = float32(v)
		}

		filterData, hasFilter := condMap["filter"]
		decayData, hasDecay := condMap["decay"]

		if hasFilter && hasDecay {
			return nil, fmt.Errorf("prefer condition[%d]: exactly one of 'filter' or 'decay' must be set, both are set", i)
		}
		if !hasFilter && !hasDecay {
			return nil, fmt.Errorf("prefer condition[%d]: exactly one of 'filter' or 'decay' must be set", i)
		}

		if hasFilter {
			filterMap, ok := filterData.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("prefer condition[%d]: filter must be an object", i)
			}

			// Wrap the filter data so ExtractFilters can find it under "where"
			wrappedArgs := map[string]interface{}{
				"where": filterMap,
			}
			localFilter, err := ExtractFilters(wrappedArgs, rootClass)
			if err != nil {
				return nil, fmt.Errorf("prefer condition[%d] filter: %w", i, err)
			}
			cond.Filter = localFilter
		}

		if hasDecay {
			decayMap, ok := decayData.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("prefer condition[%d]: decay must be an object", i)
			}
			decay, err := extractDecay(decayMap, i)
			if err != nil {
				return nil, err
			}
			cond.Decay = decay
		}

		conditions = append(conditions, cond)
	}

	return conditions, nil
}

func extractDecay(source map[string]interface{}, condIdx int) (*filters.Decay, error) {
	decay := &filters.Decay{
		Curve:      DefaultDecayCurve,
		DecayValue: DefaultDecayValue,
		Offset:     DefaultDecayOffset,
	}

	// path is required
	pathRaw, ok := source["path"]
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: path is required", condIdx)
	}
	pathList, ok := pathRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: path must be a list of strings", condIdx)
	}
	if len(pathList) == 0 {
		return nil, fmt.Errorf("prefer condition[%d] decay: path must not be empty", condIdx)
	}
	propName, ok := pathList[0].(string)
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: path elements must be strings, got %T", condIdx, pathList[0])
	}
	decay.Path = &filters.Path{
		Property: schema.PropertyName(propName),
	}

	// origin is required
	originRaw, ok := source["origin"]
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: origin is required", condIdx)
	}
	originStr, ok := originRaw.(string)
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: origin must be a string, got %T", condIdx, originRaw)
	}
	decay.Origin = originStr

	// scale is required
	scaleRaw, ok := source["scale"]
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: scale is required", condIdx)
	}
	scaleStr, ok := scaleRaw.(string)
	if !ok {
		return nil, fmt.Errorf("prefer condition[%d] decay: scale must be a string, got %T", condIdx, scaleRaw)
	}
	decay.Scale = scaleStr

	// optional overrides
	if curveRaw, ok := source["curve"]; ok {
		curveStr, ok := curveRaw.(string)
		if !ok {
			return nil, fmt.Errorf("prefer condition[%d] decay: curve must be a string, got %T", condIdx, curveRaw)
		}
		decay.Curve = curveStr
	}

	if decayValRaw, ok := source["decayValue"]; ok {
		v, ok := decayValRaw.(float64)
		if !ok {
			return nil, fmt.Errorf("prefer condition[%d] decay: decayValue must be a number, got %T", condIdx, decayValRaw)
		}
		decay.DecayValue = float32(v)
	}

	if offsetRaw, ok := source["offset"]; ok {
		offsetStr, ok := offsetRaw.(string)
		if !ok {
			return nil, fmt.Errorf("prefer condition[%d] decay: offset must be a string, got %T", condIdx, offsetRaw)
		}
		decay.Offset = offsetStr
	}

	return decay, nil
}

// PreferArgument returns the GraphQL argument config for the prefer parameter.
func PreferArgument(prefix string) *graphql.ArgumentConfig {
	return &graphql.ArgumentConfig{
		Description: "Prefer soft-ranking conditions to boost matching documents",
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:        fmt.Sprintf("%sPreferInpObj", prefix),
				Fields:      preferFields(prefix),
				Description: "Soft preference conditions for boosting results",
			},
		),
	}
}

func preferFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"conditions": &graphql.InputObjectFieldConfig{
			Description: "List of preference conditions",
			Type: graphql.NewNonNull(graphql.NewList(graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sPreferConditionInpObj", prefix),
					Fields:      preferConditionFields(prefix),
					Description: "A single preference condition (exactly one of filter or decay must be set)",
				},
			))),
		},
		"strength": &graphql.InputObjectFieldConfig{
			Description: "Blending weight [0,1] for combining primary search and prefer scores (default 0.5)",
			Type:        graphql.Float,
		},
	}
}

func preferConditionFields(prefix string) graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"filter": &graphql.InputObjectFieldConfig{
			Description: "Binary filter condition (same format as where filter)",
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sPreferConditionFilterInpObj", prefix),
					Fields:      BuildNew(fmt.Sprintf("%sPreferConditionFilter", prefix)),
					Description: "Filter condition using the same format as where",
				},
			),
		},
		"decay": &graphql.InputObjectFieldConfig{
			Description: "Distance-based decay scoring function",
			Type: graphql.NewInputObject(
				graphql.InputObjectConfig{
					Name:        fmt.Sprintf("%sPreferConditionDecayInpObj", prefix),
					Fields:      preferDecayFields(),
					Description: "Decay function for continuous scoring based on distance from origin",
				},
			),
		},
		"weight": &graphql.InputObjectFieldConfig{
			Description: "Per-condition weight (default 1.0)",
			Type:        graphql.Float,
		},
	}
}

func preferDecayFields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"path": &graphql.InputObjectFieldConfig{
			Description: "Property path for the decay function",
			Type:        graphql.NewNonNull(graphql.NewList(graphql.String)),
		},
		"origin": &graphql.InputObjectFieldConfig{
			Description: "Origin point for distance calculation (e.g. \"now\", ISO date, or numeric string)",
			Type:        graphql.NewNonNull(graphql.String),
		},
		"scale": &graphql.InputObjectFieldConfig{
			Description: "Distance from origin at which the score equals decayValue (e.g. \"7d\", \"100\")",
			Type:        graphql.NewNonNull(graphql.String),
		},
		"offset": &graphql.InputObjectFieldConfig{
			Description: "Documents within this distance from origin get full score (default \"0\")",
			Type:        graphql.String,
		},
		"curve": &graphql.InputObjectFieldConfig{
			Description: "Decay curve type: \"exp\" (default), \"gauss\", or \"linear\"",
			Type:        graphql.String,
		},
		"decayValue": &graphql.InputObjectFieldConfig{
			Description: "Score at scale distance from origin (default 0.5)",
			Type:        graphql.Float,
		},
	}
}
