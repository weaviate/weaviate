//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common_filters

import (
	"fmt"
	"strconv"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/dto"
)

func AddTargetArgument(fieldMap graphql.InputObjectConfigFieldMap, prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	if !addTarget { // not supported by aggregate and explore
		return fieldMap
	}
	fieldMap["targets"] = &graphql.InputObjectFieldConfig{
		Description: "Subsearch list",
		Type:        targetFields(prefix),
	}
	return fieldMap
}

var targetCombinationEnum = graphql.NewEnum(graphql.EnumConfig{
	Name: "targetCombinationEnum",
	Values: graphql.EnumValueConfigMap{
		"minimum":       &graphql.EnumValueConfig{Value: dto.Minimum},
		"average":       &graphql.EnumValueConfig{Value: dto.Average},
		"sum":           &graphql.EnumValueConfig{Value: dto.Sum},
		"manualWeights": &graphql.EnumValueConfig{Value: dto.ManualWeights},
		"relativeScore": &graphql.EnumValueConfig{Value: dto.RelativeScore},
	},
})

var WeightsScalar = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "Weights",
	Description: "A custom scalar type for a map with strings as keys and floats as values",
	Serialize: func(value interface{}) interface{} {
		return value
	},
	ParseValue: func(value interface{}) interface{} {
		return value
	},
	ParseLiteral: func(valueAST ast.Value) interface{} {
		switch v := valueAST.(type) {
		case *ast.ObjectValue:
			result := make(map[string]interface{})
			for _, field := range v.Fields {
				key := field.Name.Value
				switch value := field.Value.(type) {
				case *ast.FloatValue:
					floatValue, err := strconv.ParseFloat(value.Value, 64)
					if err != nil {
						return nil
					}
					result[key] = floatValue
				case *ast.IntValue:
					intValue, err := strconv.ParseFloat(value.Value, 64)
					if err != nil {
						return nil
					}
					result[key] = intValue
				case *ast.ListValue:
					var list []float64
					for _, item := range value.Values {
						switch item := item.(type) {
						case *ast.FloatValue:
							floatValue, err := strconv.ParseFloat(item.Value, 64)
							if err != nil {
								return nil
							}
							list = append(list, floatValue)
						case *ast.IntValue:
							intValue, err := strconv.ParseFloat(item.Value, 64)
							if err != nil {
								return nil
							}
							list = append(list, intValue)
						default:
							return nil
						}
					}
					result[key] = list
				default:
					return nil
				}
			}
			return result
		default:
			return nil
		}
	},
})

func targetFields(prefix string) *graphql.InputObject {
	return graphql.NewInputObject(
		graphql.InputObjectConfig{
			Name: fmt.Sprintf("%sTargetInpObj", prefix),
			Fields: graphql.InputObjectConfigFieldMap{
				"targetVectors": &graphql.InputObjectFieldConfig{
					Description: "Target vectors",
					Type:        graphql.NewList(graphql.String),
				},
				"combinationMethod": &graphql.InputObjectFieldConfig{
					Description: "Combination method",
					Type:        targetCombinationEnum,
				},
				"weights": &graphql.InputObjectFieldConfig{
					Description: "Weights for target vectors",
					Type:        WeightsScalar,
				},
			},
		},
	)
}
