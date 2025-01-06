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

	"github.com/tailor-inc/graphql/language/ast"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
)

func NearVectorArgument(argumentPrefix, className string, addTarget bool) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("%s%s", argumentPrefix, className)
	return &graphql.ArgumentConfig{
		// Description: descriptions.GetExplore,
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearVectorInpObj", prefix),
				Fields: NearVectorFields(prefix, addTarget),
			},
		),
	}
}

func NearVectorFields(prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	fieldMap := graphql.InputObjectConfigFieldMap{
		"vector": &graphql.InputObjectFieldConfig{
			Description: descriptions.Vector,
			Type:        Vector(prefix),
		},
		"vectorPerTarget": &graphql.InputObjectFieldConfig{
			Description: "Vector per target",
			Type:        vectorPerTarget,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
	fieldMap = AddTargetArgument(fieldMap, prefix+"nearVector", addTarget)
	return fieldMap
}

func NearObjectArgument(argumentPrefix, className string, addTarget bool) *graphql.ArgumentConfig {
	prefix := fmt.Sprintf("%s%s", argumentPrefix, className)
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name:   fmt.Sprintf("%sNearObjectInpObj", prefix),
				Fields: nearObjectFields(prefix, addTarget),
			},
		),
	}
}

func nearObjectFields(prefix string, addTarget bool) graphql.InputObjectConfigFieldMap {
	fieldMap := graphql.InputObjectConfigFieldMap{
		"id": &graphql.InputObjectFieldConfig{
			Description: descriptions.ID,
			Type:        graphql.String,
		},
		"beacon": &graphql.InputObjectFieldConfig{
			Description: descriptions.Beacon,
			Type:        graphql.String,
		},
		"certainty": &graphql.InputObjectFieldConfig{
			Description: descriptions.Certainty,
			Type:        graphql.Float,
		},
		"distance": &graphql.InputObjectFieldConfig{
			Description: descriptions.Distance,
			Type:        graphql.Float,
		},
		"targetVectors": &graphql.InputObjectFieldConfig{
			Description: "Target vectors",
			Type:        graphql.NewList(graphql.String),
		},
	}
	fieldMap = AddTargetArgument(fieldMap, prefix+"nearObject", addTarget)
	return fieldMap
}

var vectorPerTarget = graphql.NewScalar(graphql.ScalarConfig{
	Name:        "VectorPerTarget",
	Description: "A custom scalar type for a map with strings as keys and list of floats or list of lists of floats as values",
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
				case *ast.ListValue:
					if len(value.Values) > 0 {
						switch value.Values[0].(type) {
						case *ast.ListValue:
							// Handle list of lists of floats
							listOfLists := make([][]float32, len(value.Values))
							for i, listValue := range value.Values {
								innerList := listValue.(*ast.ListValue)
								floatValues := make([]float32, len(innerList.Values))
								for j, innerValue := range innerList.Values {
									floatValue, err := strconv.ParseFloat(innerValue.GetValue().(string), 64)
									if err != nil {
										return nil
									}
									floatValues[j] = float32(floatValue)
								}
								listOfLists[i] = floatValues
							}
							result[key] = listOfLists
						default:
							// Handle list of floats
							floatValues := make([]float32, len(value.Values))
							for i, value := range value.Values {
								floatValue, err := strconv.ParseFloat(value.GetValue().(string), 64)
								if err != nil {
									return nil
								}
								floatValues[i] = float32(floatValue)
							}
							result[key] = floatValues
						}
					}
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
