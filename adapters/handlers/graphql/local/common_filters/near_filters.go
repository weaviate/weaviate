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
	"errors"
	"fmt"
	"strconv"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
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
	ParseLiteral: vectorPerTargetParseLiteral,
})

func vectorPerTargetParseLiteral(valueAST ast.Value) interface{} {
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
						areMultiVectors, err := valuesAreMultiVectors(value.Values)
						if err != nil {
							return nil
						}
						if areMultiVectors {
							r, err := getListOfMultiVectors(value.Values)
							if err != nil {
								return nil
							}
							result[key] = r
						} else {
							r, err := getListOfNormalVectors(value.Values)
							if err != nil {
								return nil
							}
							result[key] = r
						}
					default:
						normalVector, err := getNormalVector(value.Values)
						if err != nil {
							return nil
						}
						result[key] = normalVector
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
}

func getNormalVector(values []ast.Value) ([]float32, error) {
	normalVector := make([]float32, len(values))
	for i, value := range values {
		vStr, ok := value.GetValue().(string)
		if !ok {
			return nil, fmt.Errorf("value is not a string: %T", value)
		}
		floatValue, err := strconv.ParseFloat(vStr, 64)
		if err != nil {
			return nil, err
		}
		normalVector[i] = float32(floatValue)
	}
	return normalVector, nil
}

func getListOfNormalVectors(values []ast.Value) ([][]float32, error) {
	normalVectors := make([][]float32, len(values))
	for i, value := range values {
		vector, ok := value.(*ast.ListValue)
		if !ok {
			return nil, fmt.Errorf("value is not a list: %T", value)
		}
		v, err := getNormalVector(vector.Values)
		if err != nil {
			return nil, err
		}
		normalVectors[i] = v
	}
	return normalVectors, nil
}

func getListOfMultiVectors(values []ast.Value) ([][][]float32, error) {
	multiVectors := make([][][]float32, len(values))
	for i, value := range values {
		multiVector, ok := value.(*ast.ListValue)
		if !ok {
			return nil, fmt.Errorf("value is not a multivector list: %T", value)
		}
		mv, err := getListOfNormalVectors(multiVector.Values)
		if err != nil {
			return nil, err
		}
		multiVectors[i] = mv
	}
	return multiVectors, nil
}

func valuesAreMultiVectors(values []ast.Value) (bool, error) {
	if len(values) == 0 {
		return false, errors.New(("values are empty"))
	}
	firstValue := values[0]
	switch firstValue.(type) {
	case *ast.ListValue:
		switch firstValue.(type) {
		case *ast.ListValue:
			vList := firstValue.(*ast.ListValue)
			if len(vList.Values) == 0 {
				return false, errors.New("empty list")
			}
			vv := vList.Values[0]
			if _, vvIsList := vv.(*ast.ListValue); vvIsList {
				return true, nil
			}
			if _, vvIsFloat := vv.(*ast.FloatValue); vvIsFloat {
				return false, nil
			}
			if _, vvIsInt := vv.(*ast.IntValue); vvIsInt {
				return false, nil
			}
			return false, fmt.Errorf("unknown type: %T", vv)
		}
	default:
		return false, errors.New("not list value")
	}
	return false, errors.New("failed to determine")
}
