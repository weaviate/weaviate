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

// Package common_filters provides the filters for the graphql endpoint for Weaviate
package common_filters

import (
	"fmt"
	"strconv"

	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
)

func newValueTextType(path string) graphql.Input {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        fmt.Sprintf("Text%v", path),
		Description: "String or String[]",
		Serialize: func(value interface{}) interface{} {
			return graphql.String.Serialize(value)
		},
		ParseValue: func(value interface{}) interface{} {
			return graphql.String.ParseValue(value)
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.StringValue:
				return valueAST.Value
			case *ast.ListValue:
				result := make([]string, len(valueAST.Values))
				for i := range valueAST.Values {
					result[i] = valueAST.Values[i].GetValue().(string)
				}
				return result
			}
			return nil
		},
	})
}

func newValueStringType(path string) graphql.Input {
	return newValueTextType(fmt.Sprintf("String%v", path))
}

func newValueDateType(path string) graphql.Input {
	return newValueTextType(fmt.Sprintf("Date%v", path))
}

func newValueIntType(path string) graphql.Input {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        fmt.Sprintf("Int%v", path),
		Description: "Int or Int[]",
		Serialize: func(value interface{}) interface{} {
			return graphql.Int.Serialize(value)
		},
		ParseValue: func(value interface{}) interface{} {
			return graphql.Int.ParseValue(value)
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.IntValue:
				if intValue, err := strconv.Atoi(valueAST.Value); err == nil {
					return intValue
				}
			case *ast.ListValue:
				result := make([]int, len(valueAST.Values))
				for i := range valueAST.Values {
					if intValue, err := strconv.Atoi(valueAST.Values[i].GetValue().(string)); err == nil {
						result[i] = int(intValue)
					}
				}
				return result
			}
			return nil
		},
	})
}

func newValueNumberType(path string) graphql.Input {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        fmt.Sprintf("Float%v", path),
		Description: "Float or Float[]",
		Serialize: func(value interface{}) interface{} {
			return graphql.Float.Serialize(value)
		},
		ParseValue: func(value interface{}) interface{} {
			return graphql.Float.ParseValue(value)
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.FloatValue:
				if floatValue, err := strconv.ParseFloat(valueAST.Value, 64); err == nil {
					return floatValue
				}
			case *ast.IntValue:
				if floatValue, err := strconv.ParseFloat(valueAST.Value, 64); err == nil {
					return floatValue
				}
			case *ast.ListValue:
				result := make([]float64, len(valueAST.Values))
				for i := range valueAST.Values {
					if floatValue, err := strconv.ParseFloat(valueAST.Values[i].GetValue().(string), 64); err == nil {
						result[i] = floatValue
					}
				}
				return result
			}
			return nil
		},
	})
}

func newValueBooleanType(path string) graphql.Input {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        fmt.Sprintf("Boolean%v", path),
		Description: "Boolean or Boolean[]",
		Serialize: func(value interface{}) interface{} {
			return graphql.Boolean.Serialize(value)
		},
		ParseValue: func(value interface{}) interface{} {
			return graphql.Boolean.ParseValue(value)
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.BooleanValue:
				return valueAST.Value
			case *ast.ListValue:
				result := make([]bool, len(valueAST.Values))
				for i, val := range valueAST.Values {
					switch v := val.(type) {
					case *ast.BooleanValue:
						result[i] = v.Value
					}
				}
				return result
			}
			return nil
		},
	})
}
