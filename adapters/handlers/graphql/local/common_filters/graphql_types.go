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
)

var Vector func(prefix string) *graphql.Scalar = func(prefix string) *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name:        fmt.Sprintf("%sNearVectorVectorScalar", prefix),
		Description: "A type that can be either a regular or colbert embedding",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case []float32, [][]float32:
				return v
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			return nil // do nothing, this type is meant to only serialize vectors
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.ListValue:
				var vector []float32
				var multiVector [][]float32
				for i := range valueAST.Values {
					switch val := valueAST.Values[i].(type) {
					case *ast.ListValue:
						vec := make([]float32, len(val.Values))
						for j := range val.Values {
							switch v := val.Values[j].(type) {
							case *ast.FloatValue:
								if floatValue, err := strconv.ParseFloat(v.Value, 64); err == nil {
									vec[j] = float32(floatValue)
								}
							case *ast.IntValue:
								if floatValue, err := strconv.ParseFloat(v.Value, 64); err == nil {
									vec[j] = float32(floatValue)
								}
							}
						}
						multiVector = append(multiVector, vec)
					case *ast.FloatValue:
						if floatValue, err := strconv.ParseFloat(val.Value, 64); err == nil {
							vector = append(vector, float32(floatValue))
						}
					case *ast.IntValue:
						if floatValue, err := strconv.ParseFloat(val.Value, 64); err == nil {
							vector = append(vector, float32(floatValue))
						}
					}
				}
				if len(multiVector) > 0 {
					return multiVector
				}
				return vector
			default:
				return nil
			}
		},
	})
}
