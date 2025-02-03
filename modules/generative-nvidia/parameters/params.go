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

package parameters

import (
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/usecases/modulecomponents/gqlparser"
)

type Params struct {
	BaseURL     string
	Model       string
	MaxTokens   *int
	Temperature *float64
	N           *int
	TopP        *float64
}

func extract(field *ast.ObjectField) interface{} {
	out := Params{}
	fields, ok := field.Value.GetValue().([]*ast.ObjectField)
	if ok {
		for _, f := range fields {
			switch f.Name.Value {
			case "model":
				out.Model = gqlparser.GetValueAsStringOrEmpty(f)
			case "maxTokens":
				out.MaxTokens = gqlparser.GetValueAsInt(f)
			case "temperature":
				out.Temperature = gqlparser.GetValueAsFloat64(f)
			case "n":
				out.N = gqlparser.GetValueAsInt(f)
			case "topP":
				out.TopP = gqlparser.GetValueAsFloat64(f)
			default:
				// do nothing
			}
		}
	}
	return out
}
