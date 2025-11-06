//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package parameters

import (
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/usecases/modulecomponents/gqlparser"
)

const Name = "contextualai"

type Params struct {
	Model           string
	Temperature     *float64
	TopP            *float64
	MaxNewTokens    *int
	SystemPrompt    string
	AvoidCommentary *bool
	Knowledge       []string
}

func extract(field *ast.ObjectField) any {
	out := Params{}
	fields, ok := field.Value.GetValue().([]*ast.ObjectField)
	if ok {
		for _, f := range fields {
			switch f.Name.Value {
			case "model":
				out.Model = gqlparser.GetValueAsStringOrEmpty(f)
			case "temperature":
				out.Temperature = gqlparser.GetValueAsFloat64(f)
			case "topP":
				out.TopP = gqlparser.GetValueAsFloat64(f)
			case "maxNewTokens":
				out.MaxNewTokens = gqlparser.GetValueAsInt(f)
			case "systemPrompt":
				out.SystemPrompt = gqlparser.GetValueAsStringOrEmpty(f)
			case "avoidCommentary":
				out.AvoidCommentary = gqlparser.GetValueAsBool(f)
			case "knowledge":
				out.Knowledge = gqlparser.GetValueAsStringArray(f)
			default:
				// do nothing
			}
		}
	}
	return out
}
