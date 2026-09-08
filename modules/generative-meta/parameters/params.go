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

package parameters

import (
	"github.com/tailor-platform/graphql/language/ast"
	"github.com/weaviate/weaviate/usecases/modulecomponents/gqlparser"
)

type Params struct {
	BaseURL          string
	Model            string
	Temperature      *float64
	TopP             *float64
	MaxTokens        *int
	FrequencyPenalty *float64
	PresencePenalty  *float64
	ReasoningEffort  *string
	Images           []*string
	ImageProperties  []string
}

func extract(field *ast.ObjectField) any {
	out := Params{}
	fields, ok := field.Value.GetValue().([]*ast.ObjectField)
	if ok {
		for _, f := range fields {
			switch f.Name.Value {
			case "baseURL":
				out.BaseURL = gqlparser.GetValueAsStringOrEmpty(f)
			case "model":
				out.Model = gqlparser.GetValueAsStringOrEmpty(f)
			case "temperature":
				out.Temperature = gqlparser.GetValueAsFloat64(f)
			case "topP":
				out.TopP = gqlparser.GetValueAsFloat64(f)
			case "maxTokens":
				out.MaxTokens = gqlparser.GetValueAsInt(f)
			case "frequencyPenalty":
				out.FrequencyPenalty = gqlparser.GetValueAsFloat64(f)
			case "presencePenalty":
				out.PresencePenalty = gqlparser.GetValueAsFloat64(f)
			case "reasoningEffort":
				out.ReasoningEffort = gqlparser.GetValueAsString(f)
			case "images":
				out.Images = gqlparser.GetValueAsStringPtrArray(f)
			case "imageProperties":
				out.ImageProperties = gqlparser.GetValueAsStringArray(f)
			default:
				// do nothing
			}
		}
	}
	return out
}
