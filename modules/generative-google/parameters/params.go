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
	ApiEndpoint      string
	ProjectID        string
	EndpointID       string
	Region           string
	Model            string
	Temperature      *float64
	MaxTokens        *int
	TopP             *float64
	TopK             *int
	StopSequences    []string
	PresencePenalty  *float64
	FrequencyPenalty *float64
}

func extract(field *ast.ObjectField) interface{} {
	out := Params{}
	fields, ok := field.Value.GetValue().([]*ast.ObjectField)
	if ok {
		for _, f := range fields {
			switch f.Name.Value {
			case "apiEndpoint":
				out.ApiEndpoint = gqlparser.GetValueAsStringOrEmpty(f)
			case "projectId":
				out.ProjectID = gqlparser.GetValueAsStringOrEmpty(f)
			case "endpointId":
				out.EndpointID = gqlparser.GetValueAsStringOrEmpty(f)
			case "region":
				out.Region = gqlparser.GetValueAsStringOrEmpty(f)
			case "model":
				out.Model = gqlparser.GetValueAsStringOrEmpty(f)
			case "temperature":
				out.Temperature = gqlparser.GetValueAsFloat64(f)
			case "maxTokens":
				out.MaxTokens = gqlparser.GetValueAsInt(f)
			case "topP":
				out.TopP = gqlparser.GetValueAsFloat64(f)
			case "topK":
				out.TopK = gqlparser.GetValueAsInt(f)
			case "stopSequences":
				out.StopSequences = gqlparser.GetValueAsStringArray(f)
			case "presencePenalty":
				out.PresencePenalty = gqlparser.GetValueAsFloat64(f)
			case "frequencyPenalty":
				out.FrequencyPenalty = gqlparser.GetValueAsFloat64(f)
			default:
				// do nothing
			}
		}
	}
	return out
}
