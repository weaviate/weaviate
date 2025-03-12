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
	BaseURL          string
	ApiVersion       string
	ResourceName     string
	DeploymentID     string
	IsAzure          bool
	Model            string
	FrequencyPenalty *float64
	MaxTokens        *int
	N                *int
	PresencePenalty  *float64
	Stop             []string
	Temperature      *float64
	TopP             *float64
	Images           []*string
	ImageProperties  []string
}

func extract(field *ast.ObjectField) interface{} {
	out := Params{}
	fields, ok := field.Value.GetValue().([]*ast.ObjectField)
	if ok {
		for _, f := range fields {
			switch f.Name.Value {
			case "baseURL":
				out.BaseURL = gqlparser.GetValueAsStringOrEmpty(f)
			case "apiVersion":
				out.ApiVersion = gqlparser.GetValueAsStringOrEmpty(f)
			case "resourceName":
				out.ResourceName = gqlparser.GetValueAsStringOrEmpty(f)
			case "deploymentId":
				out.DeploymentID = gqlparser.GetValueAsStringOrEmpty(f)
			case "isAzure":
				out.IsAzure = gqlparser.GetValueAsBoolOrFalse(f)
			case "model":
				out.Model = gqlparser.GetValueAsStringOrEmpty(f)
			case "frequencyPenalty":
				out.FrequencyPenalty = gqlparser.GetValueAsFloat64(f)
			case "maxTokens":
				out.MaxTokens = gqlparser.GetValueAsInt(f)
			case "n":
				out.N = gqlparser.GetValueAsInt(f)
			case "presencePenalty":
				out.PresencePenalty = gqlparser.GetValueAsFloat64(f)
			case "stop":
				out.Stop = gqlparser.GetValueAsStringArray(f)
			case "temperature":
				out.Temperature = gqlparser.GetValueAsFloat64(f)
			case "topP":
				out.TopP = gqlparser.GetValueAsFloat64(f)
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
