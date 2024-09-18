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
	"fmt"

	"github.com/tailor-inc/graphql"
)

func input(prefix string) *graphql.InputObjectFieldConfig {
	return &graphql.InputObjectFieldConfig{
		Description: fmt.Sprintf("%s settings", Name),
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name: fmt.Sprintf("%s%sInputObject", prefix, Name),
			Fields: graphql.InputObjectConfigFieldMap{
				"baseURL": &graphql.InputObjectFieldConfig{
					Description: "baseURL",
					Type:        graphql.String,
				},
				"model": &graphql.InputObjectFieldConfig{
					Description: "model",
					Type:        graphql.String,
				},
				"temperature": &graphql.InputObjectFieldConfig{
					Description: "temperature",
					Type:        graphql.Float,
				},
				"maxTokens": &graphql.InputObjectFieldConfig{
					Description: "maxTokens",
					Type:        graphql.Int,
				},
				"k": &graphql.InputObjectFieldConfig{
					Description: "n",
					Type:        graphql.Int,
				},
				"p": &graphql.InputObjectFieldConfig{
					Description: "n",
					Type:        graphql.Float,
				},
				"stopSequences": &graphql.InputObjectFieldConfig{
					Description: "stopSequences",
					Type:        graphql.NewList(graphql.String),
				},
				"frequencyPenalty": &graphql.InputObjectFieldConfig{
					Description: "frequencyPenalty",
					Type:        graphql.Float,
				},
				"presencePenalty": &graphql.InputObjectFieldConfig{
					Description: "presencePenalty",
					Type:        graphql.Float,
				},
			},
		}),
		DefaultValue: nil,
	}
}

func output(prefix string) *graphql.Field {
	return &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%sFields", prefix, Name),
		Fields: graphql.Fields{
			"meta": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sUsageMetadataFields", prefix, Name),
				Fields: graphql.Fields{
					"api_version": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
						Name: fmt.Sprintf("%s%sApiVersionFields", prefix, Name),
						Fields: graphql.Fields{
							"version":         &graphql.Field{Type: graphql.String},
							"is_deprecated":   &graphql.Field{Type: graphql.Boolean},
							"is_experimental": &graphql.Field{Type: graphql.Boolean},
						},
					})},
					"billed_units": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
						Name: fmt.Sprintf("%s%sBilledUnitsFields", prefix, Name),
						Fields: graphql.Fields{
							"input_tokens":    &graphql.Field{Type: graphql.Float},
							"output_tokens":   &graphql.Field{Type: graphql.Float},
							"search_units":    &graphql.Field{Type: graphql.Float},
							"classifications": &graphql.Field{Type: graphql.Float},
						},
					})},
					"tokens": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
						Name: fmt.Sprintf("%s%sTokensFields", prefix, Name),
						Fields: graphql.Fields{
							"input_tokens":  &graphql.Field{Type: graphql.Float},
							"output_tokens": &graphql.Field{Type: graphql.Float},
						},
					})},
					"warnings": &graphql.Field{Type: graphql.NewList(graphql.String)},
				},
			})},
		},
	})}
}
