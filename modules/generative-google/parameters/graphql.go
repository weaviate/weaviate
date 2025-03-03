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
				"apiEndpoint": &graphql.InputObjectFieldConfig{
					Description: "apiEndpoint",
					Type:        graphql.String,
				},
				"projectId": &graphql.InputObjectFieldConfig{
					Description: "projectId",
					Type:        graphql.String,
				},
				"endpointId": &graphql.InputObjectFieldConfig{
					Description: "endpointId",
					Type:        graphql.String,
				},
				"region": &graphql.InputObjectFieldConfig{
					Description: "region",
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
				"topP": &graphql.InputObjectFieldConfig{
					Description: "topP",
					Type:        graphql.Float,
				},
				"topK": &graphql.InputObjectFieldConfig{
					Description: "topK",
					Type:        graphql.Int,
				},
				"maxTokens": &graphql.InputObjectFieldConfig{
					Description: "maxTokens",
					Type:        graphql.Int,
				},
				"presencePenalty": &graphql.InputObjectFieldConfig{
					Description: "presencePenalty",
					Type:        graphql.Float,
				},
				"frequencyPenalty": &graphql.InputObjectFieldConfig{
					Description: "frequencyPenalty",
					Type:        graphql.Float,
				},
				"stopSequences": &graphql.InputObjectFieldConfig{
					Description: "stopSequences",
					Type:        graphql.NewList(graphql.String),
				},
				"images": &graphql.InputObjectFieldConfig{
					Description: "images",
					Type:        graphql.NewList(graphql.String),
				},
				"imageProperties": &graphql.InputObjectFieldConfig{
					Description: "imageProperties",
					Type:        graphql.NewList(graphql.String),
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
			"usageMetadata": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sUsageMetadataFields", prefix, Name),
				Fields: graphql.Fields{
					"promptTokenCount":     &graphql.Field{Type: graphql.Int},
					"candidatesTokenCount": &graphql.Field{Type: graphql.Int},
					"totalTokenCount":      &graphql.Field{Type: graphql.Int},
				},
			})},
			"metadata": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sMetadataFields", prefix, Name),
				Fields: graphql.Fields{
					"tokenMetadata": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
						Name: fmt.Sprintf("%s%sTokenMetadataFields", prefix, Name),
						Fields: graphql.Fields{
							"inputTokenCount": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
								Name: fmt.Sprintf("%s%sMetadataInputTokenCountFields", prefix, Name),
								Fields: graphql.Fields{
									"totalBillableCharacters": &graphql.Field{Type: graphql.Int},
									"totalTokens":             &graphql.Field{Type: graphql.Int},
								},
							})},
							"outputTokenCount": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
								Name: fmt.Sprintf("%s%sMetadataOutputTokenCountFields", prefix, Name),
								Fields: graphql.Fields{
									"totalBillableCharacters": &graphql.Field{Type: graphql.Int},
									"totalTokens":             &graphql.Field{Type: graphql.Int},
								},
							})},
						},
					})},
				},
			})},
		},
	})}
}
