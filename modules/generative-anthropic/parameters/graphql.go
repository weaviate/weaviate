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
				"stopSequences": &graphql.InputObjectFieldConfig{
					Description: "stopSequences",
					Type:        graphql.NewList(graphql.String),
				},
				"topP": &graphql.InputObjectFieldConfig{
					Description: "topP",
					Type:        graphql.Float,
				},
				"topK": &graphql.InputObjectFieldConfig{
					Description: "topK",
					Type:        graphql.Int,
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
			"type": &graphql.Field{Type: graphql.String},
			"error": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sErrorFields", prefix, Name),
				Fields: graphql.Fields{
					"type":    &graphql.Field{Type: graphql.String},
					"message": &graphql.Field{Type: graphql.String},
				},
			})},
			"id":   &graphql.Field{Type: graphql.String},
			"role": &graphql.Field{Type: graphql.String},
			"content": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sContentFields", prefix, Name),
				Fields: graphql.Fields{
					"type": &graphql.Field{Type: graphql.String},
					"text": &graphql.Field{Type: graphql.String},
				},
			}))},
			"model":        &graphql.Field{Type: graphql.String},
			"stopReason":   &graphql.Field{Type: graphql.String},
			"stopSequence": &graphql.Field{Type: graphql.String},
			"usage": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sUsageFields", prefix, Name),
				Fields: graphql.Fields{
					"inputTokens":  &graphql.Field{Type: graphql.Int},
					"outputTokens": &graphql.Field{Type: graphql.Int},
				},
			})},
		},
	})}
}
