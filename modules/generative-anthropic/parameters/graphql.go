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
		Description: fmt.Sprintf("%s settings", name),
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name: fmt.Sprintf("%s%sInputObject", prefix, name),
			Fields: graphql.InputObjectConfigFieldMap{
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
				"topK": &graphql.InputObjectFieldConfig{
					Description: "topK",
					Type:        graphql.Int,
				},
				"topP": &graphql.InputObjectFieldConfig{
					Description: "topP",
					Type:        graphql.Float,
				},
				"stopSequences": &graphql.InputObjectFieldConfig{
					Description: "stopSequences",
					Type:        graphql.NewList(graphql.String),
				},
			},
		}),
		DefaultValue: nil,
	}
}

func output(prefix string) *graphql.Field {
	return &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%sFields", prefix, name),
		Fields: graphql.Fields{
			"type": &graphql.Field{Type: graphql.String},
			"error": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sErrorFields", prefix, name),
				Fields: graphql.Fields{
					"type":    &graphql.Field{Type: graphql.String},
					"message": &graphql.Field{Type: graphql.String},
				},
			})},
			"id":   &graphql.Field{Type: graphql.String},
			"role": &graphql.Field{Type: graphql.String},
			"content": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sContentFields", prefix, name),
				Fields: graphql.Fields{
					"type": &graphql.Field{Type: graphql.String},
					"text": &graphql.Field{Type: graphql.String},
				},
			}))},
			"model":         &graphql.Field{Type: graphql.String},
			"stop_reason":   &graphql.Field{Type: graphql.String},
			"stop_sequence": &graphql.Field{Type: graphql.String},
			"usage": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sUsageFields", prefix, name),
				Fields: graphql.Fields{
					"input_tokens":  &graphql.Field{Type: graphql.Int},
					"output_tokens": &graphql.Field{Type: graphql.Int},
				},
			})},
		},
	})}
}
