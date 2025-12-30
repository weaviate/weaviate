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
					Description: "Custom API Base URL",
					Type:        graphql.String,
				},
				"model": &graphql.InputObjectFieldConfig{
					Description: "DeepSeek model to use (e.g. deepseek-chat)",
					Type:        graphql.String,
				},
				"temperature": &graphql.InputObjectFieldConfig{
					Description: "Sampling temperature",
					Type:        graphql.Float,
				},
				"maxTokens": &graphql.InputObjectFieldConfig{
					Description: "Maximum number of tokens to generate",
					Type:        graphql.Int,
				},
				"frequencyPenalty": &graphql.InputObjectFieldConfig{
					Description: "Penalty for frequent tokens",
					Type:        graphql.Float,
				},
				"presencePenalty": &graphql.InputObjectFieldConfig{
					Description: "Penalty for new tokens",
					Type:        graphql.Float,
				},
				"topP": &graphql.InputObjectFieldConfig{
					Description: "Nucleus sampling probability",
					Type:        graphql.Float,
				},
				"stop": &graphql.InputObjectFieldConfig{
					Description: "Stop sequences",
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
			"usage": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%s%sUsageMetadataFields", prefix, Name),
				Fields: graphql.Fields{
					"prompt_tokens":     &graphql.Field{Type: graphql.Int},
					"completion_tokens": &graphql.Field{Type: graphql.Int},
					"total_tokens":      &graphql.Field{Type: graphql.Int},
				},
			})},
		},
	})}
}
