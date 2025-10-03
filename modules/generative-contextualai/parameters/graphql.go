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

func requestParamsFunction(classname string) *graphql.InputObjectFieldConfig {
	return &graphql.InputObjectFieldConfig{
		Description: fmt.Sprintf("Settings for %s module", Name),
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: fmt.Sprintf("%sParams", classname),
				Fields: graphql.InputObjectConfigFieldMap{
					"model": &graphql.InputObjectFieldConfig{
						Description: "The version of Contextual's GLM to use. Currently supports 'v1' and 'v2'.",
						Type:        graphql.String,
					},
					"temperature": &graphql.InputObjectFieldConfig{
						Description: "The sampling temperature, which affects the randomness in the response. Note that higher temperature values can reduce groundedness.",
						Type:        graphql.Float,
					},
					"topP": &graphql.InputObjectFieldConfig{
						Description: "A parameter for nucleus sampling, an alternative to temperature which also affects the randomness of the response. Note that higher top_p values can reduce groundedness.",
						Type:        graphql.Float,
					},
					"maxNewTokens": &graphql.InputObjectFieldConfig{
						Description: "The maximum number of tokens that the model can generate in the response.",
						Type:        graphql.Int,
					},
					"systemPrompt": &graphql.InputObjectFieldConfig{
						Description: "Instructions that the model follows when generating responses. Note that we do not guarantee that the model follows these instructions exactly.",
						Type:        graphql.String,
					},
					"avoidCommentary": &graphql.InputObjectFieldConfig{
						Description: "Flag to indicate whether the model should avoid providing additional commentary in responses.",
						Type:        graphql.Boolean,
					},
					"knowledge": &graphql.InputObjectFieldConfig{
						Description: "The knowledge sources the model can use when generating a response.",
						Type:        graphql.NewList(graphql.String),
					},
				},
			},
		),
	}
}

func responseParamsFunction(classname string) *graphql.Field {
	return &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
		Name: fmt.Sprintf("%sUsage", classname),
		Fields: graphql.Fields{
			"usage": &graphql.Field{Type: graphql.NewObject(graphql.ObjectConfig{
				Name: fmt.Sprintf("%sUsageMetadata", classname),
				Fields: graphql.Fields{
					"inputTokens":  &graphql.Field{Type: graphql.Int},
					"outputTokens": &graphql.Field{Type: graphql.Int},
					"totalTokens":  &graphql.Field{Type: graphql.Int},
				},
			})},
		},
	})}
}
