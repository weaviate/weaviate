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
	"fmt"

	"github.com/tailor-platform/graphql"
)

func input(prefix string) *graphql.InputObjectFieldConfig {
	return &graphql.InputObjectFieldConfig{
		Description: "Meta generative parameters",
		Type: graphql.NewInputObject(graphql.InputObjectConfig{
			Name: fmt.Sprintf("%s%sInputObject", prefix, Name),
			Fields: graphql.InputObjectConfigFieldMap{
				"baseURL":          {Description: "Custom API URL", Type: graphql.String},
				"model":            {Description: "Meta model", Type: graphql.String},
				"temperature":      {Description: "Sampling temperature", Type: graphql.Float},
				"topP":             {Description: "Top P", Type: graphql.Float},
				"maxTokens":        {Description: "Max tokens", Type: graphql.Int},
				"frequencyPenalty": {Description: "Frequency penalty", Type: graphql.Float},
				"presencePenalty":  {Description: "Presence penalty", Type: graphql.Float},
				"reasoningEffort": {
					Description: "Reasoning effort",
					Type: graphql.NewEnum(graphql.EnumConfig{
						Name: fmt.Sprintf("%s%sReasoningEffort", prefix, Name),
						Values: graphql.EnumValueConfigMap{
							"none":    &graphql.EnumValueConfig{Value: "none"},
							"minimal": &graphql.EnumValueConfig{Value: "minimal"},
							"low":     &graphql.EnumValueConfig{Value: "low"},
							"medium":  &graphql.EnumValueConfig{Value: "medium"},
							"high":    &graphql.EnumValueConfig{Value: "high"},
							"xhigh":   &graphql.EnumValueConfig{Value: "xhigh"},
						},
					}),
				},
				"images":          {Description: "Base64 encoded images", Type: graphql.NewList(graphql.String)},
				"imageProperties": {Description: "Properties which contain base64 encoded images", Type: graphql.NewList(graphql.String)},
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
