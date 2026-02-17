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
		Description: "DeepSeek generative parameters",
		Type:        graphql.NewInputObject(inputConfig(prefix)),
	}
}

func inputConfig(prefix string) graphql.InputObjectConfig {
	return graphql.InputObjectConfig{
		Name:   fmt.Sprintf("%s%sInput", prefix, Name),
		Fields: inputFields(),
	}
}

func inputFields() graphql.InputObjectConfigFieldMap {
	return graphql.InputObjectConfigFieldMap{
		"baseURL":          {Description: "Custom API URL", Type: graphql.String},
		"model":            {Description: "DeepSeek Model", Type: graphql.String},
		"temperature":      {Description: "Sampling temperature", Type: graphql.Float},
		"maxTokens":        {Description: "Max tokens", Type: graphql.Int},
		"frequencyPenalty": {Description: "Freq penalty", Type: graphql.Float},
		"presencePenalty":  {Description: "Presence penalty", Type: graphql.Float},
		"topP":             {Description: "Top P", Type: graphql.Float},
		"stop":             {Description: "Stop sequences", Type: graphql.NewList(graphql.String)},
	}
}

func output(prefix string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(outputObject(prefix)),
	}
}

func outputObject(prefix string) graphql.ObjectConfig {
	return graphql.ObjectConfig{
		Name: fmt.Sprintf("%s%sFields", prefix, Name),
		Fields: graphql.Fields{
			"usage": usageField(prefix),
		},
	}
}

func usageField(prefix string) *graphql.Field {
	return &graphql.Field{
		Type: graphql.NewObject(graphql.ObjectConfig{
			Name: fmt.Sprintf("%s%sUsageMetadata", prefix, Name),
			Fields: graphql.Fields{
				"prompt_tokens":     &graphql.Field{Type: graphql.Int},
				"completion_tokens": &graphql.Field{Type: graphql.Int},
				"total_tokens":      &graphql.Field{Type: graphql.Int},
			},
		}),
	}
}
