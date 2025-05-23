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

package generative

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	anthropic "github.com/weaviate/weaviate/modules/generative-anthropic/parameters"
	anyscale "github.com/weaviate/weaviate/modules/generative-anyscale/parameters"
	aws "github.com/weaviate/weaviate/modules/generative-aws/parameters"
	cohere "github.com/weaviate/weaviate/modules/generative-cohere/parameters"
	databricks "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
	friendliai "github.com/weaviate/weaviate/modules/generative-friendliai/parameters"
	google "github.com/weaviate/weaviate/modules/generative-google/parameters"
	mistral "github.com/weaviate/weaviate/modules/generative-mistral/parameters"
	nvidia "github.com/weaviate/weaviate/modules/generative-nvidia/parameters"
	ollama "github.com/weaviate/weaviate/modules/generative-ollama/parameters"
	openai "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	xai "github.com/weaviate/weaviate/modules/generative-xai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
)

func makeStrPtr(s string) *string {
	return &s
}

func makeInt64Ptr(i int) *int64 {
	v := int64(i)
	return &v
}

func makeIntPtr(i int) *int {
	return &i
}

func makeFloat64Ptr(f float64) *float64 {
	return &f
}

func makeBoolPtr(b bool) *bool {
	return &b
}

func getPropNames(props []*models.Property) []string {
	names := make([]string, len(props))
	for i, prop := range props {
		names[i] = prop.Name
	}
	return names
}

func Test_RequestParser(t *testing.T) {
	class := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{
				Name:     "prop",
				DataType: []string{"text"},
			},
		},
	}
	tests := []struct {
		name       string
		uses127Api bool
		in         *pb.GenerativeSearch
		expected   *generate.Params
	}{
		{
			name:       "empty request; old",
			uses127Api: false,
		},
		{
			name:       "empty request; new",
			uses127Api: true,
		},
		{
			name:       "generative search without props; old",
			uses127Api: false,
			in: &pb.GenerativeSearch{
				SingleResponsePrompt: "prompt",
				GroupedResponseTask:  "task",
			},
			expected: &generate.Params{
				Prompt:              makeStrPtr("prompt"),
				Task:                makeStrPtr("task"),
				PropertiesToExtract: getPropNames(class.Properties),
			},
		},
		{
			name:       "generative search with props; old",
			uses127Api: false,
			in: &pb.GenerativeSearch{
				SingleResponsePrompt: "prompt",
				GroupedResponseTask:  "task",
				GroupedProperties:    getPropNames(class.Properties),
			},
			expected: &generate.Params{
				Prompt:              makeStrPtr("prompt"),
				Task:                makeStrPtr("task"),
				Properties:          getPropNames(class.Properties),
				PropertiesToExtract: getPropNames(class.Properties),
			},
		},
		{
			name:       "generative search without props; new non-dynamic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
				},
				Grouped: &pb.GenerativeSearch_Grouped{
					Task: "task",
				},
			},
			expected: &generate.Params{
				Prompt:              makeStrPtr("prompt"),
				Task:                makeStrPtr("task"),
				PropertiesToExtract: getPropNames(class.Properties),
			},
		},
		{
			name:       "generative search with props; new non-dynamic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
				},
				Grouped: &pb.GenerativeSearch_Grouped{
					Task: "task",
					Properties: &pb.TextArray{
						Values: getPropNames(class.Properties),
					},
				},
			},
			expected: &generate.Params{
				Prompt:              makeStrPtr("prompt"),
				Task:                makeStrPtr("task"),
				Properties:          getPropNames(class.Properties),
				PropertiesToExtract: getPropNames(class.Properties),
			},
		},
		{
			name:       "generative search; single response; nil modelValue dynamic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: nil,
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; nil dynamic anthropic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anthropic{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic anthropic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anthropic{
								Anthropic: &pb.GenerativeAnthropic{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"anthropic": anthropic.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic anthropic",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anthropic{
								Anthropic: &pb.GenerativeAnthropic{
									BaseUrl:     makeStrPtr("url"),
									MaxTokens:   makeInt64Ptr(10),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
									TopK:        makeInt64Ptr(5),
									TopP:        makeFloat64Ptr(0.5),
									StopSequences: &pb.TextArray{
										Values: []string{"stop"},
									},
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"anthropic": anthropic.Params{
						BaseURL:       "url",
						MaxTokens:     makeIntPtr(10),
						Model:         "model",
						Temperature:   makeFloat64Ptr(0.5),
						TopK:          makeIntPtr(5),
						TopP:          makeFloat64Ptr(0.5),
						StopSequences: []string{"stop"},
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic anyscale",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anyscale{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic anyscale",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anyscale{
								Anyscale: &pb.GenerativeAnyscale{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"anyscale": anyscale.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic anyscale",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Anyscale{
								Anyscale: &pb.GenerativeAnyscale{
									BaseUrl:     makeStrPtr("url"),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"anyscale": anyscale.Params{
						BaseURL:     "url",
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic aws",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Aws{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic aws",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Aws{
								Aws: &pb.GenerativeAWS{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"aws": aws.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic aws",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Aws{
								Aws: &pb.GenerativeAWS{
									Service:       makeStrPtr("service"),
									Region:        makeStrPtr("region"),
									Endpoint:      makeStrPtr("endpoint"),
									TargetModel:   makeStrPtr("targetModel"),
									TargetVariant: makeStrPtr("targetVariant"),
									Model:         makeStrPtr("model"),
									Temperature:   makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"aws": aws.Params{
						Service:       "service",
						Region:        "region",
						Endpoint:      "endpoint",
						TargetModel:   "targetModel",
						TargetVariant: "targetVariant",
						Model:         "model",
						Temperature:   makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic cohere",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Cohere{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic cohere",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Cohere{
								Cohere: &pb.GenerativeCohere{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"cohere": cohere.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic cohere",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Cohere{
								Cohere: &pb.GenerativeCohere{
									BaseUrl:          makeStrPtr("url"),
									MaxTokens:        makeInt64Ptr(10),
									Model:            makeStrPtr("model"),
									Temperature:      makeFloat64Ptr(0.5),
									K:                makeInt64Ptr(5),
									P:                makeFloat64Ptr(0.5),
									FrequencyPenalty: makeFloat64Ptr(0.5),
									PresencePenalty:  makeFloat64Ptr(0.5),
									StopSequences: &pb.TextArray{
										Values: []string{"stop"},
									},
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"cohere": cohere.Params{
						BaseURL:          "url",
						MaxTokens:        makeIntPtr(10),
						Model:            "model",
						Temperature:      makeFloat64Ptr(0.5),
						K:                makeIntPtr(5),
						P:                makeFloat64Ptr(0.5),
						FrequencyPenalty: makeFloat64Ptr(0.5),
						PresencePenalty:  makeFloat64Ptr(0.5),
						StopSequences:    []string{"stop"},
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic mistral",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Mistral{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic mistral",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Mistral{
								Mistral: &pb.GenerativeMistral{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"mistral": mistral.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic mistral",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Mistral{
								Mistral: &pb.GenerativeMistral{
									BaseUrl:     makeStrPtr("url"),
									MaxTokens:   makeInt64Ptr(10),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
									TopP:        makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"mistral": mistral.Params{
						BaseURL:     "url",
						MaxTokens:   makeIntPtr(10),
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
						TopP:        makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic ollama",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Ollama{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic ollama",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Ollama{
								Ollama: &pb.GenerativeOllama{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"ollama": ollama.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic ollama",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Ollama{
								Ollama: &pb.GenerativeOllama{
									ApiEndpoint: makeStrPtr("url"),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"ollama": ollama.Params{
						ApiEndpoint: "url",
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic openai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Openai{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic openai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Openai{
								Openai: &pb.GenerativeOpenAI{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"openai": openai.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic openai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Openai{
								Openai: &pb.GenerativeOpenAI{
									BaseUrl:          makeStrPtr("baseURL"),
									ApiVersion:       makeStrPtr("apiVersion"),
									ResourceName:     makeStrPtr("resourceName"),
									DeploymentId:     makeStrPtr("deploymentId"),
									IsAzure:          makeBoolPtr(true),
									MaxTokens:        makeInt64Ptr(10),
									Model:            makeStrPtr("model"),
									Temperature:      makeFloat64Ptr(0.5),
									N:                makeInt64Ptr(5),
									TopP:             makeFloat64Ptr(0.5),
									FrequencyPenalty: makeFloat64Ptr(0.5),
									PresencePenalty:  makeFloat64Ptr(0.5),
									Stop: &pb.TextArray{
										Values: []string{"stop"},
									},
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"openai": openai.Params{
						BaseURL:          "baseURL",
						ApiVersion:       "apiVersion",
						ResourceName:     "resourceName",
						DeploymentID:     "deploymentId",
						IsAzure:          true,
						MaxTokens:        makeIntPtr(10),
						Model:            "model",
						Temperature:      makeFloat64Ptr(0.5),
						N:                makeIntPtr(5),
						TopP:             makeFloat64Ptr(0.5),
						FrequencyPenalty: makeFloat64Ptr(0.5),
						PresencePenalty:  makeFloat64Ptr(0.5),
						Stop:             []string{"stop"},
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic google",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Google{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic google",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Google{
								Google: &pb.GenerativeGoogle{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"google": google.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic google",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Google{
								Google: &pb.GenerativeGoogle{
									MaxTokens:        makeInt64Ptr(10),
									Model:            makeStrPtr("model"),
									Temperature:      makeFloat64Ptr(0.5),
									TopK:             makeInt64Ptr(5),
									TopP:             makeFloat64Ptr(0.5),
									FrequencyPenalty: makeFloat64Ptr(0.5),
									PresencePenalty:  makeFloat64Ptr(0.5),
									StopSequences: &pb.TextArray{
										Values: []string{"stop"},
									},
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"google": google.Params{
						MaxTokens:        makeIntPtr(10),
						Model:            "model",
						Temperature:      makeFloat64Ptr(0.5),
						TopK:             makeIntPtr(5),
						TopP:             makeFloat64Ptr(0.5),
						FrequencyPenalty: makeFloat64Ptr(0.5),
						PresencePenalty:  makeFloat64Ptr(0.5),
						StopSequences:    []string{"stop"},
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic databricks",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Databricks{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic databricks",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Databricks{
								Databricks: &pb.GenerativeDatabricks{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"databricks": databricks.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic databricks",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Databricks{
								Databricks: &pb.GenerativeDatabricks{
									Endpoint:         makeStrPtr("endpoint"),
									Model:            makeStrPtr("model"),
									FrequencyPenalty: makeFloat64Ptr(0.5),
									LogProbs:         makeBoolPtr(true),
									TopLogProbs:      makeInt64Ptr(1),
									MaxTokens:        makeInt64Ptr(10),
									N:                makeInt64Ptr(5),
									PresencePenalty:  makeFloat64Ptr(0.5),
									Stop: &pb.TextArray{
										Values: []string{"stop"},
									},
									Temperature: makeFloat64Ptr(0.5),
									TopP:        makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"databricks": databricks.Params{
						Endpoint:         "endpoint",
						Model:            "model",
						FrequencyPenalty: makeFloat64Ptr(0.5),
						Logprobs:         makeBoolPtr(true),
						TopLogprobs:      makeIntPtr(1),
						MaxTokens:        makeIntPtr(10),
						N:                makeIntPtr(5),
						PresencePenalty:  makeFloat64Ptr(0.5),
						Stop:             []string{"stop"},
						Temperature:      makeFloat64Ptr(0.5),
						TopP:             makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic friendli",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Friendliai{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic friendli",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Friendliai{
								Friendliai: &pb.GenerativeFriendliAI{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"friendliai": friendliai.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic friendli",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Friendliai{
								Friendliai: &pb.GenerativeFriendliAI{
									BaseUrl:     makeStrPtr("baseURL"),
									Model:       makeStrPtr("model"),
									MaxTokens:   makeInt64Ptr(10),
									Temperature: makeFloat64Ptr(0.5),
									N:           makeInt64Ptr(5),
									TopP:        makeFloat64Ptr(0.5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"friendliai": friendliai.Params{
						BaseURL:     "baseURL",
						Model:       "model",
						MaxTokens:   makeIntPtr(10),
						N:           makeIntPtr(5),
						Temperature: makeFloat64Ptr(0.5),
						TopP:        makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic nvidia",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Nvidia{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic nvidia",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Nvidia{
								Nvidia: &pb.GenerativeNvidia{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"nvidia": nvidia.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic nvidia",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Nvidia{
								Nvidia: &pb.GenerativeNvidia{
									BaseUrl:     makeStrPtr("baseURL"),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
									TopP:        makeFloat64Ptr(0.5),
									MaxTokens:   makeInt64Ptr(10),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"nvidia": nvidia.Params{
						BaseURL:     "baseURL",
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
						TopP:        makeFloat64Ptr(0.5),
						MaxTokens:   makeIntPtr(10),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic xai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Xai{},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt:  makeStrPtr("prompt"),
				Options: nil,
			},
		},
		{
			name:       "generative search; single response; empty dynamic xai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Xai{
								Xai: &pb.GenerativeXAI{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"xai": xai.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic xai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Xai{
								Xai: &pb.GenerativeXAI{
									BaseUrl:     makeStrPtr("baseURL"),
									Model:       makeStrPtr("model"),
									Temperature: makeFloat64Ptr(0.5),
									TopP:        makeFloat64Ptr(0.5),
									MaxTokens:   makeInt64Ptr(10),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"xai": xai.Params{
						BaseURL:     "baseURL",
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
						TopP:        makeFloat64Ptr(0.5),
						MaxTokens:   makeIntPtr(10),
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.uses127Api)
			extracted := parser.Extract(test.in, class)
			require.Equal(t, test.expected, extracted)
		})
	}
}
