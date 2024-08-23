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
	mistral "github.com/weaviate/weaviate/modules/generative-mistral/parameters"
	octoai "github.com/weaviate/weaviate/modules/generative-octoai/parameters"
	ollama "github.com/weaviate/weaviate/modules/generative-ollama/parameters"
	openai "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	palm "github.com/weaviate/weaviate/modules/generative-palm/parameters"
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
					"generative-anthropic": anthropic.Params{},
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
					"generative-anthropic": anthropic.Params{
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
					"generative-anyscale": anyscale.Params{},
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
					"generative-anyscale": anyscale.Params{
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
					"generative-aws": aws.Params{},
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
					"generative-aws": aws.Params{
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
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
					"generative-cohere": cohere.Params{},
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
					"generative-cohere": cohere.Params{
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
					"generative-mistral": mistral.Params{},
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
					"generative-mistral": mistral.Params{
						MaxTokens:   makeIntPtr(10),
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
						TopP:        makeFloat64Ptr(0.5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic octoai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Octoai{},
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
			name:       "generative search; single response; empty dynamic octoai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Octoai{
								Octoai: &pb.GenerativeOctoAI{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"generative-octoai": octoai.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic octoai",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Octoai{
								Octoai: &pb.GenerativeOctoAI{
									MaxTokens:   makeInt64Ptr(10),
									Model:       makeStrPtr("model"),
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
					"generative-octoai": octoai.Params{
						MaxTokens:   makeIntPtr(10),
						Model:       "model",
						Temperature: makeFloat64Ptr(0.5),
						N:           makeIntPtr(5),
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
					"generative-ollama": ollama.Params{},
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
					"generative-ollama": ollama.Params{
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
					"generative-openai": openai.Params{},
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
									MaxTokens:        makeInt64Ptr(10),
									Model:            "model",
									Temperature:      makeFloat64Ptr(0.5),
									N:                makeInt64Ptr(5),
									TopP:             makeFloat64Ptr(0.5),
									FrequencyPenalty: makeFloat64Ptr(0.5),
									PresencePenalty:  makeFloat64Ptr(0.5),
									Stop: &pb.TextArray{
										Values: []string{"stop"},
									},
									LogProbs:    makeBoolPtr(true),
									TopLogProbs: makeInt64Ptr(5),
								},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"generative-openai": openai.Params{
						MaxTokens:        makeIntPtr(10),
						Model:            "model",
						Temperature:      makeFloat64Ptr(0.5),
						N:                makeIntPtr(5),
						TopP:             makeFloat64Ptr(0.5),
						FrequencyPenalty: makeFloat64Ptr(0.5),
						PresencePenalty:  makeFloat64Ptr(0.5),
						Stop:             []string{"stop"},
						Logprobs:         makeBoolPtr(true),
						TopLogprobs:      makeIntPtr(5),
					},
				},
			},
		},
		{
			name:       "generative search; single response; nil dynamic palm",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Palm{},
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
			name:       "generative search; single response; empty dynamic palm",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Palm{
								Palm: &pb.GenerativePaLM{},
							},
						},
					},
				},
			},
			expected: &generate.Params{
				Prompt: makeStrPtr("prompt"),
				Options: map[string]any{
					"generative-palm": palm.Params{},
				},
			},
		},
		{
			name:       "generative search; single response; full dynamic palm",
			uses127Api: true,
			in: &pb.GenerativeSearch{
				Single: &pb.GenerativeSearch_Single{
					Prompt: "prompt",
					Queries: []*pb.GenerativeProvider{
						{
							Kind: &pb.GenerativeProvider_Palm{
								Palm: &pb.GenerativePaLM{
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
					"generative-palm": palm.Params{
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.uses127Api)
			extracted := parser.Extract(test.in, class)
			require.Equal(t, test.expected, extracted)
		})
	}
}
