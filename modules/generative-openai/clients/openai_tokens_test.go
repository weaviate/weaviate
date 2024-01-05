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

package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getTokensCount(t *testing.T) {
	prompt := `
	Summarize the following in a tweet:

	As generative language models such as GPT-4 continue to push the boundaries of what AI can do,
	the excitement surrounding its potential is spreading quickly. Many applications and projects are
	built on top of GPT-4 to extend its capabilities and features. Additionally, many tools were created
	in order to interact with large language models, like LangChain as an example. Auto-GPT is one of the fastest
	rising open-source python projects harnessing the power of GPT-4!
	`
	messages := []message{
		{Role: "user", Content: prompt},
	}
	// Example messages from: https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
	// added for sanity check that getTokensCount method computes tokens accordingly to above examples provided by OpenAI
	exampleMessages := []message{
		{
			Role:    "system",
			Content: "You are a helpful, pattern-following assistant that translates corporate jargon into plain English.",
		},
		{
			Role:    "system",
			Name:    "example_user",
			Content: "New synergies will help drive top-line growth.",
		},
		{
			Role:    "system",
			Name:    "example_assistant",
			Content: "Things working well together will increase revenue.",
		},
		{
			Role:    "system",
			Name:    "example_user",
			Content: "Let's circle back when we have more bandwidth to touch base on opportunities for increased leverage.",
		},
		{
			Role:    "system",
			Name:    "example_assistant",
			Content: "Let's talk later when we're less busy about how to do better.",
		},
		{
			Role:    "user",
			Content: "This late pivot means we don't have time to boil the ocean for the client deliverable.",
		},
	}
	tests := []struct {
		name     string
		model    string
		messages []message
		want     int
		wantErr  string
	}{
		{
			name:     "text-davinci-002",
			model:    "text-davinci-002",
			messages: messages,
			want:     128,
		},
		{
			name:     "text-davinci-003",
			model:    "text-davinci-003",
			messages: messages,
			want:     128,
		},
		{
			name:     "gpt-3.5-turbo",
			model:    "gpt-3.5-turbo",
			messages: messages,
			want:     122,
		},
		{
			name:     "gpt-4",
			model:    "gpt-4",
			messages: messages,
			want:     121,
		},
		{
			name:     "gpt-4-32k",
			model:    "gpt-4-32k",
			messages: messages,
			want:     121,
		},
		{
			name:     "non-existent-model",
			model:    "non-existent-model",
			messages: messages,
			wantErr:  "encoding for model non-existent-model: no encoding for model non-existent-model",
		},
		{
			name:     "OpenAI cookbook example - gpt-3.5-turbo-0301",
			model:    "gpt-3.5-turbo-0301",
			messages: exampleMessages,
			want:     127,
		},
		{
			name:     "OpenAI cookbook example - gpt-4",
			model:    "gpt-4",
			messages: exampleMessages,
			want:     129,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getTokensCount(tt.model, tt.messages)
			if err != nil {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
