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

package batch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/tiktoken-go"
)

func Test_getTokensCount(t *testing.T) {
	shortTestText := "I am a short message. Teddy is the best and biggest dog ever."

	tests := []struct {
		name     string
		model    string
		messages string
		want     int
		wantErr  string
	}{
		{
			name:     "text-davinci-002",
			model:    "text-davinci-002",
			messages: shortTestText,
			want:     18,
		},
		{
			name:     "gpt-3.5-turbo",
			model:    "gpt-3.5-turbo",
			messages: shortTestText,
			want:     19,
		},
		{
			name:     "gpt-4",
			model:    "gpt-4",
			messages: shortTestText,
			want:     18,
		},
		{
			name:     "non-existent-model",
			model:    "non-existent-model",
			messages: shortTestText,
			wantErr:  "no encoding for model non-existent-model",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tke, err := tiktoken.EncodingForModel(tt.model)
			if err != nil {
				assert.EqualError(t, err, tt.wantErr)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.want, GetTokensCount(tt.model, tt.messages, tke))
			}
		})
	}
}

func Test_getTokensCount_nilTokenizer(t *testing.T) {
	// When the tiktoken vocabulary is unavailable (download blocked), the
	// tokenizer is nil and the count must fall back to a char-based estimate
	// instead of panicking.
	tests := []struct {
		name  string
		model string
		input string
		want  int
	}{
		{name: "empty input", model: "text-embedding-ada-002", input: "", want: 3},
		{name: "short input", model: "text-embedding-ada-002", input: "hello world", want: 3 + 3},
		{name: "gpt-3.5-turbo per-message offset", model: "gpt-3.5-turbo", input: "hello world", want: 4 + 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				assert.Equal(t, tt.want, GetTokensCount(tt.model, tt.input, nil))
			})
		})
	}
}
