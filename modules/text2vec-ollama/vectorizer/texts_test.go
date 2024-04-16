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

package vectorizer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// as used in the nearText searcher
func TestVectorizingTexts(t *testing.T) {
	type testCase struct {
		name                string
		input               []string
		expectedOllamaModel string
		ollamaModel         string
	}

	tests := []testCase{
		{
			name:                "single word",
			input:               []string{"hello"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple words",
			input:               []string{"hello world, this is me!"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple sentences (joined with a dot)",
			input:               []string{"this is sentence 1", "and here's number 2"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple sentences already containing a dot",
			input:               []string{"this is sentence 1.", "and here's number 2"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple sentences already containing a question mark",
			input:               []string{"this is sentence 1?", "and here's number 2"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple sentences already containing an exclamation mark",
			input:               []string{"this is sentence 1!", "and here's number 2"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
		{
			name:                "multiple sentences already containing comma",
			input:               []string{"this is sentence 1,", "and here's number 2"},
			ollamaModel:         "nomic-embed-text",
			expectedOllamaModel: "nomic-embed-text",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			cfg := &fakeClassConfig{
				apiEndpoint: "",
				modelID:     test.ollamaModel,
			}
			vec, err := v.Texts(context.Background(), test.input, cfg)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, vec)
			assert.Equal(t, test.input[len(test.input)-1], client.lastInput)
			assert.Equal(t, DefaultApiEndpoint, client.lastConfig.ApiEndpoint)
			assert.Equal(t, test.ollamaModel, client.lastConfig.Model)
		})
	}
}
