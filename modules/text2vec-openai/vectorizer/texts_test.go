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
		name                 string
		input                []string
		expectedOpenAIType   string
		openAIType           string
		expectedOpenAIModel  string
		openAIModel          string
		modelVersion         string
		expectedModelVersion string
	}

	tests := []testCase{
		{
			name:                "single word",
			input:               []string{"hello"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple words",
			input:               []string{"hello world, this is me!"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple sentences (joined with a dot)",
			input:               []string{"this is sentence 1", "and here's number 2"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple sentences already containing a dot",
			input:               []string{"this is sentence 1.", "and here's number 2"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple sentences already containing a question mark",
			input:               []string{"this is sentence 1?", "and here's number 2"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple sentences already containing an exclamation mark",
			input:               []string{"this is sentence 1!", "and here's number 2"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
		{
			name:                "multiple sentences already containing comma",
			input:               []string{"this is sentence 1,", "and here's number 2"},
			openAIType:          "text",
			expectedOpenAIType:  "text",
			openAIModel:         "ada",
			expectedOpenAIModel: "ada",

			// use something that doesn't exist on purpose to rule out that this was
			// set by a default, but validate that the version was set explicitly
			// due to https://github.com/weaviate/weaviate/issues/2458
			modelVersion:         "003",
			expectedModelVersion: "003",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			cfg := &fakeClassConfig{
				classConfig: map[string]interface{}{
					"type":         test.openAIType,
					"model":        test.openAIModel,
					"modelVersion": test.modelVersion,
				},
			}
			vec, err := v.Texts(context.Background(), test.input, cfg)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, test.input, client.lastInput)
			assert.Equal(t, client.lastConfig.Type, test.expectedOpenAIType)
			assert.Equal(t, client.lastConfig.Model, test.expectedOpenAIModel)
			assert.Equal(t, client.lastConfig.ModelVersion, test.expectedModelVersion)
		})
	}
}
