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
		name                     string
		input                    []string
		expectedHuggingFaceModel string
		huggingFaceModel         string
		huggingFaceEndpointURL   string
	}

	tests := []testCase{
		{
			name:                     "single word",
			input:                    []string{"hello"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple words",
			input:                    []string{"hello world, this is me!"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple sentences (joined with a dot)",
			input:                    []string{"this is sentence 1", "and here's number 2"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple sentences already containing a dot",
			input:                    []string{"this is sentence 1.", "and here's number 2"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple sentences already containing a question mark",
			input:                    []string{"this is sentence 1?", "and here's number 2"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple sentences already containing an exclamation mark",
			input:                    []string{"this is sentence 1!", "and here's number 2"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "multiple sentences already containing comma",
			input:                    []string{"this is sentence 1,", "and here's number 2"},
			huggingFaceModel:         "sentence-transformers/gtr-t5-xl",
			expectedHuggingFaceModel: "sentence-transformers/gtr-t5-xl",
		},
		{
			name:                     "single word with inference url",
			input:                    []string{"hello"},
			huggingFaceEndpointURL:   "http://url.cloud",
			expectedHuggingFaceModel: "sentence-transformers/msmarco-bert-base-dot-v5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			settings := &fakeClassConfig{
				model:       test.huggingFaceModel,
				endpointURL: test.huggingFaceEndpointURL,
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, client.lastConfig.Model, test.expectedHuggingFaceModel)
		})
	}
}
