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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/modules/text2vec-voyageai/ent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// as used in the nearText searcher
func TestVectorizingTexts(t *testing.T) {
	type testCase struct {
		name                  string
		input                 []string
		expectedVoyageAIModel string
		voyageaiModel         string
	}
	logger, _ := test.NewNullLogger()

	tests := []testCase{
		{
			name:                  "single word",
			input:                 []string{"hello"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple words",
			input:                 []string{"hello world, this is me!"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple sentences (joined with a dot)",
			input:                 []string{"this is sentence 1", "and here's number 2"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple sentences already containing a dot",
			input:                 []string{"this is sentence 1.", "and here's number 2"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple sentences already containing a question mark",
			input:                 []string{"this is sentence 1?", "and here's number 2"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple sentences already containing an exclamation mark",
			input:                 []string{"this is sentence 1!", "and here's number 2"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
		{
			name:                  "multiple sentences already containing comma",
			input:                 []string{"this is sentence 1,", "and here's number 2"},
			voyageaiModel:         "large",
			expectedVoyageAIModel: "large",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client, logger)

			settings := &fakeClassConfig{
				voyageaiModel: test.voyageaiModel,
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, test.input, client.lastInput)
			config := ent.NewClassSettings(client.lastConfig)
			assert.Equal(t, test.expectedVoyageAIModel, config.Model())
		})
	}
}
