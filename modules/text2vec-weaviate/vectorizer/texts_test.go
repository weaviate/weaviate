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
	"github.com/weaviate/weaviate/modules/text2vec-weaviate/ent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// as used in the nearText searcher
func TestVectorizingTexts(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name                       string
		input                      []string
		expectedWeaviateEmbedModel string
		weaviateEmbedModel         string
	}

	tests := []testCase{
		{
			name:                       "single word",
			input:                      []string{"hello"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple words",
			input:                      []string{"hello world, this is me!"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple sentences (joined with a dot)",
			input:                      []string{"this is sentence 1", "and here's number 2"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple sentences already containing a dot",
			input:                      []string{"this is sentence 1.", "and here's number 2"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple sentences already containing a question mark",
			input:                      []string{"this is sentence 1?", "and here's number 2"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple sentences already containing an exclamation mark",
			input:                      []string{"this is sentence 1!", "and here's number 2"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
		{
			name:                       "multiple sentences already containing comma",
			input:                      []string{"this is sentence 1,", "and here's number 2"},
			weaviateEmbedModel:         "Snowflake/snowflake-arctic-embed-m-v1.5",
			expectedWeaviateEmbedModel: "Snowflake/snowflake-arctic-embed-m-v1.5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client, logger)

			settings := &fakeClassConfig{
				weaviateEmbedModel: test.weaviateEmbedModel,
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, test.input, client.lastInput)
			conf := ent.NewClassSettings(client.lastConfig)

			assert.Equal(t, test.expectedWeaviateEmbedModel, conf.Model())
		})
	}
}
