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
	"github.com/weaviate/weaviate/modules/text2vec-wcs/ent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// as used in the nearText searcher
func TestVectorizingTexts(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name                  string
		input                 []string
		expectedWcsEmbedModel string
		wcsEmbedModel         string
	}

	tests := []testCase{
		{
			name:                  "single word",
			input:                 []string{"hello"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple words",
			input:                 []string{"hello world, this is me!"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple sentences (joined with a dot)",
			input:                 []string{"this is sentence 1", "and here's number 2"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple sentences already containing a dot",
			input:                 []string{"this is sentence 1.", "and here's number 2"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple sentences already containing a question mark",
			input:                 []string{"this is sentence 1?", "and here's number 2"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple sentences already containing an exclamation mark",
			input:                 []string{"this is sentence 1!", "and here's number 2"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
		{
			name:                  "multiple sentences already containing comma",
			input:                 []string{"this is sentence 1,", "and here's number 2"},
			wcsEmbedModel:         "mixedbread-ai/mxbai-embed-large-v1",
			expectedWcsEmbedModel: "mixedbread-ai/mxbai-embed-large-v1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client, logger)

			settings := &fakeClassConfig{
				wcsEmbedModel: test.wcsEmbedModel,
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, test.input, client.lastInput)
			conf := ent.NewClassSettings(client.lastConfig)

			assert.Equal(t, test.expectedWcsEmbedModel, conf.Model())
		})
	}
}
