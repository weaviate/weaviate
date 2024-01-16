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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// as used in the nearText searcher
func TestVectorizingTexts(t *testing.T) {
	type testCase struct {
		name               string
		input              []string
		expectedClientCall string
		expectedService    string
		expectedRegion     string
		expectedModel      string
	}

	tests := []testCase{
		{
			name:               "single word",
			input:              []string{"hello"},
			expectedClientCall: "hello",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple words",
			input:              []string{"hello world, this is me!"},
			expectedClientCall: "hello world, this is me!",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple sentences (joined with a dot)",
			input:              []string{"this is sentence 1", "and here's number 2"},
			expectedClientCall: "and here's number 2",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple sentences already containing a dot",
			input:              []string{"this is sentence 1.", "and here's number 2"},
			expectedClientCall: "and here's number 2",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple sentences already containing a question mark",
			input:              []string{"this is sentence 1?", "and here's number 2"},
			expectedClientCall: "and here's number 2",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple sentences already containing an exclamation mark",
			input:              []string{"this is sentence 1!", "and here's number 2"},
			expectedClientCall: "and here's number 2",
			expectedService:    "bedrock",
		},
		{
			name:               "multiple sentences already containing comma",
			input:              []string{"this is sentence 1,", "and here's number 2"},
			expectedClientCall: "and here's number 2",
			expectedService:    "bedrock",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			settings := &fakeClassConfig{
				service: "bedrock",
				region:  "",
				model:   "",
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0.1, 1.1, 2.1, 3.1}, vec)
			assert.Equal(t, test.expectedClientCall, strings.Join(client.lastInput, ","))
			assert.Equal(t, test.expectedService, client.lastConfig.Service)
			assert.Equal(t, test.expectedRegion, client.lastConfig.Region)
			assert.Equal(t, test.expectedModel, client.lastConfig.Model)
		})
	}
}
