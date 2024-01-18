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
		name               string
		input              []string
		expectedClientCall string
	}

	tests := []testCase{
		{
			name:               "single word",
			input:              []string{"hello"},
			expectedClientCall: "hello",
		},
		{
			name:               "multiple words",
			input:              []string{"hello world, this is me!"},
			expectedClientCall: "hello world, this is me!",
		},

		{
			name:               "multiple sentences",
			input:              []string{"this is sentence 1", "and here's number 2"},
			expectedClientCall: "and here's number 2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			settings := &fakeClassConfig{}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, vec)
			assert.Equal(t, test.expectedClientCall, client.lastInput)
		})
	}
}
