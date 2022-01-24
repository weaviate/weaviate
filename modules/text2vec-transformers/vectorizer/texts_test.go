//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
		name                    string
		input                   []string
		expectedClientCall      string
		expectedPoolingStrategy string
		poolingStrategy         string
	}

	tests := []testCase{
		testCase{
			name:                    "single word",
			input:                   []string{"hello"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "hello",
		},
		testCase{
			name:                    "multiple words",
			input:                   []string{"hello world, this is me!"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "hello world, this is me!",
		},

		testCase{
			name:                    "multiple sentences (joined with a dot)",
			input:                   []string{"this is sentence 1", "and here's number 2"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "this is sentence 1. and here's number 2",
		},

		testCase{
			name:                    "multiple sentences already containing a dot",
			input:                   []string{"this is sentence 1.", "and here's number 2"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "this is sentence 1. and here's number 2",
		},
		testCase{
			name:                    "multiple sentences already containing a question mark",
			input:                   []string{"this is sentence 1?", "and here's number 2"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "this is sentence 1? and here's number 2",
		},
		testCase{
			name:                    "multiple sentences already containing an exclamation mark",
			input:                   []string{"this is sentence 1!", "and here's number 2"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "this is sentence 1! and here's number 2",
		},
		testCase{
			name:                    "multiple sentences already containing comma",
			input:                   []string{"this is sentence 1,", "and here's number 2"},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "this is sentence 1, and here's number 2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			settings := &fakeSettings{
				poolingStrategy: test.poolingStrategy,
			}
			vec, err := v.Texts(context.Background(), test.input, settings)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, vec)
			assert.Equal(t, test.expectedClientCall, client.lastInput)
			assert.Equal(t, client.lastConfig.PoolingStrategy, test.expectedPoolingStrategy)
		})
	}
}
