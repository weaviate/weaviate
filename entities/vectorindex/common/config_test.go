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

package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// optionalFromMapCase is the shared table-row shape for Test_Optional*FromMap:
// each case supplies an input map and asserts both the returned error and the
// value (or lack thereof) observed through the setFn callback.
type optionalFromMapCase[T any] struct {
	name      string
	input     map[string]interface{}
	expectErr bool
	expected  T
}

// runOptionalFromMapCases drives a table of optionalFromMapCase against fn,
// factoring out the identical setFn-capture/assert loop that Test_OptionalStringFromMap,
// Test_OptionalBoolFromMap, and Test_OptionalIntFromMap would otherwise each repeat.
func runOptionalFromMapCases[T any](
	t *testing.T,
	cases []optionalFromMapCase[T],
	unset T,
	fn func(input map[string]interface{}, setFn func(T)) error,
) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := unset
			err := fn(tt.input, func(v T) { got = v })

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, got)
		})
	}
}

func Test_OptionalStringFromMap(t *testing.T) {
	runOptionalFromMapCases(t, []optionalFromMapCase[string]{
		{
			name:      "key absent, setFn not called, no error",
			input:     map[string]interface{}{},
			expectErr: false,
			expected:  "unset",
		},
		{
			name:      "valid string",
			input:     map[string]interface{}{"distance": "cosine"},
			expectErr: false,
			expected:  "cosine",
		},
		{
			name:      "explicit JSON null must error, not silently skip",
			input:     map[string]interface{}{"distance": nil},
			expectErr: true,
			expected:  "unset",
		},
		{
			name:      "wrong type (number) must error, not silently skip",
			input:     map[string]interface{}{"distance": 123},
			expectErr: true,
			expected:  "unset",
		},
		{
			name:      "wrong type (bool) must error, not silently skip",
			input:     map[string]interface{}{"distance": true},
			expectErr: true,
			expected:  "unset",
		},
	}, "unset", func(input map[string]interface{}, setFn func(string)) error {
		return OptionalStringFromMap(input, "distance", setFn)
	})
}

func Test_OptionalBoolFromMap(t *testing.T) {
	runOptionalFromMapCases(t, []optionalFromMapCase[bool]{
		{
			name:      "key absent, setFn not called, no error",
			input:     map[string]interface{}{},
			expectErr: false,
			expected:  false,
		},
		{
			name:      "valid bool",
			input:     map[string]interface{}{"skip": true},
			expectErr: false,
			expected:  true,
		},
		{
			name:      "explicit JSON null must error, not silently skip",
			input:     map[string]interface{}{"skip": nil},
			expectErr: true,
			expected:  false,
		},
		{
			name:      "wrong type (string) must error, not silently skip",
			input:     map[string]interface{}{"skip": "true"},
			expectErr: true,
			expected:  false,
		},
	}, false, func(input map[string]interface{}, setFn func(bool)) error {
		return OptionalBoolFromMap(input, "skip", setFn)
	})
}

func Test_OptionalIntFromMap(t *testing.T) {
	runOptionalFromMapCases(t, []optionalFromMapCase[int]{
		{
			name:      "key absent, setFn not called, no error",
			input:     map[string]interface{}{},
			expectErr: false,
			expected:  -1,
		},
		{
			name:      "valid float64 (from REST API JSON decoding)",
			input:     map[string]interface{}{"vectorCacheMaxObjects": float64(42)},
			expectErr: false,
			expected:  42,
		},
		{
			name:      "valid json.Number (from disk)",
			input:     map[string]interface{}{"vectorCacheMaxObjects": json.Number("42")},
			expectErr: false,
			expected:  42,
		},
		{
			name:      "explicit JSON null must error, not silently default to 0",
			input:     map[string]interface{}{"vectorCacheMaxObjects": nil},
			expectErr: true,
			expected:  -1,
		},
		{
			name:      "wrong type (string) must error, not silently default to 0",
			input:     map[string]interface{}{"vectorCacheMaxObjects": "42"},
			expectErr: true,
			expected:  -1,
		},
		{
			name:      "wrong type (bool) must error, not silently default to 0",
			input:     map[string]interface{}{"vectorCacheMaxObjects": true},
			expectErr: true,
			expected:  -1,
		},
	}, -1, func(input map[string]interface{}, setFn func(int)) error {
		return OptionalIntFromMap(input, "vectorCacheMaxObjects", setFn)
	})
}
