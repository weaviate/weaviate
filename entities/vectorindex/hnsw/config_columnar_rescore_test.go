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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnarRescoreParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		expectErr bool
		expected  bool
	}{
		{
			name:     "default is false (nil input)",
			input:    nil,
			expected: false,
		},
		{
			name:     "default is false (key absent)",
			input:    map[string]interface{}{},
			expected: false,
		},
		{
			name:     "explicit true",
			input:    map[string]interface{}{"columnarRescore": true},
			expected: true,
		},
		{
			name:     "explicit false",
			input:    map[string]interface{}{"columnarRescore": false},
			expected: false,
		},
		{
			// OptionalBoolFromMap silently ignores non-bool values — same
			// behavior as every other optional bool in this config
			name:     "non-bool value ignored, default kept",
			input:    map[string]interface{}{"columnarRescore": "yes"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseAndValidateConfig(tt.input, false)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			parsed, ok := cfg.(UserConfig)
			require.True(t, ok)
			assert.Equal(t, tt.expected, parsed.ColumnarRescore)
		})
	}
}

// Mutability of columnarRescore across config updates is pinned in
// adapters/repos/db/vector/hnsw (TestColumnarRescoreIsMutableOnUpdate),
// next to ValidateUserConfigUpdate.
