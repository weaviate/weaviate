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

package flat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func Test_FlatUserConfig(t *testing.T) {
	type test struct {
		name         string
		input        interface{}
		expected     UserConfig
		expectErr    bool
		expectErrMsg string
	}

	tests := []test{
		{
			name:  "nothing specified, all defaults",
			input: nil,
			expected: UserConfig{
				VectorCacheMaxObjects: common.DefaultVectorCacheMaxObjects,
				Distance:              common.DefaultDistanceMetric,
				PQ: CompressionUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
			},
		},
		{
			name: "bq enabled",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"bq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expected: UserConfig{
				VectorCacheMaxObjects: 100,
				Distance:              common.DefaultDistanceMetric,
				PQ: CompressionUserConfig{
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:      true,
					RescoreLimit: 100,
					Cache:        true,
				},
				SQ: CompressionUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
			},
		},
		{
			name: "sq enabled",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"sq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(20),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "SQ is not currently supported for flat indices",
		},
		{
			name: "pq enabled",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"pq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "PQ is not currently supported for flat indices",
		},
		{
			name: "sq and bq enabled",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"sq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
				"bq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "cannot enable multiple quantization methods at the same time",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := ParseAndValidateConfig(test.input)
			if test.expectErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expectErrMsg)
				return
			} else {
				assert.Nil(t, err)
				assert.Equal(t, test.expected, cfg)
			}
		})
	}
}
