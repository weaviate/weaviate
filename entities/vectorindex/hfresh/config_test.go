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

package hfresh

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func Test_UserConfig(t *testing.T) {
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
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with maxPostingSizeKB",
			input: map[string]interface{}{
				"maxPostingSizeKB": json.Number("100"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: 100,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with replicas",
			input: map[string]interface{}{
				"replicas": json.Number("8"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         8,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with rngFactor",
			input: map[string]interface{}{
				"rngFactor": json.Number("15"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        15.0,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with searchProbe",
			input: map[string]interface{}{
				"searchProbe": json.Number("128"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      128,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with rescoreLimit",
			input: map[string]interface{}{
				"rescoreLimit": json.Number("500"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     500,
			},
		},
		{
			name: "with distance cosine",
			input: map[string]interface{}{
				"distance": "cosine",
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         "cosine",
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with distance l2-squared",
			input: map[string]interface{}{
				"distance": "l2-squared",
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         "l2-squared",
				RescoreLimit:     DefaultRescoreLimit,
			},
		},
		{
			name: "with all optional fields",
			input: map[string]interface{}{
				"maxPostingSizeKB": json.Number("9"),
				"replicas":         json.Number("8"),
				"rngFactor":        json.Number("15"),
				"searchProbe":      json.Number("128"),
				"rescoreLimit":     json.Number("500"),
				"distance":         "l2-squared",
			},
			expected: UserConfig{
				MaxPostingSizeKB: 9,
				Replicas:         8,
				RNGFactor:        15.0,
				SearchProbe:      128,
				Distance:         "l2-squared",
				RescoreLimit:     500,
			},
		},
		{
			name: "with raw data as floats",
			input: map[string]interface{}{
				"maxPostingSizeKB": float64(100),
				"replicas":         float64(8),
				"rngFactor":        float64(15),
				"searchProbe":      float64(128),
				"rescoreLimit":     float64(500),
			},
			expected: UserConfig{
				MaxPostingSizeKB: 100,
				Replicas:         8,
				RNGFactor:        15.0,
				SearchProbe:      128,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     500,
			},
		},
		{
			name: "with rescoreLimit zero",
			input: map[string]interface{}{
				"rescoreLimit": json.Number("0"),
			},
			expected: UserConfig{
				MaxPostingSizeKB: DefaultMaxPostingSizeKB,
				Replicas:         DefaultReplicas,
				RNGFactor:        DefaultRNGFactor,
				SearchProbe:      DefaultSearchProbe,
				Distance:         common.DefaultDistanceMetric,
				RescoreLimit:     0,
			},
		},
		{
			name: "with invalid distance manhattan",
			input: map[string]interface{}{
				"distance": "manhattan",
			},
			expectErr:    true,
			expectErrMsg: "unsupported distance type 'manhattan', HFresh only supports 'cosine' or 'l2-squared' for the distance metric",
		},
		{
			name: "with invalid distance dot",
			input: map[string]interface{}{
				"distance": "dot",
			},
			expectErr:    true,
			expectErrMsg: "unsupported distance type 'dot', HFresh only supports 'cosine' or 'l2-squared' for the distance metric",
		},
		{
			name: "with invalid distance hamming",
			input: map[string]interface{}{
				"distance": "hamming",
			},
			expectErr:    true,
			expectErrMsg: "unsupported distance type 'hamming', HFresh only supports 'cosine' or 'l2-squared' for the distance metric",
		},
		{
			name:         "with invalid input type (not a map)",
			input:        "not a map",
			expectErr:    true,
			expectErrMsg: "input must be a non-nil map",
		},
		{
			name:         "with nil map",
			input:        map[string]interface{}(nil),
			expectErr:    true,
			expectErrMsg: "input must be a non-nil map",
		},
		{
			name: "with too small maxPostingSizeKB",
			input: map[string]interface{}{
				"maxPostingSizeKB": json.Number("7"),
			},
			expectErr:    true,
			expectErrMsg: "invalid hnsw config: maxPostingSizeKB is '7' but must be at least 8",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := ParseAndValidateConfig(test.input, false)
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
