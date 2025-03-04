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

package ivfpq

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
				Distance:       common.DefaultDistanceMetric,
				ProbingSize:    DefaultProbingSize,
				DistanceCutOff: DefaultDistanceCutOff,
			},
		},
		{
			name: "probingSize specified, all defaults",
			input: map[string]interface{}{
				"distance":    "cosine",
				"probingSize": float64(500),
			},
			expected: UserConfig{
				Distance:       common.DefaultDistanceMetric,
				ProbingSize:    500,
				DistanceCutOff: DefaultDistanceCutOff,
			},
		},
		{
			name: "distanceCutOff specified, all defaults",
			input: map[string]interface{}{
				"distance":       "cosine",
				"distanceCutOff": float64(0.4),
			},
			expected: UserConfig{
				Distance:       common.DefaultDistanceMetric,
				ProbingSize:    DefaultProbingSize,
				DistanceCutOff: 0.4,
			},
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
