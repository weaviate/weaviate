//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"encoding/json"
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
				Distance:                  common.DefaultDistanceMetric,
				MaxPostingSize:            DefaultMaxPostingSize,
				MinPostingSize:            DefaultMinPostingSize,
				SplitWorkers:              DefaultSplitWorkers,
				ReassignWorkers:           DefaultReassignWorkers,
				InternalPostingCandidates: DefaultInternalPostingCandidates,
				ReassignNeighbors:         DefaultReassignNeighbors,
				Replicas:                  DefaultReplicas,
				RNGFactor:                 DefaultRNGFactor,
				MaxDistanceRatio:          DefaultMaxDistanceRatio,
			},
		},
		{
			name: "all specified",
			input: map[string]interface{}{
				"distance":                  "cosine",
				"maxPostingSize":            json.Number("1"),
				"minPostingSize":            json.Number("2"),
				"splitWorkers":              json.Number("3"),
				"reassignWorkers":           float64(4),
				"internalPostingCandidates": float64(5),
				"reassignNeighbors":         float64(6),
				"replicas":                  float64(7),
				"rngFactor":                 json.Number("1.1"),
				"maxDistanceRatio":          float64(1.2),
			},
			expected: UserConfig{
				Distance:                  "cosine",
				MaxPostingSize:            1,
				MinPostingSize:            2,
				SplitWorkers:              3,
				ReassignWorkers:           4,
				InternalPostingCandidates: 5,
				ReassignNeighbors:         6,
				Replicas:                  7,
				RNGFactor:                 1.1,
				MaxDistanceRatio:          1.2,
			},
			expectErr:    false,
			expectErrMsg: "",
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
			expectErrMsg: "BQ, PQ, RQ, and SQ are not currently supported for spfresh indices",
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
