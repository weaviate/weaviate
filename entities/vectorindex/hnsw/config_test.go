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

package hnsw

import (
	"encoding/json"
	"math"
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
				CleanupIntervalSeconds: DefaultCleanupIntervalSeconds,
				MaxConnections:         DefaultMaxConnections,
				EFConstruction:         DefaultEFConstruction,
				VectorCacheMaxObjects:  common.DefaultVectorCacheMaxObjects,
				EF:                     DefaultEF,
				Skip:                   DefaultSkip,
				FlatSearchCutoff:       DefaultFlatSearchCutoff,
				DynamicEFMin:           DefaultDynamicEFMin,
				DynamicEFMax:           DefaultDynamicEFMax,
				DynamicEFFactor:        DefaultDynamicEFFactor,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with maximum connections",
			input: map[string]interface{}{
				"maxConnections": json.Number("100"),
			},
			expected: UserConfig{
				CleanupIntervalSeconds: DefaultCleanupIntervalSeconds,
				MaxConnections:         100,
				EFConstruction:         DefaultEFConstruction,
				VectorCacheMaxObjects:  common.DefaultVectorCacheMaxObjects,
				EF:                     DefaultEF,
				FlatSearchCutoff:       DefaultFlatSearchCutoff,
				DynamicEFMin:           DefaultDynamicEFMin,
				DynamicEFMax:           DefaultDynamicEFMax,
				DynamicEFFactor:        DefaultDynamicEFFactor,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with all optional fields",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": json.Number("11"),
				"maxConnections":         json.Number("12"),
				"efConstruction":         json.Number("13"),
				"vectorCacheMaxObjects":  json.Number("14"),
				"ef":                     json.Number("15"),
				"flatSearchCutoff":       json.Number("16"),
				"dynamicEfMin":           json.Number("17"),
				"dynamicEfMax":           json.Number("18"),
				"dynamicEfFactor":        json.Number("19"),
				"skip":                   true,
				"distance":               "l2-squared",
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Skip:                   true,
				Distance:               "l2-squared",
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with all optional fields",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": json.Number("11"),
				"maxConnections":         json.Number("12"),
				"efConstruction":         json.Number("13"),
				"vectorCacheMaxObjects":  json.Number("14"),
				"ef":                     json.Number("15"),
				"flatSearchCutoff":       json.Number("16"),
				"dynamicEfMin":           json.Number("17"),
				"dynamicEfMax":           json.Number("18"),
				"dynamicEfFactor":        json.Number("19"),
				"skip":                   true,
				"distance":               "manhattan",
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Skip:                   true,
				Distance:               "manhattan",
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with all optional fields",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": json.Number("11"),
				"maxConnections":         json.Number("12"),
				"efConstruction":         json.Number("13"),
				"vectorCacheMaxObjects":  json.Number("14"),
				"ef":                     json.Number("15"),
				"flatSearchCutoff":       json.Number("16"),
				"dynamicEfMin":           json.Number("17"),
				"dynamicEfMax":           json.Number("18"),
				"dynamicEfFactor":        json.Number("19"),
				"skip":                   true,
				"distance":               "hamming",
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Skip:                   true,
				Distance:               "hamming",
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			// opposed to from the API
			name: "with raw data as floats",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(11),
				"maxConnections":         float64(12),
				"efConstruction":         float64(13),
				"vectorCacheMaxObjects":  float64(14),
				"ef":                     float64(15),
				"flatSearchCutoff":       float64(16),
				"dynamicEfMin":           float64(17),
				"dynamicEfMax":           float64(18),
				"dynamicEfFactor":        float64(19),
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with pq tile normal encoder",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(11),
				"maxConnections":         float64(12),
				"efConstruction":         float64(13),
				"vectorCacheMaxObjects":  float64(14),
				"ef":                     float64(15),
				"flatSearchCutoff":       float64(16),
				"dynamicEfMin":           float64(17),
				"dynamicEfMax":           float64(18),
				"dynamicEfFactor":        float64(19),
				"pq": map[string]interface{}{
					"enabled":        true,
					"bitCompression": false,
					"segments":       float64(64),
					"centroids":      float64(DefaultPQCentroids),
					"trainingLimit":  float64(DefaultPQTrainingLimit),
					"encoder": map[string]interface{}{
						"type":         "tile",
						"distribution": "normal",
					},
				},
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:       true,
					Segments:      64,
					Centroids:     DefaultPQCentroids,
					TrainingLimit: DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         "tile",
						Distribution: "normal",
					},
				},
			},
		},

		{
			name: "with pq kmeans normal encoder",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(11),
				"maxConnections":         float64(12),
				"efConstruction":         float64(13),
				"vectorCacheMaxObjects":  float64(14),
				"ef":                     float64(15),
				"flatSearchCutoff":       float64(16),
				"dynamicEfMin":           float64(17),
				"dynamicEfMax":           float64(18),
				"dynamicEfFactor":        float64(19),
				"pq": map[string]interface{}{
					"enabled":        true,
					"bitCompression": false,
					"segments":       float64(64),
					"centroids":      float64(DefaultPQCentroids),
					"trainingLimit":  float64(DefaultPQTrainingLimit),
					"encoder": map[string]interface{}{
						"type": PQEncoderTypeKMeans,
					},
				},
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:       true,
					Segments:      64,
					Centroids:     DefaultPQCentroids,
					TrainingLimit: DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},

		{
			name: "with invalid encoder",
			input: map[string]interface{}{
				"pq": map[string]interface{}{
					"enabled": true,
					"encoder": map[string]interface{}{
						"type": "bernoulli",
					},
				},
			},
			expectErr:    true,
			expectErrMsg: "invalid encoder type bernoulli",
		},

		{
			name: "with invalid distribution",
			input: map[string]interface{}{
				"pq": map[string]interface{}{
					"enabled": true,
					"encoder": map[string]interface{}{
						"distribution": "lognormal",
					},
				},
			},
			expectErr:    true,
			expectErrMsg: "invalid encoder distribution lognormal",
		},

		{
			// opposed to from the API
			name: "with rounded vectorCacheMaxObjects that would otherwise overflow",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": json.Number("11"),
				"maxConnections":         json.Number("12"),
				"efConstruction":         json.Number("13"),
				"vectorCacheMaxObjects":  json.Number("9223372036854776000"),
				"ef":                     json.Number("15"),
				"flatSearchCutoff":       json.Number("16"),
				"dynamicEfMin":           json.Number("17"),
				"dynamicEfMax":           json.Number("18"),
				"dynamicEfFactor":        json.Number("19"),
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  math.MaxInt64,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:        DefaultPQEnabled,
					BitCompression: DefaultPQBitCompression,
					Segments:       DefaultPQSegments,
					Centroids:      DefaultPQCentroids,
					TrainingLimit:  DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
			},
		},
		{
			name: "invalid max connections (json)",
			input: map[string]interface{}{
				"maxConnections": json.Number("0"),
			},
			expectErr: true,
			expectErrMsg: "maxConnections must be a positive integer " +
				"with a minimum of 4",
		},
		{
			name: "invalid max connections (float)",
			input: map[string]interface{}{
				"maxConnections": float64(3),
			},
			expectErr: true,
			expectErrMsg: "maxConnections must be a positive integer " +
				"with a minimum of 4",
		},
		{
			name: "invalid efConstruction (json)",
			input: map[string]interface{}{
				"efConstruction": json.Number("0"),
			},
			expectErr: true,
			expectErrMsg: "efConstruction must be a positive integer " +
				"with a minimum of 4",
		},
		{
			name: "invalid efConstruction (float)",
			input: map[string]interface{}{
				"efConstruction": float64(3),
			},
			expectErr: true,
			expectErrMsg: "efConstruction must be a positive integer " +
				"with a minimum of 4",
		},
		{
			name: "with bq",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(11),
				"maxConnections":         float64(12),
				"efConstruction":         float64(13),
				"vectorCacheMaxObjects":  float64(14),
				"ef":                     float64(15),
				"flatSearchCutoff":       float64(16),
				"dynamicEfMin":           float64(17),
				"dynamicEfMax":           float64(18),
				"dynamicEfFactor":        float64(19),
				"bq": map[string]interface{}{
					"enabled": true,
				},
			},
			expected: UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               common.DefaultDistanceMetric,
				PQ: PQConfig{
					Enabled:       false,
					Segments:      0,
					Centroids:     DefaultPQCentroids,
					TrainingLimit: DefaultPQTrainingLimit,
					Encoder: PQEncoder{
						Type:         DefaultPQEncoderType,
						Distribution: DefaultPQEncoderDistribution,
					},
				},
				BQ: BQConfig{
					Enabled: true,
				},
			},
		},
		{
			name: "with invalid compression",
			input: map[string]interface{}{
				"pq": map[string]interface{}{
					"enabled": true,
					"encoder": map[string]interface{}{
						"type": "kmeans",
					},
				},
				"bq": map[string]interface{}{
					"enabled": true,
				},
			},
			expectErr:    true,
			expectErrMsg: "invalid hnsw config: two compression methods enabled: PQ and BQ",
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
