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

package dynamic

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_DynamicUserConfig(t *testing.T) {
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
				Distance:  common.DefaultDistanceMetric,
				Threshold: DefaultThreshold,
				HnswUC: hnsw.UserConfig{
					CleanupIntervalSeconds: hnsw.DefaultCleanupIntervalSeconds,
					MaxConnections:         hnsw.DefaultMaxConnections,
					EFConstruction:         hnsw.DefaultEFConstruction,
					VectorCacheMaxObjects:  common.DefaultVectorCacheMaxObjects,
					EF:                     hnsw.DefaultEF,
					Skip:                   hnsw.DefaultSkip,
					FlatSearchCutoff:       hnsw.DefaultFlatSearchCutoff,
					DynamicEFMin:           hnsw.DefaultDynamicEFMin,
					DynamicEFMax:           hnsw.DefaultDynamicEFMax,
					DynamicEFFactor:        hnsw.DefaultDynamicEFFactor,
					Distance:               common.DefaultDistanceMetric,
					PQ: hnsw.PQConfig{
						Enabled:       hnsw.DefaultPQEnabled,
						Segments:      hnsw.DefaultPQSegments,
						Centroids:     hnsw.DefaultPQCentroids,
						TrainingLimit: hnsw.DefaultPQTrainingLimit,
						Encoder: hnsw.PQEncoder{
							Type:         hnsw.DefaultPQEncoderType,
							Distribution: hnsw.DefaultPQEncoderDistribution,
						},
					},
					SQ: hnsw.SQConfig{
						Enabled:       hnsw.DefaultSQEnabled,
						TrainingLimit: hnsw.DefaultSQTrainingLimit,
						RescoreLimit:  hnsw.DefaultSQRescoreLimit,
					},
					FilterStrategy: hnsw.DefaultFilterStrategy,
					Multivector: hnsw.MultivectorConfig{
						Enabled:     hnsw.DefaultMultivectorEnabled,
						Aggregation: hnsw.DefaultMultivectorAggregation,
						MuveraConfig: hnsw.MuveraConfig{
							Enabled:      hnsw.DefaultMultivectorMuveraEnabled,
							KSim:         hnsw.DefaultMultivectorKSim,
							DProjections: hnsw.DefaultMultivectorDProjections,
							Repetitions:  hnsw.DefaultMultivectorRepetitions,
						},
					},
				},
				FlatUC: flat.UserConfig{
					VectorCacheMaxObjects: common.DefaultVectorCacheMaxObjects,
					Distance:              common.DefaultDistanceMetric,
					PQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					BQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					SQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
				},
			},
		},
		{
			name: "threshold is properly set",
			input: map[string]interface{}{
				"threshold": float64(100),
			},
			expected: UserConfig{
				Distance:  common.DefaultDistanceMetric,
				Threshold: 100,
				HnswUC: hnsw.UserConfig{
					CleanupIntervalSeconds: hnsw.DefaultCleanupIntervalSeconds,
					MaxConnections:         hnsw.DefaultMaxConnections,
					EFConstruction:         hnsw.DefaultEFConstruction,
					VectorCacheMaxObjects:  common.DefaultVectorCacheMaxObjects,
					EF:                     hnsw.DefaultEF,
					Skip:                   hnsw.DefaultSkip,
					FlatSearchCutoff:       hnsw.DefaultFlatSearchCutoff,
					DynamicEFMin:           hnsw.DefaultDynamicEFMin,
					DynamicEFMax:           hnsw.DefaultDynamicEFMax,
					DynamicEFFactor:        hnsw.DefaultDynamicEFFactor,
					Distance:               common.DefaultDistanceMetric,
					PQ: hnsw.PQConfig{
						Enabled:       hnsw.DefaultPQEnabled,
						Segments:      hnsw.DefaultPQSegments,
						Centroids:     hnsw.DefaultPQCentroids,
						TrainingLimit: hnsw.DefaultPQTrainingLimit,
						Encoder: hnsw.PQEncoder{
							Type:         hnsw.DefaultPQEncoderType,
							Distribution: hnsw.DefaultPQEncoderDistribution,
						},
					},
					SQ: hnsw.SQConfig{
						Enabled:       hnsw.DefaultSQEnabled,
						TrainingLimit: hnsw.DefaultSQTrainingLimit,
						RescoreLimit:  hnsw.DefaultSQRescoreLimit,
					},
					FilterStrategy: hnsw.DefaultFilterStrategy,
					Multivector: hnsw.MultivectorConfig{
						Enabled:     hnsw.DefaultMultivectorEnabled,
						Aggregation: hnsw.DefaultMultivectorAggregation,
						MuveraConfig: hnsw.MuveraConfig{
							Enabled:      hnsw.DefaultMultivectorMuveraEnabled,
							KSim:         hnsw.DefaultMultivectorKSim,
							DProjections: hnsw.DefaultMultivectorDProjections,
							Repetitions:  hnsw.DefaultMultivectorRepetitions,
						},
					},
				},
				FlatUC: flat.UserConfig{
					VectorCacheMaxObjects: common.DefaultVectorCacheMaxObjects,
					Distance:              common.DefaultDistanceMetric,
					PQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					BQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					SQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
				},
			},
		},
		{
			name: "hnsw is properly set",
			input: map[string]interface{}{
				"hnsw": map[string]interface{}{
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
						"centroids":      float64(200),
						"trainingLimit":  float64(100),
						"encoder": map[string]interface{}{
							"type": hnsw.PQEncoderTypeKMeans,
						},
					},
					"filterStrategy": hnsw.FilterStrategyAcorn,
				},
			},
			expected: UserConfig{
				Distance:  common.DefaultDistanceMetric,
				Threshold: DefaultThreshold,
				HnswUC: hnsw.UserConfig{
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
					PQ: hnsw.PQConfig{
						Enabled:       true,
						Segments:      64,
						Centroids:     200,
						TrainingLimit: 100,
						Encoder: hnsw.PQEncoder{
							Type:         hnsw.DefaultPQEncoderType,
							Distribution: hnsw.DefaultPQEncoderDistribution,
						},
					},
					SQ: hnsw.SQConfig{
						Enabled:       hnsw.DefaultSQEnabled,
						TrainingLimit: hnsw.DefaultSQTrainingLimit,
						RescoreLimit:  hnsw.DefaultSQRescoreLimit,
					},
					FilterStrategy: hnsw.FilterStrategyAcorn,
					Multivector: hnsw.MultivectorConfig{
						Enabled:     hnsw.DefaultMultivectorEnabled,
						Aggregation: hnsw.DefaultMultivectorAggregation,
						MuveraConfig: hnsw.MuveraConfig{
							Enabled:      hnsw.DefaultMultivectorMuveraEnabled,
							KSim:         hnsw.DefaultMultivectorKSim,
							DProjections: hnsw.DefaultMultivectorDProjections,
							Repetitions:  hnsw.DefaultMultivectorRepetitions,
						},
					},
				},
				FlatUC: flat.UserConfig{
					VectorCacheMaxObjects: common.DefaultVectorCacheMaxObjects,
					Distance:              common.DefaultDistanceMetric,
					PQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					BQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					SQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
				},
			},
		},
		{
			name: "dynamic index is set with multivector",
			input: map[string]interface{}{
				"hnsw": map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
					},
				},
			},
			expectErr:    true,
			expectErrMsg: "multi vector index is not supported for dynamic index",
		},
		{
			name: "flat is properly set",
			input: map[string]interface{}{
				"flat": map[string]interface{}{
					"vectorCacheMaxObjects": float64(100),
					"distance":              "cosine",
					"bq": map[string]interface{}{
						"enabled":      true,
						"rescoreLimit": float64(100),
						"cache":        true,
					},
				},
			},
			expected: UserConfig{
				Distance:  common.DefaultDistanceMetric,
				Threshold: DefaultThreshold,
				HnswUC: hnsw.UserConfig{
					CleanupIntervalSeconds: hnsw.DefaultCleanupIntervalSeconds,
					MaxConnections:         hnsw.DefaultMaxConnections,
					EFConstruction:         hnsw.DefaultEFConstruction,
					VectorCacheMaxObjects:  common.DefaultVectorCacheMaxObjects,
					EF:                     hnsw.DefaultEF,
					Skip:                   hnsw.DefaultSkip,
					FlatSearchCutoff:       hnsw.DefaultFlatSearchCutoff,
					DynamicEFMin:           hnsw.DefaultDynamicEFMin,
					DynamicEFMax:           hnsw.DefaultDynamicEFMax,
					DynamicEFFactor:        hnsw.DefaultDynamicEFFactor,
					Distance:               common.DefaultDistanceMetric,
					PQ: hnsw.PQConfig{
						Enabled:       hnsw.DefaultPQEnabled,
						Segments:      hnsw.DefaultPQSegments,
						Centroids:     hnsw.DefaultPQCentroids,
						TrainingLimit: hnsw.DefaultPQTrainingLimit,
						Encoder: hnsw.PQEncoder{
							Type:         hnsw.DefaultPQEncoderType,
							Distribution: hnsw.DefaultPQEncoderDistribution,
						},
					},
					SQ: hnsw.SQConfig{
						Enabled:       hnsw.DefaultSQEnabled,
						TrainingLimit: hnsw.DefaultSQTrainingLimit,
						RescoreLimit:  hnsw.DefaultSQRescoreLimit,
					},
					FilterStrategy: hnsw.DefaultFilterStrategy,
					Multivector: hnsw.MultivectorConfig{
						Enabled:     hnsw.DefaultMultivectorEnabled,
						Aggregation: hnsw.DefaultMultivectorAggregation,
						MuveraConfig: hnsw.MuveraConfig{
							Enabled:      hnsw.DefaultMultivectorMuveraEnabled,
							KSim:         hnsw.DefaultMultivectorKSim,
							DProjections: hnsw.DefaultMultivectorDProjections,
							Repetitions:  hnsw.DefaultMultivectorRepetitions,
						},
					},
				},
				FlatUC: flat.UserConfig{
					VectorCacheMaxObjects: 100,
					Distance:              common.DefaultDistanceMetric,
					PQ: flat.CompressionUserConfig{
						Enabled:      false,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
					BQ: flat.CompressionUserConfig{
						Enabled:      true,
						RescoreLimit: 100,
						Cache:        true,
					},
					SQ: flat.CompressionUserConfig{
						Enabled:      flat.DefaultCompressionEnabled,
						RescoreLimit: flat.DefaultCompressionRescore,
						Cache:        flat.DefaultVectorCache,
					},
				},
			},
		},
		{
			name: "pq enabled with flat returns error",
			input: map[string]interface{}{
				"flat": map[string]interface{}{
					"vectorCacheMaxObjects": float64(100),
					"distance":              "cosine",
					"pq": map[string]interface{}{
						"enabled":      true,
						"rescoreLimit": float64(100),
						"cache":        true,
					},
				},
			},
			expectErr:    true,
			expectErrMsg: "PQ is not currently supported for flat indices",
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
