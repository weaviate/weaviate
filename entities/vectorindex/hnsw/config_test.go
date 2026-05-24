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
	"encoding/json"
	"math"
	"os"
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				"filterStrategy":         "sweeping",
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: FilterStrategySweeping,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
			name: "with negative pq segments",
			input: map[string]interface{}{
				"pq": map[string]interface{}{
					"enabled":  true,
					"segments": float64(-1),
				},
			},
			expectErr:    true,
			expectErrMsg: "pq segments must be non-negative",
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
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
			// Reproduces gh-11399: dynamicEfMin must not exceed
			// dynamicEfMax.
			name: "invalid dynamicEf bounds (min > max)",
			input: map[string]interface{}{
				"dynamicEfMin": float64(500),
				"dynamicEfMax": float64(10),
			},
			expectErr: true,
			expectErrMsg: "dynamicEfMin (500) must be less than or " +
				"equal to dynamicEfMax (10)",
		},
		{
			// Reproduces gh-11400: negative flatSearchCutoff has no
			// meaningful interpretation.
			name: "invalid flatSearchCutoff (negative)",
			input: map[string]interface{}{
				"flatSearchCutoff": float64(-100),
			},
			expectErr:    true,
			expectErrMsg: "flatSearchCutoff must not be negative",
		},
		{
			name: "invalid dynamicEfMin (negative)",
			input: map[string]interface{}{
				"dynamicEfMin": float64(-1),
			},
			expectErr:    true,
			expectErrMsg: "dynamicEfMin must not be negative",
		},
		{
			name: "invalid dynamicEfMax (negative)",
			input: map[string]interface{}{
				"dynamicEfMax": float64(-1),
			},
			expectErr:    true,
			expectErrMsg: "dynamicEfMax must not be negative",
		},
		{
			name: "invalid dynamicEfFactor (negative)",
			input: map[string]interface{}{
				"dynamicEfFactor": float64(-1),
			},
			expectErr:    true,
			expectErrMsg: "dynamicEfFactor must not be negative",
		},
		{
			name: "invalid cleanupIntervalSeconds (negative)",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(-1),
			},
			expectErr:    true,
			expectErrMsg: "cleanupIntervalSeconds must not be negative",
		},
		{
			name: "invalid vectorCacheMaxObjects (negative)",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(-1),
			},
			expectErr:    true,
			expectErrMsg: "vectorCacheMaxObjects must not be negative",
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "with sq",
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
				"sq": map[string]interface{}{
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
				SQ: SQConfig{
					Enabled:       true,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
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
			expectErrMsg: "invalid hnsw config: more than a single compression methods enabled",
		},
		{
			name: "with invalid filter strategy",
			input: map[string]interface{}{
				"filterStrategy": "chestnut",
			},
			expectErr:    true,
			expectErrMsg: "invalid hnsw config: filterStrategy must be either 'sweeping' or 'acorn'",
		},
		{
			name: "acorn enabled, all defaults",
			input: map[string]interface{}{
				"filterStrategy": "acorn",
			},
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: FilterStrategyAcorn,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "max connections at maximum allowed value (2047)",
			input: map[string]interface{}{
				"maxConnections": json.Number("2047"),
			},
			expected: UserConfig{
				CleanupIntervalSeconds: DefaultCleanupIntervalSeconds,
				MaxConnections:         2047,
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      DefaultRQEnabled,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "max connections exceeds maximum allowed value (2048)",
			input: map[string]interface{}{
				"maxConnections": json.Number("2048"),
			},
			expectErr:    true,
			expectErrMsg: "maxConnections must be less than 2048",
		},
		{
			name: "with rq defaults",
			input: map[string]interface{}{
				"rq": map[string]interface{}{
					"enabled": true,
				},
			},
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      true,
					Bits:         DefaultRQBits,
					RescoreLimit: DefaultRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "with rq 8 and rescore limit 0",
			input: map[string]interface{}{
				"rq": map[string]interface{}{
					"enabled":      true,
					"bits":         float64(8),
					"rescoreLimit": float64(0),
				},
			},
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      true,
					Bits:         DefaultRQBits,
					RescoreLimit: 0,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "with rq 1",
			input: map[string]interface{}{
				"rq": map[string]interface{}{
					"enabled": true,
					"bits":    float64(1),
				},
			},
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      true,
					Bits:         1,
					RescoreLimit: DefaultBRQRescoreLimit,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
		},
		{
			name: "with rq 1 and rescore limit 100",
			input: map[string]interface{}{
				"rq": map[string]interface{}{
					"enabled":      true,
					"bits":         float64(1),
					"rescoreLimit": float64(100),
				},
			},
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
				SQ: SQConfig{
					Enabled:       DefaultSQEnabled,
					TrainingLimit: DefaultSQTrainingLimit,
					RescoreLimit:  DefaultSQRescoreLimit,
				},
				RQ: RQConfig{
					Enabled:      true,
					Bits:         1,
					RescoreLimit: 100,
				},
				FilterStrategy: DefaultFilterStrategy,
				Multivector: MultivectorConfig{
					Enabled:     DefaultMultivectorEnabled,
					Aggregation: DefaultMultivectorAggregation,
					MuveraConfig: MuveraConfig{
						Enabled:      DefaultMultivectorMuveraEnabled,
						KSim:         DefaultMultivectorKSim,
						DProjections: DefaultMultivectorDProjections,
						Repetitions:  DefaultMultivectorRepetitions,
					},
				},
			},
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

func Test_ParseDefaultQuantization(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		expectErr   bool
		expectPQ    bool
		expectSQ    bool
		expectBQ    bool
		expectRQ    bool
	}{
		{name: "empty string is no-op", compression: "", expectErr: false},
		{name: "none is no-op", compression: "none", expectErr: false},
		{name: "pq enables PQ", compression: "pq", expectPQ: true},
		{name: "sq enables SQ", compression: "sq", expectSQ: true},
		{name: "bq enables BQ", compression: "bq", expectBQ: true},
		{name: "rq-1 enables RQ", compression: "rq-1", expectRQ: true},
		{name: "rq-8 enables RQ", compression: "rq-8", expectRQ: true},
		{name: "invalid compression", compression: "invalid", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := NewDefaultUserConfig()
			result, err := ParseDefaultQuantization(uc, tt.compression)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			cfg := result.(UserConfig)
			assert.Equal(t, tt.expectPQ, cfg.PQ.Enabled, "PQ.Enabled")
			assert.Equal(t, tt.expectSQ, cfg.SQ.Enabled, "SQ.Enabled")
			assert.Equal(t, tt.expectBQ, cfg.BQ.Enabled, "BQ.Enabled")
			assert.Equal(t, tt.expectRQ, cfg.RQ.Enabled, "RQ.Enabled")
		})
	}
}

func Test_UserConfigFilterStrategy(t *testing.T) {
	t.Run("default filter strategy is sweeping", func(t *testing.T) {
		cfg := UserConfig{}
		cfg.SetDefaults()
		assert.Equal(t, FilterStrategyAcorn, cfg.FilterStrategy)
	})

	t.Run("can override default strategy", func(t *testing.T) {
		os.Setenv("HNSW_DEFAULT_FILTER_STRATEGY", FilterStrategySweeping)
		cfg := UserConfig{}
		cfg.SetDefaults()
		assert.Equal(t, FilterStrategySweeping, cfg.FilterStrategy)
		assert.Nil(t, os.Unsetenv("HNSW_DEFAULT_FILTER_STRATEGY"))
	})
}

// Test_UserConfigDynamicEFBoundary covers the dynamicEfMin <= dynamicEfMax
// boundary directly via validate() so the table above does not need to spell
// out a full UserConfig literal for an "expected equals defaults" case.
func Test_UserConfigDynamicEFBoundary(t *testing.T) {
	cfg := NewDefaultUserConfig()
	cfg.DynamicEFMin = 100
	cfg.DynamicEFMax = 100
	assert.NoError(t, cfg.validate(), "min == max must be accepted")

	cfg.DynamicEFMin = 99
	cfg.DynamicEFMax = 100
	assert.NoError(t, cfg.validate(), "min < max must be accepted")
}
