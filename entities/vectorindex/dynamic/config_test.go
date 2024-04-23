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
				},
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
