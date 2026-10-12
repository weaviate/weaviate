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

package flat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

func defaultExpectedUserConfig() UserConfig {
	compression := CompressionUserConfig{
		Enabled:      DefaultCompressionEnabled,
		RescoreLimit: DefaultCompressionRescore,
		Cache:        DefaultVectorCache,
	}
	return UserConfig{
		VectorCacheMaxObjects: common.DefaultVectorCacheMaxObjects,
		Distance:              common.DefaultDistanceMetric,
		PQ:                    compression,
		BQ:                    compression,
		SQ:                    compression,
		RQ: RQUserConfig{
			Enabled:      DefaultCompressionEnabled,
			RescoreLimit: DefaultCompressionRescore,
			Cache:        DefaultVectorCache,
			Bits:         DefaultRQBits,
		},
	}
}

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
				RQ: RQUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
					Bits:         DefaultRQBits,
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
				RQ: RQUserConfig{
					Enabled:      DefaultCompressionEnabled,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
					Bits:         DefaultRQBits,
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
		{
			name: "rq enabled with default bits",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
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
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:      true,
					RescoreLimit: 100,
					Cache:        true,
					Bits:         DefaultRQBits,
				},
			},
		},
		{
			name: "rq enabled with 8 bits",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
					"bits":         float64(8),
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
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:      true,
					RescoreLimit: 100,
					Cache:        true,
					Bits:         8,
				},
			},
		},
		{
			name: "rq enabled with 1 bit",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
					"bits":         float64(1),
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
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:      false,
					RescoreLimit: DefaultCompressionRescore,
					Cache:        DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:      true,
					RescoreLimit: 100,
					Cache:        true,
					Bits:         1,
				},
			},
		},
		{
			name: "rq enabled with invalid bits",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
					"bits":         float64(4),
				},
			},
			expectErr:    true,
			expectErrMsg: "RQ bits must be either 1 or 8",
		},
		{
			name: "rq enabled with cache but not enabled",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      false,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "not possible to use the cache without compression",
		},
		{
			name: "rq and bq enabled together",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
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
		{
			name: "rq and pq enabled together",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
				"pq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "cannot enable multiple quantization methods at the same time",
		},
		{
			name: "rq and sq enabled together",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
				"sq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(100),
					"cache":        true,
				},
			},
			expectErr:    true,
			expectErrMsg: "cannot enable multiple quantization methods at the same time",
		},
		{
			name: "bq with negative rescoreLimit is rejected",
			input: map[string]interface{}{
				"distance": "cosine",
				"bq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(-1),
				},
			},
			expectErr:    true,
			expectErrMsg: "bq.rescoreLimit must be non-negative, got -1",
		},
		{
			name: "pq with negative rescoreLimit is rejected",
			input: map[string]interface{}{
				"distance": "cosine",
				"pq": map[string]interface{}{
					"enabled":      false,
					"rescoreLimit": float64(-5),
				},
			},
			expectErr:    true,
			expectErrMsg: "pq.rescoreLimit must be non-negative, got -5",
		},
		{
			name: "sq with negative rescoreLimit is rejected",
			input: map[string]interface{}{
				"distance": "cosine",
				"sq": map[string]interface{}{
					"enabled":      false,
					"rescoreLimit": float64(-100),
				},
			},
			expectErr:    true,
			expectErrMsg: "sq.rescoreLimit must be non-negative, got -100",
		},
		{
			name: "rq with negative rescoreLimit is rejected",
			input: map[string]interface{}{
				"distance": "cosine",
				"rq": map[string]interface{}{
					"enabled":      true,
					"bits":         8,
					"rescoreLimit": float64(-1),
				},
			},
			expectErr:    true,
			expectErrMsg: "rq.rescoreLimit must be non-negative, got -1",
		},
		{
			name: "bq with zero rescoreLimit is allowed",
			input: map[string]interface{}{
				"vectorCacheMaxObjects": float64(100),
				"distance":              "cosine",
				"bq": map[string]interface{}{
					"enabled":      true,
					"rescoreLimit": float64(0),
				},
			},
			expected: func() UserConfig {
				e := defaultExpectedUserConfig()
				e.VectorCacheMaxObjects = 100
				e.BQ.Enabled = true
				e.BQ.RescoreLimit = 0
				return e
			}(),
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

func Test_ParseDefaultQuantization(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		expectErr   bool
		expectBQ    bool
		expectRQ    bool
	}{
		{name: "empty string is no-op", compression: "", expectErr: false},
		{name: "none is no-op", compression: "none", expectErr: false},
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
			assert.Equal(t, tt.expectBQ, cfg.BQ.Enabled, "BQ.Enabled")
			assert.Equal(t, tt.expectRQ, cfg.RQ.Enabled, "RQ.Enabled")
		})
	}
}

func Test_RQUserConfigDefaults(t *testing.T) {
	t.Run("UserConfig SetDefaults includes RQ", func(t *testing.T) {
		config := UserConfig{}
		config.SetDefaults()

		expectedRQ := RQUserConfig{
			Enabled:      DefaultCompressionEnabled,
			RescoreLimit: DefaultCompressionRescore,
			Cache:        DefaultVectorCache,
			Bits:         DefaultRQBits,
		}

		assert.Equal(t, expectedRQ, config.RQ)
	})
}
