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
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
					Bits:          DefaultRQBits,
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
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:       true,
					RescoreLimit:  100,
					RescoreFactor: RescoreFactorAuto,
					Cache:         true,
				},
				SQ: CompressionUserConfig{
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:       DefaultCompressionEnabled,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
					Bits:          DefaultRQBits,
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
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:       true,
					RescoreLimit:  100,
					RescoreFactor: RescoreFactorAuto,
					Cache:         true,
					Bits:          DefaultRQBits,
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
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:       true,
					RescoreLimit:  100,
					RescoreFactor: RescoreFactorAuto,
					Cache:         true,
					Bits:          8,
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
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				BQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				SQ: CompressionUserConfig{
					Enabled:       false,
					RescoreLimit:  DefaultCompressionRescore,
					RescoreFactor: RescoreFactorAuto,
					Cache:         DefaultVectorCache,
				},
				RQ: RQUserConfig{
					Enabled:       true,
					RescoreLimit:  100,
					RescoreFactor: RescoreFactorAuto,
					Cache:         true,
					Bits:          1,
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
			Enabled:       DefaultCompressionEnabled,
			RescoreLimit:  DefaultCompressionRescore,
			RescoreFactor: RescoreFactorAuto,
			Cache:         DefaultVectorCache,
			Bits:          DefaultRQBits,
		}

		assert.Equal(t, expectedRQ, config.RQ)
	})
}

func Test_FlatRescoreFactor(t *testing.T) {
	tests := []struct {
		name      string
		quantizer string
		input     interface{}
		stored    float64
		effective float64
		expectErr string
	}{
		{name: "bq absent is automatic", quantizer: "bq", stored: RescoreFactorAuto, effective: DefaultCompressionRescoreFactor},
		{name: "bq -1 is automatic", quantizer: "bq", input: float64(-1), stored: RescoreFactorAuto, effective: DefaultCompressionRescoreFactor},
		{name: "bq 0 is automatic", quantizer: "bq", input: float64(0), stored: 0, effective: DefaultCompressionRescoreFactor},
		{name: "bq explicit is kept", quantizer: "bq", input: 2.5, stored: 2.5, effective: 2.5},
		{name: "bq below 1 is rejected", quantizer: "bq", input: 0.5, expectErr: "bq rescoreFactor must be -1 (automatic) or a number of at least 1, got 0.5"},
		{name: "bq string is rejected", quantizer: "bq", input: "2", expectErr: "bq rescoreFactor must be a number, got string"},
		{name: "rq absent is automatic", quantizer: "rq", stored: RescoreFactorAuto, effective: DefaultCompressionRescoreFactor},
		{name: "rq explicit is kept", quantizer: "rq", input: float64(4), stored: 4, effective: 4},
		{name: "rq below -1 is rejected", quantizer: "rq", input: float64(-2), expectErr: "rq rescoreFactor must be -1 (automatic) or a number of at least 1, got -2"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			quantizerMap := map[string]interface{}{"enabled": true}
			if test.quantizer == "rq" {
				quantizerMap["bits"] = float64(8)
			}
			if test.input != nil {
				quantizerMap["rescoreFactor"] = test.input
			}
			cfg, err := ParseAndValidateConfig(map[string]interface{}{test.quantizer: quantizerMap})
			if test.expectErr != "" {
				require.ErrorContains(t, err, test.expectErr)
				return
			}
			require.NoError(t, err)
			uc := cfg.(UserConfig)
			stored, policy := uc.BQ.RescoreFactor, uc.BQ.RescorePolicy()
			if test.quantizer == "rq" {
				stored, policy = uc.RQ.RescoreFactor, uc.RQ.RescorePolicy()
			}
			assert.Equal(t, test.stored, stored, "stored")
			assert.Equal(t, test.effective, policy.Factor, "effective")
		})
	}
}

// Test_FlatRescoreLegacyConfig feeds a config an older version stored, which
// has no rescoreFactor: it must land on automatic, which keeps the old
// max(k, rescoreLimit).
func Test_FlatRescoreLegacyConfig(t *testing.T) {
	legacy := map[string]interface{}{
		"bq": map[string]interface{}{"enabled": true, "rescoreLimit": float64(100), "cache": true},
	}
	cfg, err := ParseAndValidateConfig(legacy)
	require.NoError(t, err)
	uc := cfg.(UserConfig)
	assert.Equal(t, RescoreFactorAuto, uc.BQ.RescoreFactor)
	assert.Equal(t, 100, uc.BQ.RescorePolicy().Candidates(10))
	assert.Equal(t, 200, uc.BQ.RescorePolicy().Candidates(200))
}
