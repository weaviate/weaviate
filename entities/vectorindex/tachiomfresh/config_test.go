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

package tachiomfresh

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func defaultMultivector() hnsw.MultivectorConfig {
	return hnsw.MultivectorConfig{
		Enabled:     true,
		Aggregation: hnsw.DefaultMultivectorAggregation,
	}
}

func defaults() UserConfig {
	return UserConfig{
		MaxPostingSize: DefaultMaxPostingSize,
		KCentroids:     DefaultKCentroids,
		KDocsToScore:   DefaultKDocsToScore,
		EFConstruction: DefaultEFConstruction,
		Distance:       common.DistanceDot,
		Multivector:    defaultMultivector(),
	}
}

func Test_UserConfig(t *testing.T) {
	t.Run("the index type and the multi-vector flag", func(t *testing.T) {
		cfg := NewDefaultUserConfig()
		assert.Equal(t, "tachiomfresh", cfg.IndexType())
		assert.True(t, cfg.IsMultiVector())
		assert.Equal(t, common.DistanceDot, cfg.DistanceName())
	})

	type test struct {
		name         string
		input        interface{}
		expected     UserConfig
		expectErrMsg string
	}

	withDefaults := func(set func(uc *UserConfig)) UserConfig {
		uc := defaults()
		set(&uc)
		return uc
	}

	tests := []test{
		{
			name:     "nothing specified, all defaults",
			input:    nil,
			expected: defaults(),
		},
		{
			name:     "empty map, all defaults",
			input:    map[string]interface{}{},
			expected: defaults(),
		},
		{
			name:         "not a map",
			input:        "tachiomfresh",
			expectErrMsg: "input must be a non-nil map",
		},
		{
			name:     "with maxPostingSize",
			input:    map[string]interface{}{"maxPostingSize": json.Number("128")},
			expected: withDefaults(func(uc *UserConfig) { uc.MaxPostingSize = 128 }),
		},
		{
			name:     "with kCentroids",
			input:    map[string]interface{}{"kCentroids": json.Number("16")},
			expected: withDefaults(func(uc *UserConfig) { uc.KCentroids = 16 }),
		},
		{
			name:     "with kDocsToScore",
			input:    map[string]interface{}{"kDocsToScore": json.Number("100")},
			expected: withDefaults(func(uc *UserConfig) { uc.KDocsToScore = 100 }),
		},
		{
			name:     "with efConstruction",
			input:    map[string]interface{}{"efConstruction": json.Number("128")},
			expected: withDefaults(func(uc *UserConfig) { uc.EFConstruction = 128 }),
		},
		{
			name:     "with distance dot",
			input:    map[string]interface{}{"distance": "dot"},
			expected: withDefaults(func(uc *UserConfig) { uc.Distance = "dot" }),
		},
		{
			name:         "with distance l2-squared",
			input:        map[string]interface{}{"distance": "l2-squared"},
			expectErrMsg: "only supports 'dot'",
		},
		{
			name:         "with distance cosine",
			input:        map[string]interface{}{"distance": "cosine"},
			expectErrMsg: "only supports 'dot'",
		},
		{
			name: "with all optional fields",
			input: map[string]interface{}{
				"maxPostingSize": json.Number("16"),
				"kCentroids":     json.Number("8"),
				"kDocsToScore":   json.Number("50"),
				"efConstruction": json.Number("32"),
				"distance":       "dot",
				"multivector": map[string]interface{}{
					"enabled":     true,
					"aggregation": "maxSim",
					"muvera":      map[string]interface{}{"enabled": false},
				},
			},
			expected: UserConfig{
				MaxPostingSize: 16,
				KCentroids:     8,
				KDocsToScore:   50,
				EFConstruction: 32,
				Distance:       "dot",
				Multivector:    defaultMultivector(),
			},
		},
		{
			name:         "maxPostingSize negative",
			input:        map[string]interface{}{"maxPostingSize": json.Number("-1")},
			expectErrMsg: "maxPostingSize is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "maxPostingSize negative, as a float",
			input:        map[string]interface{}{"maxPostingSize": float64(-1)},
			expectErrMsg: "maxPostingSize is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "maxPostingSize one above the uint32 range",
			input:        map[string]interface{}{"maxPostingSize": json.Number("4294967296")},
			expectErrMsg: "maxPostingSize is '4294967296' but must be between 0 and 4294967295",
		},
		{
			name:         "maxPostingSize that a cast would wrap to a small value",
			input:        map[string]interface{}{"maxPostingSize": json.Number("4294967301")},
			expectErrMsg: "maxPostingSize is '4294967301' but must be between 0 and 4294967295",
		},
		{
			name:         "maxPostingSize above the int64 range",
			input:        map[string]interface{}{"maxPostingSize": json.Number("9223372036854775808")},
			expectErrMsg: "maxPostingSize is '9223372036854775807' but must be between 0 and 4294967295",
		},
		{
			name:         "maxPostingSize above the int64 range, as a float",
			input:        map[string]interface{}{"maxPostingSize": float64(1e30)},
			expectErrMsg: "maxPostingSize is '",
		},
		{
			name:         "kCentroids negative",
			input:        map[string]interface{}{"kCentroids": json.Number("-1")},
			expectErrMsg: "kCentroids is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "kCentroids negative, as a float",
			input:        map[string]interface{}{"kCentroids": float64(-1)},
			expectErrMsg: "kCentroids is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "kCentroids one above the uint32 range",
			input:        map[string]interface{}{"kCentroids": json.Number("4294967296")},
			expectErrMsg: "kCentroids is '4294967296' but must be between 0 and 4294967295",
		},
		{
			name:         "kCentroids that a cast would wrap to a small value",
			input:        map[string]interface{}{"kCentroids": json.Number("4294967301")},
			expectErrMsg: "kCentroids is '4294967301' but must be between 0 and 4294967295",
		},
		{
			name:         "kCentroids above the int64 range",
			input:        map[string]interface{}{"kCentroids": json.Number("9223372036854775808")},
			expectErrMsg: "kCentroids is '9223372036854775807' but must be between 0 and 4294967295",
		},
		{
			name:         "kCentroids above the int64 range, as a float",
			input:        map[string]interface{}{"kCentroids": float64(1e30)},
			expectErrMsg: "kCentroids is '",
		},
		{
			name:         "kDocsToScore negative",
			input:        map[string]interface{}{"kDocsToScore": json.Number("-1")},
			expectErrMsg: "kDocsToScore is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "kDocsToScore negative, as a float",
			input:        map[string]interface{}{"kDocsToScore": float64(-1)},
			expectErrMsg: "kDocsToScore is '-1' but must be between 0 and 4294967295",
		},
		{
			name:         "kDocsToScore one above the uint32 range",
			input:        map[string]interface{}{"kDocsToScore": json.Number("4294967296")},
			expectErrMsg: "kDocsToScore is '4294967296' but must be between 0 and 4294967295",
		},
		{
			name:         "kDocsToScore that a cast would wrap to a small value",
			input:        map[string]interface{}{"kDocsToScore": json.Number("4294967301")},
			expectErrMsg: "kDocsToScore is '4294967301' but must be between 0 and 4294967295",
		},
		{
			name:         "kDocsToScore above the int64 range",
			input:        map[string]interface{}{"kDocsToScore": json.Number("9223372036854775808")},
			expectErrMsg: "kDocsToScore is '9223372036854775807' but must be between 0 and 4294967295",
		},
		{
			name:         "kDocsToScore above the int64 range, as a float",
			input:        map[string]interface{}{"kDocsToScore": float64(1e30)},
			expectErrMsg: "kDocsToScore is '",
		},
		{
			name:     "kCentroids at the top of the uint32 range",
			input:    map[string]interface{}{"kCentroids": json.Number("4294967295")},
			expected: withDefaults(func(uc *UserConfig) { uc.KCentroids = math.MaxUint32 }),
		},
		{
			name:     "kDocsToScore at the top of the uint32 range",
			input:    map[string]interface{}{"kDocsToScore": json.Number("4294967295")},
			expected: withDefaults(func(uc *UserConfig) { uc.KDocsToScore = math.MaxUint32 }),
		},
		{
			name:         "maxPostingSize below the floor",
			input:        map[string]interface{}{"maxPostingSize": json.Number("3")},
			expectErrMsg: "maxPostingSize is '3' but must be at least 4",
		},
		{
			name:         "kCentroids zero",
			input:        map[string]interface{}{"kCentroids": json.Number("0")},
			expectErrMsg: "kCentroids must be greater than 0",
		},
		{
			name:         "kDocsToScore zero",
			input:        map[string]interface{}{"kDocsToScore": json.Number("0")},
			expectErrMsg: "kDocsToScore must be greater than 0",
		},
		{
			name:         "efConstruction zero",
			input:        map[string]interface{}{"efConstruction": json.Number("0")},
			expectErrMsg: "efConstruction must be greater than 0",
		},
		{
			name:         "multivector disabled",
			input:        map[string]interface{}{"multivector": map[string]interface{}{"enabled": false}},
			expectErrMsg: "multivector must be enabled",
		},
		{
			name: "muvera enabled",
			input: map[string]interface{}{"multivector": map[string]interface{}{
				"enabled": true,
				"muvera":  map[string]interface{}{"enabled": true},
			}},
			expectErrMsg: "does not support muvera",
		},
		{
			name: "aggregation other than maxSim",
			input: map[string]interface{}{"multivector": map[string]interface{}{
				"enabled":     true,
				"aggregation": "bogus",
			}},
			expectErrMsg: "aggregation",
		},
		{
			name:         "pq refused",
			input:        map[string]interface{}{"pq": map[string]interface{}{"enabled": true}},
			expectErrMsg: "pq is not supported",
		},
		{
			name:         "rq refused even disabled",
			input:        map[string]interface{}{"rq": map[string]interface{}{"enabled": false}},
			expectErrMsg: "rq is not supported",
		},
		{
			name:         "sq refused",
			input:        map[string]interface{}{"sq": map[string]interface{}{"enabled": true}},
			expectErrMsg: "sq is not supported",
		},
		{
			name:         "bq refused",
			input:        map[string]interface{}{"bq": map[string]interface{}{"enabled": true}},
			expectErrMsg: "bq is not supported",
		},
	}

	for _, isMultiVector := range []bool{false, true} {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				cfg, err := ParseAndValidateConfig(test.input, isMultiVector)
				if test.expectErrMsg != "" {
					require.ErrorContains(t, err, test.expectErrMsg)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, test.expected, cfg)
			})
		}
	}
}

// TestValidateUpperBounds pins the schema-write bounds on maxPostingSize,
// efConstruction, kCentroids and kDocsToScore, and that the parser, which
// also runs at startup, does not apply them.
func TestValidateUpperBounds(t *testing.T) {
	tests := []struct {
		name           string
		maxPostingSize string
		efConstruction string
		kCentroids     string
		kDocsToScore   string
		wantErr        string
	}{
		{name: "the default", maxPostingSize: "64"},
		{name: "the maximum", maxPostingSize: "100000"},
		{
			name: "the maximum plus one", maxPostingSize: "100001",
			wantErr: "maxPostingSize is 100001 but must be at most 100000",
		},
		{name: "efConstruction at the maximum", efConstruction: "1024"},
		{
			name: "efConstruction at the maximum plus one", efConstruction: "1025",
			wantErr: "efConstruction is 1025 but must be at most 1024",
		},
		{name: "kCentroids at the maximum", kCentroids: "4096"},
		{
			name: "kCentroids at the maximum plus one", kCentroids: "4097",
			wantErr: "kCentroids is 4097 but must be at most 4096",
		},
		{name: "kDocsToScore at the maximum", kDocsToScore: "20000"},
		{
			name: "kDocsToScore at the maximum plus one", kDocsToScore: "20001",
			wantErr: "kDocsToScore is 20001 but must be at most 20000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := map[string]interface{}{}
			if tt.maxPostingSize != "" {
				input["maxPostingSize"] = json.Number(tt.maxPostingSize)
			}
			if tt.efConstruction != "" {
				input["efConstruction"] = json.Number(tt.efConstruction)
			}
			if tt.kCentroids != "" {
				input["kCentroids"] = json.Number(tt.kCentroids)
			}
			if tt.kDocsToScore != "" {
				input["kDocsToScore"] = json.Number(tt.kDocsToScore)
			}
			parsed, err := ParseAndValidateConfig(input, true)
			require.NoError(t, err)
			err = ValidateUpperBounds(parsed.(UserConfig))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
