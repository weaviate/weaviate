package hnsw

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_UserConfig(t *testing.T) {
	type test struct {
		name     string
		input    interface{}
		expected UserConfig
	}

	tests := []test{
		{
			name:  "nothing specified, all defaults",
			input: nil,
			expected: UserConfig{
				CleanupIntervalSeconds: DefaultCleanupIntervalSeconds,
				MaxConnections:         DefaultMaxConnections,
				EFConstruction:         DefaultEFConstruction,
				VectorCacheMaxObjects:  DefaultVectorCacheMaxObjects,
				EF:                     DefaultEF,
				Skip:                   DefaultSkip,
				FlatSearchCutoff:       DefaultFlatSearchCutoff,
				DynamicEFMin:           DefaultDynamicEFMin,
				DynamicEFMax:           DefaultDynamicEFMax,
				DynamicEFFactor:        DefaultDynamicEFFactor,
				Distance:               DefaultDistanceMetric,
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
				VectorCacheMaxObjects:  DefaultVectorCacheMaxObjects,
				EF:                     DefaultEF,
				FlatSearchCutoff:       DefaultFlatSearchCutoff,
				DynamicEFMin:           DefaultDynamicEFMin,
				DynamicEFMax:           DefaultDynamicEFMax,
				DynamicEFFactor:        DefaultDynamicEFFactor,
				Distance:               DefaultDistanceMetric,
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
				Distance:               DefaultDistanceMetric,
			},
		},
		{
			// opposed to from the API
			name: "with rounded vectorCacheMaxObjects that would otherwise overflow",
			input: map[string]interface{}{
				"cleanupIntervalSeconds": float64(11),
				"maxConnections":         float64(12),
				"efConstruction":         float64(13),
				"vectorCacheMaxObjects":  float64(9223372036854776000),
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
				VectorCacheMaxObjects:  math.MaxInt64,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               DefaultDistanceMetric,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := ParseUserConfig(test.input)
			assert.Nil(t, err)

			assert.Equal(t, test.expected, cfg)
		})
	}
}
