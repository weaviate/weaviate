//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/stretchr/testify/assert"
)

func Test_ValidConfig(t *testing.T) {
	err := validConfig().Validate()
	assert.Nil(t, err)
}

func Test_InValidConfig(t *testing.T) {
	type test struct {
		config      func() Config
		expectedErr error
	}

	tests := []test{
		{
			config: func() Config {
				v := validConfig()
				v.ID = ""
				return v
			},
			expectedErr: errors.Errorf("id cannot be empty"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.RootPath = ""
				return v
			},
			expectedErr: errors.Errorf("rootPath cannot be empty"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.MakeCommitLoggerThunk = nil
				return v
			},
			expectedErr: errors.Errorf("makeCommitLoggerThunk cannot be nil"),
		},
		{
			config: func() Config {
				v := validConfig()
				v.VectorForIDThunk = nil
				return v
			},
			expectedErr: errors.Errorf("vectorForIDThunk cannot be nil"),
		},
	}

	for _, test := range tests {
		t.Run(test.expectedErr.Error(), func(t *testing.T) {
			err := test.config().Validate()
			assert.Equal(t, test.expectedErr.Error(), err.Error())
		})
	}
}

func validConfig() Config {
	return Config{
		RootPath:              "some path",
		ID:                    "someid",
		MakeCommitLoggerThunk: func() (CommitLogger, error) { return nil, nil },
		VectorForIDThunk:      func(context.Context, uint64) ([]float32, error) { return nil, nil },
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
	}
}

func Test_UserConfig(t *testing.T) {
	type test struct {
		name     string
		input    interface{}
		expected ent.UserConfig
	}

	tests := []test{
		{
			name:  "nothing specified, all defaults",
			input: nil,
			expected: ent.UserConfig{
				CleanupIntervalSeconds: ent.DefaultCleanupIntervalSeconds,
				MaxConnections:         ent.DefaultMaxConnections,
				EFConstruction:         ent.DefaultEFConstruction,
				VectorCacheMaxObjects:  ent.DefaultVectorCacheMaxObjects,
				EF:                     ent.DefaultEF,
				Skip:                   ent.DefaultSkip,
				FlatSearchCutoff:       ent.DefaultFlatSearchCutoff,
				DynamicEFMin:           ent.DefaultDynamicEFMin,
				DynamicEFMax:           ent.DefaultDynamicEFMax,
				DynamicEFFactor:        ent.DefaultDynamicEFFactor,
				Distance:               ent.DefaultDistanceMetric,
			},
		},

		{
			name: "with maximum connections",
			input: map[string]interface{}{
				"maxConnections": json.Number("100"),
			},
			expected: ent.UserConfig{
				CleanupIntervalSeconds: ent.DefaultCleanupIntervalSeconds,
				MaxConnections:         100,
				EFConstruction:         ent.DefaultEFConstruction,
				VectorCacheMaxObjects:  ent.DefaultVectorCacheMaxObjects,
				EF:                     ent.DefaultEF,
				FlatSearchCutoff:       ent.DefaultFlatSearchCutoff,
				DynamicEFMin:           ent.DefaultDynamicEFMin,
				DynamicEFMax:           ent.DefaultDynamicEFMax,
				DynamicEFFactor:        ent.DefaultDynamicEFFactor,
				Distance:               ent.DefaultDistanceMetric,
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
			expected: ent.UserConfig{
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
			expected: ent.UserConfig{
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
			expected: ent.UserConfig{
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
			expected: ent.UserConfig{
				CleanupIntervalSeconds: 11,
				MaxConnections:         12,
				EFConstruction:         13,
				VectorCacheMaxObjects:  14,
				EF:                     15,
				FlatSearchCutoff:       16,
				DynamicEFMin:           17,
				DynamicEFMax:           18,
				DynamicEFFactor:        19,
				Distance:               ent.DefaultDistanceMetric,
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
