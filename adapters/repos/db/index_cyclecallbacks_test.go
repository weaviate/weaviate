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

package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestVectorTombstoneCleanupInterval(t *testing.T) {
	defaultInterval := time.Duration(hnsw.DefaultCleanupIntervalSeconds) * time.Second

	tests := []struct {
		name     string
		configs  map[string]schemaConfig.VectorIndexConfig
		expected time.Duration
	}{
		{
			name:     "no configs falls back to the hnsw default",
			configs:  map[string]schemaConfig.VectorIndexConfig{},
			expected: defaultInterval,
		},
		{
			name: "legacy hnsw config only",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"": hnsw.UserConfig{CleanupIntervalSeconds: 5},
			},
			expected: 5 * time.Second,
		},
		{
			name: "single named hnsw config is honoured",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"default": hnsw.UserConfig{CleanupIntervalSeconds: 5},
			},
			expected: 5 * time.Second,
		},
		{
			name: "named dynamic config uses its hnsw settings",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"default": dynamicent.UserConfig{HnswUC: hnsw.UserConfig{CleanupIntervalSeconds: 7}},
			},
			expected: 7 * time.Second,
		},
		{
			name: "shortest interval across named vectors wins",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"slow":   hnsw.UserConfig{CleanupIntervalSeconds: 600},
				"fast":   hnsw.UserConfig{CleanupIntervalSeconds: 10},
				"medium": dynamicent.UserConfig{HnswUC: hnsw.UserConfig{CleanupIntervalSeconds: 60}},
			},
			expected: 10 * time.Second,
		},
		{
			name: "shortest interval across legacy and named vectors wins",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"":    hnsw.UserConfig{CleanupIntervalSeconds: 300},
				"foo": hnsw.UserConfig{CleanupIntervalSeconds: 5},
			},
			expected: 5 * time.Second,
		},
		{
			name: "legacy shorter than named wins",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"":    hnsw.UserConfig{CleanupIntervalSeconds: 5},
				"foo": hnsw.UserConfig{CleanupIntervalSeconds: 300},
			},
			expected: 5 * time.Second,
		},
		{
			name: "flat only falls back to the hnsw default",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"default": flat.UserConfig{},
			},
			expected: defaultInterval,
		},
		{
			name: "flat vectors do not affect the hnsw interval",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"flat": flat.UserConfig{},
				"hnsw": hnsw.UserConfig{CleanupIntervalSeconds: 42},
			},
			expected: 42 * time.Second,
		},
		{
			name: "non-positive intervals are ignored",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"zero":     hnsw.UserConfig{CleanupIntervalSeconds: 0},
				"negative": hnsw.UserConfig{CleanupIntervalSeconds: -1},
				"set":      hnsw.UserConfig{CleanupIntervalSeconds: 30},
			},
			expected: 30 * time.Second,
		},
		{
			name: "only non-positive intervals fall back to the hnsw default",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"":     hnsw.UserConfig{CleanupIntervalSeconds: 0},
				"zero": dynamicent.UserConfig{HnswUC: hnsw.UserConfig{CleanupIntervalSeconds: -5}},
			},
			expected: defaultInterval,
		},
		{
			name: "nil config entries are ignored",
			configs: map[string]schemaConfig.VectorIndexConfig{
				"nil": nil,
				"set": hnsw.UserConfig{CleanupIntervalSeconds: 3},
			},
			expected: 3 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, vectorTombstoneCleanupInterval(tt.configs))
		})
	}
}
