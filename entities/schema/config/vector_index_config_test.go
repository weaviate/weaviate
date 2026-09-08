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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestExtractVectorConfigs(t *testing.T) {
	legacyConfig := &MockVectorIndexConfig{}
	namedConfig := models.VectorConfig{
		Vectorizer:        map[string]any{"none": map[string]any{}},
		VectorIndexType:   "hnsw",
		VectorIndexConfig: &MockVectorIndexConfig{},
	}

	tests := []struct {
		name    string
		class   *models.Class
		want    map[string]models.VectorConfig
		wantErr bool
	}{
		{
			name:  "no vectors at all",
			class: &models.Class{Class: "C"},
			want:  nil,
		},
		{
			name: "named vectors only",
			class: &models.Class{
				Class:        "C",
				VectorConfig: map[string]models.VectorConfig{"named": namedConfig},
			},
			want: map[string]models.VectorConfig{"named": namedConfig},
		},
		{
			name: "legacy vector only",
			class: &models.Class{
				Class:             "C",
				Vectorizer:        "none",
				VectorIndexType:   "hnsw",
				VectorIndexConfig: legacyConfig,
			},
			want: map[string]models.VectorConfig{
				"": {Vectorizer: "none", VectorIndexType: "hnsw", VectorIndexConfig: legacyConfig},
			},
		},
		{
			// named vectors added to a class created with a legacy vector: both
			// must be reported, the legacy one under the empty key
			name: "legacy and named vectors",
			class: &models.Class{
				Class:             "C",
				Vectorizer:        "none",
				VectorIndexType:   "hnsw",
				VectorIndexConfig: legacyConfig,
				VectorConfig:      map[string]models.VectorConfig{"named": namedConfig},
			},
			want: map[string]models.VectorConfig{
				"named": namedConfig,
				"":      {Vectorizer: "none", VectorIndexType: "hnsw", VectorIndexConfig: legacyConfig},
			},
		},
		{
			name: "legacy vector with unparsed config",
			class: &models.Class{
				Class:             "C",
				Vectorizer:        "none",
				VectorIndexType:   "hnsw",
				VectorIndexConfig: map[string]any{"distance": "cosine"},
			},
			wantErr: true,
		},
		{
			name: "legacy and named vectors with unparsed legacy config",
			class: &models.Class{
				Class:             "C",
				Vectorizer:        "none",
				VectorIndexType:   "hnsw",
				VectorIndexConfig: map[string]any{"distance": "cosine"},
				VectorConfig:      map[string]models.VectorConfig{"named": namedConfig},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractVectorConfigs(tt.class)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractVectorConfigs_DoesNotMutateClass(t *testing.T) {
	class := &models.Class{
		Class:             "C",
		Vectorizer:        "none",
		VectorIndexType:   "hnsw",
		VectorIndexConfig: &MockVectorIndexConfig{},
		VectorConfig: map[string]models.VectorConfig{
			"named": {Vectorizer: map[string]any{"none": map[string]any{}}, VectorIndexType: "hnsw", VectorIndexConfig: &MockVectorIndexConfig{}},
		},
	}

	_, err := ExtractVectorConfigs(class)
	require.NoError(t, err)

	// the merged result must be a copy: the class's own map, shared with the
	// schema, must not receive the legacy entry
	require.Len(t, class.VectorConfig, 1)
	_, exists := class.VectorConfig[""]
	assert.False(t, exists)
}
