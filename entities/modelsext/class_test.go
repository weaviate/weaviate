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

package modelsext

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestIsVectorIndexDropped(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  models.VectorConfig
		want bool
	}{
		{
			name: "none type is dropped",
			cfg:  models.VectorConfig{VectorIndexType: VectorIndexTypeNone},
			want: true,
		},
		{
			name: "hnsw type is not dropped",
			cfg:  models.VectorConfig{VectorIndexType: "hnsw"},
			want: false,
		},
		{
			name: "flat type is not dropped",
			cfg:  models.VectorConfig{VectorIndexType: "flat"},
			want: false,
		},
		{
			name: "empty type is not dropped",
			cfg:  models.VectorConfig{VectorIndexType: ""},
			want: false,
		},
		{
			name: "none with other fields set",
			cfg: models.VectorConfig{
				VectorIndexType: VectorIndexTypeNone,
				Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
			},
			want: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsVectorIndexDropped(tt.cfg))
		})
	}
}

func TestClassHasLegacyVectorIndex(t *testing.T) {
	for _, tt := range []struct {
		name  string
		class *models.Class
		want  bool
	}{
		{
			name: "all fields are empty or nil",
			class: &models.Class{
				Vectorizer:        "",
				VectorIndexConfig: nil,
				VectorIndexType:   "",
			},
			want: false,
		},
		{
			name: "Vectorizer is not empty",
			class: &models.Class{
				Vectorizer: "some_vectorizer",
			},
			want: true,
		},
		{
			name: "VectorIndexConfig is not nil",
			class: &models.Class{
				VectorIndexConfig: map[string]interface{}{"distance": "cosine"},
			},
			want: true,
		},
		{
			name: "VectorIndexType is not empty",
			class: &models.Class{
				VectorIndexType: "hnsw",
			},
			want: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ClassHasLegacyVectorIndex(tt.class))
		})
	}
}

func TestClassGetVectorConfig(t *testing.T) {
	var (
		customConfig = models.VectorConfig{
			Vectorizer:      "custom-vectorizer",
			VectorIndexType: "flat",
			VectorIndexConfig: map[string]interface{}{
				"distance": "euclidean",
			},
		}

		legacyConfig = models.VectorConfig{
			Vectorizer:      "legacy-vectorizer",
			VectorIndexType: "hnsw",
			VectorIndexConfig: map[string]interface{}{
				"distance": "cosine",
			},
		}

		mixedClass = &models.Class{
			Vectorizer:      "legacy-vectorizer",
			VectorIndexType: "hnsw",
			VectorIndexConfig: map[string]interface{}{
				"distance": "cosine",
			},
			VectorConfig: map[string]models.VectorConfig{
				"custom": customConfig,
			},
		}
	)

	for _, tt := range []struct {
		name         string
		class        *models.Class
		targetVector string

		expectConfig *models.VectorConfig
	}{
		{
			name:         "named vector not present",
			class:        mixedClass,
			targetVector: "non-existent",

			expectConfig: nil,
		},
		{
			name:         "legacy vector via empty string",
			class:        mixedClass,
			targetVector: "",

			expectConfig: &legacyConfig,
		},
		{
			name:         "legacy vector via default named target vector",
			class:        mixedClass,
			targetVector: DefaultNamedVectorName,

			expectConfig: &legacyConfig,
		},
		{
			name:         "named vector via its name",
			class:        mixedClass,
			targetVector: "custom",

			expectConfig: &customConfig,
		},
		{
			name: "legacy vector without named vectors",
			class: &models.Class{
				Vectorizer:      "legacy-vectorizer",
				VectorIndexType: "hnsw",
				VectorIndexConfig: map[string]interface{}{
					"distance": "cosine",
				},
			},
			targetVector: "",

			expectConfig: &legacyConfig,
		},
		{
			name: "named vector without legacy vectors",
			class: &models.Class{
				VectorConfig: map[string]models.VectorConfig{
					"custom": customConfig,
				},
			},
			targetVector: "custom",

			expectConfig: &customConfig,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, ok := ClassGetVectorConfig(tt.class, tt.targetVector)
			if tt.expectConfig == nil {
				require.False(t, ok)
			} else {
				require.True(t, ok)
				require.Equal(t, *tt.expectConfig, cfg)
			}
		})
	}
}
