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

package traverser

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestGetTargetVectorOrDefault(t *testing.T) {
	tests := []struct {
		name          string
		class         *models.Class
		targetVectors []string
		want          []string
		wantErr       bool
		wantGetClass  bool
	}{
		{
			name:          "target vectors provided are returned without reading a class",
			targetVectors: []string{"foo"},
			want:          []string{"foo"},
			wantGetClass:  false,
		},
		{
			name:         "no target vectors, legacy vector index defaults to empty name",
			class:        &models.Class{Vectorizer: "text2vec-contextionary"},
			want:         []string{""},
			wantGetClass: true,
		},
		{
			name:         "no target vectors, empty vector config defaults to empty name",
			class:        &models.Class{VectorConfig: map[string]models.VectorConfig{}},
			want:         []string{""},
			wantGetClass: true,
		},
		{
			name: "no target vectors, single named vector returns its name",
			class: &models.Class{VectorConfig: map[string]models.VectorConfig{
				"named": {},
			}},
			want:         []string{"named"},
			wantGetClass: true,
		},
		{
			name: "no target vectors, multiple named vectors errors",
			class: &models.Class{VectorConfig: map[string]models.VectorConfig{
				"a": {},
				"b": {},
			}},
			wantErr:      true,
			wantGetClass: true,
		},
		{
			name:         "no target vectors, missing class errors",
			class:        nil,
			wantErr:      true,
			wantGetClass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			getClass := func(string) *models.Class {
				called = true
				return tt.class
			}

			got, err := NewTargetParamHelper().GetTargetVectorOrDefault(getClass, "SomeClass", tt.targetVectors)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.Equal(t, tt.wantGetClass, called)
		})
	}
}
