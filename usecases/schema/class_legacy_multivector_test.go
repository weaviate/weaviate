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

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// TestLegacyMultiVectorConfigErrorNamesTheType pins that the error for a
// multi-vector index configured at class level says which index type the
// user chose, on the parser's path and on the handler's.
func TestLegacyMultiVectorConfigErrorNamesTheType(t *testing.T) {
	const prefix = "class.VectorIndexConfig multi vector type index type is only configurable using named vectors"

	tests := []struct {
		name   string
		typ    string
		config map[string]interface{}
	}{
		{
			name:   "tachiomfresh",
			typ:    "tachiomfresh",
			config: map[string]interface{}{"maxPostingSize": float64(64)},
		},
		{
			name:   "hnsw with a multi-vector config",
			typ:    "hnsw",
			config: map[string]interface{}{"multivector": map[string]interface{}{"enabled": true}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := prefix + `: vector index type "` + tt.typ + `"`
			handler := newTestHandlerWithRealVectorConfigParser(t)
			class := func() *models.Class {
				return &models.Class{Class: "Legacy", Vectorizer: "none", VectorIndexType: tt.typ, VectorIndexConfig: tt.config}
			}

			t.Run("parser", func(t *testing.T) {
				require.EqualError(t, handler.parser.parseVectorIndexConfig(class()), want)
			})
			t.Run("handler", func(t *testing.T) {
				require.EqualError(t, handler.validateVectorSettingsAgainst(class(), nil), want)
			})
		})
	}
}
