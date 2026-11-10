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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/restrictions"
)

// TestParseClassTachiomFresh: which classes the parser takes with a tachiomfresh
// vector, through the production config parser.
func TestParseClassTachiomFresh(t *testing.T) {
	named := func(vectorizer string, indexConfig map[string]interface{}) *models.Class {
		vector := models.VectorConfig{
			VectorIndexType: "tachiomfresh",
			Vectorizer:      map[string]interface{}{vectorizer: map[string]interface{}{}},
		}
		if indexConfig != nil {
			vector.VectorIndexConfig = indexConfig
		}
		return &models.Class{Class: "Tachiom", VectorConfig: map[string]models.VectorConfig{"mv": vector}}
	}
	tests := []struct {
		name    string
		class   *models.Class
		wantErr string
	}{
		{
			name: "named, a multi-vector vectorizer", class: named("colbert", nil),
			wantErr: "multi vector index is not supported for vector index type: \"tachiomfresh\", only supported type is hnsw",
		},
		{name: "named, no vectorizer", class: named("none", nil)},
		{
			name: "named, a single-vector vectorizer", class: named("text2vec-contextionary", nil),
			wantErr: "doesn't support multi vectors",
		},
		{
			name: "named, the l2 distance", class: named("none", map[string]interface{}{"distance": "l2-squared"}),
			wantErr: "only supports 'dot'",
		},
		{
			name: "named, the cosine distance", class: named("none", map[string]interface{}{"distance": "cosine"}),
			wantErr: "only supports 'dot'",
		},
		{
			name: "class level",
			class: &models.Class{
				Class: "Tachiom", Vectorizer: "none", VectorIndexType: "tachiomfresh",
				VectorIndexConfig: map[string]interface{}{"maxPostingSize": float64(64)},
			},
			wantErr: "only configurable using named vectors",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := newTestHandlerWithRealVectorConfigParser(t)
			err := handler.parser.ParseClass(tt.class)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			parsed := tt.class.VectorConfig["mv"].VectorIndexConfig.(config.VectorIndexConfig)
			assert.Equal(t, "tachiomfresh", parsed.IndexType())
			assert.Equal(t, ent.DefaultDistance, parsed.DistanceName())
		})
	}
}

// TestTachiomFreshRestrictions: the allow-list applies to tachiomfresh, and the
// compression allow-list does not, as it has no compression settings.
func TestTachiomFreshRestrictions(t *testing.T) {
	t.Run("allow-list", func(t *testing.T) {
		for _, tt := range []struct {
			name    string
			allow   []string
			wantErr bool
		}{
			{name: "no list"},
			{name: "listed", allow: []string{"tachiomfresh", "hnsw"}},
			{name: "not listed", allow: []string{"hnsw"}, wantErr: true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				handler, _ := newTestHandler(t, &fakeDB{})
				if tt.allow != nil {
					handler.config.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue(tt.allow)
				}
				require.NoError(t, handler.validateVectorIndexTypeBasic(vectorindex.VectorIndexTypeTachiomFresh))
				err := handler.validateVectorIndexTypeAllowList(vectorindex.VectorIndexTypeTachiomFresh, false)
				if !tt.wantErr {
					require.NoError(t, err)
					return
				}
				v, ok := restrictions.AsViolation(err)
				require.True(t, ok, "%v", err)
				assert.Equal(t, restrictions.RestrictionVectorIndexType, v.Restriction)
				assert.Equal(t, "tachiomfresh", v.Value)
			})
		}
	})

	t.Run("compression allow-list is skipped", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})
		handler.config.Restrictions.AllowedCompressionTypes = runtime.NewDynamicValue([]string{"rq-8"})
		require.NoError(t, handler.validateAllowedCompression(vectorindex.VectorIndexTypeTachiomFresh, ent.NewDefaultUserConfig()))
	})
}
