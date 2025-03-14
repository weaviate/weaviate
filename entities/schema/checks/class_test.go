package checks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestHasLegacyVectorIndex(t *testing.T) {
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
			require.Equal(t, tt.want, HasLegacyVectorIndex(tt.class))
		})
	}
}
