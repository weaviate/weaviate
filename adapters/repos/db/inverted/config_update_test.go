package inverted

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateUserConfigUpdate(t *testing.T) {
	validInitial := &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 1,
		Bm25: &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		},
	}

	t.Run("with valid updated config all fields", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 2,
			Bm25: &models.BM25Config{
				K1: 1.3,
				B:  0.778,
			},
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.Nil(t, err)
	})

	t.Run("with valid updated config missing BM25", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 2,
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.Nil(t, err)
		assert.Equal(t, validInitial.Bm25.K1, updated.Bm25.K1)
		assert.Equal(t, validInitial.Bm25.B, updated.Bm25.B)
	})

	t.Run("with invalid cleanup interval", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: -1,
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "cleanup interval seconds must be > 0")
	})

	t.Run("with invalid updated Bm25 config", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 1,
			Bm25: &models.BM25Config{
				K1: 1.2,
				B:  1.2,
			},
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "BM25.b must be <= 0 and <= 1")
	})
}
