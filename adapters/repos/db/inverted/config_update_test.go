//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestValidateUserConfigUpdate(t *testing.T) {
	validInitial := &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 1,
		Bm25: &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		},
		Stopwords: &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		},
	}

	t.Run("with valid updated config all fields", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 2,
			Bm25: &models.BM25Config{
				K1: 1.3,
				B:  0.778,
			},
			Stopwords: &models.StopwordConfig{
				Preset:    "en",
				Additions: []string{"star", "nebula"},
				Removals:  []string{"the", "a"},
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

	t.Run("with valid updated config missing Stopwords", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 2,
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.Nil(t, err)
		assert.Equal(t, validInitial.Stopwords.Preset, updated.Stopwords.Preset)
		assert.Equal(t, validInitial.Stopwords.Additions, updated.Stopwords.Additions)
		assert.Equal(t, validInitial.Stopwords.Removals, updated.Stopwords.Removals)
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

	t.Run("with invalid updated Stopwords preset config", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 1,
			Stopwords: &models.StopwordConfig{
				Preset: "mongolian",
			},
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "stopwordPreset 'mongolian' does not exist")
	})

	t.Run("with invalid updated Stopwords addition/removal config", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 1,
			Stopwords: &models.StopwordConfig{
				Additions: []string{"duplicate"},
				Removals:  []string{"duplicate"},
			},
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "found 'duplicate' in both stopwords.additions and stopwords.removals")
	})

	t.Run("with invalid updated inverted index null state change", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			IndexNullState: true,
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "IndexNullState cannot be changed when updating a schema")
	})

	t.Run("with invalid updated inverted index property length change", func(t *testing.T) {
		updated := &models.InvertedIndexConfig{
			IndexPropertyLength: true,
		}

		err := ValidateUserConfigUpdate(validInitial, updated)
		require.EqualError(t, err, "IndexPropertyLength cannot be changed when updating a schema")
	})
}
