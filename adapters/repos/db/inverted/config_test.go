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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

const float64EqualityThreshold = 1e-6

func almostEqual(t *testing.T, a, b float64) bool {
	closeEnough := math.Abs(a-b) <= float64EqualityThreshold
	if !closeEnough {
		t.Logf("%f and %f differ by more than a threshold of %f",
			a, b, float64EqualityThreshold)
	}
	return closeEnough
}

func TestValidateConfig(t *testing.T) {
	t.Run("with invalid BM25.k1", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: -1,
				B:  0.7,
			},
		}

		err := ValidateConfig(in)
		assert.EqualError(t, err, "BM25.k1 must be >= 0")
	})

	t.Run("with invalid BM25.b", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: 1,
				B:  1.001,
			},
		}

		err := ValidateConfig(in)
		assert.EqualError(t, err, "BM25.b must be <= 0 and <= 1")
	})

	t.Run("with valid config", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: 1,
				B:  0.1,
			},
		}

		err := ValidateConfig(in)
		assert.Nil(t, err)
	})

	t.Run("with nonexistent stopword preset", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			Stopwords: &models.StopwordConfig{
				Preset: "DNE",
			},
		}

		err := ValidateConfig(in)
		assert.EqualError(t, err, "stopwordPreset 'DNE' does not exist")
	})

	t.Run("with whitespace stopword additions", func(t *testing.T) {
		additions := [][]string{
			{"bats", " "},
			{""},
			{"something", "   ", "skippable"},
		}

		for _, addList := range additions {
			in := &models.InvertedIndexConfig{
				Stopwords: &models.StopwordConfig{
					Additions: addList,
				},
			}

			err := ValidateConfig(in)
			assert.EqualError(t, err, "cannot use whitespace in stopword.additions")
		}
	})

	t.Run("with whitespace stopword removals", func(t *testing.T) {
		removals := [][]string{
			{"bats", " "},
			{""},
			{"something", "   ", "skippable"},
		}

		for _, remList := range removals {
			in := &models.InvertedIndexConfig{
				Stopwords: &models.StopwordConfig{
					Removals: remList,
				},
			}

			err := ValidateConfig(in)
			assert.EqualError(t, err, "cannot use whitespace in stopword.removals")
		}
	})

	t.Run("with shared additions/removals items", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			Stopwords: &models.StopwordConfig{
				Additions: []string{"some", "words", "are", "different"},
				Removals:  []string{"and", "some", "the", "same"},
			},
		}

		err := ValidateConfig(in)
		assert.EqualError(t, err,
			"found 'some' in both stopwords.additions and stopwords.removals")
	})

	t.Run("with additions that exist in preset", func(t *testing.T) {
		tests := []struct {
			additions      []string
			expectedLength int
		}{
			{
				additions:      []string{"superfluous", "extravagant", "a"},
				expectedLength: 2,
			},
			{
				additions:      []string{"a", "are", "the"},
				expectedLength: 0,
			},
			{
				additions:      []string{"everyone", "sleeps", "eventually"},
				expectedLength: 3,
			},
		}

		for _, test := range tests {
			in := &models.InvertedIndexConfig{
				Stopwords: &models.StopwordConfig{
					Preset:    "en",
					Additions: test.additions,
				},
			}

			err := ValidateConfig(in)
			assert.Nil(t, err)
			assert.Equal(t, test.expectedLength, len(in.Stopwords.Additions))
		}
	})
}

func TestConfigFromModel(t *testing.T) {
	t.Run("with all fields set", func(t *testing.T) {
		k1 := 1.12
		b := 0.7

		in := &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: float32(k1),
				B:  float32(b),
			},
			Stopwords: &models.StopwordConfig{
				Preset: "en",
			},
		}

		expected := schema.InvertedIndexConfig{
			BM25: schema.BM25Config{
				K1: k1,
				B:  b,
			},
			Stopwords: models.StopwordConfig{
				Preset: "en",
			},
		}

		conf := ConfigFromModel(in)
		assert.True(t, almostEqual(t, conf.BM25.K1, expected.BM25.K1))
		assert.True(t, almostEqual(t, conf.BM25.B, expected.BM25.B))
		assert.Equal(t, expected.Stopwords, conf.Stopwords)
	})

	t.Run("with no BM25 params set", func(t *testing.T) {
		interval := int64(1)

		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
		}

		expected := schema.InvertedIndexConfig{
			BM25: schema.BM25Config{
				K1: float64(config.DefaultBM25k1),
				B:  float64(config.DefaultBM25b),
			},
		}

		conf := ConfigFromModel(in)
		assert.True(t, almostEqual(t, conf.BM25.K1, expected.BM25.K1))
		assert.True(t, almostEqual(t, conf.BM25.B, expected.BM25.B))
	})

	t.Run("with no Stopword config set", func(t *testing.T) {
		interval := int64(1)

		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
		}

		expected := schema.InvertedIndexConfig{
			Stopwords: models.StopwordConfig{
				Preset: "en",
			},
		}

		conf := ConfigFromModel(in)
		assert.Equal(t, expected.Stopwords, conf.Stopwords)
	})
}
