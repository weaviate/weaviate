//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"math"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
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
	t.Run("with invalid cleanup interval", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: -1,
		}

		err := ValidateConfig(in)
		assert.EqualError(t, err, "cleanup interval seconds must be > 0")
	})

	t.Run("with invalid BM25.k1", func(t *testing.T) {
		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 1,
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
			CleanupIntervalSeconds: 1,
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
			CleanupIntervalSeconds: 1,
			Bm25: &models.BM25Config{
				K1: 1,
				B:  0.1,
			},
		}

		err := ValidateConfig(in)
		assert.Nil(t, err)
	})
}

func TestConfigFromModel(t *testing.T) {
	t.Run("with all fields set", func(t *testing.T) {
		interval := int64(1)
		k1 := 1.12
		b := 0.7

		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
			Bm25: &models.BM25Config{
				K1: float32(k1),
				B:  float32(b),
			},
		}

		expected := schema.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
			BM25: schema.BM25Config{
				K1: k1,
				B:  b,
			},
		}

		conf := ConfigFromModel(in)
		assert.Equal(t, conf.CleanupIntervalSeconds, expected.CleanupIntervalSeconds)
		assert.True(t, almostEqual(t, conf.BM25.K1, expected.BM25.K1))
		assert.True(t, almostEqual(t, conf.BM25.B, expected.BM25.B))
	})

	t.Run("with no BM25 params set", func(t *testing.T) {
		interval := int64(1)

		in := &models.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
		}

		expected := schema.InvertedIndexConfig{
			CleanupIntervalSeconds: interval,
			BM25: schema.BM25Config{
				K1: float64(config.DefaultBM25k1),
				B:  float64(config.DefaultBM25b),
			},
		}

		conf := ConfigFromModel(in)
		assert.Equal(t, conf.CleanupIntervalSeconds, expected.CleanupIntervalSeconds)
		assert.True(t, almostEqual(t, conf.BM25.K1, expected.BM25.K1))
		assert.True(t, almostEqual(t, conf.BM25.B, expected.BM25.B))
	})
}
