//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestFromAndToModel(t *testing.T) {
	tests := []struct {
		name       string
		inputModel *models.InvertedIndexConfig
	}{
		{name: "empty", inputModel: &models.InvertedIndexConfig{}},
		{name: "non pointer elements", inputModel: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 3,
			IndexTimestamps:        true,
			IndexNullState:         true,
			IndexPropertyLength:    true,
		}},
		{name: "only pointer elements", inputModel: &models.InvertedIndexConfig{
			Bm25:      &models.BM25Config{K1: 1.0, B: 1.0},
			Stopwords: &models.StopwordConfig{Additions: []string{"1", "2"}, Preset: "1", Removals: []string{"1", "2"}},
		}},
		{name: "only bm25", inputModel: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{K1: 1.0, B: 1.0},
		}},
		{name: "only bm25, k1", inputModel: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{K1: 1.0},
		}},
		{name: "only bm25, b", inputModel: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{B: 1.0},
		}},
		{name: "only stopwords, additions", inputModel: &models.InvertedIndexConfig{
			Stopwords: &models.StopwordConfig{Additions: []string{"1", "2"}},
		}},
		{name: "only stopwords, preset", inputModel: &models.InvertedIndexConfig{
			Bm25:      &models.BM25Config{K1: 1.0, B: 1.0},
			Stopwords: &models.StopwordConfig{Preset: "1"},
		}},
		{name: "only stopwords, removals", inputModel: &models.InvertedIndexConfig{
			Bm25:      &models.BM25Config{K1: 1.0, B: 1.0},
			Stopwords: &models.StopwordConfig{Removals: []string{"1", "2"}},
		}},
		{name: "all elements", inputModel: &models.InvertedIndexConfig{
			Bm25:                   &models.BM25Config{K1: 1.0, B: 1.0},
			Stopwords:              &models.StopwordConfig{Additions: []string{"1", "2"}, Preset: "1", Removals: []string{"1", "2"}},
			CleanupIntervalSeconds: 3,
			IndexTimestamps:        true,
			IndexNullState:         true,
			IndexPropertyLength:    true,
		}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			i := InvertedIndexConfig{}

			(&i).FromModel(tc.inputModel)
			m := i.ToModel()

			require.Equal(t, tc.inputModel, m)
		})
	}
}
