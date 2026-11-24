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

package rest_search

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func model2vecOn(property string) models.VectorConfig {
	return models.VectorConfig{
		Vectorizer: map[string]any{
			"text2vec-model2vec": map[string]any{"vectorizeClassName": false, "properties": []any{property}},
		},
		VectorIndexType: "hnsw",
	}
}

func idsOf(t *testing.T, out map[string]any) []string {
	t.Helper()
	res := results(t, out)
	ids := make([]string, len(res))
	for i := range res {
		ids[i] = idOf(t, hit(t, out, i))
	}
	return ids
}

// TestRESTSearchMultiTarget covers the vectorizer-backed searches with
// several target vectors: near-text and hybrid, with per-target params.
func TestRESTSearchMultiTarget(t *testing.T) {
	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(restSearchServerURI(t))

	const class = "Article"
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "body", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{
			"title_vec": model2vecOn("title"),
			"body_vec":  model2vecOn("body"),
		},
	})
	defer helper.DeleteClass(t, class)

	articles := []struct{ title, body string }{
		{"spaceship galaxy adventure", "a crew travels between distant stars"},
		{"cooking pasta at home", "boil water, add salt and the noodles"},
		{"stock market basics", "shares, bonds and how prices move"},
		{"gardening for beginners", "plant tomatoes in spring and water them"},
		{"history of the roman empire", "emperors, legions and the senate"},
		{"learning to play guitar", "chords, strumming and finger exercises"},
	}
	objects := make([]*models.Object, len(articles))
	for i, a := range articles {
		objects[i] = &models.Object{Class: class, Properties: map[string]any{"title": a.title, "body": a.body}}
	}
	helper.CreateObjectsBatch(t, objects)

	targets := []string{"title_vec", "body_vec"}
	onlyTitle := map[string]any{"method": "manualWeights", "weights": map[string]float64{"title_vec": 1, "body_vec": 0}}

	t.Run("near-text with per-target params matches the single title target", func(t *testing.T) {
		status, out := postNearText(t, class, map[string]any{
			"query": []string{"rockets and planets"}, "limit": 3,
			"targetVectors":            targets,
			"targetCombination":        onlyTitle,
			"vectorSearchParams":       map[string]any{"ef": 32},
			"targetVectorSearchParams": map[string]any{"title_vec": map[string]any{"ef": 64}},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		got := idsOf(t, out)
		require.Len(t, got, 3)

		status, out = postNearText(t, class, map[string]any{
			"query": []string{"rockets and planets"}, "limit": 3,
			"targetVector": "title_vec", "vectorSearchParams": map[string]any{"ef": 64},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Equal(t, idsOf(t, out), got)
	})

	t.Run("near-text without a combination uses minimum", func(t *testing.T) {
		status, out := postNearText(t, class, map[string]any{
			"query": []string{"rockets and planets"}, "limit": 3, "targetVectors": targets,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 3)
	})

	t.Run("hybrid with per-target params matches the single title target", func(t *testing.T) {
		status, out := postHybrid(t, class, map[string]any{
			"query": "rockets and planets", "alpha": 1, "limit": 3,
			"targetVectors":            targets,
			"targetCombination":        onlyTitle,
			"targetVectorSearchParams": map[string]any{"title_vec": map[string]any{"ef": 64}, "body_vec": map[string]any{"ef": 16}},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		got := idsOf(t, out)
		require.Len(t, got, 3)

		status, out = postHybrid(t, class, map[string]any{
			"query": "rockets and planets", "alpha": 1, "limit": 3, "targetVector": "title_vec",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Equal(t, idsOf(t, out), got)
	})

	t.Run("an unknown per-target key is a 422", func(t *testing.T) {
		for searchType, query := range map[string]any{"near-text": []string{"rockets"}, "hybrid": "rockets"} {
			status, out := postSearch(t, class, searchType, map[string]any{
				"query": query, "limit": 3, "targetVectors": targets,
				"targetVectorSearchParams": map[string]any{"summary_vec": map[string]any{"ef": 64}},
			})
			require.Equal(t, http.StatusUnprocessableEntity, status, "%s: %v", searchType, out)
			require.Contains(t, errMessage(t, out), "targetVectorSearchParams[summary_vec]", searchType)
		}
	})
}
