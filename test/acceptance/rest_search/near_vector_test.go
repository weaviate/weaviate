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

// This file covers POST /v1/search/{collection}/near-vector end to end: the
// raw wire contract and the live error-status mapping. The caller brings the
// query vector, so the whole suite runs against a module-free Weaviate — no
// vectorizer is configured anywhere.
package rest_search

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	// hand-crafted unit vectors: cosine distances from [1, 0] are exactly
	// 0 (verse1), 0.2 (verse2) and 1 (verse3)
	verse1ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300001")
	verse2ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300002")
	verse3ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300003")

	sonnet1ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300004")
	sonnet2ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300005")

	panel1ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300006")
	panel2ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300007")

	// one, two and three tokens: a flat query vector scored against this
	// index favours the object with the most tokens, so a two-object
	// fixture cannot tell a wrong answer from a right one
	album1ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300008")
	album2ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300009")
	album3ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300010")

	journal1ID = strfmt.UUID("ee44bbee-ca5f-4db7-a412-5fc6a2300011")
)

func postNearVector(t *testing.T, collection string, body map[string]any) (int, map[string]any) {
	return postSearch(t, collection, "near-vector", body)
}

func TestRESTSearchNearVector(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	verseClass := &models.Class{
		Class:      "Verse",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
		},
	}
	// a single named vector, so no targetVector is needed to select it
	sonnetClass := &models.Class{
		Class: "Sonnet",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{
			"solo": {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
		},
	}
	// two named vectors, the second on a non-cosine index so certainty
	// cannot be computed for it
	panelClass := &models.Class{
		Class: "Panel",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{
			"first": {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
			"second": {
				Vectorizer:        map[string]any{"none": map[string]any{}},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: map[string]any{"distance": "l2-squared"},
			},
		},
	}
	// a multi-vector index beside a regular one: the endpoint searches the
	// regular vector and rejects the multi-vector one
	albumClass := &models.Class{
		Class: "Album",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{
			"colbert": {
				Vectorizer:        map[string]any{"none": map[string]any{}},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: map[string]any{"multivector": map[string]any{"enabled": true}},
			},
			"plain": {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
		},
	}
	journalClass := &models.Class{
		Class:      "Journal",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	classes := []*models.Class{verseClass, sonnetClass, panelClass, albumClass, journalClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Journal", []*models.Tenant{{Name: "tenantA"}})

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:         verse1ID,
			Class:      "Verse",
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]any{"title": "the spaceship", "year": 2021},
		},
		{
			ID:         verse2ID,
			Class:      "Verse",
			Vector:     models.C11yVector{0.8, 0.6},
			Properties: map[string]any{"title": "the galaxy", "year": 1999},
		},
		{
			ID:         verse3ID,
			Class:      "Verse",
			Vector:     models.C11yVector{0, 1},
			Properties: map[string]any{"title": "the kitchen", "year": 2010},
		},
		{
			ID:         sonnet1ID,
			Class:      "Sonnet",
			Vectors:    models.Vectors{"solo": []float32{1, 0}},
			Properties: map[string]any{"title": "the only vector"},
		},
		{
			ID:         sonnet2ID,
			Class:      "Sonnet",
			Vectors:    models.Vectors{"solo": []float32{0, 1}},
			Properties: map[string]any{"title": "the other way"},
		},
		{
			ID:    panel1ID,
			Class: "Panel",
			Vectors: models.Vectors{
				"first":  []float32{1, 0},
				"second": []float32{1, 0},
			},
			Properties: map[string]any{"title": "two vectors"},
		},
		{
			ID:    panel2ID,
			Class: "Panel",
			Vectors: models.Vectors{
				"first":  []float32{0, 1},
				"second": []float32{0, 1},
			},
			Properties: map[string]any{"title": "two other vectors"},
		},
		{
			ID:    album1ID,
			Class: "Album",
			Vectors: models.Vectors{
				"colbert": [][]float32{{1, 0}},
				"plain":   []float32{1, 0},
			},
			Properties: map[string]any{"title": "one token"},
		},
		{
			ID:    album2ID,
			Class: "Album",
			Vectors: models.Vectors{
				"colbert": [][]float32{{1, 0}, {0, 1}},
				"plain":   []float32{0.8, 0.6},
			},
			Properties: map[string]any{"title": "two tokens"},
		},
		{
			ID:    album3ID,
			Class: "Album",
			Vectors: models.Vectors{
				"colbert": [][]float32{{1, 0}, {0, 1}, {0.6, 0.8}},
				"plain":   []float32{0, 1},
			},
			Properties: map[string]any{"title": "three tokens"},
		},
		{
			ID:         journal1ID,
			Class:      "Journal",
			Tenant:     "tenantA",
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]any{"title": "travel journal"},
		},
	})

	t.Run("happy path: ordered by distance from the query vector", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector":           []float32{1, 0},
			"returnProperties": []string{"title"},
			"returnMetadata":   []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		res := results(t, out)
		require.Len(t, res, 3)
		_, ok := out["tookMs"].(float64)
		assert.True(t, ok, "tookMs missing or not a number: %v", out)

		assert.Equal(t, verse1ID.String(), idOf(t, hit(t, out, 0)))
		assert.InDelta(t, 0, metadataOf(t, hit(t, out, 0))["distance"], 1e-5)
		assert.Equal(t, verse2ID.String(), idOf(t, hit(t, out, 1)))
		assert.InDelta(t, 0.2, metadataOf(t, hit(t, out, 1))["distance"], 1e-5)
		assert.Equal(t, verse3ID.String(), idOf(t, hit(t, out, 2)))
		assert.InDelta(t, 1, metadataOf(t, hit(t, out, 2))["distance"], 1e-5)
	})

	t.Run("distance cuts off far objects", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector":   []float32{1, 0},
			"distance": 0.5,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
		assert.Equal(t, verse1ID.String(), idOf(t, hit(t, out, 0)))
		assert.Equal(t, verse2ID.String(), idOf(t, hit(t, out, 1)))
	})

	t.Run("certainty cuts off far objects on a cosine index", func(t *testing.T) {
		// certainty = 1 - distance/2: 1.0, 0.9 and 0.5 for the three verses
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector":    []float32{1, 0},
			"certainty": 0.85,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
	})

	t.Run("absent vector is rejected at bind time", func(t *testing.T) {
		for name, body := range map[string]map[string]any{
			"omitted": {"limit": 1},
			"null":    {"vector": nil},
		} {
			status, out := postNearVector(t, "Verse", body)
			require.Equal(t, http.StatusUnprocessableEntity, status, "%s: %v", name, out)
			assert.Contains(t, errMessage(t, out), "vector", "%s: %v", name, out)
			// bind-tier errors use the same ErrorResponse shape as handler errors
			assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
		}
	})

	t.Run("empty vector is a 400", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector": []float32{},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "non-empty array of numbers")
	})

	t.Run("a vector that is not an array of numbers is a 400", func(t *testing.T) {
		for _, vector := range []any{
			[]any{"0.1"},
			[]any{0.1, nil},
			[]any{true},
			0.5,
			map[string]any{"solo": []float32{0.1}},
		} {
			status, out := postNearVector(t, "Verse", map[string]any{"vector": vector})
			require.Equal(t, http.StatusBadRequest, status, "vector %v: %v", vector, out)
			assert.Contains(t, errMessage(t, out), "non-empty array of numbers", "vector %v: %v", vector, out)
		}
	})

	t.Run("an array of vectors is a 422", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector": [][]float32{{1, 0}, {0, 1}},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "multi-vector search is not yet supported")
	})

	t.Run("a flat vector at a multi-vector target is a 422", func(t *testing.T) {
		status, out := postNearVector(t, "Album", map[string]any{
			"vector":       []float32{1, 0},
			"targetVector": "colbert",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "multi-vector index")
	})

	t.Run("the regular vector of a collection with a multi-vector one is searchable", func(t *testing.T) {
		status, out := postNearVector(t, "Album", map[string]any{
			"vector":       []float32{1, 0},
			"targetVector": "plain",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 3)
		assert.Equal(t, album1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("named vectors: the sole named vector is selected implicitly", func(t *testing.T) {
		status, out := postNearVector(t, "Sonnet", map[string]any{
			"vector": []float32{1, 0},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
		assert.Equal(t, sonnet1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("named vectors: targetVector selects the vector searched", func(t *testing.T) {
		status, out := postNearVector(t, "Panel", map[string]any{
			"vector":       []float32{1, 0},
			"targetVector": "first",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.NotEmpty(t, results(t, out))
		assert.Equal(t, panel1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("named vectors: unknown targetVector is a 400", func(t *testing.T) {
		status, out := postNearVector(t, "Panel", map[string]any{
			"vector":       []float32{1, 0},
			"targetVector": "third",
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
	})

	t.Run("named vectors: missing targetVector on several named vectors is a 422", func(t *testing.T) {
		status, out := postNearVector(t, "Panel", map[string]any{
			"vector": []float32{1, 0},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "target")
	})

	t.Run("certainty on a non-cosine index is a 422", func(t *testing.T) {
		status, out := postNearVector(t, "Panel", map[string]any{
			"vector":       []float32{1, 0},
			"targetVector": "second",
			"certainty":    0.7,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("both certainty and distance is a 400", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector":    []float32{1, 0},
			"certainty": 0.7,
			"distance":  0.3,
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector": []float32{1, 0},
			"where": map[string]any{
				"path":     []string{"year"},
				"operator": "LessThan",
				"valueInt": 2000,
			},
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)
		assert.Equal(t, "the galaxy", propertiesOf(t, hit(t, out, 0))["title"])
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postNearVector(t, "Ghosts", map[string]any{
			"vector": []float32{1, 0},
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("reserved fields are a 422", func(t *testing.T) {
		status, out := postNearVector(t, "Verse", map[string]any{
			"vector": []float32{1, 0},
			"rerank": map[string]any{"property": "title"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "not yet supported")
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postNearVector(t, "Journal", map[string]any{
			"vector": []float32{1, 0},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postNearVector(t, "Journal", map[string]any{
			"vector": []float32{1, 0},
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postNearVector(t, "Journal", map[string]any{
			"vector": []float32{1, 0},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postNearVector(t, "Verse", map[string]any{
			"vector": []float32{1, 0},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})
}
