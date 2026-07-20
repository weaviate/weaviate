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

// This file covers POST /v1/search/{collection}/hybrid end to end: the raw
// wire contract and the live error-status mapping. The vector leg embeds the
// query server-side, so the suite runs with the contextionary vectorizer —
// except the alpha-0 cases, which pin that a pure keyword hybrid search
// needs no vectorizer at all.
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
	hybridSong1ID = strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300001")
	hybridSong2ID = strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300002")
)

func postHybrid(t *testing.T, collection string, body map[string]interface{}) (int, map[string]interface{}) {
	return postSearch(t, collection, "hybrid", body)
}

func TestRESTSearchHybrid(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		// the endpoint is experimental and off by default; enable it
		WithWeaviateEnv("EXPERIMENTAL_REST_SEARCH_ENABLED", "true").
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	songClass := &models.Class{
		Class:      "Song",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "lyrics", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
		},
	}
	// vectorizer "none": the vector leg has nothing to embed the query with
	zineClass := &models.Class{
		Class:      "Zine",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	zettelClass := &models.Class{
		Class:      "Zettel",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	// no searchable property: the keyword leg has nothing to expand to
	ledgerClass := &models.Class{
		Class:      "Ledger",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			{
				Name: "code", DataType: schema.DataTypeText.PropString(),
				IndexSearchable: func() *bool { b := false; return &b }(),
			},
		},
	}

	classes := []*models.Class{songClass, zineClass, zettelClass, ledgerClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Zettel", []*models.Tenant{{Name: "tenantA"}})

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:    hybridSong1ID,
			Class: "Song",
			Properties: map[string]interface{}{
				"title":  "spaceship galaxy adventure",
				"lyrics": "stars and planets in the night",
				"year":   2021,
			},
		},
		{
			ID:    hybridSong2ID,
			Class: "Song",
			Properties: map[string]interface{}{
				"title":  "cooking dinner recipes",
				"lyrics": "pasta and cooking at home",
				"year":   1999,
			},
		},
		{
			ID:    strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300003"),
			Class: "Zine",
			Properties: map[string]interface{}{
				"title": "travel stories",
			},
		},
		{
			ID:     strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300004"),
			Class:  "Zettel",
			Tenant: "tenantA",
			Properties: map[string]interface{}{
				"title": "travel diary",
			},
		},
	})

	t.Run("happy path: envelope with id, properties, score metadata, tookMs", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":            "spaceship galaxy",
			"returnProperties": []string{"title"},
			"returnMetadata":   []string{"score"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		// the vector leg returns every object by distance, so both songs come
		// back; the keyword match must rank first
		res := results(t, out)
		require.Len(t, res, 2)
		_, ok := out["tookMs"].(float64)
		assert.True(t, ok, "tookMs missing or not a number: %v", out)

		prev := float64(-1)
		for i := range res {
			h := hit(t, out, i)
			id := idOf(t, h)
			require.True(t, strfmt.IsUUID(id), "id is not a UUID: %q", id)
			metadata := metadataOf(t, h)
			score, ok := metadata["score"].(float64)
			require.True(t, ok, "score missing in metadata: %v", metadata)
			if prev >= 0 {
				assert.LessOrEqual(t, score, prev, "fused scores must descend")
			}
			prev = score
		}
		assert.Equal(t, hybridSong1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("alpha steers the blend", func(t *testing.T) {
		// alpha 0 is a pure keyword search: only the bm25 match returns
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":            "cooking",
			"alpha":            0,
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)
		assert.Equal(t, hybridSong2ID.String(), idOf(t, hit(t, out, 0)))

		// alpha 1 is a pure vector search: every object returns, by distance
		status, out = postHybrid(t, "Song", map[string]interface{}{
			"query":            "cooking",
			"alpha":            1,
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
		assert.Equal(t, hybridSong2ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("both fusion algorithms run", func(t *testing.T) {
		for _, fusion := range []string{"ranked", "relativeScore"} {
			status, out := postHybrid(t, "Song", map[string]interface{}{
				"query":          "spaceship galaxy",
				"fusionType":     fusion,
				"returnMetadata": []string{"score"},
			})
			require.Equal(t, http.StatusOK, status, "fusionType %s: %v", fusion, out)
			require.NotEmpty(t, results(t, out))
			_, ok := metadataOf(t, hit(t, out, 0))["score"].(float64)
			assert.True(t, ok, "fusionType %s: score missing", fusion)
		}
	})

	t.Run("unknown fusionType is rejected at bind time", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":      "spaceship",
			"fusionType": "best",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "fusionType")
		// bind-tier errors use the same ErrorResponse shape as handler errors
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})

	t.Run("alpha outside [0,1] is a 400", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query": "spaceship",
			"alpha": 1.5,
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "alpha")
	})

	t.Run("maxVectorDistance cuts off far objects", func(t *testing.T) {
		// hybrid hits never carry distance metadata (the fused list carries
		// scores; the explorer emits distance only for plain vector
		// searches), so probe the cutoff by its effect: the loosest cosine
		// cutoff keeps everything, a near-zero one drops everything —
		// keyword-leg matches included
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":             "spaceship galaxy",
			"maxVectorDistance": 1.99,
			"returnProperties":  []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)

		status, out = postHybrid(t, "Song", map[string]interface{}{
			"query":             "spaceship galaxy",
			"maxVectorDistance": 0.0001,
			"returnProperties":  []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Empty(t, results(t, out))
	})

	t.Run("distance and certainty metadata are silently omitted", func(t *testing.T) {
		// both are in the shared returnMetadata enum and stay requested
		// (hybrid is a vector search, gRPC keeps the flags), but the fused
		// result list only carries scores — the response omits them
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":          "spaceship galaxy",
			"returnMetadata": []string{"distance", "certainty", "score"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		metadata := metadataOf(t, hit(t, out, 0))
		assert.Contains(t, metadata, "score")
		assert.NotContains(t, metadata, "distance")
		assert.NotContains(t, metadata, "certainty")
	})

	t.Run("queryProperties restricts the keyword leg", func(t *testing.T) {
		// alpha 0 isolates the keyword leg: "cooking" appears in song2's
		// title and lyrics, so restricting to title still matches, while
		// restricting to a property without the term matches nothing
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":           "cooking",
			"alpha":           0,
			"queryProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postHybrid(t, "Song", map[string]interface{}{
			"query":           "spaceship",
			"alpha":           0,
			"queryProperties": []string{"lyrics"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Empty(t, results(t, out))
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query": "spaceship galaxy",
			"where": map[string]interface{}{
				"path":     []string{"year"},
				"operator": "LessThan",
				"valueInt": 2000,
			},
			"returnProperties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		res := results(t, out)
		require.Len(t, res, 1)
		assert.Equal(t, "cooking dinner recipes", propertiesOf(t, hit(t, out, 0))["title"])
	})

	t.Run("no vectorizer is a 422 above alpha 0, fine at alpha 0", func(t *testing.T) {
		// live guard for the alpha-gated vectorizer pre-check
		status, out := postHybrid(t, "Zine", map[string]interface{}{
			"query": "travel",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "vectorizer")

		status, out = postHybrid(t, "Zine", map[string]interface{}{
			"query": "travel",
			"alpha": 0,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)
	})

	t.Run("empty query is a 400", func(t *testing.T) {
		// an explicit empty string passes swagger's required validation (the
		// pointer is non-nil) and reaches the handler
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query": "",
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
	})

	t.Run("absent query is rejected at bind time", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"limit": 1,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postHybrid(t, "Ghosts", map[string]interface{}{
			"query": "anything",
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("no searchable properties: keyword leg 422 below alpha 1, skipped at 1", func(t *testing.T) {
		// with queryProperties omitted the keyword leg expands to all
		// searchable properties; a collection with none must be a 422, not
		// the engine's untyped expansion error (a 500)
		status, out := postHybrid(t, "Ledger", map[string]interface{}{
			"query": "spaceship",
			"alpha": 0.5,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "no searchable properties")

		// at alpha 1 the keyword leg never runs: pure vector search works
		status, out = postHybrid(t, "Ledger", map[string]interface{}{
			"query": "spaceship",
			"alpha": 1,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
	})

	t.Run("reserved fields are a 422", func(t *testing.T) {
		status, out := postHybrid(t, "Song", map[string]interface{}{
			"query":  "spaceship",
			"rerank": map[string]interface{}{"property": "title"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "not yet supported")
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postHybrid(t, "Zettel", map[string]interface{}{
			"query":  "travel",
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postHybrid(t, "Zettel", map[string]interface{}{
			"query":  "travel",
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postHybrid(t, "Zettel", map[string]interface{}{
			"query": "travel",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postHybrid(t, "Song", map[string]interface{}{
			"query":  "spaceship",
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})
}
