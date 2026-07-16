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

// This file covers POST /v1/search/{collection}/bm25 end to end: the raw
// wire contract and the live error-status mapping. bm25 is a pure keyword
// search, so the whole suite runs against a module-free Weaviate — no
// vectorizer is configured anywhere.
package rest_search

import (
	"bytes"
	"context"
	"encoding/json"
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
	bm25Movie1ID = strfmt.UUID("bb44bbee-ca5f-4db7-a412-5fc6a2300001")
	bm25Movie2ID = strfmt.UUID("bb44bbee-ca5f-4db7-a412-5fc6a2300002")
)

// postBm25 POSTs a raw JSON bm25 search and decodes the raw JSON reply, so
// assertions run against the wire shape, not generated models.
func postBm25(t *testing.T, collection string, body map[string]interface{}) (int, map[string]interface{}) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s:%s/v1/search/%s/bm25",
		helper.ServerHost, helper.ServerPort, collection)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

// titlesOf collects properties.title over all hits, in rank order.
func titlesOf(t *testing.T, out map[string]interface{}) []string {
	t.Helper()
	res := results(t, out)
	titles := make([]string, len(res))
	for i := range res {
		titles[i], _ = propertiesOf(t, hit(t, out, i))["title"].(string)
	}
	return titles
}

func TestRESTSearchBm25(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		// the endpoint is experimental and off by default; enable it
		WithWeaviateEnv("EXPERIMENTAL_REST_SEARCH_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	bookClass := &models.Class{
		Class:      "Book",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "description", DataType: schema.DataTypeText.PropString()},
			// int property: filterable, but never keyword-searchable
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
		},
	}
	diaryClass := &models.Class{
		Class:      "Diary",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	classes := []*models.Class{bookClass, diaryClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Diary", []*models.Tenant{{Name: "tenantA"}})

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:    bm25Movie1ID,
			Class: "Book",
			Properties: map[string]interface{}{
				"title":       "spaceship galaxy adventure",
				"description": "cooking pasta at home",
				"year":        2021,
			},
		},
		{
			ID:    bm25Movie2ID,
			Class: "Book",
			Properties: map[string]interface{}{
				"title":       "cooking dinner recipes",
				"description": "a spaceship voyage through the galaxy stars",
				"year":        1999,
			},
		},
		{
			ID:     strfmt.UUID("bb44bbee-ca5f-4db7-a412-5fc6a2300003"),
			Class:  "Diary",
			Tenant: "tenantA",
			Properties: map[string]interface{}{
				"title": "travel diary",
			},
		},
	})

	t.Run("happy path: envelope with id, properties, score metadata, took_ms", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":             "spaceship galaxy",
			"return_properties": []string{"title"},
			"return_metadata":   []string{"score"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		// both books carry the query terms (title of one, description of
		// the other) — bm25 over all searchable properties finds both
		res := results(t, out)
		require.Len(t, res, 2)
		_, ok := out["took_ms"].(float64)
		assert.True(t, ok, "took_ms missing or not a number: %v", out)

		prev := float64(-1)
		for i := range res {
			h := hit(t, out, i)
			id := idOf(t, h)
			require.True(t, strfmt.IsUUID(id), "id is not a UUID: %q", id)
			metadata := metadataOf(t, h)
			score, ok := metadata["score"].(float64)
			require.True(t, ok, "score missing in metadata: %v", metadata)
			assert.Greater(t, score, float64(0))
			if prev >= 0 {
				assert.LessOrEqual(t, score, prev, "scores must descend")
			}
			prev = score
		}
	})

	t.Run("query_properties restricts the searched properties", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":             "spaceship",
			"query_properties":  []string{"title"},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)
		assert.Equal(t, bm25Movie1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("a ^boost reweights properties", func(t *testing.T) {
		// the same query with the boost flipped between the two properties
		// must flip the ranking: boosting title favors the title match,
		// boosting description favors the description match
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":             "spaceship galaxy",
			"query_properties":  []string{"title^10", "description"},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
		assert.Equal(t, []string{"spaceship galaxy adventure", "cooking dinner recipes"}, titlesOf(t, out))

		status, out = postBm25(t, "Book", map[string]interface{}{
			"query":             "spaceship galaxy",
			"query_properties":  []string{"title", "description^10"},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
		assert.Equal(t, []string{"cooking dinner recipes", "spaceship galaxy adventure"}, titlesOf(t, out))
	})

	t.Run("explain_score explains the bm25 score", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":           "spaceship",
			"return_metadata": []string{"score", "explain_score"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		metadata := metadataOf(t, hit(t, out, 0))
		explain, ok := metadata["explain_score"].(string)
		require.True(t, ok, "explain_score missing in metadata: %v", metadata)
		assert.NotEmpty(t, explain)
	})

	t.Run("inapplicable metadata keys are silently dropped", func(t *testing.T) {
		// distance and certainty are in the shared return_metadata enum but
		// cannot be computed for a keyword search: the request succeeds and
		// the response omits them (gRPC-parity silent drop)
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":           "spaceship",
			"return_metadata": []string{"distance", "certainty", "score"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		metadata := metadataOf(t, hit(t, out, 0))
		assert.Contains(t, metadata, "score")
		assert.NotContains(t, metadata, "distance")
		assert.NotContains(t, metadata, "certainty")
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query": "spaceship galaxy",
			"where": map[string]interface{}{
				"path":     []string{"year"},
				"operator": "LessThan",
				"valueInt": 2000,
			},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		res := results(t, out)
		require.Len(t, res, 1)
		assert.Equal(t, "cooking dinner recipes", propertiesOf(t, hit(t, out, 0))["title"])
	})

	t.Run("query_properties on a non-searchable property is a 422", func(t *testing.T) {
		// int properties have no searchable index; live guard for the
		// MissingIndexError mapping on the keyword path
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":            "2021",
			"query_properties": []string{"year"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "indexSearchable")
	})

	t.Run("absent query is rejected at bind time", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"limit": 1,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
		// bind-tier errors use the same ErrorResponse shape as handler errors
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})

	t.Run("empty query is a 400", func(t *testing.T) {
		// an explicit empty string passes swagger's required validation (the
		// pointer is non-nil) and reaches the handler
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query": "",
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
	})

	t.Run("query is string-only: the array form fails decode", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query": []string{"spaceship"},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postBm25(t, "Ghosts", map[string]interface{}{
			"query": "anything",
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("reserved fields are a 422", func(t *testing.T) {
		status, out := postBm25(t, "Book", map[string]interface{}{
			"query":           "spaceship",
			"rerank_property": "title",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "not yet supported")
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postBm25(t, "Diary", map[string]interface{}{
			"query":  "travel",
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postBm25(t, "Diary", map[string]interface{}{
			"query":  "travel",
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postBm25(t, "Diary", map[string]interface{}{
			"query": "travel",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postBm25(t, "Book", map[string]interface{}{
			"query":  "spaceship",
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})
}
