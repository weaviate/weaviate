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

// Package rest_search covers POST /v1/search/{collection}/near-text end to
// end: the raw wire contract and the live error-status mapping.
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
	authorID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300001")
	movie1ID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300002")
	movie2ID = strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300003")
)

// postNearText POSTs a raw JSON near-text search and decodes the raw JSON
// reply, so assertions run against the wire shape, not generated models.
func postNearText(t *testing.T, collection string, body map[string]interface{}) (int, map[string]interface{}) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s:%s/v1/search/%s/near-text",
		helper.ServerHost, helper.ServerPort, collection)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

// errMessage extracts the message from either error shape: the handler's
// ErrorResponse ({"error":[{"message":...}]}) or the swagger bind-tier
// ({"code":...,"message":...}).
func errMessage(t *testing.T, out map[string]interface{}) string {
	t.Helper()
	if items, ok := out["error"].([]interface{}); ok && len(items) > 0 {
		if item, ok := items[0].(map[string]interface{}); ok {
			if msg, ok := item["message"].(string); ok {
				return msg
			}
		}
	}
	if msg, ok := out["message"].(string); ok {
		return msg
	}
	t.Fatalf("no error message in response: %v", out)
	return ""
}

func results(t *testing.T, out map[string]interface{}) []interface{} {
	t.Helper()
	res, ok := out["results"].([]interface{})
	require.True(t, ok, "response has no results array: %v", out)
	return res
}

func hit(t *testing.T, out map[string]interface{}, i int) map[string]interface{} {
	t.Helper()
	res := results(t, out)
	require.Greater(t, len(res), i)
	h, ok := res[i].(map[string]interface{})
	require.True(t, ok)
	return h
}

func metadataOf(t *testing.T, h map[string]interface{}) map[string]interface{} {
	t.Helper()
	metadata, ok := h["metadata"].(map[string]interface{})
	require.True(t, ok, "hit has no metadata key: %v", h)
	return metadata
}

func propertiesOf(t *testing.T, h map[string]interface{}) map[string]interface{} {
	t.Helper()
	props, ok := h["properties"].(map[string]interface{})
	require.True(t, ok, "hit has no properties key: %v", h)
	return props
}

func idOf(t *testing.T, h map[string]interface{}) string {
	t.Helper()
	id, ok := h["id"].(string)
	require.True(t, ok, "hit has no id: %v", h)
	return id
}

func movieClass() *models.Class {
	return &models.Class{
		Class:      "Movie",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			// no inverted index: filtering on this property cannot run
			{
				Name: "rating", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: func() *bool { b := false; return &b }(),
			},
			// must behave as ordinary user data under "properties", despite
			// sharing its name with the envelope's metadata field
			{Name: "metadata", DataType: schema.DataTypeText.PropString()},
			{
				Name: "details", DataType: schema.DataTypeObject.PropString(),
				NestedProperties: []*models.NestedProperty{
					{Name: "duration", DataType: schema.DataTypeInt.PropString()},
					{Name: "summary", DataType: schema.DataTypeText.PropString()},
				},
			},
			{Name: "hasAuthor", DataType: []string{"Author"}},
		},
	}
}

func TestRESTSearchNearText(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	authorClass := &models.Class{
		Class:      "Author",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
		},
	}
	// vectorizer "none": near-text has nothing to embed the query with
	notesClass := &models.Class{
		Class:      "Notes",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	// non-cosine index: certainty cannot be computed
	paintingClass := &models.Class{
		Class:             "Painting",
		Vectorizer:        "text2vec-contextionary",
		VectorIndexConfig: map[string]interface{}{"distance": "l2-squared"},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	journalClass := &models.Class{
		Class:      "Journal",
		Vectorizer: "text2vec-contextionary",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	classes := []*models.Class{authorClass, movieClass(), notesClass, paintingClass, journalClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Journal", []*models.Tenant{{Name: "tenantA"}})

	// the author must exist before the batch: movie1's hasAuthor beacon is
	// validated against the pre-batch state
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID:    authorID,
		Class: "Author",
		Properties: map[string]interface{}{
			"name": "famous writer",
		},
	}))

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:    movie1ID,
			Class: "Movie",
			Properties: map[string]interface{}{
				"title":    "spaceship galaxy adventure",
				"year":     2021,
				"metadata": "user data",
				"details": map[string]interface{}{
					"duration": 120,
					"summary":  "a journey through space",
				},
				"hasAuthor": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/Author/%s", authorID),
					},
				},
			},
		},
		{
			ID:    movie2ID,
			Class: "Movie",
			Properties: map[string]interface{}{
				"title":    "cooking dinner recipes",
				"year":     1999,
				"metadata": "more user data",
			},
		},
		{
			ID:     strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300004"),
			Class:  "Journal",
			Tenant: "tenantA",
			Properties: map[string]interface{}{
				"title": "travel diary",
			},
		},
		{
			ID:    strfmt.UUID("aa44bbee-ca5f-4db7-a412-5fc6a2300005"),
			Class: "Painting",
			Properties: map[string]interface{}{
				"title": "sunflowers",
			},
		},
	})

	t.Run("happy path: envelope with id, properties, metadata, took_ms", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship galaxy"},
			"return_properties": []string{"title"},
			"return_metadata":   []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		res := results(t, out)
		require.Len(t, res, 2)
		_, ok := out["took_ms"].(float64)
		assert.True(t, ok, "took_ms missing or not a number: %v", out)

		first := hit(t, out, 0)
		props := propertiesOf(t, first)
		assert.Equal(t, "spaceship galaxy adventure", props["title"])
		assert.NotContains(t, props, "year", "unselected property must not be returned")

		var prev float64
		for i := range res {
			h := hit(t, out, i)
			id := idOf(t, h)
			require.True(t, strfmt.IsUUID(id), "id is not a UUID: %q", id)
			metadata := metadataOf(t, h)
			assert.NotContains(t, metadata, "id", "the id lives on the envelope, not in metadata")
			distance, ok := metadata["distance"].(float64)
			require.True(t, ok, "distance missing in metadata: %v", metadata)
			assert.GreaterOrEqual(t, distance, prev, "distances must ascend")
			prev = distance
		}
		assert.Equal(t, movie1ID.String(), idOf(t, first))
	})

	t.Run("the id is always returned, even without return_metadata", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship galaxy"},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		assert.Equal(t, movie1ID.String(), idOf(t, first))
		// no non-id metadata was requested: the metadata block is omitted
		assert.NotContains(t, first, "metadata")
	})

	t.Run("id is not a return_metadata value", func(t *testing.T) {
		// return_metadata selects metadata keys only; "id" is outside the
		// swagger enum and is rejected at bind time with the standard
		// ErrorResponse body
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":           []string{"spaceship galaxy"},
			"return_metadata": []string{"id"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "return_metadata")
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})

	t.Run("a property named metadata is ordinary user data under properties", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":           []string{"spaceship galaxy"},
			"return_metadata": []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		// the user property and the envelope's metadata coexist: no shadowing
		assert.Equal(t, "user data", propertiesOf(t, first)["metadata"])
		metadata := metadataOf(t, first)
		_, ok := metadata["distance"].(float64)
		assert.True(t, ok, "distance missing in metadata: %v", metadata)
		assert.Equal(t, movie1ID.String(), idOf(t, first))
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query": []string{"spaceship galaxy"},
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

	t.Run("filter on a property without an inverted index is a 422", func(t *testing.T) {
		// live guard for the inverted.MissingIndexError mapping
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query": []string{"spaceship galaxy"},
			"where": map[string]interface{}{
				"path":     []string{"rating"},
				"operator": "GreaterThan",
				"valueInt": 3,
			},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "indexFilterable")
	})

	t.Run("references come back under references as arrays", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship galaxy"},
			"return_properties": []string{"title", "hasAuthor.name"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		assert.NotContains(t, propertiesOf(t, first), "hasAuthor",
			"reference selections must not appear under properties")
		references, ok := first["references"].(map[string]interface{})
		require.True(t, ok, "hit has no references key: %v", first)
		refs, ok := references["hasAuthor"].([]interface{})
		require.True(t, ok, "hasAuthor missing or not an array: %v", references)
		require.Len(t, refs, 1)
		ref, ok := refs[0].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "famous writer", ref["name"])
	})

	t.Run("references omitted when nothing selects across a reference", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship galaxy"},
			"return_properties": []string{"title"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.NotContains(t, hit(t, out, 0), "references")
	})

	t.Run("nested object properties are returned as nested maps", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship galaxy"},
			"return_properties": []string{"details"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		first := hit(t, out, 0)
		details, ok := propertiesOf(t, first)["details"].(map[string]interface{})
		require.True(t, ok, "details missing or not an object: %v", first)
		assert.Equal(t, "a journey through space", details["summary"])
		assert.Equal(t, float64(120), details["duration"])
	})

	t.Run("no vectorizer is a 422, not a 502", func(t *testing.T) {
		// live guard for the typed-error ordering (config 422, not 502)
		status, out := postNearText(t, "Notes", map[string]interface{}{
			"query": []string{"anything"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "vectorizer")
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postNearText(t, "Ghosts", map[string]interface{}{
			"query": []string{"anything"},
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postNearText(t, "Journal", map[string]interface{}{
			"query":  []string{"travel"},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postNearText(t, "Journal", map[string]interface{}{
			"query":  []string{"travel"},
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postNearText(t, "Journal", map[string]interface{}{
			"query": []string{"travel"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postNearText(t, "Movie", map[string]interface{}{
			"query":  []string{"spaceship"},
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})

	t.Run("certainty on a non-cosine index is a 422", func(t *testing.T) {
		status, out := postNearText(t, "Painting", map[string]interface{}{
			"query":     []string{"sunflowers"},
			"certainty": 0.7,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("unknown property in return_properties is a 400", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"query":             []string{"spaceship"},
			"return_properties": []string{"nonexistent"},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "no such prop")
	})

	t.Run("absent query is rejected at bind time", func(t *testing.T) {
		status, out := postNearText(t, "Movie", map[string]interface{}{
			"limit": 1,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "query")
		// bind-tier errors use the same ErrorResponse shape as handler errors
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
	})
}

// TestRESTSearchDisabled pins the DISABLE_REST_SEARCH opt-out: every search
// answers 422 before any schema access.
func TestRESTSearchDisabled(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("DISABLE_REST_SEARCH", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	status, out := postNearText(t, "Anything", map[string]interface{}{
		"query": []string{"anything"},
	})
	require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
	assert.Contains(t, errMessage(t, out), "disabled")
}
