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

// This file covers POST /v1/search/{collection}/near-object end to end: the
// raw wire contract and the live error-status mapping. The search is
// anchored at a stored object vector, so the whole suite runs against a
// module-free Weaviate with client-supplied vectors — no vectorizer is
// configured anywhere.
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
	// hand-crafted unit vectors: cosine distances from poem1 are exactly
	// 0 (itself), 0.2 (poem2) and 1 (poem3)
	poem1ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300001")
	poem2ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300002")
	poem3ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300003")
	// stored without any vector
	poem4ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300004")

	sketch1ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300005")
	// stored with only the "first" named vector
	sketch2ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300006")

	chart1ID  = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300007")
	ledger1ID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300008")

	// well-formed but matching no object
	ghostID = strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2399999")
)

// postNearObject POSTs a raw JSON near-object search and decodes the raw
// JSON reply, so assertions run against the wire shape, not generated
// models.
func postNearObject(t *testing.T, collection string, body map[string]interface{}) (int, map[string]interface{}) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s:%s/v1/search/%s/near-object",
		helper.ServerHost, helper.ServerPort, collection)
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func TestRESTSearchNearObject(t *testing.T) {
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

	poemClass := &models.Class{
		Class:      "Poem",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
		},
	}
	// two named vectors, both client-supplied (vectorizer none)
	sketchClass := &models.Class{
		Class: "Sketch",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		VectorConfig: map[string]models.VectorConfig{
			"first": {
				Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
				VectorIndexType: "hnsw",
			},
			"second": {
				Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
				VectorIndexType: "hnsw",
			},
		},
	}
	// non-cosine index: certainty cannot be computed
	chartClass := &models.Class{
		Class:             "Chart",
		Vectorizer:        "none",
		VectorIndexConfig: map[string]interface{}{"distance": "l2-squared"},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	ledgerClass := &models.Class{
		Class:      "Ledger",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	classes := []*models.Class{poemClass, sketchClass, chartClass, ledgerClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Ledger", []*models.Tenant{{Name: "tenantA"}})

	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:         poem1ID,
			Class:      "Poem",
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]interface{}{"title": "the spaceship", "year": 2021},
		},
		{
			ID:         poem2ID,
			Class:      "Poem",
			Vector:     models.C11yVector{0.8, 0.6},
			Properties: map[string]interface{}{"title": "the galaxy", "year": 1999},
		},
		{
			ID:         poem3ID,
			Class:      "Poem",
			Vector:     models.C11yVector{0, 1},
			Properties: map[string]interface{}{"title": "the kitchen", "year": 2010},
		},
		{
			ID:         poem4ID,
			Class:      "Poem",
			Properties: map[string]interface{}{"title": "unwritten", "year": 2024},
		},
		{
			ID:    sketch1ID,
			Class: "Sketch",
			Vectors: models.Vectors{
				"first":  []float32{1, 0},
				"second": []float32{0, 1},
			},
			Properties: map[string]interface{}{"title": "two vectors"},
		},
		{
			ID:    sketch2ID,
			Class: "Sketch",
			Vectors: models.Vectors{
				"first": []float32{0.6, 0.8},
			},
			Properties: map[string]interface{}{"title": "one vector"},
		},
		{
			ID:         chart1ID,
			Class:      "Chart",
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]interface{}{"title": "l2 chart"},
		},
		{
			ID:         ledger1ID,
			Class:      "Ledger",
			Tenant:     "tenantA",
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]interface{}{"title": "travel ledger"},
		},
	})

	t.Run("happy path: ordered by distance from the source object, source included", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id":               poem1ID.String(),
			"returnProperties": []string{"title"},
			"returnMetadata":   []string{"distance"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		res := results(t, out)
		require.Len(t, res, 3)
		_, ok := out["tookMs"].(float64)
		assert.True(t, ok, "tookMs missing or not a number: %v", out)

		// the source object is a hit too, at distance 0
		first := hit(t, out, 0)
		assert.Equal(t, poem1ID.String(), idOf(t, first))
		assert.InDelta(t, 0, metadataOf(t, first)["distance"], 1e-5)

		assert.Equal(t, poem2ID.String(), idOf(t, hit(t, out, 1)))
		assert.InDelta(t, 0.2, metadataOf(t, hit(t, out, 1))["distance"], 1e-5)
		assert.Equal(t, poem3ID.String(), idOf(t, hit(t, out, 2)))
		assert.InDelta(t, 1, metadataOf(t, hit(t, out, 2))["distance"], 1e-5)
	})

	t.Run("distance cuts off far objects", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id":       poem1ID.String(),
			"distance": 0.5,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		res := results(t, out)
		require.Len(t, res, 2)
		assert.Equal(t, poem1ID.String(), idOf(t, hit(t, out, 0)))
		assert.Equal(t, poem2ID.String(), idOf(t, hit(t, out, 1)))
	})

	t.Run("certainty cuts off far objects on a cosine index", func(t *testing.T) {
		// certainty = 1 - distance/2: 1.0, 0.9 and 0.5 for the three poems
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id":        poem1ID.String(),
			"certainty": 0.85,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 2)
	})

	t.Run("well-formed unknown id is a 400", func(t *testing.T) {
		// the engine cannot resolve the source object: a bad body value
		// (typed ErrSourceObjectNotFound), not a 404 and not a 502
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id": ghostID.String(),
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "vector not found")
	})

	t.Run("structurally invalid id is rejected at bind time", func(t *testing.T) {
		for _, id := range []interface{}{"not-a-uuid", ""} {
			status, out := postNearObject(t, "Poem", map[string]interface{}{
				"id": id,
			})
			require.Equal(t, http.StatusUnprocessableEntity, status, "id %q: %v", id, out)
			assert.Contains(t, errMessage(t, out), "uuid", "id %q: %v", id, out)
			// bind-tier errors use the same ErrorResponse shape as handler errors
			assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)
		}
	})

	t.Run("absent id is rejected at bind time", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"limit": 1,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "id")
	})

	t.Run("source object without a vector is a 422", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id": poem4ID.String(),
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "has no vector")
	})

	t.Run("named vectors: targetVector selects the anchor vector", func(t *testing.T) {
		status, out := postNearObject(t, "Sketch", map[string]interface{}{
			"id":           sketch1ID.String(),
			"targetVector": "first",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.NotEmpty(t, results(t, out))
		assert.Equal(t, sketch1ID.String(), idOf(t, hit(t, out, 0)))
	})

	t.Run("named vectors: missing targetVector on a multi-vector collection is a 422", func(t *testing.T) {
		status, out := postNearObject(t, "Sketch", map[string]interface{}{
			"id": sketch1ID.String(),
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "target")
	})

	t.Run("named vectors: unknown targetVector is a 400", func(t *testing.T) {
		status, out := postNearObject(t, "Sketch", map[string]interface{}{
			"id":           sketch1ID.String(),
			"targetVector": "third",
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
	})

	t.Run("source object without the target vector is a 422", func(t *testing.T) {
		status, out := postNearObject(t, "Sketch", map[string]interface{}{
			"id":           sketch2ID.String(),
			"targetVector": "second",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "vector not found for target")
	})

	t.Run("certainty on a non-cosine index is a 422", func(t *testing.T) {
		status, out := postNearObject(t, "Chart", map[string]interface{}{
			"id":        chart1ID.String(),
			"certainty": 0.7,
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("both certainty and distance is a 400", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id":        poem1ID.String(),
			"certainty": 0.7,
			"distance":  0.3,
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "certainty")
	})

	t.Run("where filter narrows results", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id": poem1ID.String(),
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
		assert.Equal(t, "the galaxy", propertiesOf(t, hit(t, out, 0))["title"])
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postNearObject(t, "Ghosts", map[string]interface{}{
			"id": poem1ID.String(),
		})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("reserved fields are a 422", func(t *testing.T) {
		status, out := postNearObject(t, "Poem", map[string]interface{}{
			"id":     poem1ID.String(),
			"rerank": map[string]interface{}{"property": "title"},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "not yet supported")
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postNearObject(t, "Ledger", map[string]interface{}{
			"id":     ledger1ID.String(),
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, results(t, out), 1)

		status, out = postNearObject(t, "Ledger", map[string]interface{}{
			"id":     ledger1ID.String(),
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postNearObject(t, "Ledger", map[string]interface{}{
			"id": ledger1ID.String(),
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postNearObject(t, "Poem", map[string]interface{}{
			"id":     poem1ID.String(),
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})
}
