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

// This file covers POST /v1/aggregate/{collection} end to end: the raw wire
// contract (flat count vs grouped shapes) and the live error-status mapping.
// Phase 1 is counts only and never vectorizes, so the whole suite runs
// against a module-free Weaviate.
package rest_search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	aggNovelist1ID = strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300001")
	aggNovelist2ID = strfmt.UUID("cc44bbee-ca5f-4db7-a412-5fc6a2300002")
)

// postAggregateRaw POSTs a raw payload (nil = no body) and decodes the raw
// JSON reply, so assertions run against the wire shape, not generated models.
func postAggregateRaw(t *testing.T, collection string, payload []byte) (int, map[string]interface{}) {
	t.Helper()
	url := fmt.Sprintf("http://%s:%s/v1/aggregate/%s",
		helper.ServerHost, helper.ServerPort, collection)
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	resp, err := http.Post(url, "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func postAggregate(t *testing.T, collection string, body map[string]interface{}) (int, map[string]interface{}) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	return postAggregateRaw(t, collection, payload)
}

// countOf reads the flat form's count and asserts the grouped key is absent.
func countOf(t *testing.T, out map[string]interface{}) float64 {
	t.Helper()
	count, ok := out["count"].(float64)
	require.True(t, ok, "response has no count: %v", out)
	assert.NotContains(t, out, "groups", "flat replies must not carry groups")
	return count
}

// groupsOf reads the grouped form's groups and asserts the flat key is
// absent.
func groupsOf(t *testing.T, out map[string]interface{}) []interface{} {
	t.Helper()
	groups, ok := out["groups"].([]interface{})
	require.True(t, ok, "response has no groups array: %v", out)
	assert.NotContains(t, out, "count", "grouped replies must not carry the flat count")
	return groups
}

// groupCounts flattens groups into value → count for order-free assertions.
func groupCounts(t *testing.T, out map[string]interface{}) map[interface{}]float64 {
	t.Helper()
	counts := map[interface{}]float64{}
	for _, raw := range groupsOf(t, out) {
		g, ok := raw.(map[string]interface{})
		require.True(t, ok)
		count, ok := g["count"].(float64)
		require.True(t, ok, "group has no count: %v", g)
		groupedBy, ok := g["grouped_by"].(map[string]interface{})
		require.True(t, ok, "group has no grouped_by: %v", g)
		counts[groupedBy["value"]] = count
	}
	return counts
}

func TestRESTAggregate(t *testing.T) {
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

	novelistClass := &models.Class{
		Class:      "Novelist",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
		},
	}
	novelClass := &models.Class{
		Class:      "Novel",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "genre", DataType: schema.DataTypeText.PropString()},
			{Name: "year", DataType: schema.DataTypeInt.PropString()},
			{Name: "inStock", DataType: schema.DataTypeBoolean.PropString()},
			// no inverted index: filtering on this property cannot run
			{
				Name: "pages", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: func() *bool { b := false; return &b }(),
			},
			{Name: "hasNovelist", DataType: []string{"Novelist"}},
		},
	}
	ledgerClass := &models.Class{
		Class:      "Ledger",
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "entry", DataType: schema.DataTypeText.PropString()},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}

	classes := []*models.Class{novelistClass, novelClass, ledgerClass}
	for _, class := range classes {
		helper.CreateClass(t, class)
	}
	defer func() {
		for _, class := range classes {
			helper.DeleteClass(t, class.Class)
		}
	}()
	helper.CreateTenants(t, "Ledger", []*models.Tenant{{Name: "tenantA"}})

	beacon := func(id strfmt.UUID) []interface{} {
		return []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/Novelist/%s", id),
			},
		}
	}
	// the ref targets must exist before the batch below validates its beacons
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID: aggNovelist1ID, Class: "Novelist", Properties: map[string]interface{}{"name": "prolific writer"},
	}))
	require.NoError(t, helper.CreateObject(t, &models.Object{
		ID: aggNovelist2ID, Class: "Novelist", Properties: map[string]interface{}{"name": "occasional writer"},
	}))
	helper.CreateObjectsBatch(t, []*models.Object{
		{
			Class: "Novel",
			Properties: map[string]interface{}{
				"title": "galaxy voyage", "genre": "scifi", "year": 1999,
				"inStock": true, "pages": 300, "hasNovelist": beacon(aggNovelist1ID),
			},
		},
		{
			Class: "Novel",
			Properties: map[string]interface{}{
				"title": "star drifter", "genre": "scifi", "year": 2021,
				"inStock": false, "pages": 250, "hasNovelist": beacon(aggNovelist1ID),
			},
		},
		{
			Class: "Novel",
			Properties: map[string]interface{}{
				"title": "pasta at home", "genre": "cooking", "year": 1999,
				"inStock": true, "pages": 120, "hasNovelist": beacon(aggNovelist1ID),
			},
		},
		{
			Class: "Novel",
			Properties: map[string]interface{}{
				"title": "quiet rooms", "genre": "drama", "year": 2005,
				"inStock": true, "pages": 200, "hasNovelist": beacon(aggNovelist2ID),
			},
		},
		{Class: "Ledger", Tenant: "tenantA", Properties: map[string]interface{}{"entry": "first"}},
		{Class: "Ledger", Tenant: "tenantA", Properties: map[string]interface{}{"entry": "second"}},
	})

	t.Run("empty body returns the total count", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(4), countOf(t, out))
		_, ok := out["took_ms"].(float64)
		assert.True(t, ok, "took_ms missing or not a number: %v", out)
	})

	t.Run("return_metrics count is equivalent to omitting it", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"return_metrics": []string{"count"},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(4), countOf(t, out))
	})

	t.Run("where filter narrows the count", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "Equal", "path": []string{"year"}, "valueInt": 1999,
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(2), countOf(t, out))
	})

	t.Run("a filter matching nothing still returns count 0", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "Equal", "path": []string{"genre"}, "valueText": "poetry",
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(0), countOf(t, out))
	})

	t.Run("group_by returns per-group counts, largest first", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "genre",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)

		groups := groupsOf(t, out)
		require.Len(t, groups, 3)
		first, ok := groups[0].(map[string]interface{})
		require.True(t, ok)
		groupedBy, ok := first["grouped_by"].(map[string]interface{})
		require.True(t, ok, "group has no grouped_by: %v", first)
		assert.Equal(t, []interface{}{"genre"}, groupedBy["path"])
		assert.Equal(t, "scifi", groupedBy["value"], "largest group first")
		assert.Equal(t, float64(2), first["count"])

		assert.Equal(t, map[interface{}]float64{
			"scifi": 2, "cooking": 1, "drama": 1,
		}, groupCounts(t, out))
	})

	t.Run("group_by combines with where", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "genre",
			"where": map[string]interface{}{
				"operator": "Equal", "path": []string{"year"}, "valueInt": 1999,
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, map[interface{}]float64{
			"scifi": 1, "cooking": 1,
		}, groupCounts(t, out))
	})

	t.Run("limit caps the number of groups", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "genre",
			"limit":    1,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, map[interface{}]float64{"scifi": 2}, groupCounts(t, out))
	})

	t.Run("group values keep the property's JSON type", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "year",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		// int property: values arrive as JSON numbers, not strings
		assert.Equal(t, map[interface{}]float64{
			float64(1999): 2, float64(2021): 1, float64(2005): 1,
		}, groupCounts(t, out))

		status, out = postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "inStock",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		// boolean property: values arrive as JSON booleans
		assert.Equal(t, map[interface{}]float64{
			true: 3, false: 1,
		}, groupCounts(t, out))
	})

	t.Run("group_by a reference property groups by beacon", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "hasNovelist",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, map[interface{}]float64{
			fmt.Sprintf("weaviate://localhost/Novelist/%s", aggNovelist1ID): 3,
			fmt.Sprintf("weaviate://localhost/Novelist/%s", aggNovelist2ID): 1,
		}, groupCounts(t, out))
	})

	t.Run("group_by property spelling is normalized", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "Genre",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		require.Len(t, groupsOf(t, out), 3)
	})

	t.Run("group_by matching nothing omits groups", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "genre",
			"where": map[string]interface{}{
				"operator": "Equal", "path": []string{"genre"}, "valueText": "poetry",
			},
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.NotContains(t, out, "groups")
		assert.NotContains(t, out, "count")
		assert.Contains(t, out, "took_ms")
	})

	t.Run("unknown group_by property is a 400", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "nonexistent",
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "nonexistent")
	})

	t.Run("dotted group_by is a 422", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "hasNovelist.name",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, errMessage(t, out), "not yet supported")
	})

	t.Run("limit tiers", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"limit": 5,
		})
		require.Equal(t, http.StatusBadRequest, status, "limit without group_by: %v", out)
		assert.Contains(t, errMessage(t, out), "requires group_by")

		status, out = postAggregate(t, "Novel", map[string]interface{}{
			"group_by": "genre", "limit": 0,
		})
		require.Equal(t, http.StatusBadRequest, status, "zero limit: %v", out)
		assert.Contains(t, errMessage(t, out), "positive")
	})

	t.Run("reserved parameters are a 422", func(t *testing.T) {
		for name, body := range map[string]map[string]interface{}{
			"over":           {"over": map[string]interface{}{"near_text": map[string]interface{}{"query": []string{"x"}}}},
			"object_limit":   {"object_limit": 100},
			"metric grammar": {"return_metrics": []string{"price:mean"}},
			"unknown metric": {"return_metrics": []string{"median"}},
		} {
			status, out := postAggregate(t, "Novel", body)
			require.Equal(t, http.StatusUnprocessableEntity, status, "%s: %v", name, out)
		}
	})

	t.Run("filter on a property without an inverted index is a 422", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "GreaterThan", "path": []string{"pages"}, "valueInt": 100,
			},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
		assert.Contains(t, strings.ToLower(errMessage(t, out)), "index")
	})

	t.Run("unknown filter property is a 400", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "Equal", "path": []string{"nonexistent"}, "valueText": "x",
			},
		})
		require.Equal(t, http.StatusBadRequest, status, "%v", out)
	})

	t.Run("multi-tenancy statuses", func(t *testing.T) {
		status, out := postAggregate(t, "Ledger", map[string]interface{}{
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(2), countOf(t, out))

		status, out = postAggregate(t, "Ledger", map[string]interface{}{
			"tenant": "ghostTenant",
		})
		require.Equal(t, http.StatusNotFound, status, "unknown tenant: %v", out)

		status, out = postAggregate(t, "Ledger", map[string]interface{}{})
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing tenant: %v", out)

		status, out = postAggregate(t, "Novel", map[string]interface{}{
			"tenant": "tenantA",
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "tenant on non-MT collection: %v", out)
	})

	t.Run("unknown collection is a 404", func(t *testing.T) {
		status, out := postAggregate(t, "NoSuchCollection", map[string]interface{}{})
		require.Equal(t, http.StatusNotFound, status, "%v", out)
	})

	t.Run("unknown body fields are ignored", func(t *testing.T) {
		status, out := postAggregate(t, "Novel", map[string]interface{}{
			"not_a_field": 1,
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		assert.Equal(t, float64(4), countOf(t, out))
	})

	t.Run("bind tiers: malformed body, wrong types, missing body", func(t *testing.T) {
		status, out := postAggregateRaw(t, "Novel", []byte(`{"group_by":`))
		require.Equal(t, http.StatusBadRequest, status, "malformed JSON: %v", out)
		assert.Contains(t, out, "error", "bind errors must be ErrorResponse-shaped: %v", out)

		status, out = postAggregateRaw(t, "Novel", []byte(`{"limit":"three"}`))
		require.Equal(t, http.StatusBadRequest, status, "wrong field type: %v", out)

		status, out = postAggregate(t, "Novel", map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "NotAnOperator", "path": []string{"genre"}, "valueText": "x",
			},
		})
		require.Equal(t, http.StatusUnprocessableEntity, status, "bad where operator enum: %v", out)
		assert.Contains(t, out, "error")

		status, out = postAggregateRaw(t, "Novel", nil)
		require.Equal(t, http.StatusUnprocessableEntity, status, "missing body: %v", out)
		assert.Contains(t, out, "error", "swagger-native errors must be ErrorResponse-shaped: %v", out)
	})
}
