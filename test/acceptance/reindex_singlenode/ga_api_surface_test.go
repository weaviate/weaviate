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

package reindex_singlenode

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// testGAApiSurface pins the v1.39 GA reindex surface end-to-end against a live
// node: declarative-upsert idempotency (200 NO_OP), the rangeable→rangeFilters
// write-path alias plus canonical status output, the POST rebuild / cancel
// sub-resources, and the removal of the old Preview PUT /indexes/{prop} route.
func testGAApiSurface(t *testing.T, restURI string) {
	const class = "GAApiSurface"
	reindexhelpers.SetupClass(t, class, []*models.Property{
		{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}},
		{Name: "price", DataType: []string{"int"}},
	})
	objects := make([]map[string]interface{}, 0, 8)
	for i := 0; i < 8; i++ {
		objects = append(objects, map[string]interface{}{
			"title": fmt.Sprintf("weaviate doc %d", i),
			"score": i,
			"price": i * 2,
		})
	}
	reindexhelpers.ImportObjects(t, class, objects)

	t.Run("upsert idempotency returns 200 NO_OP", func(t *testing.T) {
		// searchable with the current tokenization is a no-op.
		got := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "title", "searchable", `{"tokenization":"word"}`)
		require.Equal(t, http.StatusOK, got.StatusCode, "same-tokenization upsert must be 200 NO_OP: %s", got.Body)
		assert.Contains(t, got.Body, "NO_OP")

		// filterable on an already-filterable property is a no-op.
		got = reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "score", "filterable", `{}`)
		require.Equal(t, http.StatusOK, got.StatusCode, "already-filterable upsert must be 200 NO_OP: %s", got.Body)
		assert.Contains(t, got.Body, "NO_OP")

		// rangeFilters: first create (202), then a repeat is a no-op (200).
		taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "score", "rangeFilters", `{}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		reindexhelpers.AwaitReindexViaIndexes(t, restURI, class, "score", "rangeFilters")

		got = reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "score", "rangeFilters", `{}`)
		require.Equal(t, http.StatusOK, got.StatusCode, "repeat rangeFilters create must be 200 NO_OP: %s", got.Body)
		assert.Contains(t, got.Body, "NO_OP")
	})

	t.Run("rangeable alias accepted on write, canonical rangeFilters in status", func(t *testing.T) {
		// The `rangeable` alias creates the same index as `rangeFilters`.
		taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "price", "rangeable", `{}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		reindexhelpers.AwaitReindexViaIndexes(t, restURI, class, "price", "rangeFilters")

		// Status must always spell it canonically, never echo the alias.
		idx := reindexhelpers.GetIndexes(t, restURI, class)
		for _, p := range idx.Properties {
			for _, e := range p.Indexes {
				assert.NotEqual(t, "rangeable", e.Type,
					"status must use canonical rangeFilters, never the rangeable alias")
			}
		}
		var sawPriceRange bool
		for _, p := range idx.Properties {
			if p.Name != "price" {
				continue
			}
			for _, e := range p.Indexes {
				if e.Type == "rangeFilters" {
					sawPriceRange = true
				}
			}
		}
		assert.True(t, sawPriceRange, "price should report a rangeFilters index entry")

		// The alias is also accepted on the DELETE write path (byte-compatible).
		delURL := fmt.Sprintf("http://%s/v1/schema/%s/properties/price/index/rangeable", restURI, class)
		delReq, err := http.NewRequest(http.MethodDelete, delURL, nil)
		require.NoError(t, err)
		delResp, err := http.DefaultClient.Do(delReq)
		require.NoError(t, err)
		delResp.Body.Close()
		require.Equal(t, http.StatusOK, delResp.StatusCode, "DELETE via rangeable alias must be 200")
	})

	t.Run("rebuild via POST sub-resource", func(t *testing.T) {
		taskID := reindexhelpers.RebuildIndex(t, restURI, class, "title", "filterable")
		require.NotEmpty(t, taskID)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	})

	t.Run("cancel via POST sub-resource is idempotent NO_OP when nothing in flight", func(t *testing.T) {
		resp := reindexhelpers.CancelIndex(t, restURI, class, "title", "searchable")
		require.Equal(t, "NO_OP", resp.Status, "cancel with nothing in flight must be a 202 NO_OP")
		require.Empty(t, resp.TaskID)
	})

	t.Run("old Preview PUT /indexes/{prop} route is gone", func(t *testing.T) {
		oldURL := fmt.Sprintf("http://%s/v1/schema/%s/indexes/title", restURI, class)
		req, err := http.NewRequest(http.MethodPut, oldURL,
			bytes.NewReader([]byte(`{"searchable":{"algorithm":"blockmax"}}`)))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		// The Preview route was removed outright — the router no longer matches it.
		assert.Contains(t, []int{http.StatusNotFound, http.StatusMethodNotAllowed}, resp.StatusCode,
			"removed Preview route must return 404/405, got %d", resp.StatusCode)
	})
}
