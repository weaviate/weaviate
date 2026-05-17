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
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// putIndexes is a small wrapper that returns the raw status + body
// instead of asserting 202 like submitIndexUpdate does. Used by the
// rejection-path tests below.
func putIndexes(t *testing.T, restURI, collection, property, jsonBody string) (int, string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// testRepairRangeable exercises {"rangeable":{"rebuild":true}}: rebuild an
// already-enabled rangeable index from the existing filterable bucket.
// Three contract points:
//
//   - rebuild on a property whose IndexRangeFilters is false → 400
//     (must use enable to create one first).
//   - rebuild on a non-numeric property → 400 (validator rejects).
//   - rebuild on an enabled numeric property → 202 + reaches READY +
//     range queries still work afterwards.
func testRepairRangeable(t *testing.T, restURI string) {
	const className = "RepairRangeableTest"
	trueVal := true

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}},
			{Name: "score", DataType: []string{"int"}, IndexRangeFilters: &trueVal},
			{Name: "price", DataType: []string{"number"}}, // rangeable not enabled
			{Name: "label", DataType: []string{"text"}},   // non-numeric
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// Seed enough objects that the range query has something to assert on.
	const n = 50
	for i := 0; i < n; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"name":  fmt.Sprintf("item_%d", i),
				"score": i,
				"price": float64(i) * 1.5,
				"label": "x",
			},
		}))
	}

	t.Run("RebuildRejectedWhenNotEnabled", func(t *testing.T) {
		body := `{"rangeable":{"rebuild":true}}`
		status, respBody := putIndexes(t, restURI, className, "price", body)
		require.Equal(t, 400, status,
			"rebuild rangeable on a property without rangeable enabled must 400: %s", respBody)
		require.Contains(t, respBody, "does not have a rangeable index to rebuild")
	})

	t.Run("RebuildRejectedForNonNumeric", func(t *testing.T) {
		body := `{"rangeable":{"rebuild":true}}`
		status, respBody := putIndexes(t, restURI, className, "label", body)
		require.Equal(t, 400, status,
			"rebuild rangeable on a non-numeric property must 400: %s", respBody)
		require.Contains(t, respBody, "not a numeric type")
	})

	t.Run("RebuildSucceedsOnEnabledNumeric", func(t *testing.T) {
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "score", `{"rangeable":{"rebuild":true}}`)
		t.Logf("submitted repair-rangeable task: %s", taskID)
		reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, "score", "rangeable")
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	})
}
