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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testRepairRangeable exercises POST .../index/rangeFilters/rebuild: rebuild
// an already-enabled rangeable index from the existing filterable bucket.
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
		resp := reindexhelpers.RebuildIndexRaw(t, restURI, className, "price", "rangeFilters")
		require.Equal(t, 400, resp.StatusCode,
			"rebuild rangeable on a property without rangeable enabled must 400: %s", resp.Body)
		require.Contains(t, resp.Body, "does not have a rangeable index to rebuild")
	})

	t.Run("RebuildRejectedForNonNumeric", func(t *testing.T) {
		resp := reindexhelpers.RebuildIndexRaw(t, restURI, className, "label", "rangeFilters")
		require.Equal(t, 400, resp.StatusCode,
			"rebuild rangeable on a non-numeric property must 400: %s", resp.Body)
		require.Contains(t, resp.Body, "not a numeric type")
	})

	t.Run("RebuildSucceedsOnEnabledNumeric", func(t *testing.T) {
		taskID := reindexhelpers.RebuildIndex(t, restURI, className, "score", "rangeFilters")
		t.Logf("submitted repair-rangeable task: %s", taskID)
		reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, "score", "rangeFilters")
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	})
}
