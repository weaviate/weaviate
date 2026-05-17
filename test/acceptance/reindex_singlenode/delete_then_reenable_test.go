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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeleteThenReEnable pins the journey: enable an index via the reindex
// API (which creates the .migrations/<dir>/tidied sentinel on disk), DELETE
// it via DELETE /properties/{prop}/index/{indexName}, then enable it again.
// The second enable MUST actually re-build the bucket and the index MUST
// serve queries afterwards.
//
// Failure mode this guards against: stale .migrations/<dir>/tidied sentinel
// surviving the DELETE, causing the second enable to short-circuit on
// rt.IsTidied()=true, call OnMigrationComplete on an empty bucket, re-flip
// the schema flag to true, and report "ready" while leaving the customer
// with an empty index — silent data loss.
//
// Three sub-tests, one per index type. Each uses its own collection so the
// shared container doesn't tangle state.
func testDeleteThenReEnable(t *testing.T, restURI string) {
	t.Run("searchable", func(t *testing.T) {
		testDeleteThenReEnableSearchable(t, restURI)
	})
	t.Run("filterable", func(t *testing.T) {
		testDeleteThenReEnableFilterable(t, restURI)
	})
	t.Run("rangeFilters", func(t *testing.T) {
		testDeleteThenReEnableRangeable(t, restURI)
	})
}

func testDeleteThenReEnableSearchable(t *testing.T, restURI string) {
	const class = "DeleteReenableSearchable"
	falseVal := false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			// Start with searchable=false so the first enable goes through
			// the reindex pipeline and lays down the .migrations sentinel
			// whose stale survival across DELETE we are guarding against.
			{Name: "body", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	// Don't use English stopwords in the corpus query: the default BM25
	// stopword list filters "the", "a", "and", etc.
	for i, text := range []string{"quick brown fox", "lazy fox dog", "another fox document"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"body": text},
		}), "object %d", i)
	}

	// Step 1: first enable via the reindex API — lays down the
	// .migrations/enable_searchable_body/ tidied sentinel on disk.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
		`{"searchable":{"enabled":true,"tokenization":"word"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireSearchableEnabled(t, class, "body")
	require.GreaterOrEqual(t, bm25Hits(t, class, "fox"), 3,
		"after first enable-searchable, bm25 must find all three docs")

	// Step 2: DELETE the searchable index.
	deleteIndex(t, restURI, class, "body", "searchable")

	// Step 3: re-enable. The crux of this test. If the .migrations
	// sentinel from step 1 survived the DELETE, OnAfterLsmInitAsync
	// will short-circuit on rt.IsTidied()=true and re-flip the schema
	// flag while the freshly-removed bucket stays empty.
	taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
		`{"searchable":{"enabled":true,"tokenization":"word"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireSearchableEnabled(t, class, "body")

	hits := bm25Hits(t, class, "fox")
	require.GreaterOrEqual(t, hits, 3,
		"post-DELETE-then-re-enable: bm25('fox') must return all 3 docs; got %d. If 0, the migration short-circuited on a stale .migrations sentinel and the bucket is empty — schema reports ready but customer queries are broken (Sev 1)", hits)
}

func testDeleteThenReEnableFilterable(t *testing.T, restURI string) {
	const class = "DeleteReenableFilterable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}, IndexFilterable: &falseVal, IndexSearchable: &trueVal, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": name},
		}))
	}

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"filterable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireFilterableEnabled(t, class, "name")
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"after first enable-filterable, Equal('alpha') must match 1 object")

	deleteIndex(t, restURI, class, "name", "filterable")

	taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"filterable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireFilterableEnabled(t, class, "name")

	hits := equalFilterHits(t, class, "name", "alpha")
	require.Equal(t, 1, hits,
		"post-DELETE-then-re-enable: filterable Equal('alpha') must return 1; got %d. If 0, the migration silently no-opped on a stale .migrations sentinel (Sev 1)", hits)
}

func testDeleteThenReEnableRangeable(t *testing.T, restURI string) {
	const class = "DeleteReenableRangeable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal, IndexRangeFilters: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, score := range []int{10, 20, 30, 40, 50} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"score": score},
		}))
	}

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "score",
		`{"rangeable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireRangeableEnabled(t, class, "score")
	require.Equal(t, 2, rangeFilterHits(t, class, "score", 30),
		"after first enable-rangeable, LessThan(30) must match 2 (10, 20)")

	deleteIndex(t, restURI, class, "score", "rangeFilters")

	taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, class, "score",
		`{"rangeable":{"enabled":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireRangeableEnabled(t, class, "score")

	hits := rangeFilterHits(t, class, "score", 30)
	require.Equal(t, 2, hits,
		"post-DELETE-then-re-enable: range LessThan(30) must return 2; got %d. If 0, migration silently no-opped on a stale .migrations sentinel (Sev 1)", hits)
}

// deleteIndex calls DELETE /v1/schema/{class}/properties/{prop}/index/{indexName}.
// indexName values: "filterable", "searchable", "rangeFilters" (the URL spelling;
// the GET-response field is "rangeable" instead — distinct from the URL segment).
func deleteIndex(t *testing.T, restURI, class, prop, indexName string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/properties/%s/index/%s",
		restURI, class, prop, indexName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"DELETE %s on %s/%s must return 200", indexName, class, prop)
}

// requireSearchableEnabled waits up to 30s for the property's IndexSearchable
// flag to flip to true after a reindex finishes. awaitReindexFinished returns
// as soon as the DTM marks the task FINISHED, which is a separate scheduler
// tick from OnGroupCompleted's swap + OnMigrationComplete schema flip.
func requireSearchableEnabled(t *testing.T, class, prop string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == prop {
				return p.IndexSearchable != nil && *p.IndexSearchable
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"IndexSearchable on %s.%s must be true after reindex finishes", class, prop)
}

func requireFilterableEnabled(t *testing.T, class, prop string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == prop {
				return p.IndexFilterable != nil && *p.IndexFilterable
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"IndexFilterable on %s.%s must be true after reindex finishes", class, prop)
}

func requireRangeableEnabled(t *testing.T, class, prop string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == prop {
				return p.IndexRangeFilters != nil && *p.IndexRangeFilters
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"IndexRangeFilters on %s.%s must be true after reindex finishes", class, prop)
}

func bm25Hits(t *testing.T, class, query string) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["body"]}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, query)
	ids, err := runGraphQLQuery(t, class, gqlQuery)
	if err != nil {
		t.Logf("bm25 query for %q errored: %v", query, err)
		return 0
	}
	return len(ids)
}

func equalFilterHits(t *testing.T, class, prop, value string) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: Equal, valueText: %q}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, prop, value)
	ids, err := runGraphQLQuery(t, class, gqlQuery)
	if err != nil {
		t.Logf("equal filter for %s=%q errored: %v", prop, value, err)
		return 0
	}
	return len(ids)
}

func rangeFilterHits(t *testing.T, class, prop string, lessThan int) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: LessThan, valueInt: %d}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, prop, lessThan)
	ids, err := runGraphQLQuery(t, class, gqlQuery)
	if err != nil {
		t.Logf("range filter for %s<%d errored: %v", prop, lessThan, err)
		return 0
	}
	return len(ids)
}

// TestSuppress ensures this file compiles in isolation. The actual entry
// point is the suite's subtest registered via t.Run("DeleteThenReEnable",
// testDeleteThenReEnable).
func TestSuppress_DeleteThenReEnable(t *testing.T) {
	assert.NotNil(t, testDeleteThenReEnable)
}
