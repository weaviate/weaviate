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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeleteThenReEnableIndexingBleed pins the Sev 1 frontend repro on
// 2026-05-14 in https://github.com/weaviate/weaviate/issues/10675: after multiple enable→DELETE cycles
// on the same property, GET /indexes shows a synthetic "indexing(1)" entry
// for the just-deleted index, "carrying over from the previous FINISHED
// task". The synthetic status sticks: a follow-up enable's swap doesn't
// dislodge it because the FINISHED task it keys off is stale.
//
// Root cause: mergeReindexStatus has a finalize-window override that says
// "FINISHED task with flag-off → still finalizing → indexing@100%". The
// override forgets that after DELETE the flag is intentionally off, NOT
// "swap hasn't propagated yet". A previous FINISHED task whose swap
// completed (flag flipped on, then off again by DELETE) is silently
// reclassified as "still finalizing", and the synthetic "indexing(1)"
// entry bleeds across cycles.
//
// Distinct from testDeleteThenReEnableMultiCycle (which pins the
// silent-failure family for the bucket itself): this test pins the
// HTTP visible state — the GET response must not lie about what
// indexes exist after a DELETE.
func testDeleteThenReEnableIndexingBleed(t *testing.T, restURI string) {
	t.Run("searchable_bleed_after_delete", func(t *testing.T) {
		const class = "BleedSearchable"
		falseVal := false
		helper.CreateClass(t, &models.Class{
			Class: class,
			Properties: []*models.Property{
				{Name: "body", DataType: []string{"text"}, IndexSearchable: &falseVal, Tokenization: "word"},
			},
			Vectorizer: "none",
		})
		defer helper.DeleteClass(t, class)

		for i, text := range []string{"quick brown fox", "lazy fox dog", "another fox document"} {
			require.NoError(t, helper.CreateObject(t, &models.Object{
				Class: class, Properties: map[string]interface{}{"body": text},
			}), "object %d", i)
		}

		// Cycle 1: enable → wait FINISHED → DELETE.
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireSearchableEnabled(t, class, "body")
		deleteIndex(t, restURI, class, "body", "searchable")

		// Right after DELETE: schema flag has flipped to false. GET /indexes
		// must NOT surface a synthetic "indexing"/"pending"/"failed"/
		// "cancelled" entry for the searchable index. The prior FINISHED
		// task's flag-flip completed before DELETE; it is NOT in finalize
		// window any more. Any synthetic entry here is the bug.
		//
		// Eventually-poll because the schema-flag flip may take a moment
		// to propagate. Use a tight window (5s) — the bug surfaces
		// immediately, the fix lets the poll converge inside the window.
		require.Eventually(t, func() bool {
			resp := reindexhelpers.GetIndexes(t, restURI, class)
			for _, prop := range resp.Properties {
				if prop.Name != "body" {
					continue
				}
				for _, idx := range prop.Indexes {
					if idx.Type == "searchable" {
						// Any searchable entry surfacing right after DELETE
						// is the bleed. Even "ready" would be wrong because
						// the schema flag is off — synthetic statuses
						// (indexing/pending/failed/cancelled) are the bug.
						t.Logf("bleed detected: cycle 1 DELETE — searchable entry still present: %+v", idx)
						return false
					}
				}
				return true
			}
			return false
		}, 12*time.Second, 250*time.Millisecond,
			"after DELETE: GET /indexes must NOT surface any searchable entry — the schema flag is off and no reindex is in flight. A bleed here means the synthetic-status override in mergeReindexStatus is treating a stale FINISHED task as still finalizing.")

		// Cycle 2: enable again. After this cycle's FINISHED+DELETE, the
		// bleed reproduces with two stale FINISHED tasks competing.
		taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireSearchableEnabled(t, class, "body")
		deleteIndex(t, restURI, class, "body", "searchable")

		// Same assertion after cycle 2's DELETE — the bleed must NOT appear
		// even with multiple FINISHED tasks in DTM history.
		require.Eventually(t, func() bool {
			resp := reindexhelpers.GetIndexes(t, restURI, class)
			for _, prop := range resp.Properties {
				if prop.Name != "body" {
					continue
				}
				for _, idx := range prop.Indexes {
					if idx.Type == "searchable" {
						t.Logf("bleed detected: cycle 2 DELETE — searchable entry still present: %+v", idx)
						return false
					}
				}
				return true
			}
			return false
		}, 12*time.Second, 250*time.Millisecond,
			"after 2nd DELETE: GET /indexes must NOT surface any searchable entry. Two stale FINISHED tasks must not be picked as the finalize-window winner.")

		// Cycle 3: enable again. After this cycle's FINISHED+DELETE, three
		// stale FINISHED tasks accumulate.
		taskID = reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireSearchableEnabled(t, class, "body")
		deleteIndex(t, restURI, class, "body", "searchable")

		require.Eventually(t, func() bool {
			resp := reindexhelpers.GetIndexes(t, restURI, class)
			for _, prop := range resp.Properties {
				if prop.Name != "body" {
					continue
				}
				for _, idx := range prop.Indexes {
					if idx.Type == "searchable" {
						t.Logf("bleed detected: cycle 3 DELETE — searchable entry still present: %+v", idx)
						return false
					}
				}
				return true
			}
			return false
		}, 12*time.Second, 250*time.Millisecond,
			"after 3rd DELETE: GET /indexes must NOT surface any searchable entry. Three stale FINISHED tasks accumulated.")
	})

	t.Run("filterable_bleed_after_delete", func(t *testing.T) {
		const class = "BleedFilterable"
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

		// Two enable→DELETE cycles, then assert no bleed.
		for cycle := 1; cycle <= 2; cycle++ {
			taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
				`{"filterable":{"enabled":true}}`)
			reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
			requireFilterableEnabled(t, class, "name")
			deleteIndex(t, restURI, class, "name", "filterable")

			require.Eventually(t, func() bool {
				resp := reindexhelpers.GetIndexes(t, restURI, class)
				for _, prop := range resp.Properties {
					if prop.Name != "name" {
						continue
					}
					for _, idx := range prop.Indexes {
						if idx.Type == "filterable" {
							t.Logf("filterable bleed detected (cycle %d): %+v", cycle, idx)
							return false
						}
					}
					return true
				}
				return false
			}, 12*time.Second, 250*time.Millisecond,
				"cycle %d: after DELETE filterable, GET /indexes must NOT surface any filterable entry on %q", cycle, class)
		}
	})

	t.Run("rangeable_bleed_after_delete", func(t *testing.T) {
		const class = "BleedRangeable"
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

		// Two enable→DELETE cycles. Rangeable is non-semantic so it
		// doesn't go through OnGroupCompleted's swap — the bleed mode
		// for non-semantic migrations is slightly different but the
		// post-DELETE invariant is the same: no synthetic entry must
		// surface for the deleted index.
		for cycle := 1; cycle <= 2; cycle++ {
			taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "score",
				`{"rangeable":{"enabled":true}}`)
			reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
			requireRangeableEnabled(t, class, "score")
			deleteIndex(t, restURI, class, "score", "rangeFilters")

			require.Eventually(t, func() bool {
				resp := reindexhelpers.GetIndexes(t, restURI, class)
				for _, prop := range resp.Properties {
					if prop.Name != "score" {
						continue
					}
					for _, idx := range prop.Indexes {
						if idx.Type == "rangeable" {
							t.Logf("rangeable bleed detected (cycle %d): %+v", cycle, idx)
							return false
						}
					}
					return true
				}
				return false
			}, 12*time.Second, 250*time.Millisecond,
				"cycle %d: after DELETE rangeFilters, GET /indexes must NOT surface any rangeable entry on %q", cycle, class)
		}
	})
}

// TestSuppress ensures this file compiles in isolation. The actual entry
// point is the suite's subtest registered via t.Run("DeleteThenReEnableIndexingBleed",
// testDeleteThenReEnableIndexingBleed).
func TestSuppress_DeleteThenReEnableIndexingBleed(t *testing.T) {
	assert.NotNil(t, testDeleteThenReEnableIndexingBleed)
}
