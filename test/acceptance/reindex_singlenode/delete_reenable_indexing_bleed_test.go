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

		// Three enable→FINISHED→DELETE cycles. After each one, the GET
		// /indexes surface must not surface a synthetic "indexing"/"pending"/
		// "failed"/"cancelled" entry for the just-deleted searchable index.
		// The bleed mode is the synthetic-status override in
		// mergeReindexStatus reclassifying a stale FINISHED task as "still
		// finalizing" — and the bug compounds with each completed-then-
		// deleted cycle (more stale FINISHED tasks in DTM history to
		// compete for the "finalize-window winner" pick).
		for cycle := 1; cycle <= 3; cycle++ {
			runEnableThenDeleteCycle(t, restURI, class, "body",
				`{"searchable":{"enabled":true,"tokenization":"word"}}`,
				"searchable",
				func() { requireSearchableEnabled(t, class, "body") })
			assertNoIndexBleedAfterDelete(t, restURI, class, "body", "searchable",
				"cycle %d: after DELETE: GET /indexes must NOT surface any searchable entry — schema flag is off and no reindex is in flight.", cycle)
		}
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

		for cycle := 1; cycle <= 2; cycle++ {
			runEnableThenDeleteCycle(t, restURI, class, "name",
				`{"filterable":{"enabled":true}}`,
				"filterable",
				func() { requireFilterableEnabled(t, class, "name") })
			assertNoIndexBleedAfterDelete(t, restURI, class, "name", "filterable",
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

		// Rangeable is non-semantic so it doesn't go through OnGroupCompleted's
		// swap. The bleed mode for non-semantic migrations is slightly
		// different but the post-DELETE invariant is the same: no synthetic
		// entry must surface for the deleted index.
		for cycle := 1; cycle <= 2; cycle++ {
			runEnableThenDeleteCycle(t, restURI, class, "score",
				`{"rangeable":{"enabled":true}}`,
				"rangeFilters",
				func() { requireRangeableEnabled(t, class, "score") })
			assertNoIndexBleedAfterDelete(t, restURI, class, "score", "rangeable",
				"cycle %d: after DELETE rangeFilters, GET /indexes must NOT surface any rangeable entry on %q", cycle, class)
		}
	})
}

// runEnableThenDeleteCycle submits a PUT to enable an index on the named
// property, waits for FINISHED, asserts the schema flag flipped on via the
// per-index-type require* helper, then DELETEs the index by its REST name
// (which is "searchable"/"filterable"/"rangeFilters" — distinct from the
// GET /indexes status-block type name "rangeable" for the rangeable case).
//
// Shared by the bleed test's 3 subtests (searchable / filterable /
// rangeable) so the enable→FINISHED→DELETE shape stays identical across
// the per-migration-type variants of the same Sev 1 repro.
func runEnableThenDeleteCycle(
	t *testing.T,
	restURI, class, propName, putBody, deleteIndexName string,
	requireEnabled func(),
) {
	t.Helper()
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, propName, putBody)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireEnabled()
	deleteIndex(t, restURI, class, propName, deleteIndexName)
}

// assertNoIndexBleedAfterDelete is the inverse of the FINISHED-task
// "finalize-window override" bug surface: after a DELETE, GET /indexes
// must NOT surface any entry of the named type on the named property. The
// schema flag is off and no reindex is in flight; a synthetic entry here
// means the mergeReindexStatus override is treating a stale FINISHED task
// as still finalizing.
//
// Eventually-polls because the schema-flag flip may take a moment to
// propagate. 12s window with 250ms poll cadence — the bug surfaces
// immediately at t=0, the fix lets the poll converge inside the window.
//
// `idxStatusType` is the GET /indexes status-block type name (one of
// "searchable" / "filterable" / "rangeable"), distinct from the DELETE
// URL's `indexName` ("searchable" / "filterable" / "rangeFilters").
func assertNoIndexBleedAfterDelete(
	t *testing.T,
	restURI, class, propName, idxStatusType, msgAndArgsFmt string,
	msgArgs ...interface{},
) {
	t.Helper()
	args := append([]interface{}{msgAndArgsFmt}, msgArgs...)
	require.Eventually(t, func() bool {
		resp := reindexhelpers.GetIndexes(t, restURI, class)
		for _, prop := range resp.Properties {
			if prop.Name != propName {
				continue
			}
			for _, idx := range prop.Indexes {
				if idx.Type == idxStatusType {
					t.Logf("%s bleed detected: %+v", idxStatusType, idx)
					return false
				}
			}
			return true
		}
		return false
	}, 12*time.Second, 250*time.Millisecond, args...)
}

// TestSuppress ensures this file compiles in isolation. The actual entry
// point is the suite's subtest registered via t.Run("DeleteThenReEnableIndexingBleed",
// testDeleteThenReEnableIndexingBleed).
func TestSuppress_DeleteThenReEnableIndexingBleed(t *testing.T) {
	assert.NotNil(t, testDeleteThenReEnableIndexingBleed)
}
