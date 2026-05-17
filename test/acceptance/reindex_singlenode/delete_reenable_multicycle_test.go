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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testDeleteThenReEnableMultiCycle drives the DELETE→re-enable journey three
// times in a row on the same property without restarting the server. The
// fixed single-cycle bug (commit 6b7dc23768) cleaned per-property migration
// dir + sidecar dirs at DELETE, but a frontend repro on 2026-05-14 surfaced
// the same silent-failure family on cycle 3: schema reports `indexing(1)`
// indefinitely, bm25 errors with "indexSearchable not enabled", task
// FINISHED in 1.6s (way under the migration's natural duration).
//
// Cause hypothesis (to confirm): some on-disk leftover state accumulates
// across cycles that the per-cycle DELETE cleanup misses. Candidates:
//   - `.migrations/<dir>/properties.mig` outside the per-property
//     dir-name pattern.
//   - DTM task records persisted to disk that survive across reindex
//     submissions and short-circuit on the second/third call.
//   - In-memory state on the shard (markSearchableBlockmaxProperties
//     list, cached strategies in ReindexProvider) that doesn't reset.
//
// The test asserts the *post-cycle-3 state*, which is the strictest:
// schema flag IndexSearchable = true, bm25() returns all docs. Same
// shape for filterable + rangeable variants.
func testDeleteThenReEnableMultiCycle(t *testing.T, restURI string) {
	t.Run("searchable_three_cycles", func(t *testing.T) {
		testDeleteThenReEnableSearchableMultiCycle(t, restURI, 3)
	})
	t.Run("filterable_three_cycles", func(t *testing.T) {
		testDeleteThenReEnableFilterableMultiCycle(t, restURI, 3)
	})
	t.Run("rangeable_three_cycles", func(t *testing.T) {
		testDeleteThenReEnableRangeableMultiCycle(t, restURI, 3)
	})
}

func testDeleteThenReEnableSearchableMultiCycle(t *testing.T, restURI string, cycles int) {
	const class = "MultiCycleSearchable"
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

	for cycle := 1; cycle <= cycles; cycle++ {
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "body",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireSearchableEnabled(t, class, "body")
		require.GreaterOrEqual(t, bm25Hits(t, class, "fox"), 3,
			"cycle %d/%d: post-enable bm25('fox') must return all 3 docs", cycle, cycles)

		if cycle < cycles {
			deleteIndex(t, restURI, class, "body", "searchable")
		}
	}

	require.GreaterOrEqual(t, bm25Hits(t, class, "fox"), 3,
		"after %d DELETE→enable cycles: bm25('fox') must return all 3 docs. If this fails, stale on-disk state from cycle %d-1 short-circuited cycle %d's reindex (Sev 1 family)",
		cycles, cycles, cycles)
}

func testDeleteThenReEnableFilterableMultiCycle(t *testing.T, restURI string, cycles int) {
	const class = "MultiCycleFilterable"
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

	for cycle := 1; cycle <= cycles; cycle++ {
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"enabled":true}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireFilterableEnabled(t, class, "name")
		require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
			"cycle %d/%d: post-enable Equal('alpha') must return 1", cycle, cycles)

		if cycle < cycles {
			deleteIndex(t, restURI, class, "name", "filterable")
		}
	}

	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"after %d DELETE→enable cycles: Equal('alpha') must return 1. If 0, stale state short-circuited the final cycle's reindex (Sev 1 family)", cycles)
}

func testDeleteThenReEnableRangeableMultiCycle(t *testing.T, restURI string, cycles int) {
	const class = "MultiCycleRangeable"
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

	for cycle := 1; cycle <= cycles; cycle++ {
		taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "score",
			`{"rangeable":{"enabled":true}}`)
		reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
		requireRangeableEnabled(t, class, "score")
		require.Equal(t, 2, rangeFilterHits(t, class, "score", 30),
			"cycle %d/%d: post-enable LessThan(30) must return 2", cycle, cycles)

		if cycle < cycles {
			deleteIndex(t, restURI, class, "score", "rangeFilters")
		}
	}

	require.Equal(t, 2, rangeFilterHits(t, class, "score", 30),
		"after %d DELETE→enable cycles: LessThan(30) must return 2. If 0, stale state short-circuited the final cycle's reindex (Sev 1 family)", cycles)
}

// TestSuppress ensures this file compiles in isolation. The actual entry
// point is the suite's subtest registered via t.Run("DeleteThenReEnableMultiCycle",
// testDeleteThenReEnableMultiCycle).
func TestSuppress_DeleteThenReEnableMultiCycle(t *testing.T) {
	assert.NotNil(t, testDeleteThenReEnableMultiCycle)
}
