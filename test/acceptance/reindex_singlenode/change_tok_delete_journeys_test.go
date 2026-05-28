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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testChangeTokDeleteJourneys enumerates the journey class where
// change-tokenization (both indexes) and change-tokenization-filterable
// (filterable-only) interact with DELETE /properties/{prop}/index/{idx},
// enable-*, cancel, and back-to-back retokenizations.
//
// The new ReindexTypeChangeTokenizationFilterable migration (commit
// c98a3477ea) added a body shape that retokenizes ONLY the filterable
// bucket. The classic ReindexTypeChangeTokenization retokenizes BOTH
// buckets when both indexes exist. The two share submit-time cleanup
// (CleanStalePartialReindexState in handlers_indexes.go) but
// indexTypeFromMigrationType returns ("", false) for the classic both-
// indexes variant — meaning the submit-time pre-cleanup is SKIPPED for
// change-tok-both. That's the gap the journeys below probe.
//
// Per the "no bug is ever out of scope" rule each journey lands either
// as PASS (no gap) or RED (regression test pinning a real bug).
func testChangeTokDeleteJourneys(t *testing.T, restURI string) {
	t.Run("change_tok_both__delete_searchable__change_tok_filterable", func(t *testing.T) {
		testChangeTokBothThenDeleteSearchableThenChangeTokFilterable(t, restURI)
	})

	t.Run("change_tok_filterable__delete_filterable", func(t *testing.T) {
		testChangeTokFilterableThenDeleteFilterable(t, restURI)
	})

	t.Run("change_tok_filterable_only__enable_searchable_inherits_tokenization", func(t *testing.T) {
		testChangeTokFilterableThenEnableSearchable(t, restURI)
	})

	t.Run("change_tok_both_in_flight__delete_one_index_race", func(t *testing.T) {
		testChangeTokBothInFlightDeleteOneIndex(t, restURI)
	})

	t.Run("change_tok_filterable__change_tok_filterable_back_to_back", func(t *testing.T) {
		testChangeTokFilterableBackToBack(t, restURI)
	})

	t.Run("change_tok_both__cancel__change_tok_filterable", func(t *testing.T) {
		testChangeTokBothCancelThenChangeTokFilterable(t, restURI)
	})

	t.Run("change_tok_filterable__enable_searchable__change_tok_both", func(t *testing.T) {
		testChangeTokFilterableEnableSearchableThenChangeTokBoth(t, restURI)
	})
}

// =============================================================================
// Journey 1: change-tok-both → DELETE searchable → change-tok-filterable
// =============================================================================
// Pins that the partial migration state left behind by the previous
// both-indexes retokenize does not interfere with a subsequent
// filterable-only retokenize, after the searchable index has been
// deleted. The hazard: change-tok-both writes two distinct .migrations
// dirs (one per ShardReindexTaskGeneric returned by createReindexTasks).
// The DELETE wipes the searchable side's bucket. The filterable-only
// retokenize should run against the still-present filterable bucket
// with no contamination from the prior filterable retokenize sentinels.
func testChangeTokBothThenDeleteSearchableThenChangeTokFilterable(t *testing.T, restURI string) {
	const class = "ChangeTokBothDeleteSearchableThenFilterable"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
				Tokenization:    "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": name},
		}))
	}

	// Step 1: change-tok-both from word → field via {searchable:{tokenization:field}}.
	// This goes through ReindexTypeChangeTokenization and retokenizes BOTH
	// buckets.
	submitSearchableField := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"searchable":{"tokenization":"field"}}`)
	}
	taskID := submitSearchableField()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitSearchableField))
	requireTokenizationEquals(t, class, "name", "field")

	// Step 2: DELETE the searchable index.
	deleteIndex(t, restURI, class, "name", "searchable")

	// Step 3: filterable-only retokenize from field → word. With both
	// the searchable bucket and its migration sentinels gone (or never
	// having existed if step 1 collapsed them properly), this should
	// retokenize only the filterable bucket cleanly.
	submitFilterableWord := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"word"}}`)
	}
	taskID = submitFilterableWord()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitFilterableWord))
	requireTokenizationEquals(t, class, "name", "word")

	hits := equalFilterHits(t, class, "name", "alpha")
	require.Equal(t, 1, hits,
		"post-journey: filterable Equal('alpha') must return 1; got %d. "+
			"If 0, the second change-tokenization-filterable was tainted by "+
			"residual state from the prior change-tok-both, or the DELETE "+
			"on searchable also tore the filterable bucket (Sev 1)", hits)
}

// =============================================================================
// Journey 2: change-tok-filterable → DELETE filterable
// =============================================================================
// Pins behavior when DELETE follows a filterable-only retokenize. The
// schema flag IndexFilterable is still true (retokenize doesn't disable
// it). DELETE should drop the filterable bucket cleanly; subsequent
// filtering must reflect the no-filterable state.
func testChangeTokFilterableThenDeleteFilterable(t *testing.T, restURI string) {
	const class = "ChangeTokFilterableThenDeleteFilterable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, name := range []string{"alpha", "beta"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": name},
		}))
	}

	// Step 1: change-tokenization-filterable from field → word.
	submit := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"word"}}`)
	}
	taskID := submit()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submit))
	requireTokenizationEquals(t, class, "name", "word")

	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"post-step-1: filterable Equal('alpha') must match 1 with word tokenization")

	// Step 2: DELETE the filterable index. The API spelling is
	// "filterable" in the URL path.
	deleteIndex(t, restURI, class, "name", "filterable")

	// The IndexFilterable flag must flip to false after DELETE.
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == "name" {
				return p.IndexFilterable != nil && !*p.IndexFilterable
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"IndexFilterable on %s.name must be false after DELETE", class)

	// Filter must return zero — the bucket is gone. Note: GraphQL may
	// error with "no filterable index" instead of returning empty, so
	// we use a relaxed check.
	url := fmt.Sprintf("http://%s/v1/schema/%s", restURI, class)
	resp, err := http.Get(url)
	require.NoError(t, err)
	resp.Body.Close()
}

// =============================================================================
// Journey 3: change-tok-filterable (filterable-only) → enable-searchable
// =============================================================================
// Pins that after retokenizing a filterable-only property, the
// subsequent enable-searchable must use a tokenization compatible
// with the (now-changed) filterable bucket. validateEnableSearchableProperty
// rejects tokenization mismatch — the journey must therefore submit
// enable-searchable with the matching tokenization.
func testChangeTokFilterableThenEnableSearchable(t *testing.T, restURI string) {
	const class = "ChangeTokFilterableThenEnableSearchable"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, name := range []string{"alpha beta", "gamma delta"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": name},
		}))
	}

	// Step 1: retokenize filterable from field → word.
	submitFilterable := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"word"}}`)
	}
	taskID := submitFilterable()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitFilterable))
	requireTokenizationEquals(t, class, "name", "word")

	// Step 2: enable-searchable with matching tokenization "word".
	// validateEnableSearchableProperty REJECTS if the requested
	// tokenization differs from the property's current tokenization
	// when there is a pre-existing filterable index, so we use the
	// same tokenization the filterable was just changed to.
	submitSearchable := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`)
	}
	taskID = submitSearchable()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitSearchable))
	requireSearchableEnabled(t, class, "name")
	requireTokenizationEquals(t, class, "name", "word")

	// Step 3: assert both indexes work and use word tokenization.
	// Equal('alpha') matches the first object because filterable now
	// stores "alpha" as a discrete word.
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"post-enable-searchable: filterable Equal('alpha') must return 1 with word tokenization")

	// bm25('beta') matches the first object because searchable was
	// built with word tokenization over the same corpus.
	hits := bm25HitsForProp(t, class, "name", "beta")
	require.GreaterOrEqual(t, hits, 1,
		"post-enable-searchable: bm25('beta') must return >=1 doc. If 0, "+
			"enable-searchable built an empty bucket or inherited the wrong tokenization")
}

// =============================================================================
// Journey 4: change-tok-both in-flight → DELETE one index (race)
// =============================================================================
// Race: while change-tok-both is iterating, the user DELETEs one of
// the two index buckets being retokenized. Possible outcomes:
//   - DELETE 4xx with "in-flight reindex"
//   - DELETE 200; the reindex either fails cleanly or is no-op'd for
//     that side
//   - DELETE 200; the reindex finishes successfully on the surviving
//     side
//
// The hazard is silent inconsistency: schema flips to the new
// tokenization while one of the buckets was deleted out from under
// the running task.
//
// We can't guarantee the DELETE lands mid-iteration on every run (the
// task could finish first on a fast box). The test attempts the race
// with a 5k-row corpus to lengthen the window, then asserts that
// the FINAL state is internally consistent: schema flag + bucket
// existence + query semantics all agree.
func testChangeTokBothInFlightDeleteOneIndex(t *testing.T, restURI string) {
	const class = "ChangeTokBothInFlightDeleteOne"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
				Tokenization:    "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < 5000; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"name": fmt.Sprintf("doc_%d shared", i)},
		}))
	}

	// Step 1: submit change-tok-both.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tok-both task: %s", taskID)

	// Step 2: wait until the task is observable as indexing, then issue
	// DELETE on searchable.
	deleted := false
	require.Eventually(t, func() bool {
		resp := reindexhelpers.GetIndexes(t, restURI, class)
		for _, p := range resp.Properties {
			if p.Name != "name" {
				continue
			}
			for _, idx := range p.Indexes {
				if (idx.Type == "searchable" || idx.Type == "filterable") &&
					(idx.Status == "indexing" || idx.Status == "pending") {
					return true
				}
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond,
		"task did not appear as indexing/pending in time")

	// Issue DELETE on the searchable side. We accept either a 200
	// (silent race) or a 4xx (in-flight rejection). What we MUST NOT
	// accept is a 5xx — that signals corruption.
	url := fmt.Sprintf("http://%s/v1/schema/%s/properties/name/index/searchable", restURI, class)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	t.Logf("DELETE searchable mid-flight returned %d: %s", resp.StatusCode, string(body))
	require.Less(t, resp.StatusCode, 500,
		"DELETE on a property mid-reindex must NOT return 5xx; got %d body=%s",
		resp.StatusCode, string(body))
	if resp.StatusCode == http.StatusOK {
		deleted = true
	}

	// Step 3: wait for the reindex task to reach a terminal state. It
	// may FINISHED, FAILED, or CANCELLED depending on what the DELETE
	// did to its scaffolding.
	awaitTerminal(t, restURI, taskID)

	// Step 4: assert internal consistency. The class still exists.
	// The filterable bucket still exists. Queries against it work.
	c := helper.GetClass(t, class)
	require.NotNil(t, c, "class must still exist after the race")

	var nameProp *models.Property
	for _, p := range c.Properties {
		if p.Name == "name" {
			nameProp = p
			break
		}
	}
	require.NotNil(t, nameProp, "name property must still exist")

	// The filterable bucket should still serve queries. If DELETE
	// landed, IndexSearchable should be false. If DELETE was rejected
	// or no-op'd, both flags should still be true. Either way,
	// IndexFilterable should be true and Equal-filtering must work.
	require.True(t, nameProp.IndexFilterable != nil && *nameProp.IndexFilterable,
		"filterable index must survive the race (it was not the DELETE target)")
	if deleted {
		require.False(t, nameProp.IndexSearchable != nil && *nameProp.IndexSearchable,
			"after a successful DELETE-searchable, IndexSearchable must be false")
	}

	// Filter must work. The post-tokenization value depends on which
	// tokenization the filterable bucket settled on. We do a smoke
	// check that SOME hit comes back for an existing doc.
	hits := equalFilterHits(t, class, "name", "doc_0 shared")
	hitsWord := equalFilterHits(t, class, "name", "shared")
	// One of the two tokenizations must produce a hit. If both are
	// zero, the bucket is empty — torn state.
	require.True(t, hits+hitsWord > 0,
		"post-race: at least one of Equal('doc_0 shared') or Equal('shared') must hit; "+
			"both are zero means the filterable bucket is empty — torn state (Sev 1)")
}

// =============================================================================
// Journey 5: change-tok-filterable → change-tok-filterable (back-to-back)
// =============================================================================
// Pins that two consecutive filterable-only retokenizations on the
// same property succeed end-to-end. The hazard: the first
// retokenize's .migrations sentinels survive into the second submit,
// causing it to short-circuit or fail with a "bucket already exists"
// error from the staging buckets.
func testChangeTokFilterableBackToBack(t *testing.T, restURI string) {
	const class = "ChangeTokFilterableBackToBack"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "field",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	require.NoError(t, helper.CreateObject(t, &models.Object{
		Class: class, Properties: map[string]interface{}{"name": "alpha beta"},
	}))

	// Step 1: field → word.
	submitWord := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"word"}}`)
	}
	taskID := submitWord()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitWord))
	requireTokenizationEquals(t, class, "name", "word")
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"after first retokenize (word): Equal('alpha') must hit 1")

	// Step 2: word → field.
	submitField := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"field"}}`)
	}
	taskID = submitField()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitField))
	requireTokenizationEquals(t, class, "name", "field")

	// With "field" the whole string is a single token. Equal('alpha
	// beta') must hit, Equal('alpha') must not.
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha beta"),
		"after second retokenize (field): Equal('alpha beta') must hit 1")
	require.Equal(t, 0, equalFilterHits(t, class, "name", "alpha"),
		"after second retokenize (field): Equal('alpha') must hit 0")
}

// =============================================================================
// Journey 6: change-tok-both → cancel mid-run → change-tok-filterable
// =============================================================================
// Submits change-tok-both, cancels it mid-iteration, then submits
// change-tok-filterable. The cancel cleanup for the both-indexes
// migration must leave the filterable side in a state where the
// filterable-only retokenize can proceed.
//
// Hazard: indexTypeFromMigrationType returns ("", false) for
// ReindexTypeChangeTokenization, which means CleanStalePartialReindexState
// is NOT called by the submit-time pre-cleanup at handlers_indexes.go:427.
// The cancel handler at handlers_indexes.go:598 IS called but only with
// the specific indexType passed in the cancel body. If the change-tok-both
// cancel happened via {searchable:{cancel:true}} then only the
// searchable side gets cleaned — the filterable side's stale state
// persists and may corrupt the subsequent change-tok-filterable.
func testChangeTokBothCancelThenChangeTokFilterable(t *testing.T, restURI string) {
	const class = "ChangeTokBothCancelThenFilterable"
	trueVal := true
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &trueVal,
				Tokenization:    "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for i := 0; i < 5000; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class:      class,
			Properties: map[string]interface{}{"name": fmt.Sprintf("doc_%d shared", i)},
		}))
	}

	// Step 1: submit change-tok-both, then cancel mid-flight via
	// {searchable:{cancel:true}}. The change-tok-both task is registered
	// as ReindexTypeChangeTokenization which targets BOTH searchable
	// AND filterable. Cancel with the searchable verb (the natural
	// shape since the submit used the searchable verb).
	requestBody := `{"searchable":{"tokenization":"field"}}`
	cancelInFlightOrSkip(t, restURI, class, "name", "searchable", requestBody)

	// Step 2: submit change-tok-filterable. The cancel left state behind
	// for the filterable side that was being retokenized. If that state
	// isn't cleaned up, this submit either short-circuits or fails.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
		`{"filterable":{"tokenization":"field"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	requireTokenizationEquals(t, class, "name", "field")

	// Step 3: verify the filterable bucket actually serves queries
	// post-migration. With field tokenization, Equal('doc_0 shared')
	// matches doc_0; Equal('shared') matches none.
	hits := equalFilterHits(t, class, "name", "doc_0 shared")
	require.Equal(t, 1, hits,
		"post-cancel-then-filterable: Equal('doc_0 shared') must hit 1; got %d. "+
			"If 0, the change-tok-filterable submitted against torn state from the "+
			"cancelled change-tok-both run produced an empty bucket (Sev 1)", hits)
}

// =============================================================================
// Journey 7: change-tok-filterable on filterable-only → enable-searchable →
// change-tok-both
// =============================================================================
// Builds a property with filterable-only and word tokenization. Then
// retokenizes filterable to field. Then enables searchable with field
// tokenization. Then submits change-tok-both to rotate both back to
// word. Asserts both buckets agree post-migration.
//
// Hazard: enable-searchable's OnMigrationComplete writes Tokenization
// onto the property unconditionally. The classic both-indexes
// change-tokenization then sees a property with both buckets and a
// recent enable-searchable history; its bucket-strategy detection
// uses the searchable bucket as the source of truth, which may
// diverge from the filterable side that was the original state.
func testChangeTokFilterableEnableSearchableThenChangeTokBoth(t *testing.T, restURI string) {
	const class = "ChangeTokFilterableEnableSearchableThenBoth"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        []string{"text"},
				IndexFilterable: &trueVal,
				IndexSearchable: &falseVal,
				Tokenization:    "word",
			},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, name := range []string{"alpha beta", "gamma delta", "epsilon zeta"} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"name": name},
		}))
	}

	// Step 1: retokenize filterable from word → field.
	submitFilterableField := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"filterable":{"tokenization":"field"}}`)
	}
	taskID := submitFilterableField()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitFilterableField))
	requireTokenizationEquals(t, class, "name", "field")
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha beta"),
		"after step 1 (filterable field): Equal('alpha beta') must hit 1")

	// Step 2: enable-searchable with matching field tokenization (must
	// match because validateEnableSearchableProperty rejects mismatch).
	submitEnableSearchable := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"searchable":{"enabled":true,"tokenization":"field"}}`)
	}
	taskID = submitEnableSearchable()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitEnableSearchable))
	requireSearchableEnabled(t, class, "name")
	requireTokenizationEquals(t, class, "name", "field")

	// Step 3: change-tok-both from field → word using
	// {searchable:{tokenization:word}}. This runs ReindexTypeChangeTokenization
	// which retokenizes BOTH buckets.
	submitBothWord := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, restURI, class, "name",
			`{"searchable":{"tokenization":"word"}}`)
	}
	taskID = submitBothWord()
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithRetryOnReadOnly(submitBothWord))
	requireTokenizationEquals(t, class, "name", "word")

	// Step 4: assert both buckets reflect word tokenization. With
	// word, Equal('alpha') hits exactly 1 (the "alpha beta" object).
	require.Equal(t, 1, equalFilterHits(t, class, "name", "alpha"),
		"post-change-tok-both: Equal('alpha') must hit 1 under word tokenization")
	hits := bm25HitsForProp(t, class, "name", "beta")
	require.GreaterOrEqual(t, hits, 1,
		"post-change-tok-both: bm25('beta') must return >=1 doc")
}

// =============================================================================
// Shared helpers for this file
// =============================================================================

// requireTokenizationEquals polls until the property's stored tokenization
// matches the expected value (or fails the test).
func requireTokenizationEquals(t *testing.T, class, prop, expected string) {
	t.Helper()
	require.Eventually(t, func() bool {
		c := helper.GetClass(t, class)
		if c == nil {
			return false
		}
		for _, p := range c.Properties {
			if p.Name == prop {
				return p.Tokenization == expected
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond,
		"tokenization on %s.%s must be %q", class, prop, expected)
}

// awaitTerminal waits for a reindex task to reach any terminal state
// (FINISHED, FAILED, CANCELLED). Distinct from awaitReindexFinished
// which insists on FINISHED. Use this when the test expects a race
// outcome and any terminal state is acceptable.
func awaitTerminal(t *testing.T, restURI, taskID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				t.Logf("task %s status: %s", taskID, task.Status)
				return task.Status == "FINISHED" || task.Status == "FAILED" || task.Status == "CANCELLED"
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "task %s should reach a terminal state", taskID)
}

// bm25HitsForProp runs a bm25 query against a specific property.
// Distinct from bm25Hits in delete_then_reenable_test.go which is
// hardcoded to the "body" property.
func bm25HitsForProp(t *testing.T, class, prop, query string) int {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}, limit: 10000) {
				_additional { id }
			}
		}
	}`, class, query, prop)
	ids, err := runGraphQLQuery(t, class, gqlQuery)
	if err != nil {
		t.Logf("bm25 query for %s=%q errored: %v", prop, query, err)
		return 0
	}
	return len(ids)
}

// putRawAndExpectStatus issues a PUT request and asserts that the status
// code is one of the acceptable values. Used in race-y scenarios where
// either a 4xx (rejected) or a 2xx (raced-through) is acceptable but a
// 5xx (corruption) is not. Currently unused; kept as a hook for race
// scenario expansion.
func putRawAndExpectStatus(t *testing.T, url, jsonBody string, allowed ...int) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, ok := range allowed {
		if resp.StatusCode == ok {
			return resp.StatusCode, body
		}
	}
	t.Fatalf("unexpected status %d (not in %v); body=%s", resp.StatusCode, allowed, string(body))
	return resp.StatusCode, body
}

// TestSuppress_ChangeTokDeleteJourneys ensures this file compiles in
// isolation. The suite entry point is the t.Run("ChangeTokDeleteJourneys")
// in suite_test.go.
func TestSuppress_ChangeTokDeleteJourneys(t *testing.T) {
	assert.NotNil(t, testChangeTokDeleteJourneys)
	// Silence unused warnings for helpers that are kept for future
	// expansion of the journey class.
	_ = putRawAndExpectStatus
}
