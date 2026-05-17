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

package reindex_multinode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// TestLiveQueriesDuringChangeTokenization pins the correctness
// invariant of the per-shard tokenization overlay introduced for
// 0-weaviate-issues#216 Gap B. Without the overlay, the FINALIZING
// window of a change-tokenization migration leaks a per-replica
// mismatch where the bucket pointer has flipped to NEW-tokenized data
// but the cluster-wide schema flag still serves the OLD tokenization
// to the query analyzer. Frontend Claude observed this as a
// `7→4→1→7→3→2` count flap on a steady BM25 probe across replicas —
// the misalignment produces wrong-tokenized lookups against the new
// bucket, returning values that lie OUTSIDE the valid steady-state
// range.
//
// The strongest direction-symmetric assertion: every probe sample
// during the migration must lie within [min(baseline, expectedAfter),
// max(baseline, expectedAfter)]. Out-of-range samples are direct
// evidence of the #216 misalignment shape; with the overlay landed,
// no sample is ever out of range because every replica's query
// analyzer is aligned with its bucket content from the moment the
// bucket pointer flips:
//
//   - Pre-swap: overlay=NEW, bucket=OLD (in-memory swap latency
//     bounds this to microseconds; query returns OLD baseline).
//   - Post-swap: overlay=NEW, bucket=NEW → query returns NEW
//     expectedAfter.
//   - Post-flip: overlay self-clears or is explicitly cleared by
//     OnTaskCompleted; live schema=NEW matches bucket=NEW → query
//     still returns NEW expectedAfter.
//
// In-range partials ARE still expected — the cluster-wide cutover
// has a real cross-shard spread (every node's reactive
// OnGroupCompleted swap and the cluster-wide schema-flip RAFT entry
// propagate independently). The partial window bound here matches
// TestPartialResultsDuringChangeTokenization's pre-#216 ceiling (15
// samples / 700ms) — #216 does NOT shrink that window, it removes a
// specific failure mode (out-of-range counts) within it.
//
// Coverage: forward (word→field), reverse (field→word), filterable
// variant. Each subtest creates a fresh collection on the shared
// 3-node cluster.
//
// The reverse direction (field→word) is the clean #216 test: 0 is
// not a valid steady state (baseline=1, expectedAfter=1500), so any
// 0 sample is necessarily an alignment failure and trips
// c.OutOfRange. The forward direction is included for completeness
// and to cover the BM25 path under word→field reindex; the
// out-of-range assertion is degenerate there but the late-partial
// and budget assertions still apply.
func TestLiveQueriesDuringChangeTokenization(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	cases := []struct {
		name       string
		startTok   string // tokenization at class creation
		targetTok  string // tokenization after migration
		indexType  string // "searchable" (change-tokenization) or "filterable" (change-tokenization-filterable)
		probeQuery string // for log messages only — actual query is in `probe`
		probe      probeFn
		// expectedAfter is the count the probe should yield after the
		// migration commits. We use it only as a sanity check; the
		// in-flight assertion is the partial-sample budget.
		expectedAfter int
	}{
		{
			name:       "forward_word_to_field_searchable",
			startTok:   "word",
			targetTok:  "field",
			indexType:  "searchable",
			probeQuery: "alpha",
			probe: func(uri, cn string) (int, error) {
				return queryBM25Count(uri, cn, "alpha", 2000)
			},
			expectedAfter: 0, // "alpha" alone matches none under FIELD
		},
		{
			name:       "reverse_field_to_word_searchable",
			startTok:   "field",
			targetTok:  "word",
			indexType:  "searchable",
			probeQuery: "alpha doc number 0 filler",
			probe: func(uri, cn string) (int, error) {
				return queryBM25Count(uri, cn, "alpha doc number 0 filler", 2000)
			},
			// Under FIELD: the probe matches exactly one object (the
			// stored phrase equals the query phrase verbatim).
			// Under WORD: BM25 tokenizes the query into individual
			// words [alpha, doc, number, 0, filler]; every doc
			// contains all of these except for `0` (only one doc has
			// that), so every doc scores some non-zero match.
			//
			// The pre-#216 misalignment failure shape: a swapped
			// replica's WORD bucket queried with FIELD tokenization
			// returns 0 (the full phrase is not a key in the WORD
			// bucket). 0 falls outside [1, 1500] → out-of-range →
			// caught by c.OutOfRange == 0 assertion.
			expectedAfter: 1500,
		},
		{
			name:       "forward_word_to_field_filterable",
			startTok:   "word",
			targetTok:  "field",
			indexType:  "filterable",
			probeQuery: "alpha",
			// Equal filter exercises the Searcher (not BM25Searcher)
			// path via the aggregator's buildAllowList. Under WORD
			// tokenization, Equal("alpha") on the filterable bucket
			// matches every doc; under FIELD it matches none.
			probe: func(uri, cn string) (int, error) {
				return equalCount(uri, cn, "text", "alpha")
			},
			expectedAfter: 0,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runLiveQueryDuringChangeTokenizationCase(t, compose, tc.startTok,
				tc.targetTok, tc.indexType, tc.probeQuery, tc.probe, tc.expectedAfter)
		})
	}
}

func runLiveQueryDuringChangeTokenizationCase(
	t *testing.T,
	compose *docker.DockerCompose,
	startTok, targetTok, indexType, probeQuery string,
	probe probeFn,
	expectedAfter int,
) {
	const (
		shardCount  = 3
		rf          = 3
		objectCount = 1500
		batchSize   = 100
		// Budget for the cross-shard cutover window. The cluster-wide
		// schema flip in OnTaskCompleted propagates via RAFT after every
		// node has run its local OnGroupCompleted swap, so a brief
		// window of mixed states (some shards swapped, some not) is
		// expected on every replica regardless of #216. The bound here
		// matches TestPartialResultsDuringChangeTokenization's pre-#216
		// bound (15 / 700ms): #216 does NOT shrink the cross-shard
		// spread — it closes a specific failure MODE within it (the
		// out-of-range "0 from a swapped replica with stale schema"
		// shape, asserted separately below by c.OutOfRange == 0).
		partialSampleBudget = 15
		partialWindowBudget = 700 * time.Millisecond
	)

	className := fmt.Sprintf("LiveQueryTok_%s_%s_%s", startTok, targetTok, indexType)

	createCollection(t, compose.GetWeaviateNode(1).URI(), className, shardCount, rf,
		[]*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: startTok},
		})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	// Generate `objectCount` documents that each contain the literal
	// token "alpha" inside a multi-word phrase. Under WORD tokenization,
	// "alpha" matches every doc; under FIELD it matches none because
	// the entire phrase is one token. The full-phrase query matches
	// exactly one object under FIELD only.
	texts := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		texts = append(texts, fmt.Sprintf("alpha doc number %d filler", i))
	}
	batchImport(t, compose.GetWeaviateNode(1).URI(), className, texts, batchSize)

	// Baseline: at startTok, the probe query must return >0 objects so
	// the FINALIZING-window partial samples are visible against a
	// stable baseline. If the probe matches zero at startTok we'd
	// classify steady-state samples as partial, blowing the budget.
	baselineCount, err := probe(compose.GetWeaviateNode(1).URI(), className)
	require.NoError(t, err)
	require.Greater(t, baselineCount, 0,
		"baseline (%s tokenization) must return a stable count > 0 against probe %q "+
			"so partial samples during the migration are detectable", startTok, probeQuery)
	t.Logf("baseline (start=%s) count: %d", startTok, baselineCount)

	indexUpdateJSON := buildTokenizationIndexUpdate(t, indexType, targetTok)

	var taskID string
	samples, migrationStart := runMigrationWithProbes(t, compose, className,
		25*time.Millisecond, 2*time.Second, probe, func() {
			taskID = submitIndexUpdate(t, compose.GetWeaviateNode(1).URI(), className, "text", indexUpdateJSON)
			t.Logf("submitted change-tokenization-%s task: %s", indexType, taskID)
			awaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID)
			require.Eventually(t, func() bool {
				return tryGetPropertyTokenization(compose.GetWeaviateNode(1).URI(),
					className, "text") == targetTok
			}, 60*time.Second, 200*time.Millisecond,
				"tokenization should change to %s after swap phase", targetTok)
		})
	t.Logf("migration completed in %v, collected %d probe samples",
		time.Since(migrationStart), len(samples))

	c := classifyProbeSamples(t, samples, baselineCount, expectedAfter, migrationStart)

	// Sanity check that the post-flip steady state is what we expect
	// (not just zero everywhere — that'd be silent failure).
	postCount, perr := probe(compose.GetWeaviateNode(1).URI(), className)
	require.NoError(t, perr, "post-migration probe must succeed")
	assert.Equal(t, expectedAfter, postCount,
		"post-migration (%s tokenization) probe count should equal %d", targetTok, expectedAfter)

	// The #216-fix assertion (strongest, direction-symmetric): no
	// sample is ever OUT OF RANGE — i.e. outside [min(baseline,
	// expectedAfter), max(baseline, expectedAfter)]. The pre-#216
	// failure shape produces exactly this: a swapped replica with
	// stale schema returns the misalignment count, which falls
	// outside the valid steady-state range. With the per-shard
	// tokenization overlay this NEVER happens, because every replica's
	// query analyzer is aligned with its bucket content from the
	// moment the bucket pointer flips. Reverse direction (field→word)
	// is the clean test: 0 is not a valid steady state, so any 0
	// sample is necessarily an alignment failure. Forward direction
	// (word→field) is degenerate (0 is also expectedAfter, so
	// out-of-range coincides with above-baseline, which the cluster
	// physically cannot produce).
	assert.Zerof(t, c.OutOfRange,
		"observed %d out-of-range samples for %s→%s on %s — every sample "+
			"must lie within [min(baseline, expectedAfter)=%d, "+
			"max(baseline, expectedAfter)=%d]. The per-shard tokenization "+
			"overlay (Shard.TokenizationFor, BM25Searcher.WithTokenizationResolver, "+
			"Searcher.WithTokenizationResolver, aggregator.New tokResolver) is "+
			"not keeping the query analyzer aligned with bucket content during "+
			"the FINALIZING window. Investigate the resolver wiring and the "+
			"set/clear hooks in OnGroupCompleted / OnTaskCompleted.",
		c.OutOfRange, startTok, targetTok, indexType,
		minInt(baselineCount, expectedAfter), maxInt(baselineCount, expectedAfter))

	// Cross-shard cutover budget. With #216, partials in the valid
	// range are still expected during the cluster-wide cutover spread
	// (multiple shards swapping at slightly different times); the
	// bound matches TestPartialResultsDuringChangeTokenization. The
	// out-of-range assertion above is what's tight; this assertion
	// catches a regression in the cutover orchestration (e.g. swap
	// spread getting longer because reactive firing breaks).
	assert.LessOrEqualf(t, c.Partial, partialSampleBudget,
		"observed %d in-range partial samples (budget=%d) for %s→%s on %s — "+
			"the cross-shard cutover window is wider than the bounded "+
			"RAFT-propagation + scheduler-wake design admits; investigate the "+
			"reactive-firing path (Manager.notifySchedulerWithLock, "+
			"Scheduler.wakeCh, OnGroupCompleted, OnTaskCompleted).",
		c.Partial, partialSampleBudget, startTok, targetTok, indexType)

	if c.Partial > 0 {
		windowDuration := c.LastPartial.Sub(c.FirstPartial)
		assert.LessOrEqualf(t, windowDuration, partialWindowBudget,
			"partial-results window of %v exceeds budget of %v for %s→%s on %s — "+
				"the cross-shard cutover took longer than the design admits.",
			windowDuration, partialWindowBudget, startTok, targetTok, indexType)
	}

	// Post-window guarantee: after the bounded partial window closes
	// (or, if there were zero partials, after the migration finished),
	// every sample must equal baseline or expectedAfter. A late partial
	// indicates the cutover is not converging.
	var afterAnchor time.Time
	if !c.LastPartial.IsZero() {
		afterAnchor = c.LastPartial.Add(100 * time.Millisecond)
	} else {
		afterAnchor = migrationStart
	}
	latePartial := countLatePartials(t, samples, baselineCount, expectedAfter, afterAnchor, migrationStart)
	assert.Zerof(t, latePartial,
		"observed %d partial samples after the bounded cutover window for %s→%s on %s — "+
			"cutover is not converging; either the overlay isn't being cleared "+
			"or the schema flip isn't propagating to every replica.",
		latePartial, startTok, targetTok, indexType)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// buildTokenizationIndexUpdate produces the index-update request body
// for a change-tokenization migration. Split out of the test body so
// the assertion logic above stays readable.
func buildTokenizationIndexUpdate(t *testing.T, indexType, targetTok string) string {
	t.Helper()
	switch indexType {
	case "searchable":
		return fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTok)
	case "filterable":
		return fmt.Sprintf(`{"filterable":{"tokenization":%q}}`, targetTok)
	default:
		t.Fatalf("unsupported indexType %q", indexType)
		return ""
	}
}
