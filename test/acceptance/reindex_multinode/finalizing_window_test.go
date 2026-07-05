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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// partialWindowBudget bounds the mixed-state cutover window. 700ms matches
// the per-shard RUNSWAP+RAFT envelope on a representative disk; starved
// environments (e.g. soak VMs) exceed it without any convergence defect and
// may widen it via REINDEX_PARTIAL_WINDOW_BUDGET_MS.
func partialWindowBudget() time.Duration {
	if ms, err := strconv.Atoi(os.Getenv("REINDEX_PARTIAL_WINDOW_BUDGET_MS")); err == nil && ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return 700 * time.Millisecond
}

// TestLiveQueriesDuringChangeTokenization pins the correctness
// invariant of the per-shard tokenization overlay introduced for
// the per-shard tokenization overlay. Without the overlay, the
// FINALIZING window of a change-tokenization migration leaks a
// per-replica mismatch where the bucket pointer has flipped to
// NEW-tokenized data but the cluster-wide schema flag still serves
// the OLD tokenization to the query analyzer. The production
// reproduction observed this as a `7→4→1→7→3→2` count flap on a
// steady BM25 probe across replicas — the misalignment produces
// wrong-tokenized lookups against the new bucket, returning values
// that lie OUTSIDE the valid steady-state range.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/216 (Gap B),
// fixed in https://github.com/weaviate/weaviate/pull/11322 via the
// per-shard TokenizationFor overlay + the prep/atomic/defer phase
// split.
//
// The strongest direction-symmetric assertion: every probe sample
// during the migration must lie within [min(baseline, expectedAfter),
// max(baseline, expectedAfter)]. Out-of-range samples are direct
// evidence of the misalignment shape; with the overlay in place, no
// sample is ever out of range because every replica's query analyzer
// is aligned with its bucket content from the moment the bucket
// pointer flips:
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
// the looser cluster-wide ceiling pinned by
// TestPartialResultsDuringChangeTokenization (15 samples / 700ms) —
// the overlay does NOT shrink that window, it removes a specific
// failure mode (out-of-range counts) within it.
//
// Coverage: forward (word→field), reverse (field→word), filterable
// variant. Each subtest creates a fresh collection on the shared
// 3-node cluster.
//
// The reverse direction (field→word) is the cleanest assertion: 0
// is not a valid steady state (baseline=1, expectedAfter=1500), so
// any 0 sample is necessarily an alignment failure and trips
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
			// The misalignment failure shape the overlay closes: a
			// swapped replica's WORD bucket queried with FIELD
			// tokenization returns 0 (the full phrase is not a key
			// in the WORD bucket). 0 falls outside [1, 1500] →
			// out-of-range → caught by c.OutOfRange == 0 assertion.
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
	//
	// Wait for per-replica convergence first. batchImport uses default
	// write consistency; the synchronous POST returning does NOT
	// guarantee that every replica has finished its replication leg —
	// there can be a brief lag (typically <500ms) before all 3 nodes
	// return identical probe counts. Without this wait the baseline
	// captured here can be 5-10 below steady-state, and the
	// classifyProbeSamples helper will subsequently treat *every*
	// steady-state sample as out-of-range (count > captured baseline).
	// That misclassification produced 13 spurious failures on PR
	// #11323 CI run b19dd49366 / job 76404184658:
	//   baseline captured: 1495 (lagged replica)
	//   steady-state actual: 1500 (all replicas converged)
	//   13 samples observed count=1500, classified out-of-range.
	baselineCount := waitForProbeBaseline(t, compose, className, probe)
	require.Greater(t, baselineCount, 0,
		"baseline (%s tokenization) must return a stable count > 0 against probe %q "+
			"so partial samples during the migration are detectable", startTok, probeQuery)
	t.Logf("baseline (start=%s, converged across 3 replicas) count: %d", startTok, baselineCount)

	indexUpdateJSON := buildTokenizationIndexUpdate(t, indexType, targetTok)

	var taskID string
	samples, migrationStart := runMigrationWithProbes(t, compose, className,
		25*time.Millisecond, 2*time.Second, probe, func() {
			taskID = reindexhelpers.SubmitIndexUpdate(t, compose.GetWeaviateNode(1).URI(), className, "text", indexUpdateJSON)
			t.Logf("submitted change-tokenization-%s task: %s", indexType, taskID)
			reindexhelpers.AwaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID, reindexhelpers.WithTimeout(180*time.Second))
			require.Eventually(t, func() bool {
				return tryGetPropertyTokenization(compose.GetWeaviateNode(1).URI(),
					className, "text") == targetTok
			}, 60*time.Second, 50*time.Millisecond,
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

	// The overlay-correctness assertion (strongest,
	// direction-symmetric): no sample is ever OUT OF RANGE — i.e.
	// outside [min(baseline, expectedAfter), max(baseline,
	// expectedAfter)]. Without the per-shard tokenization overlay, a
	// swapped replica with stale schema returns the misalignment
	// count, which falls outside the valid steady-state range. With
	// the overlay this NEVER happens, because every replica's query
	// analyzer is aligned with its bucket content from the moment the
	// bucket pointer flips. Reverse direction (field→word)
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

	// Cross-shard cutover-spread bound. With weaviate/0-weaviate-issues#216,
	// partials in the valid range are still expected during the cluster-wide
	// cutover (multiple shards swapping at slightly different times); the
	// 700 ms ceiling catches a regression in the cutover orchestration
	// (e.g. swap spread getting longer because reactive firing breaks).
	//
	// We assert on the *window duration*, not the partial-sample count.
	// Count is `window × probe_rate × shard_count`, which inherits
	// CI's per-shard IO variance even when the design budget holds —
	// CI's container disk produces ~100 ms per-shard RunSwapOnShard
	// vs a healthy SSD's ~2 ms; same code, same orchestration, different
	// observable count. The window-duration assertion bounds what the
	// test author actually wanted (spread under design ceiling) and
	// is direction-symmetric and not probe-rate-dependent. The
	// out-of-range assertion above is what guards correctness.
	if c.Partial > 0 {
		windowDuration := c.LastPartial.Sub(c.FirstPartial)
		assert.LessOrEqualf(t, windowDuration, partialWindowBudget(),
			"partial-results window of %v exceeds budget of %v for %s→%s on %s — "+
				"the cross-shard cutover took longer than the design admits.",
			windowDuration, partialWindowBudget(), startTok, targetTok, indexType)
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

// TestPartialResultsDuringChangeTokenization pins the cluster-wide
// partial-results window during a change-tokenization reindex across
// multiple shards / nodes.
//
// Cutover sequence (Journey 3 canonical, reactive firing):
//
//  1. All shards finish the reindex phase. DTM's AllGroupUnitsTerminal
//     becomes true; the FSM apply path notifies each node's local
//     scheduler via SchedulerNotifier.Wake() instead of waiting for the
//     next periodic tick.
//  2. On every node, the scheduler reactively fires OnGroupCompleted →
//     RunSwapOnShard for each local shard. The strategy's
//     OnMigrationComplete is a no-op for semantic migrations; the
//     bucket pointer flip happens in-memory only.
//  3. Then OnTaskCompleted (also reactive) issues one RAFT-idempotent
//     UpdatePropertyInternal that flips the schema flag (Tokenization).
//     Multiple nodes' calls dedupe to a single committed entry.
//  4. The schema flip RAFT entry propagates to every node's local FSM,
//     which applies it and updates the in-memory schema.
//
// The partial-results window is bounded by the cross-node spread
// between "this node's OnGroupCompleted swap done" and "this node's
// FSM has applied the schema flip RAFT entry". With reactive firing
// the spread is dominated by RAFT replication latency (low tens of
// ms on a healthy cluster). Pre-reactive-firing this was bounded by
// the scheduler tick interval (default 1 minute, overridden to 1s in
// the test harness).
//
// The window is small enough that production queries during a
// change-tokenization migration may observe partial results, but the
// observation is bounded; the test pins that bound.
//
// We probe this by:
//
//   - Importing 1500 objects across 3 shards / 3 nodes (RF=3).
//   - Issuing a BM25 query for "alpha" against every node, every 25ms,
//     throughout the migration.
//   - Asserting the partial-results window is bounded:
//     1. duration <= partialWindowBudget (700ms — generous to absorb
//     CI noise; locally observed ~80ms)
//     2. AFTER the bounded window, every sample is either the full
//     baseline count or zero (no late partial samples).
//
// A more aggressive design (schema-version-aware bucket lookup via the
// BucketGeneration counter wired into the query path) would collapse
// this window to sub-ms per node. That's tracked as a follow-up; the
// current bound is the Journey 3 canonical design's best achievable.
func TestPartialResultsDuringChangeTokenization(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className   = "PartialResultsTokenize"
		shardCount  = 3
		rf          = 3
		objectCount = 1500
		batchSize   = 100
		alphaQuery  = "alpha"
		queryLimit  = 2000 // Must exceed objectCount so all matches are returned.
	)

	createCollection(t, compose.GetWeaviateNode(1).URI(), className, shardCount, rf,
		[]*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	// Every object contains the single-word token "alpha" so a BM25 query
	// against "alpha" returns ALL of them under WORD tokenization. After
	// the FIELD migration, "alpha" alone matches NONE — each object's text
	// becomes a single multi-word token.
	texts := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		texts = append(texts, fmt.Sprintf("alpha doc number %d filler", i))
	}

	// Import in batches via /v1/batch/objects for speed.
	batchImport(t, compose.GetWeaviateNode(1).URI(), className, texts, batchSize)

	// Baseline: WORD tokenization, "alpha" should return all docs.
	// Eventually-poll rather than asserting immediately so a narrow
	// post-batch indexing-visibility race in a replicated cluster
	// (one doc still propagating when the query lands) doesn't flag
	// a phantom regression — the assertion still fails fast if the
	// count never reaches objectCount.
	var baselineCount int
	require.Eventuallyf(t, func() bool {
		baselineCount = mustQueryBM25Count(t,
			compose.GetWeaviateNode(1).URI(), className, alphaQuery, queryLimit)
		return baselineCount == objectCount
	}, 10*time.Second, 50*time.Millisecond,
		"baseline BM25 'alpha' under WORD tokenization should match all %d docs (last seen=%d)",
		objectCount, baselineCount)
	t.Logf("baseline 'alpha' result count: %d", baselineCount)

	probe := func(uri, cn string) (int, error) {
		return queryBM25Count(uri, cn, alphaQuery, queryLimit)
	}

	var taskID string
	samples, migrationStart := runMigrationWithProbes(t, compose, className,
		25*time.Millisecond, 2*time.Second, probe, func() {
			taskID = reindexhelpers.SubmitIndexUpdate(t, compose.GetWeaviateNode(1).URI(),
				className, "text", `{"searchable":{"tokenization":"field"}}`)
			t.Logf("submitted change-tokenization task: %s", taskID)
			reindexhelpers.AwaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID, reindexhelpers.WithTimeout(180*time.Second))
			require.Eventually(t, func() bool {
				return tryGetPropertyTokenization(compose.GetWeaviateNode(1).URI(),
					className, "text") == "field"
			}, 60*time.Second, 50*time.Millisecond,
				"tokenization should change to field after swap phase")
		})
	t.Logf("migration completed in %v, collected %d probe samples",
		time.Since(migrationStart), len(samples))

	// Forward word→field migration: under FIELD tokenization, BM25
	// "alpha" matches none (every doc's text is one multi-word token),
	// so the post-migration steady-state count is 0.
	const expectedAfter = 0
	c := classifyProbeSamples(t, samples, baselineCount, expectedAfter, migrationStart)

	// Bound the partial-results window. The Journey 3 canonical design
	// admits a brief partial window during the cross-node cutover spread
	// (each node's reactive OnGroupCompleted swap + the cluster-wide
	// schema flip RAFT entry propagate independently). The window is
	// bounded by RAFT replication latency.
	//
	// Locally without the overlay we observe ~80ms / ~7 samples; with
	// the per-shard tokenization overlay this collapses well below the
	// budget. The test continues to enforce the looser ceiling as a
	// no-regression guard against either the cluster-wide cutover
	// spread or the per-shard alignment regressing.

	// Window-duration only. Partial-sample count is `window × probe_rate ×
	// shard_count`, which inherits per-shard IO variance even when the
	// design budget holds — CI's container disk produces ~100 ms per-shard
	// RunSwapOnShard vs a healthy SSD's ~2 ms; same code, same
	// orchestration, different observable count. The window-duration
	// assertion bounds what the test was actually pinning (spread under
	// design ceiling) and is direction-symmetric and not probe-rate
	// dependent.
	if c.Partial > 0 {
		windowDuration := c.LastPartial.Sub(c.FirstPartial)
		assert.LessOrEqual(t, windowDuration, partialWindowBudget(),
			"partial-results window of %v exceeds the budget of %v — "+
				"the cluster-wide cutover is taking longer than the bounded "+
				"RAFT-propagation + scheduler-wake design admits; investigate "+
				"the reactive-firing path (Manager.notifySchedulerWithLock, "+
				"Scheduler.wakeCh, OnGroupCompleted, OnTaskCompleted)",
			windowDuration, partialWindowBudget())
	}

	// Post-window guarantee: after the bounded partial window closes,
	// every sample must equal baseline or expectedAfter. A late partial
	// sample indicates the cutover is not bounded — a node lagged behind
	// for reasons other than RAFT propagation (e.g. its reactive wake
	// never fired). Walk forward from lastPartial + a small grace period
	// and assert no further partials.
	if !c.LastPartial.IsZero() {
		gracePeriod := 100 * time.Millisecond
		latePartial := countLatePartials(t, samples, baselineCount, expectedAfter,
			c.LastPartial.Add(gracePeriod), migrationStart)
		assert.Zero(t, latePartial,
			"observed %d partial samples after the bounded cutover window "+
				"(lastPartial + %v) — cutover is not converging",
			latePartial, gracePeriod)
	}
}

// batchImport posts objects in batches of `batchSize` using /v1/batch/objects
// at consistency_level=ALL.
//
// ALL (not the default) is load-bearing for the reverse (field→word)
// direction. Default batch consistency returns after a single-replica ack,
// leaving the other replicas' apply legs in flight. The reindex then
// snapshots each replica's object store at whatever count it currently
// holds — a lagging replica reindexes fewer objects, and its post-swap
// WORD bucket is permanently short until replication catches up. The
// forward direction's baseline gate hides this because it waits for WORD
// "alpha"==objectCount on every replica (a full-corpus convergence check),
// but the reverse baseline gate only waits for FIELD full-phrase==1 (doc #0
// alone), so under-replication of the other objects slips through and
// surfaces later as a sub-1500 count. On the starved soak VM this produced
// per-node reindex sums of 1499 / 1495 / 1500 (matching the probe reads
// bit-for-bit), failing postCount==1500 and inflating the partial window
// to seconds. ALL makes the corpus converged before the migration starts,
// so every replica reindexes the full 1500. Mirrors importObjects.
func batchImport(t *testing.T, restURI, className string, texts []string, batchSize int) {
	t.Helper()

	for start := 0; start < len(texts); start += batchSize {
		end := start + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		objects := make([]map[string]interface{}, 0, end-start)
		for _, text := range texts[start:end] {
			objects = append(objects, map[string]interface{}{
				"class": className,
				"id":    uuid.New().String(),
				"properties": map[string]interface{}{
					"text": text,
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"objects": objects})
		require.NoError(t, err)

		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"batch %d-%d failed: %s", start, end, string(respBody))

		// Verify no per-object errors.
		var batchResp []struct {
			Result struct {
				Errors *struct {
					Error []struct {
						Message string `json:"message"`
					} `json:"error"`
				} `json:"errors,omitempty"`
				Status string `json:"status"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(respBody, &batchResp))
		for i, br := range batchResp {
			if br.Result.Errors != nil && len(br.Result.Errors.Error) > 0 {
				t.Fatalf("batch %d-%d object %d errored: %s",
					start, end, i, br.Result.Errors.Error[0].Message)
			}
		}
	}
}

// queryBM25Count executes a BM25 query with an explicit limit and returns
// the count of matched objects. Returns the count + any error encountered.
func queryBM25Count(restURI, className, query string, limit int) (int, error) {
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}, limit: %d) {
				_additional { id }
			}
		}
	}`, className, query, limit)

	reqBody := map[string]interface{}{"query": gqlQuery}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return 0, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("reading response: %w", err)
	}
	var gqlResp struct {
		Data struct {
			Get map[string][]map[string]interface{} `json:"Get"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return 0, fmt.Errorf("graphql errors: %s", gqlResp.Errors[0].Message)
	}
	return len(gqlResp.Data.Get[className]), nil
}

func mustQueryBM25Count(t *testing.T, restURI, className, query string, limit int) int {
	t.Helper()
	count, err := queryBM25Count(restURI, className, query, limit)
	require.NoError(t, err)
	return count
}
