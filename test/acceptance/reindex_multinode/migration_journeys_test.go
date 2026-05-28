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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
)

// TestMultiNode_BackToBackChangeTokenization_RoundTripCounts asserts
// that a change-tokenization round-trip (word → field → word) leaves
// the searchable bucket with the correct baseline count, not a
// partial/stale shape from the second migration reusing the prior
// migration's sidecar dirs.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/212 (Issue F)
// — production reproduction (`74e6c1d`, post C+D fix) showed Phase 5
// returning `path=1` (expected 7) on the back-to-original migration
// and Phase 8-final returning `path=7` (expected 5) on the third
// change-tokenization. Root cause: the second migration reused the
// first migration's sidecar dirs, hitting an already-tidied tracker
// state that short-circuited the work. Fixed by the per-migration
// generation suffix (`4a0557ec67`) and the cluster-wide schema flip
// move into OnTaskCompleted (`435b2c4a9f`).
//
// Shape (in-process, single restart-less journey):
//
//  1. Create class with a single text property `path`, tokenization=word.
//  2. Import 10k objects with hyphenated multi-token values
//     (e.g. "alpha-foo", "beta-bar", …). Pick the values so:
//     - The word-tokenized bucket gives N_word matches for a chosen
//     multi-token Equal query (matches every doc whose path
//     contains both tokens).
//     - The field-tokenized bucket gives N_field matches for the
//     same Equal query (matches only exact-string docs).
//     - N_word != N_field.
//  3. Verify the baseline count under WORD tokenization on every
//     replica equals N_word.
//  4. Submit change-tokenization to FIELD. Await FINISHED. Sleep 3 s.
//  5. Verify every replica returns N_field.
//  6. Submit change-tokenization back to WORD. Await FINISHED. Sleep 3 s.
//  7. Verify every replica returns N_word again — and importantly that
//     the count is not 0, 1, or some degenerate partial value, which
//     is the Phase-5 / Phase-8-final shape.
//
// The exact-value assertions are the headline check. The "≥ 2" sanity
// check below also catches the Phase-5 `path=1` degenerate shape even
// if my count math is off by a few in either direction.
func TestMultiNode_BackToBackChangeTokenization_RoundTripCounts(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "BackToBackTokenization"
	const totalObjects = 10_000

	trueVal := true
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() {
		deleteCollection(t, restURIOf(compose, 1), className)
	}()

	// Five distinct path values, hyphenated so WORD tokenization
	// produces 2 tokens per path (color + "shared") and FIELD gives 1
	// token per path. Each path appears 2000 times.
	//   path values:
	//     "red-shared", "green-shared", "blue-shared", "yellow-shared", "purple-shared"
	//
	// Equality query: path = "red-shared".
	//   FIELD tokenization → exact match → 2000 docs (only the red ones)
	//   WORD tokenization → AND-of-tokens ["red", "shared"] → 2000 docs
	//     (only red ones contain "red"; everyone contains "shared", AND is 2000)
	//
	// To make N_word != N_field we need a query that distinguishes. The
	// classic trigger from the 1M demo: use a value that exists only
	// as a SUBSTRING in some paths under WORD-tokenized matching.
	//
	// Switch to: query Equal "shared" (a single token).
	//   FIELD → exact "shared" → 0 matches (no path equals just "shared")
	//   WORD  → token "shared"     → 10000 matches (every path contains it)
	//
	// 0 vs 10000 makes the assertions trivial. The Phase-5 `path=1`
	// shape would show up as 0 or 1 from the partial bucket regardless
	// of which side we query under.
	paths := []string{"red-shared", "green-shared", "blue-shared", "yellow-shared", "purple-shared"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"path": paths[i%len(paths)],
		}
	})

	const (
		queryToken         = "shared" // single token, distinguishes WORD vs FIELD
		expectedWordCount  = totalObjects
		expectedFieldCount = 0
	)

	// === Step 3: baseline — WORD tokenization in place.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		require.NoError(t, err)
		require.Equal(t, expectedWordCount, got,
			"pre-mig WORD baseline on node %d = %d (expected %d) — replication or tokenization is broken",
			nodeIdx, got, expectedWordCount)
	}

	// === Step 4: change-tokenization to FIELD.
	uri1 := restURIOf(compose, 1)
	submitField := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, uri1, className, "path",
			`{"searchable":{"tokenization":"field"}}`)
	}
	task1 := submitField()
	t.Logf("change-tokenization → field: task=%s", task1)
	reindexhelpers.AwaitReindexFinished(t, uri1, task1, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitField))
	time.Sleep(3 * time.Second)

	// === Step 5: every replica must serve the FIELD count.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		require.NoError(t, err)
		require.Equal(t, expectedFieldCount, got,
			"post-FIELD-tok node %d = %d (expected %d)",
			nodeIdx, got, expectedFieldCount)
	}

	// === Step 6: change-tokenization back to WORD.
	submitWord := func() string {
		return reindexhelpers.SubmitIndexUpdate(t, uri1, className, "path",
			`{"searchable":{"tokenization":"word"}}`)
	}
	task2 := submitWord()
	t.Logf("change-tokenization → word (round-trip): task=%s", task2)
	reindexhelpers.AwaitReindexFinished(t, uri1, task2, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitWord))
	time.Sleep(3 * time.Second)

	// === Step 7: every replica must serve the WORD baseline again.
	// The degenerate "path=1" shape from the original production
	// reproduction (Phase 5) would surface as `got=1` here — the
	// assertion captures it via both the exact-count check and the
	// "must be > N/2" defense-in-depth check.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		uri := restURIOf(compose, nodeIdx)
		got, err := equalCount(uri, className, "path", queryToken)
		assert.NoError(t, err)
		assert.Equalf(t, expectedWordCount, got,
			"GH #212 Issue F regression: round-trip WORD count on node %d = %d (expected %d) "+
				"— back-to-back change-tokenization broke the searchable bucket",
			nodeIdx, got, expectedWordCount)
		assert.Greaterf(t, got, expectedWordCount/2,
			"GH #212 Issue F degenerate-count regression: node %d returned %d (less than half of %d). "+
				"This is the Phase-5 `path=1`-shape failure — the round-trip migration left the bucket "+
				"with effectively no data even though the task reached FINISHED",
			nodeIdx, got, expectedWordCount)
	}

	// LB-side 3-call stability for the round-trip state.
	for i := 0; i < 3; i++ {
		got, err := equalCount(uri1, className, "path", queryToken)
		require.NoError(t, err)
		assert.Equalf(t, expectedWordCount, got,
			"LB-side round-trip WORD count call #%d = %d (expected %d)",
			i+1, got, expectedWordCount)
	}
}

// TestMultiNode_RepeatedParallelMigrationJourney_PerReplicaConsistency
// asserts that repeated parallel migration journeys (multiple
// generations of trackers + sidecar dirs on disk pre-restart, then
// rolling restart + re-apply) converge to per-replica consistent
// counts — no in-memory bucket-pointer divergence on any replica.
//
// History: surfaced as
// https://github.com/weaviate/0-weaviate-issues/issues/212 — the
// 1M-record production reproduction returned per-replica different
// counts (e.g. `7×32, 3×30, 2×23, 6×5` histogram) on `spec_sheet_path`
// queries across all 3 pods at Phase 7 (post-restart) and Phase
// 8-final (post-restart-then-re-apply). Same family as Issue G
// (post_restart_test.go) but the multi-generation pre-restart state
// stresses different code paths: the recovery + finalize flow has to
// resolve a deeper stack of merged-but-not-tidied trackers.
//
// What this test does differently than
// TestMultiNode_PostRestartReapplyMigrations_ExactCountsAcrossReplicas:
//
//  1. **Repeated journey** — runs the full "3 parallel migrations +
//     reset" cycle 2 times PRE-restart. The above test does one
//     forward + one restart + one re-apply, leaving the system in a
//     simpler state pre-restart. The production flow leaves multiple
//     generations of migration trackers + sidecar dirs on disk
//     before the restart.
//  2. **Per-replica histogram** — runs the same query 30 times PER
//     REPLICA directly (90 total queries hitting all 3 replicas
//     evenly), mirroring the LB-fanout poll pattern. The above test
//     runs each query once per replica, which the `7×32, 3×30, 2×23,
//     6×5` flap would only catch if the test's single poll happens to
//     hit the bad replica. With 30 samples per replica we
//     deterministically detect divergence.
//  3. **Higher object count** — 25k objects (up from 10k in the
//     simpler post-restart test). The bug is LSM-segment-layout
//     sensitive; small datasets keep most data in the memtable and
//     miss the on-disk segment race regime.
//
// A failure of this test indicates the per-replica divergence is
// reachable at <100k scale and exposes it for local debugging.
func TestMultiNode_RepeatedParallelMigrationJourney_PerReplicaConsistency(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "RepeatedJourney"
		totalObjects = 25_000
		// 2 forward-and-reset cycles before the restart. Each cycle:
		//   forward: enable-rangeable + enable-filterable + change-tok(word→field)
		//   reset:   rangeable.rebuild + filterable.rebuild + change-tok(field→word)
		// After the cycles the schema is back at the original WORD
		// tokenization with all properties' indexes already through 2
		// round-trips. Then we restart and assert per-replica.
		preRestartCycles = 2
	)

	trueVal, falseVal := true, false
	createCollection(t, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:              "price",
			DataType:          []string{"int"},
			IndexFilterable:   &trueVal,
			IndexRangeFilters: &falseVal,
		},
		{
			Name:            "category",
			DataType:        []string{"text"},
			IndexFilterable: &falseVal,
			Tokenization:    "word",
		},
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	const (
		priceLo = 3000
		priceHi = 4000
		// price = i*2 for i in [0, 25000); inclusive range [3000, 4000]
		// covers i in [1500, 2000] → 501 objects.
		expectedPriceCount = 501
		// 25k / 4 categories
		expectedCatCount = 6250
		// 25k / 5 paths
		expectedPathCount = 5000
	)
	categories := []string{"alpha", "beta", "gamma", "delta"}
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{
			"price":    i * 2,
			"category": categories[i%len(categories)],
			"path":     paths[i%len(paths)],
		}
	})

	// First forward migration (parallel): the only one that's an
	// "enable" since the initial schema has filterable=false +
	// rangeable=false on those props. After this the props are
	// permanently filterable/rangeable so subsequent cycles use
	// "rebuild" instead.
	uri := restURIOf(compose, 1)
	t.Log("initial enable migrations (cycle 0)")
	{
		submitPrice := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "price",
				`{"rangeable":{"enabled":true}}`)
		}
		submitCategory := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "category",
				`{"filterable":{"enabled":true}}`)
		}
		submitPath := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "path",
				`{"searchable":{"tokenization":"field"}}`)
		}
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitPrice()
		}()
		go func() {
			defer wg.Done()
			tc = submitCategory()
		}()
		go func() {
			defer wg.Done()
			tk = submitPath()
		}()
		wg.Wait()
		reindexhelpers.AwaitReindexFinished(t, uri, tp, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitPrice))
		reindexhelpers.AwaitReindexFinished(t, uri, tc, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitCategory))
		reindexhelpers.AwaitReindexFinished(t, uri, tk, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitPath))
	}
	time.Sleep(2 * time.Second)

	// Repeated journey: rebuild + tokenization round-trip, N times. The
	// path tokenization alternates field↔word; price+category use
	// rebuild (no-op data-wise but exercises the FlushAndSwitch and
	// generation-trim path identically).
	for cycle := 1; cycle <= preRestartCycles; cycle++ {
		// Alternate path tokenization. After enable above schema is at
		// "field"; cycle 1 → "word", cycle 2 → "field", etc.
		nextTok := "word"
		if cycle%2 == 0 {
			nextTok = "field"
		}
		t.Logf("repeated journey cycle %d: rebuilds + tok→%s", cycle, nextTok)
		submitPrice := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "price",
				`{"rangeable":{"rebuild":true}}`)
		}
		submitCategory := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "category",
				`{"filterable":{"rebuild":true}}`)
		}
		submitPath := func() string {
			return reindexhelpers.SubmitIndexUpdate(t, uri, className, "path",
				fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, nextTok))
		}
		var (
			tp, tc, tk string
			wg         sync.WaitGroup
		)
		wg.Add(3)
		go func() {
			defer wg.Done()
			tp = submitPrice()
		}()
		go func() {
			defer wg.Done()
			tc = submitCategory()
		}()
		go func() {
			defer wg.Done()
			tk = submitPath()
		}()
		wg.Wait()
		reindexhelpers.AwaitReindexFinished(t, uri, tp, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitPrice))
		reindexhelpers.AwaitReindexFinished(t, uri, tc, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitCategory))
		reindexhelpers.AwaitReindexFinished(t, uri, tk, reindexhelpers.WithTimeout(180*time.Second), reindexhelpers.WithRetryOnReadOnly(submitPath))
		time.Sleep(2 * time.Second)
	}

	// After preRestartCycles cycles, the path tokenization is:
	//   cycle 1 → word
	//   cycle 2 → field
	// For preRestartCycles=2 we land at "field" (every value in `paths`
	// is one single FIELD token; equalCount returns expectedPathCount).
	finalTok := "field"
	if preRestartCycles%2 == 1 {
		finalTok = "word"
	}
	t.Logf("post-cycles schema tokenization = %s; preparing rolling restart", finalTok)

	// Pre-restart per-replica sanity. Every replica must return the
	// baseline for every prop before we restart — otherwise the
	// post-restart assertion is meaningless.
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		preURI := restURIOf(compose, nodeIdx)
		gotPrice, err := rangeCount(preURI, className, "price", priceLo, priceHi)
		require.NoError(t, err)
		require.Equalf(t, expectedPriceCount, gotPrice,
			"PRE-restart node %d price = %d (expected %d)", nodeIdx, gotPrice, expectedPriceCount)
		gotCat, err := equalCount(preURI, className, "category", categories[0])
		require.NoError(t, err)
		require.Equalf(t, expectedCatCount, gotCat,
			"PRE-restart node %d category = %d (expected %d)", nodeIdx, gotCat, expectedCatCount)
		gotPath, err := equalCount(preURI, className, "path", paths[0])
		require.NoError(t, err)
		require.Equalf(t, expectedPathCount, gotPath,
			"PRE-restart node %d path = %d (expected %d)", nodeIdx, gotPath, expectedPathCount)
	}

	// Rolling restart (one node at a time, wait for ready between
	// each). This is the same shape as Kubernetes StatefulSet rolling
	// updates, which matches the production deployment model.
	t.Log("rolling restart cluster")
	rollingRestartCluster(ctx, t, compose)

	// Settle: testcontainers reallocates ports across stop+start, so
	// re-resolve URIs on every use below. Give shard-init time to
	// finalize the deferred migration dirs (FinalizeCompletedMigrations
	// + bucket loading).
	time.Sleep(5 * time.Second)

	// === Headline assertion: per-replica histogram.
	//
	// Hit each replica directly 30 times for the path query (90 total).
	// If the in-memory bucket pointer diverged across replicas, the
	// histogram will show distinct values per replica (Frontend's
	// `7×32, 3×30, 2×23, 6×5` shape). If the buckets are consistent,
	// every poll returns expectedPathCount.
	const pollsPerReplica = 30

	for _, prop := range []struct {
		name, kind, value string
		expected          int
	}{
		{"price", "range", "", expectedPriceCount},
		{"category", "equal", categories[0], expectedCatCount},
		{"path", "equal", paths[0], expectedPathCount},
	} {
		histogram := make(map[string]map[int]int) // nodeID -> count -> N
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			nodeKey := fmt.Sprintf("node-%d", nodeIdx)
			histogram[nodeKey] = map[int]int{}
			for i := 0; i < pollsPerReplica; i++ {
				postURI := restURIOf(compose, nodeIdx)
				var got int
				var err error
				switch prop.kind {
				case "range":
					got, err = rangeCount(postURI, className, prop.name, priceLo, priceHi)
				case "equal":
					got, err = equalCount(postURI, className, prop.name, prop.value)
				}
				if err != nil {
					// Treat a query error as a distinct histogram bucket
					// at -1 so it's visible in the failure log.
					histogram[nodeKey][-1]++
					continue
				}
				histogram[nodeKey][got]++
			}
		}

		// Pass condition: every (node, poll) returned the expected count.
		allCorrect := true
		for _, perNode := range histogram {
			for value, n := range perNode {
				if value != prop.expected || n != pollsPerReplica {
					allCorrect = false
				}
			}
		}

		if !allCorrect {
			// Render histogram in the failure message so the per-replica
			// flap shape is visible — this is the smoking gun for
			// per-replica bucket-pointer divergence.
			lines := []string{
				fmt.Sprintf("GH #212 per-replica divergence on %q (expected %d, %d polls/replica):",
					prop.name, prop.expected, pollsPerReplica),
			}
			for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
				nodeKey := fmt.Sprintf("node-%d", nodeIdx)
				lines = append(lines,
					fmt.Sprintf("  %s: %+v", nodeKey, histogram[nodeKey]))
			}
			assert.Fail(t, "per-replica divergence", "%s", joinLines(lines))
		}
	}

	// === LB-side spot check (independent of per-replica direct
	// queries). 9 round-robin calls across nodes — Frontend uses ~90
	// LB calls in their poll loop, but 9 is enough to catch a clear
	// `0/3/2` per-replica divergence pattern.
	t.Log("LB-side 9-call spot check across nodes")
	for i := 0; i < 9; i++ {
		nodeIdx := (i % 3) + 1
		lbURI := restURIOf(compose, nodeIdx)
		gotPath, err := equalCount(lbURI, className, "path", paths[0])
		assert.NoError(t, err, "LB-side path query #%d (node %d)", i+1, nodeIdx)
		assert.Equalf(t, expectedPathCount, gotPath,
			"GH #212 LB-side path query #%d (node %d) = %d, expected %d",
			i+1, nodeIdx, gotPath, expectedPathCount)
	}
}

func joinLines(lines []string) string {
	var b []byte
	for i, l := range lines {
		if i > 0 {
			b = append(b, '\n')
		}
		b = append(b, l...)
	}
	return string(b)
}
