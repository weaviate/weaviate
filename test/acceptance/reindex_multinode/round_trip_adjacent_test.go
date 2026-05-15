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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_ChangeTokenization_AdjacentJourneys enumerates every realistic
// adjacent journey to the word→field→word round-trip data-loss bug
// (weaviate/weaviate#10675). Each subtest creates a fresh collection on the
// same shared 3-node cluster (saves the ~20s per-suite startup tax).
//
// Each journey is independently RED-expected on the current branch state.
// The failure pattern to look for is the same as the pinning test:
//
//   - task reaches FINISHED on every node
//   - the schema flag flips correctly on every node
//   - but on N-1 replicas, the post-migration inverted bucket is empty,
//     so a BM25/filter query routed directly to that replica returns 0
//     hits for every term that should have matched.
//
// Cross-journey predictions encoded as assertions (any of these failing
// counts as a Sev-1 finding worth a separate fix):
//
//  1. MultiRoundRobin_3rounds / _4rounds: do the empty-bucket replicas
//     STAY broken across more rounds, or does the bug compound (more
//     replicas empty)?
//  2. DifferentTokenizations_*: is the bug specific to word↔field, or
//     does it also bite word↔whitespace, word↔lowercase, etc.?
//  3. MultipleProperties: do two simultaneous round-trips on different
//     props collide via shared migration dirs (they shouldn't — per
//     MigrationDirName they're per-prop — but if they do, that's a
//     separate Sev-1)?
//  4. FilterableOnly_RoundTrip: same bug shape on filterable=true,
//     searchable=false via the change-tokenization-filterable body
//     shape?
//  5. SearchableOnly_RoundTrip: same bug shape on searchable=true,
//     filterable=false (change-tok-both is impossible here, only
//     {"searchable":{"tokenization":X}} applies)?
//  6. EnableFilterableThenChangeTok: does enable-filterable's
//     tidied.mig poison the subsequent change-tokenization migration
//     dir state?
//  7. EnableSearchableThenChangeTok: same idea for enable-searchable.
//
// Each subtest gets its own fresh 3-node cluster. The previous
// shared-cluster pattern was non-deterministic: after several
// migrations across different collections on the same long-lived
// cluster, some node would end up with persistently divergent
// per-replica data, even though the same subtest passed cleanly in
// isolation. The pattern is the same shape as the original #10675
// data-loss (one replica with less data than the others), so the
// shared-cluster cross-collection accumulation is itself worth a
// separate Sev investigation — but the journey-specific assertions
// each subtest pins (does word↔whitespace round-trip work? does
// enable-filterable then change-tok work?) are about the journey, not
// about accumulated state. Running each in its own cluster keeps the
// journey signal clean and surfaces a different test if the
// cross-collection accumulation reproduces.
// withFreshCluster wraps a body-fn in the standard 3-node testcontainer
// lifecycle. Shared by every AdjacentJourneys top-level test so the per-
// subtest isolation that the original `bc5710c84c` refactor introduced
// is preserved: each subtest gets a fresh cluster with no inter-test
// state pollution.
func withFreshCluster(t *testing.T, body func(t *testing.T, compose *docker.DockerCompose)) {
	t.Helper()
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)
	body(t, compose)
}

// The AdjacentJourneys suite was originally a single TestMultiNode_*
// with 10 subtests, each spinning up its own 3-node cluster. Sequentially
// that exhausted the 20-minute `go test` deadline (~10×~90s startup +
// migration work = 15+min, plus container teardown). Splitting into one
// top-level Test* per logical bucket of journeys means:
//
//  1. Each Test* gets its own 20-minute deadline.
//  2. CI can shard reindex-multinode by -run filter and run shards in
//     parallel (see `test/run.sh` and `.github/workflows/*.yml`).
//
// Per-subtest cluster isolation is retained via `withFreshCluster`.
//
// The buckets group functionally-related journeys so a CI shard hitting
// one bucket doesn't take a wildly different amount of wall-time than
// the next.

// TestMultiNode_ChangeTokenization_AJ_MultiRoundRobin pins the original
// #10675-shape round-trips: alternating word↔field across 3 and 4 rounds.
// Bucket: round-robin journeys (Journey 1).
func TestMultiNode_ChangeTokenization_AJ_MultiRoundRobin(t *testing.T) {
	t.Run("MultiRoundRobin_3rounds", func(t *testing.T) {
		// Journey 1: word→field→word→field. Does the bug compound?
		// Predict: 3rd round leaves even more replicas with empty buckets
		// than the 2nd round did.
		withFreshCluster(t, func(t *testing.T, compose *docker.DockerCompose) {
			testRoundTripNRounds(t, compose,
				"RoundTrip3", "word",
				[]string{"field", "word", "field"})
		})
	})

	t.Run("MultiRoundRobin_4rounds", func(t *testing.T) {
		// Journey 1 (extended): word→field→word→field→word.
		withFreshCluster(t, func(t *testing.T, compose *docker.DockerCompose) {
			testRoundTripNRounds(t, compose,
				"RoundTrip4", "word",
				[]string{"field", "word", "field", "word"})
		})
	})
}

// TestMultiNode_ChangeTokenization_AJ_DifferentTokenizations exercises
// non-word-field tokenizer pairs to confirm the bug class is or isn't
// specific to one tokenizer. Bucket: Journey 2 variants.
func TestMultiNode_ChangeTokenization_AJ_DifferentTokenizations(t *testing.T) {
	t.Run("DifferentTokenizations_word_whitespace_word", func(t *testing.T) {
		// Journey 2: word→whitespace→word. Same migration shape but a
		// different tokenizer pair. If only word↔field is broken, this
		// passes; if the bug is universal to change-tokenization
		// round-trips, this fails.
		withFreshCluster(t, func(t *testing.T, compose *docker.DockerCompose) {
			testRoundTripNRounds(t, compose,
				"RoundTripWS", "word",
				[]string{"whitespace", "word"})
		})
	})

	t.Run("DifferentTokenizations_word_lowercase_word", func(t *testing.T) {
		// Journey 2 (variant): word→lowercase→word.
		withFreshCluster(t, func(t *testing.T, compose *docker.DockerCompose) {
			testRoundTripNRounds(t, compose,
				"RoundTripLC", "word",
				[]string{"lowercase", "word"})
		})
	})

	t.Run("DifferentTokenizations_word_field_lowercase", func(t *testing.T) {
		// Journey 2 (variant): word→field→lowercase. Asymmetric — the
		// round-trip doesn't end where it started, but the bug could
		// still manifest on the second migration.
		withFreshCluster(t, func(t *testing.T, compose *docker.DockerCompose) {
			testRoundTripNRounds(t, compose,
				"RoundTripAsym", "word",
				[]string{"field", "lowercase"})
		})
	})
}

// TestMultiNode_ChangeTokenization_AJ_MultiProperty pins the multi-property
// concurrent-round-trip case (Journey 3). Single subtest, separate Test*
// so it gets its own 20m budget — the multi-property setup is the
// slowest subtest in the suite.
func TestMultiNode_ChangeTokenization_AJ_MultiProperty(t *testing.T) {
	t.Run("MultipleProperties_simultaneous", func(t *testing.T) {
		// Journey 3: two text properties, both word→field→word in
		// sequence on the same collection. Per MigrationDirName the
		// migration dirs are per-property, so collisions across props
		// should not happen — if any of the per-property baselines goes
		// to zero on any replica, that's a separate Sev-1.
		withFreshCluster(t, testMultiPropertyRoundTrip)
	})
}

// TestMultiNode_ChangeTokenization_AJ_FilterableSearchable covers the
// filterable-only and searchable-only variants (Journeys 4 and 5).
func TestMultiNode_ChangeTokenization_AJ_FilterableSearchable(t *testing.T) {
	t.Run("FilterableOnly_RoundTrip", func(t *testing.T) {
		// Journey 4: round-trip via {"filterable":{"tokenization":X}}
		// on a filterable-only property. Different reindexer
		// (FilterableRetokenizeStrategy) but the same swap+schema-flip
		// state machine.
		withFreshCluster(t, testFilterableOnlyRoundTrip)
	})

	t.Run("SearchableOnly_RoundTrip", func(t *testing.T) {
		// Journey 5: filterable=false, searchable=true. The only valid
		// body shape is {"searchable":{"tokenization":X}}. No sub-task
		// fan-out for filterable index, so a simpler shape — but still
		// the same swap+schema-flip path.
		withFreshCluster(t, testSearchableOnlyRoundTrip)
	})
}

// TestMultiNode_ChangeTokenization_AJ_EnableThenChange covers the
// "enable-then-change-tokenization" sequences (Journeys 6 and 7), which
// stress whether one strategy's residual state leaks into the next.
func TestMultiNode_ChangeTokenization_AJ_EnableThenChange(t *testing.T) {
	t.Run("EnableFilterableThenChangeTok", func(t *testing.T) {
		// Journey 6: a property starts filterable=false. We enable
		// filterable (which writes tidied.mig to a per-prop dir under
		// .migrations/), then immediately change-tokenization on the
		// same property. Does enable-filterable's residual state
		// interfere with the change-tok migration?
		withFreshCluster(t, testEnableFilterableThenChangeTok)
	})

	t.Run("EnableSearchableThenChangeTok", func(t *testing.T) {
		// Journey 7: same idea — start searchable=false, enable
		// searchable, then change-tokenization.
		withFreshCluster(t, testEnableSearchableThenChangeTok)
	})
}

// TestMultiNode_ChangeTokenization_RestartThenRoundTrip pins journey 8:
// T1 word→field, RESTART every node (graceful), then T2 field→word.
// Hypothesis: a node restart between rounds triggers
// FinalizeCompletedMigrations on shard init, which cleans up the
// completed-but-not-tidied migration directory for the first migration.
// If that cleanup is what's missing from the in-process round-trip path,
// a restart-between should produce CONSISTENT replicas where the
// in-process version produces empty ones.
//
// Standalone test (cluster-restart shape doesn't share with the
// AdjacentJourneys cluster).
func TestMultiNode_ChangeTokenization_RestartThenRoundTrip(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartRoundTrip"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, restURI, className, testDocuments)
	// Pre-T1 baseline assertion (side-effect: also waits for per-replica
	// consistency). The actual baseline used for the post-T2 comparison
	// is recaptured after the rolling restart below, because the raft-
	// probe imports change the doc count.
	_ = waitForPerReplicaBaseline(t, compose, className, testBM25Queries)

	// T1: word → field.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// Restart every node, one at a time, so FinalizeCompletedMigrations
	// runs on each node's shard init.
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		t.Logf("restarting node %d between rounds", nodeIdx+1)
		require.NoError(t, compose.StopAt(ctx, nodeIdx, nil))
		require.NoError(t, compose.StartAt(ctx, nodeIdx))

		restartedURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
		require.Eventually(t, func() bool {
			_, err := runBM25QueryOnNode(t, restartedURI, className, "alpha")
			return err == nil
		}, 60*time.Second, 1*time.Second,
			"node %d should be ready after restart", nodeIdx+1)

		writeURI := compose.GetWeaviateNode(((nodeIdx + 1) % 3) + 1).URI()
		require.Eventually(t, func() bool {
			return tryImportObject(writeURI, className, "raft-probe") == nil
		}, 90*time.Second, 1*time.Second,
			"raft quorum should be restored after restart of node %d", nodeIdx+1)
	}
	// Re-fetch URI after the rolling restart.
	restURI = compose.GetWeaviateNode(1).URI()

	// The "raft-probe" imports above add objects (one per restart). We
	// only care about per-replica consistency post-T2, not exact baseline
	// counts, so re-record the baseline now (still has to be consistent
	// across replicas).
	baselines := waitForPerReplicaBaseline(t, compose, className, testBM25Queries)

	// T2: field → word.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	assertPerReplicaConsistent(t, compose, className, testBM25Queries, baselines,
		"restart-between-rounds")
}

// TestMultiNode_ChangeTokenization_MTRoundTrip pins journey 9: same
// word→field→word, but on a multi-tenant class. Per-tenant tracker paths
// might bypass the bug (different on-disk layout) — or they might hit
// the same root cause and break per-tenant.
func TestMultiNode_ChangeTokenization_MTRoundTrip(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "MTRoundTrip"
	const tenant = "tenanta"
	restURI := compose.GetWeaviateNode(1).URI()

	createMTCollection(t, restURI, className, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	createTenants(t, restURI, className, []string{tenant})
	importObjectsTenant(t, restURI, className, tenant, testDocuments)

	// Per-tenant baseline.
	baselines := make(map[string][]int)
	for _, q := range testBM25Queries {
		counts := perNodeBM25CountsTenant(t, compose, className, tenant, q)
		require.Equalf(t, counts[0], counts[1],
			"MT baseline %q inconsistent: n1=%d n2=%d", q, counts[0], counts[1])
		require.Equalf(t, counts[0], counts[2],
			"MT baseline %q inconsistent: n1=%d n3=%d", q, counts[0], counts[2])
		require.Greaterf(t, counts[0], 0, "MT baseline %q must match", q)
		baselines[q] = counts
	}

	// word → field.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// field → word.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	var failures []string
	for _, q := range testBM25Queries {
		actual := perNodeBM25CountsTenant(t, compose, className, tenant, q)
		expected := baselines[q]
		t.Logf("MT post-round-trip %q: baseline=%v actual=%v", q, expected, actual)
		for i := 0; i < 3; i++ {
			if actual[i] != expected[i] {
				failures = append(failures,
					fmt.Sprintf("query=%q node%d tenant=%s expected=%d actual=%d",
						q, i+1, tenant, expected[i], actual[i]))
			}
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf(
			"per-replica MT inverted-bucket mismatch after word→field→word; %d mismatches:\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}
}

// TestMultiNode_ChangeTokenization_ConcurrentDifferentProps pins
// journey 10: two distinct text properties getting change-tok migrations
// concurrently. Per MigrationDirName the dirs are per-property, so
// collisions shouldn't happen — but if the in-process scheduler
// serializes through any shared per-shard state, this can expose it.
func TestMultiNode_ChangeTokenization_ConcurrentDifferentProps(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "ConcurrentProps"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	// Each object has distinct content in title and body so we can
	// query each property independently.
	importObjectsTwoProps(t, restURI, className, testDocuments)

	baselinesTitle := captureBaselineCounts(t, compose, className, "title", testBM25Queries)
	baselinesBody := captureBaselineCounts(t, compose, className, "body", testBM25Queries)

	// Fire two change-tok migrations in parallel.
	var wg sync.WaitGroup
	var titleTaskID, bodyTaskID string
	wg.Add(2)
	go func() {
		defer wg.Done()
		titleTaskID = submitIndexUpdate(t, restURI, className, "title",
			`{"searchable":{"tokenization":"field"}}`)
	}()
	go func() {
		defer wg.Done()
		bodyTaskID = submitIndexUpdate(t, restURI, className, "body",
			`{"searchable":{"tokenization":"field"}}`)
	}()
	wg.Wait()

	awaitReindexFinished(t, restURI, titleTaskID)
	awaitReindexFinished(t, restURI, bodyTaskID)
	awaitTokenizationOnAllNodes(t, compose, className, "title", "field")
	awaitTokenizationOnAllNodes(t, compose, className, "body", "field")

	// Now reverse, in parallel.
	wg.Add(2)
	go func() {
		defer wg.Done()
		titleTaskID = submitIndexUpdate(t, restURI, className, "title",
			`{"searchable":{"tokenization":"word"}}`)
	}()
	go func() {
		defer wg.Done()
		bodyTaskID = submitIndexUpdate(t, restURI, className, "body",
			`{"searchable":{"tokenization":"word"}}`)
	}()
	wg.Wait()

	awaitReindexFinished(t, restURI, titleTaskID)
	awaitReindexFinished(t, restURI, bodyTaskID)
	awaitTokenizationOnAllNodes(t, compose, className, "title", "word")
	awaitTokenizationOnAllNodes(t, compose, className, "body", "word")

	// Poll until each property's per-replica counts settle to the
	// baseline. The schema flip propagates faster than per-node
	// OnGroupCompleted runs in some races; see perReplicaConvergenceTimeout.
	var (
		lastTitle, lastBody map[string][]int
		failures            []string
	)
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		lastTitle = make(map[string][]int, len(testBM25Queries))
		lastBody = make(map[string][]int, len(testBM25Queries))
		failures = failures[:0]
		for _, q := range testBM25Queries {
			titleActual := perNodeBM25CountsProperty(t, compose, className, "title", q)
			bodyActual := perNodeBM25CountsProperty(t, compose, className, "body", q)
			lastTitle[q] = titleActual
			lastBody[q] = bodyActual
			for i := 0; i < 3; i++ {
				if titleActual[i] != baselinesTitle[q][i] {
					failures = append(failures,
						fmt.Sprintf("prop=title query=%q node%d expected=%d actual=%d",
							q, i+1, baselinesTitle[q][i], titleActual[i]))
				}
				if bodyActual[i] != baselinesBody[q][i] {
					failures = append(failures,
						fmt.Sprintf("prop=body query=%q node%d expected=%d actual=%d",
							q, i+1, baselinesBody[q][i], bodyActual[i]))
				}
			}
		}
		if len(failures) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, q := range testBM25Queries {
		t.Logf("post-concurrent title %q: baseline=%v actual=%v", q, baselinesTitle[q], lastTitle[q])
		t.Logf("post-concurrent body %q: baseline=%v actual=%v", q, baselinesBody[q], lastBody[q])
	}
	sort.Strings(failures)
	t.Fatalf(
		"per-replica inverted-bucket mismatch after concurrent two-property round-trip (after %s wait); %d mismatches:\n  %s",
		perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

// ----------------------------------------------------------------------------
// Helpers below — kept in this file so the adjacent-journey tests are
// self-contained and can be moved/deleted as a unit.
// ----------------------------------------------------------------------------

// testRoundTripNRounds drives a sequence of change-tokenization migrations
// on a single property and asserts that after EVERY round, every replica
// returns the baseline count for every probe query. The baseline is
// captured before the first round and (for symmetric round-trip
// sequences) every subsequent round that lands back on the starting
// tokenization is expected to match the baseline exactly.
//
// For asymmetric sequences (e.g. word→field→lowercase), we only assert
// per-replica equality across nodes (no per-replica drift), not equality
// to the original baseline.
func testRoundTripNRounds(
	t *testing.T, compose *docker.DockerCompose,
	className, startTok string, sequence []string,
) {
	t.Helper()

	restURI := compose.GetWeaviateNode(1).URI()
	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: startTok},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)
	baselines := waitForPerReplicaBaseline(t, compose, className, testBM25Queries)

	currentTok := startTok
	for roundIdx, targetTok := range sequence {
		t.Logf("round %d/%d: %s → %s", roundIdx+1, len(sequence), currentTok, targetTok)
		taskID := submitIndexUpdate(t, restURI, className, "text",
			fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTok))
		awaitReindexFinished(t, restURI, taskID)
		awaitTokenizationOnAllNodes(t, compose, className, "text", targetTok)
		currentTok = targetTok

		// After every round, all three replicas must agree on counts
		// per query. If they don't, we've found a per-replica
		// divergence — that's the bug, regardless of which round it
		// shows up on.
		assertPerReplicaAgreement(t, compose, className, testBM25Queries,
			fmt.Sprintf("after round %d (now %s)", roundIdx+1, targetTok))

		// Additionally: when we're back on the starting tokenization,
		// the counts should match the baseline (semantic round-trip
		// has no information loss).
		if targetTok == startTok {
			assertPerReplicaConsistent(t, compose, className, testBM25Queries, baselines,
				fmt.Sprintf("round %d back-to-%s", roundIdx+1, startTok))
		}
	}
}

func testMultiPropertyRoundTrip(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	const className = "MultiProp"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjectsTwoProps(t, restURI, className, testDocuments)

	baselinesTitle := captureBaselineCounts(t, compose, className, "title", testBM25Queries)
	baselinesBody := captureBaselineCounts(t, compose, className, "body", testBM25Queries)

	// Sequential round-trip on title.
	taskID := submitIndexUpdate(t, restURI, className, "title",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "title", "field")
	taskID = submitIndexUpdate(t, restURI, className, "title",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "title", "word")

	// Then on body.
	taskID = submitIndexUpdate(t, restURI, className, "body",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "body", "field")
	taskID = submitIndexUpdate(t, restURI, className, "body",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "body", "word")

	// Poll both properties' per-replica counts until they match the
	// baseline. Same settle-window rationale as assertPerReplicaConsistent.
	var failures []string
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		failures = failures[:0]
		for _, q := range testBM25Queries {
			titleActual := perNodeBM25CountsProperty(t, compose, className, "title", q)
			bodyActual := perNodeBM25CountsProperty(t, compose, className, "body", q)
			for i := 0; i < 3; i++ {
				if titleActual[i] != baselinesTitle[q][i] {
					failures = append(failures,
						fmt.Sprintf("prop=title q=%q node%d expected=%d actual=%d",
							q, i+1, baselinesTitle[q][i], titleActual[i]))
				}
				if bodyActual[i] != baselinesBody[q][i] {
					failures = append(failures,
						fmt.Sprintf("prop=body q=%q node%d expected=%d actual=%d",
							q, i+1, baselinesBody[q][i], bodyActual[i]))
				}
			}
		}
		if len(failures) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	sort.Strings(failures)
	t.Fatalf(
		"per-replica multi-property round-trip mismatch (after %s wait); %d mismatches:\n  %s",
		perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

func testFilterableOnlyRoundTrip(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	const className = "FilterableOnlyRT"
	trueVal, falseVal := true, false
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{
			Name: "text", DataType: []string{"text"},
			Tokenization:    "word",
			IndexFilterable: &trueVal,
			IndexSearchable: &falseVal,
		},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Filterable-only properties don't support BM25; use Equal filter on
	// a term we know appears in baseline docs ("alpha").
	probes := []string{"alpha", "bravo", "charlie", "kilo"}
	baselines := make(map[string][]int)
	for _, p := range probes {
		counts := perNodeEqualCounts(t, compose, className, "text", p)
		require.Equalf(t, counts[0], counts[1],
			"filterable-only baseline %q inconsistent: n1=%d n2=%d", p, counts[0], counts[1])
		require.Equalf(t, counts[0], counts[2],
			"filterable-only baseline %q inconsistent: n1=%d n3=%d", p, counts[0], counts[2])
		baselines[p] = counts
	}

	// word → field via change-tokenization-filterable body shape.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"filterable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// field → word.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"filterable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	// Poll Equal-filter per-replica counts until they match baseline.
	var failures []string
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		failures = failures[:0]
		for _, p := range probes {
			actual := perNodeEqualCounts(t, compose, className, "text", p)
			for i := 0; i < 3; i++ {
				if actual[i] != baselines[p][i] {
					failures = append(failures,
						fmt.Sprintf("filter=Equal(%q) node%d expected=%d actual=%d",
							p, i+1, baselines[p][i], actual[i]))
				}
			}
		}
		if len(failures) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	sort.Strings(failures)
	t.Fatalf(
		"filterable-only per-replica mismatch after word→field→word round-trip (after %s wait); %d mismatches:\n  %s",
		perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

func testSearchableOnlyRoundTrip(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	const className = "SearchableOnlyRT"
	trueVal, falseVal := true, false
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{
			Name: "text", DataType: []string{"text"},
			Tokenization:    "word",
			IndexFilterable: &falseVal,
			IndexSearchable: &trueVal,
		},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	baselines := waitForPerReplicaBaseline(t, compose, className, testBM25Queries)

	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	assertPerReplicaConsistent(t, compose, className, testBM25Queries, baselines,
		"searchable-only round-trip")
}

func testEnableFilterableThenChangeTok(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	const className = "EnableFilterableThenTok"
	trueVal, falseVal := true, false
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{
			Name: "text", DataType: []string{"text"},
			Tokenization:    "word",
			IndexFilterable: &falseVal,
			IndexSearchable: &trueVal,
		},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)
	baselines := waitForPerReplicaBaseline(t, compose, className, testBM25Queries)

	// Step 1: enable filterable. This writes a tidied.mig per-prop.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"filterable":{"enabled":true}}`)
	awaitReindexFinished(t, restURI, taskID)
	require.Eventually(t, func() bool {
		cls := getClassFromNode(t, restURI, className)
		for _, p := range cls.Properties {
			if p.Name == "text" && p.IndexFilterable != nil && *p.IndexFilterable {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"text.IndexFilterable should be true after enable-filterable")

	// Step 2: change-tokenization word→field on the same property.
	// Hypothesis: enable-filterable's tidied.mig poisons the new
	// change-tok migration dir state, leaving N-1 replicas with empty
	// post-swap buckets.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// Step 3: round-trip back to word.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	assertPerReplicaConsistent(t, compose, className, testBM25Queries, baselines,
		"enable-filterable then change-tok round-trip")
}

func testEnableSearchableThenChangeTok(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	const className = "EnableSearchableThenTok"
	trueVal, falseVal := true, false
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{
			Name: "text", DataType: []string{"text"},
			Tokenization:    "word",
			IndexFilterable: &trueVal,
			IndexSearchable: &falseVal,
		},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Pre-state: filterable-only — baseline via Equal.
	probes := []string{"alpha", "bravo", "charlie", "kilo"}
	baselinesEqual := make(map[string][]int)
	for _, p := range probes {
		baselinesEqual[p] = perNodeEqualCounts(t, compose, className, "text", p)
	}

	// Step 1: enable searchable. The backend requires a tokenization on
	// the request body because the property's existing filterable index
	// is also tokenized and must agree (see
	// validateEnableSearchableProperty in handlers_reindex.go). We pick
	// `word` to match the seed schema.
	taskID := submitIndexUpdate(t, restURI, className, "text",
		`{"searchable":{"enabled":true,"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	require.Eventually(t, func() bool {
		cls := getClassFromNode(t, restURI, className)
		for _, p := range cls.Properties {
			if p.Name == "text" && p.IndexSearchable != nil && *p.IndexSearchable {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"text.IndexSearchable should be true after enable-searchable")

	// Now BM25 is available — record those baselines for downstream
	// post-round-trip comparison.
	baselinesBM25 := make(map[string][]int)
	for _, q := range testBM25Queries {
		baselinesBM25[q] = perNodeBM25Counts(t, compose, className, q)
	}

	// Step 2: change-tokenization word→field via filterable. The
	// property's `tokenization` field is per-property, not per-index, so
	// a single change-tok-filterable updates the cluster-wide schema and
	// rebuilds the filterable bucket. The searchable bucket is left at
	// the old tokenization for now — exercising the divergent-bucket
	// state. A second `{"searchable":{"tokenization":"field"}}` would be
	// rejected by validateTokenizationChange ("already uses tokenization
	// X") because the schema flip from step 2 already landed.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"filterable":{"tokenization":"field"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// Step 3: back to word via filterable. Same single-request shape.
	taskID = submitIndexUpdate(t, restURI, className, "text",
		`{"filterable":{"tokenization":"word"}}`)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "word")

	// Poll until both filterable (Equal) and searchable (BM25) per-replica
	// counts match the baseline. Same settle-window rationale as
	// assertPerReplicaConsistent.
	var failures []string
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		failures = failures[:0]
		for _, p := range probes {
			actual := perNodeEqualCounts(t, compose, className, "text", p)
			for i := 0; i < 3; i++ {
				if actual[i] != baselinesEqual[p][i] {
					failures = append(failures,
						fmt.Sprintf("Equal(%q) node%d expected=%d actual=%d",
							p, i+1, baselinesEqual[p][i], actual[i]))
				}
			}
		}
		for _, q := range testBM25Queries {
			actual := perNodeBM25Counts(t, compose, className, q)
			for i := 0; i < 3; i++ {
				if actual[i] != baselinesBM25[q][i] {
					failures = append(failures,
						fmt.Sprintf("BM25(%q) node%d expected=%d actual=%d",
							q, i+1, baselinesBM25[q][i], actual[i]))
				}
			}
		}
		if len(failures) == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	sort.Strings(failures)
	t.Fatalf(
		"enable-searchable+change-tok round-trip mismatch (after %s wait); %d mismatches:\n  %s",
		perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

// assertPerReplicaAgreement requires every replica to return the same
// count for the same query (not necessarily a specific baseline value).
// This is weaker than assertPerReplicaConsistent and is the right check
// after an asymmetric migration where we don't have a precomputed
// baseline.
// perReplicaConvergenceTimeout is the budget for per-replica counts to
// settle after the schema flip lands. The reindex task transitions to
// FINISHED when units are terminal, then OnGroupCompleted (the per-shard
// in-memory bucket swap) and OnTaskCompleted (the cluster-wide schema
// flip via RAFT) fire on each node's next scheduler tick. The schema
// flip RAFT-propagates instantly, but the swap on each replica is local
// and can lag the cross-node schema observation by a tick. Real
// data-loss bugs (the #10675 prod failure mode) leave a replica's
// bucket persistently empty — those still fail this assertion after the
// budget elapses. The budget exists to absorb the OnGroupCompleted tick
// lag, NOT to mask divergence.
const perReplicaConvergenceTimeout = 60 * time.Second

// assertPerReplicaAgreement polls per-replica counts for each query
// until every replica returns the same count. The poll catches the
// post-swap settle window where one replica's OnGroupCompleted has not
// yet fired even though the RAFT schema flip has already propagated.
// On timeout, fail with the last observed counts so it's clear which
// replica diverged.
func assertPerReplicaAgreement(
	t *testing.T, compose *docker.DockerCompose,
	className string, queries []string, label string,
) {
	t.Helper()

	var (
		lastCounts map[string][]int
		failures   []string
	)
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		lastCounts = make(map[string][]int, len(queries))
		failures = failures[:0]
		for _, q := range queries {
			counts := perNodeBM25Counts(t, compose, className, q)
			lastCounts[q] = counts
			if counts[0] != counts[1] || counts[0] != counts[2] {
				failures = append(failures,
					fmt.Sprintf("query=%q counts=%v (replicas disagree)", q, counts))
			}
		}
		if len(failures) == 0 {
			for _, q := range queries {
				t.Logf("%s %q: %v", label, q, lastCounts[q])
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, q := range queries {
		t.Logf("%s %q: %v", label, q, lastCounts[q])
	}
	sort.Strings(failures)
	t.Fatalf("%s — per-replica disagreement (after %s wait); %d mismatches:\n  %s",
		label, perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

// assertPerReplicaConsistent polls per-replica counts until every
// replica matches the per-node baseline exactly. Same settle-window
// rationale as assertPerReplicaAgreement.
func assertPerReplicaConsistent(
	t *testing.T, compose *docker.DockerCompose,
	className string, queries []string, baselines map[string][]int,
	label string,
) {
	t.Helper()

	var (
		lastActual map[string][]int
		failures   []string
	)
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		lastActual = make(map[string][]int, len(queries))
		failures = failures[:0]
		for _, q := range queries {
			actual := perNodeBM25Counts(t, compose, className, q)
			lastActual[q] = actual
			expected := baselines[q]
			for i := 0; i < 3; i++ {
				if actual[i] != expected[i] {
					failures = append(failures,
						fmt.Sprintf("query=%q node%d expected=%d actual=%d",
							q, i+1, expected[i], actual[i]))
				}
			}
		}
		if len(failures) == 0 {
			for _, q := range queries {
				t.Logf("%s %q: baseline=%v actual=%v", label, q, baselines[q], lastActual[q])
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, q := range queries {
		t.Logf("%s %q: baseline=%v actual=%v", label, q, baselines[q], lastActual[q])
	}
	sort.Strings(failures)
	t.Fatalf("%s — per-replica mismatch (after %s wait); %d mismatches:\n  %s",
		label, perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
}

func captureBaselineCounts(
	t *testing.T, compose *docker.DockerCompose,
	className, property string, queries []string,
) map[string][]int {
	t.Helper()

	baselines := make(map[string][]int)
	for _, q := range queries {
		counts := perNodeBM25CountsProperty(t, compose, className, property, q)
		require.Equalf(t, counts[0], counts[1],
			"baseline prop=%s q=%q inconsistent: n1=%d n2=%d", property, q, counts[0], counts[1])
		require.Equalf(t, counts[0], counts[2],
			"baseline prop=%s q=%q inconsistent: n1=%d n3=%d", property, q, counts[0], counts[2])
		baselines[q] = counts
	}
	return baselines
}

// waitForPerReplicaBaseline polls each query across all three replicas
// until counts agree and are non-zero (a real baseline must match at
// least one doc). importObjects uses the default write consistency, so
// after the synchronous POST returns there can be a brief window where
// the third replica has not finished the replication leg yet. Without
// this, the very first baseline-capture line of every test races the
// replication latency on a freshly-started cluster.
func waitForPerReplicaBaseline(
	t *testing.T, compose *docker.DockerCompose,
	className string, queries []string,
) map[string][]int {
	t.Helper()

	var (
		lastCounts map[string][]int
		failures   []string
	)
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	for time.Now().Before(deadline) {
		lastCounts = make(map[string][]int, len(queries))
		failures = failures[:0]
		for _, q := range queries {
			counts := perNodeBM25Counts(t, compose, className, q)
			lastCounts[q] = counts
			if counts[0] != counts[1] || counts[0] != counts[2] {
				failures = append(failures,
					fmt.Sprintf("baseline %q inconsistent: %v", q, counts))
			} else if counts[0] == 0 {
				failures = append(failures,
					fmt.Sprintf("baseline %q is zero on every replica", q))
			}
		}
		if len(failures) == 0 {
			for _, q := range queries {
				t.Logf("baseline %q: %v", q, lastCounts[q])
			}
			return lastCounts
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, q := range queries {
		t.Logf("baseline %q: %v", q, lastCounts[q])
	}
	sort.Strings(failures)
	t.Fatalf("baseline did not converge across replicas within %s; %d issues:\n  %s",
		perReplicaConvergenceTimeout, len(failures), strings.Join(failures, "\n  "))
	return nil
}
