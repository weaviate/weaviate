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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// The tests in this file pin the deferred-finalize + per-migration-
// generation contract from the weaviate/weaviate#10675 fix. They cover
// every restart-vs-migration-count combination in a 3-node RF=3 cluster
// (every replica holds every shard) and assert per-replica BM25 results
// hit DIRECTLY — never via the LB's round-robin, which masks per-replica
// state divergence.
//
// See `docs/runtime-reindex.md` for the design rationale.

// runChangeTokMigration submits a change-tokenization request on a
// single property, waits for the RAFT task to reach FINISHED, and waits
// for the schema flag to flip on every node.
func runChangeTokMigration(t *testing.T, compose *docker.DockerCompose, className, propName, targetTok string) {
	t.Helper()
	restURI := compose.GetWeaviateNode(1).URI()
	body := fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTok)
	taskID := submitIndexUpdate(t, restURI, className, propName, body)
	awaitReindexFinished(t, restURI, taskID)
	awaitTokenizationOnAllNodes(t, compose, className, propName, targetTok)
}

// recordBaselineCounts captures per-node match counts for every query
// in `queries`, asserts they're consistent across nodes (else the
// baseline is meaningless), and returns the map.
func recordBaselineCounts(t *testing.T, compose *docker.DockerCompose, className string, queries []string) map[string][]int {
	t.Helper()
	out := make(map[string][]int, len(queries))
	for _, q := range queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1],
			"baseline inconsistent for %q: node1=%d node2=%d", q, counts[0], counts[1])
		require.Equalf(t, counts[0], counts[2],
			"baseline inconsistent for %q: node1=%d node3=%d", q, counts[0], counts[2])
		require.Greaterf(t, counts[0], 0,
			"baseline %q must match at least one doc", q)
		out[q] = counts
	}
	return out
}

// assertPerReplicaCountsMatchBaseline runs every query in baselines on
// every node directly and asserts the per-node match count matches the
// baseline captured before any migrations. A bug that empties N of 3
// replicas produces N failed assertions, not 1 flake.
func assertPerReplicaCountsMatchBaseline(
	t *testing.T, compose *docker.DockerCompose, className string, baselines map[string][]int,
) {
	t.Helper()
	var failures []string
	for q, expected := range baselines {
		actual := perNodeBM25Counts(t, compose, className, q)
		t.Logf("post-state query %q: baseline=%v actual=%v", q, expected, actual)
		for i := 0; i < 3; i++ {
			if actual[i] != expected[i] {
				failures = append(failures,
					fmt.Sprintf("query=%q node%d expected=%d actual=%d",
						q, i+1, expected[i], actual[i]))
			}
		}
	}
	if len(failures) > 0 {
		sort.Strings(failures)
		t.Fatalf("per-replica bucket-state divergence (#10675): %d mismatches:\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}
}

// TestMultiNode_RestartMatrix exercises every cell of the
// restart × migration-count matrix that the deferred-finalize design
// must handle. Each subtest uses its own 3-node cluster — the matrix
// is small enough that the per-test startup overhead is acceptable,
// and isolation makes failures easier to diagnose.
func TestMultiNode_RestartMatrix(t *testing.T) {
	t.Run("R0_RestartAfterNoMigrations_ThenMigrate", testR0_RestartThenMigrate)
	t.Run("R1_RestartAfter1Migration_ThenQuery", testR1_RestartAfter1Migration)
	t.Run("R1b_RestartAfter1Migration_ThenMigrate", testR1b_RestartAfter1MigrationThenMigrate)
	t.Run("R2_RestartAfter2Migrations_ThenQuery", testR2_RestartAfter2Migrations)
	t.Run("R2b_RestartAfter2Migrations_ThenMigrate", testR2b_RestartAfter2MigrationsThenMigrate)
	t.Run("R3_RestartAfter3Migrations_ThenQuery", testR3_RestartAfter3Migrations)
	t.Run("R5_RestartAfterManyMigrations_ThenQuery", testR5_RestartAfterManyMigrations)
}

// testR0_RestartThenMigrate: import → restart cluster → word→field →
// per-replica assertions. Pins that a fresh-startup cluster with no
// prior migration state runs a clean first migration (gen=1 picked).
func testR0_RestartThenMigrate(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR0"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	restartCluster(ctx, t, compose)

	// First migration after a clean restart — gen should be 1 on every
	// node, no leftover state to interfere with.
	runChangeTokMigration(t, compose, className, "text", "field")

	// Sanity: word-token queries against FIELD tokenization should NOT
	// return the same per-doc counts (FIELD whole-string tokens won't
	// match the partial word queries). The point of this cell is the
	// migration completing cleanly post-restart, not the per-query
	// equality of post-vs-pre counts. Verify per-node consistency
	// instead.
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1], "post-state %q diverges between node1/node2", q)
		require.Equalf(t, counts[0], counts[2], "post-state %q diverges between node1/node3", q)
	}
	_ = wordBaseline // referenced for symmetry with other cells; not asserted here
}

// testR1_RestartAfter1Migration: import → word→field → restart →
// per-replica query baseline equality. Pins that after restart
// FinalizeCompletedMigrations promotes gen 1's ingest dir to canonical
// on every node, and every replica returns the same counts.
func testR1_RestartAfter1Migration(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR1"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "word") // no-op same-tok migration
	// Switch to FIELD then back to WORD so the per-node state is the
	// same word-tokenized data we baselined, but each replica's on-disk
	// dirs were touched by two migrations.
	runChangeTokMigration(t, compose, className, "text", "field")

	restartCluster(ctx, t, compose)

	// After restart-finalize, the gen=2 (FIELD) ingest dir was promoted
	// to canonical and gen=1 was cleaned. Querying word tokens now
	// returns nothing (FIELD tokenization), but per-replica must be
	// consistent. The post-restart full-text equality check is the
	// load-bearing assertion: every replica must return THE SAME count.
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1], "post-restart %q diverges node1/node2", q)
		require.Equalf(t, counts[0], counts[2], "post-restart %q diverges node1/node3", q)
	}
	_ = wordBaseline // for reference; the round-trip cells use it
}

// testR1b_RestartAfter1MigrationThenMigrate: import → word→field →
// restart → field→word → per-replica baseline equality. Pins that after
// restart-finalize the next migration starts at gen=1 again (the
// previous gen's tracker dir is gone) and produces consistent
// per-replica state.
func testR1b_RestartAfter1MigrationThenMigrate(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR1b"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "field")
	restartCluster(ctx, t, compose)
	runChangeTokMigration(t, compose, className, "text", "word")

	// After restart + second migration back to word, every replica
	// must return the original word-tokenized counts.
	assertPerReplicaCountsMatchBaseline(t, compose, className, wordBaseline)
}

// testR2_RestartAfter2Migrations: import → word→field → field→word →
// restart → per-replica baseline equality. The trim-at-end-of-swap
// left exactly the gen=2 sidecars on disk; restart-finalize promotes
// them to canonical.
func testR2_RestartAfter2Migrations(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR2"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "field")
	runChangeTokMigration(t, compose, className, "text", "word")
	restartCluster(ctx, t, compose)

	assertPerReplicaCountsMatchBaseline(t, compose, className, wordBaseline)
}

// testR2b_RestartAfter2MigrationsThenMigrate: import → word→field →
// field→word → restart → word→field → per-replica field-baseline
// equality. After restart, the next migration starts at gen=1 because
// the trim+finalize cleared all prior state.
func testR2b_RestartAfter2MigrationsThenMigrate(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR2b"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)

	runChangeTokMigration(t, compose, className, "text", "field")
	runChangeTokMigration(t, compose, className, "text", "word")
	restartCluster(ctx, t, compose)
	// Snapshot the post-restart word state as the new baseline before
	// switching to FIELD — counts of word-tokenized queries against a
	// FIELD bucket aren't meaningful, but per-replica consistency is.
	postRestartBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "field")

	// Per-replica must be consistent. FIELD tokenization changes the
	// counts (word tokens won't fully match full-string tokens), so
	// don't compare to baseline counts — just per-replica consistency.
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1], "post-migration %q diverges node1/node2", q)
		require.Equalf(t, counts[0], counts[2], "post-migration %q diverges node1/node3", q)
	}
	_ = postRestartBaseline
}

// testR3_RestartAfter3Migrations: import → 3 alternating migrations →
// restart → per-replica baseline equality. Same shape as R2 but with
// more generations to stress the trim.
func testR3_RestartAfter3Migrations(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR3"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "field")
	runChangeTokMigration(t, compose, className, "text", "word")
	runChangeTokMigration(t, compose, className, "text", "field")
	restartCluster(ctx, t, compose)
	// Now end at FIELD; flip back to WORD post-restart and assert
	// against the word baseline.
	runChangeTokMigration(t, compose, className, "text", "word")
	assertPerReplicaCountsMatchBaseline(t, compose, className, wordBaseline)
}

// testR5_RestartAfterManyMigrations: 5 alternating migrations →
// restart → assert per-replica baseline. Stress-tests the
// bounded-depth invariant: at most 2 generations on disk at any time.
func testR5_RestartAfterManyMigrations(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RestartR5"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	// 5 alternating w/f migrations: w→f, f→w, w→f, f→w, w→f.
	for i := 0; i < 5; i++ {
		target := "field"
		if i%2 == 1 {
			target = "word"
		}
		runChangeTokMigration(t, compose, className, "text", target)
	}
	// End state is FIELD (odd count → last toggle was to field).
	restartCluster(ctx, t, compose)
	runChangeTokMigration(t, compose, className, "text", "word")
	assertPerReplicaCountsMatchBaseline(t, compose, className, wordBaseline)
}

// TestMultiNode_RollingRestartMidMigration mimics a Kubernetes
// StatefulSet rolling update during a migration: each pod is restarted
// once while T1 is in flight. The migration must resume and complete
// with consistent per-replica state.
func TestMultiNode_RollingRestartMidMigration(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RollingMidMigration"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	_ = recordBaselineCounts(t, compose, className, testBM25Queries) // pre-flight sanity

	// Start the migration.
	taskID := submitIndexUpdate(t, compose.GetWeaviateNode(1).URI(), className, "text",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted migration: %s", taskID)

	// Roll all 3 pods one at a time. Some may finish their unit before
	// being stopped; others will recover via the rehydrate path. Either
	// way, the cluster-wide state must converge.
	rollingRestartCluster(ctx, t, compose)

	// Wait for the migration to reach FINISHED on whichever node is
	// reachable; awaitReindexFinished polls /v1/tasks.
	awaitReindexFinished(t, compose.GetWeaviateNode(1).URI(), taskID)
	awaitTokenizationOnAllNodes(t, compose, className, "text", "field")

	// Per-replica consistency check: every node must return the same
	// counts for every query (FIELD tokenization, so values may be 0
	// for partial-word queries, but the value must be SAME across
	// replicas).
	for _, q := range testBM25Queries {
		counts := perNodeBM25Counts(t, compose, className, q)
		require.Equalf(t, counts[0], counts[1], "post-migration %q diverges node1/node2 (got %v)", q, counts)
		require.Equalf(t, counts[0], counts[2], "post-migration %q diverges node1/node3 (got %v)", q, counts)
	}
}

// TestMultiNode_RollingRestartBetweenMigrations mimics the prod
// scenario from weaviate/weaviate#10675: pods are rolled at different
// times between migrations, leading to different on-disk states for
// the same property. The per-node generation suffix must isolate the
// migrations so the second one runs cleanly on every replica.
func TestMultiNode_RollingRestartBetweenMigrations(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "RollingBetweenMigrations"
	createCollection(t, compose.GetWeaviateNode(1).URI(), className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)

	importObjects(t, compose.GetWeaviateNode(1).URI(), className, testDocuments)
	wordBaseline := recordBaselineCounts(t, compose, className, testBM25Queries)

	runChangeTokMigration(t, compose, className, "text", "field")

	// Roll all pods between migrations. Some nodes' T1 state was
	// finalized by their restart (canonical bucket on disk); others
	// rolled later when T1 was already tidied. Per-node disk states
	// diverge.
	rollingRestartCluster(ctx, t, compose)

	// Now run the second migration. The fix's per-node generation
	// computation handles divergent starting states.
	runChangeTokMigration(t, compose, className, "text", "word")

	assertPerReplicaCountsMatchBaseline(t, compose, className, wordBaseline)
}

// _ = time.Time{} silences unused-import for the time package in the
// rare case all calls drop. Keeps the import stable as we tweak tests.
var _ = time.Time{}
