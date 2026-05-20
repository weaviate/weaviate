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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// 50k change-tokenization migration spends ~25-40 s in STARTED on a
// 3-node cluster — wide enough to land the restart inside.
const reindexRestartDataset = 50_000

// 0.1 is past the "just started" floor (units can sit at 0 for a few
// ticks) but well before completion.
const reindexRestartProgressFloor = 0.1

// Change-tokenization is the heaviest shape (2 sub-tasks per shard ×
// property) — widest STARTED window for the restart-mid-flight checks.
func setupRestartDuringReindex(
	ctx context.Context, t *testing.T, className string,
) (*docker.DockerCompose, func(), string) {
	t.Helper()
	compose, cleanup := start3NodeReindexCluster(ctx, t)

	uri := restURIOf(compose, 1)
	trueVal := true
	createCollection(t, uri, className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})

	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"}
	batchImportMultiProp(t, uri, className, reindexRestartDataset, func(i int) map[string]interface{} {
		return map[string]interface{}{"path": paths[i%len(paths)]}
	})

	taskID := reindexhelpers.SubmitIndexUpdate(t, uri, className, "path",
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task: %s", taskID)
	return compose, cleanup, taskID
}

func assertReindexCompleteAndConsistent(
	t *testing.T, compose *docker.DockerCompose, className, taskID string, awaitTimeout time.Duration,
) {
	t.Helper()
	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID,
		reindexhelpers.WithTimeout(awaitTimeout))

	// Schema reflection of the FINISHED flip is per-replica eventual:
	// AwaitReindexFinished sees the FSM state, but each replica's
	// in-memory schema cache catches up via the schema-update callback
	// on its own clock.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			if tryGetPropertyTokenization(restURIOf(compose, nodeIdx), className, "path") != "field" {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"tokenization should flip to field on every replica post-restart")

	// Under FIELD tokenization each path-bucket matches dataset/5 exactly.
	const expected = reindexRestartDataset / 5
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", "alpha-path")
		require.NoErrorf(t, err, "node %d: post-restart count query failed", nodeIdx)
		assert.Equalf(t, expected, got,
			"node %d: post-restart count mismatch (expected %d, got %d)",
			nodeIdx, expected, got)
	}
}

// runRestartDuringReindex is the shared scaffold: spin up the cluster,
// import the dataset, submit the migration, wait until iteration is
// truly mid-flight, then hand off to restartFn (which does the
// test-specific stop/start sequence) and verify post-restart
// completeness + per-replica consistency.
func runRestartDuringReindex(
	t *testing.T, className string, awaitTimeout time.Duration,
	restartFn func(ctx context.Context, t *testing.T, compose *docker.DockerCompose),
) {
	ctx := context.Background()
	compose, cleanup, taskID := setupRestartDuringReindex(ctx, t, className)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	awaitReindexMidFlight(t, restURIOf(compose, 1), taskID,
		reindexRestartProgressFloor, 60*time.Second)
	restartFn(ctx, t, compose)
	assertReindexCompleteAndConsistent(t, compose, className, taskID, awaitTimeout)
}

// Pins weaviate/0-weaviate-issues#239 Mode 1: graceful follower restart
// mid-reindex must resume, not mark the task FAILED.
func TestMultiNode_GracefulRestartDuringReindex(t *testing.T) {
	runRestartDuringReindex(t, "RestartDuringReindex", 240*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			t.Log("gracefully stopping node 3 (follower)")
			require.NoError(t, compose.StopAt(ctx, 2, nil))
			require.NoError(t, compose.StartAt(ctx, 2))
		})
}

// Pins weaviate/0-weaviate-issues#239 Mode 2: graceful RAFT leader
// restart mid-reindex must resume, not stay STUCK indefinitely.
func TestMultiNode_GracefulLeaderRestartDuringReindex(t *testing.T) {
	// 360 s budget: leader-restart absorbs ~30 s re-election before the
	// resumed iteration even starts.
	runRestartDuringReindex(t, "LeaderRestartDuringReindex", 360*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			t.Log("gracefully stopping node 1 (RAFT leader)")
			require.NoError(t, compose.StopAt(ctx, 0, nil))
			require.NoError(t, compose.StartAt(ctx, 0))
			// Probe via the actual "path" property — runBM25QueryOnNode
			// hardcodes "text" which doesn't exist on this class.
			require.Eventually(t, func() bool {
				_, err := equalCount(restURIOf(compose, 1), "LeaderRestartDuringReindex", "path", "alpha-path")
				return err == nil
			}, 60*time.Second, 1*time.Second, "node 1 should be ready after restart")
		})
}

// Pins weaviate/0-weaviate-issues#239 Mode 3: ungraceful SIGKILL
// follower mid-reindex must resume via sentinel-aware recovery.
func TestMultiNode_CrashDuringReindex(t *testing.T) {
	stopTimeout := 1 * time.Second
	runRestartDuringReindex(t, "CrashDuringReindex", 240*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			t.Log("SIGKILL node 3 (ungraceful)")
			require.NoError(t, compose.StopAt(ctx, 2, &stopTimeout))
			require.NoError(t, compose.StartAt(ctx, 2))
		})
}

// Quorum loss + recovery mid-reindex: SIGKILL nodes 2+3, restart both,
// task must still FINISH.
func TestMultiNode_MajorityCrashDuringReindex(t *testing.T) {
	stopTimeout := 1 * time.Second
	runRestartDuringReindex(t, "MajorityCrash", 360*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			t.Log("SIGKILL nodes 3 + 2 (majority lost)")
			require.NoError(t, compose.StopAt(ctx, 2, &stopTimeout))
			require.NoError(t, compose.StopAt(ctx, 1, &stopTimeout))
			require.NoError(t, compose.StartAt(ctx, 1)) // node 2 first → restores quorum with node 1
			require.NoError(t, compose.StartAt(ctx, 2))
		})
}

// TestMultiNode_RollingRestartAfterComplete keeps the previous
// rolling-restart-AFTER-complete test scope: a completed migration
// must survive a full rolling restart with no per-replica divergence
// on the migrated buckets.
func TestMultiNode_RollingRestartAfterComplete(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "RollingRestart"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer func() {
		// Re-fetch URI since port mapping changes after node restarts.
		deleteCollection(t, compose.GetWeaviateNode(1).URI(), className)
	}()

	importObjects(t, restURI, className, testDocuments)

	// Record baselines.
	baselines := make(map[string][][]string)
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		baselines[q] = results
	}

	// Complete a reindex.
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(180*time.Second))

	// Rolling restart all 3 nodes one at a time.
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		t.Logf("rolling restart: stopping node %d", nodeIdx+1)
		require.NoError(t, compose.StopAt(ctx, nodeIdx, nil))
		t.Logf("rolling restart: starting node %d", nodeIdx+1)
		require.NoError(t, compose.StartAt(ctx, nodeIdx))

		// After restart, verify queries on the restarted node.
		// Re-fetch URI since port mapping may change after restart.
		restartedURI := compose.GetWeaviateNode(nodeIdx + 1).URI()

		// Wait for node to be ready (local queries).
		require.Eventually(t, func() bool {
			_, err := runBM25QueryOnNode(t, restartedURI, className, "alpha")
			return err == nil
		}, 30*time.Second, 1*time.Second, "node %d should be ready after restart", nodeIdx+1)

		// Wait for Raft quorum to be restored before moving on. Object
		// imports require Raft consensus, so a successful write proves the
		// restarted node has fully rejoined the cluster.
		// Use a node that was NOT just restarted as the write target.
		writeURI := compose.GetWeaviateNode(((nodeIdx + 1) % 3) + 1).URI()
		require.Eventually(t, func() bool {
			return tryImportObject(writeURI, className, "raft-health-probe") == nil
		}, 60*time.Second, 1*time.Second, "cluster should have Raft quorum after restarting node %d", nodeIdx+1)

		for _, q := range testBM25Queries {
			ids, err := runBM25QueryOnNode(t, restartedURI, className, q)
			require.NoError(t, err, "node %d query %q failed after restart", nodeIdx+1, q)
			assert.ElementsMatch(t, baselines[q][0], ids,
				"node %d query %q results differ after rolling restart", nodeIdx+1, q)
		}
	}

	// Verify UsingBlockMaxWAND persists after rolling restart on all nodes.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should persist after rolling restart", i)
	}

	// Final consistency check across all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
	}
}
