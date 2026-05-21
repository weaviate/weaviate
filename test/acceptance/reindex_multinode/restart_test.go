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

// 50k change-tokenization gives a multi-second STARTED window on a 3-node cluster.
const reindexRestartDataset = 50_000

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

	// Per-replica schema cache catches up eventually after the FSM flip.
	require.Eventually(t, func() bool {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			if tryGetPropertyTokenization(restURIOf(compose, nodeIdx), className, "path") != "field" {
				return false
			}
		}
		return true
	}, 30*time.Second, 500*time.Millisecond,
		"tokenization should flip to field on every replica post-restart")

	const expected = reindexRestartDataset / 5
	for _, bucket := range []string{"alpha-path", "beta-path", "gamma-path", "delta-path", "epsilon-path"} {
		for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
			got, err := equalCount(restURIOf(compose, nodeIdx), className, "path", bucket)
			require.NoErrorf(t, err, "node %d bucket %q: post-restart count query failed", nodeIdx, bucket)
			assert.Equalf(t, expected, got,
				"node %d bucket %q: post-restart count mismatch (expected %d, got %d)",
				nodeIdx, bucket, expected, got)
		}
	}
}

// timeout=nil → graceful SIGTERM; non-nil → SIGKILL after expiry.
func stopStart(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeIdx int, timeout *time.Duration) {
	t.Helper()
	require.NoError(t, compose.StopAt(ctx, nodeIdx, timeout))
	require.NoError(t, compose.StartAt(ctx, nodeIdx))
}

func runRestartDuringReindex(
	t *testing.T, className string, awaitTimeout time.Duration,
	restartFn func(ctx context.Context, t *testing.T, compose *docker.DockerCompose),
) {
	ctx := context.Background()
	compose, cleanup, taskID := setupRestartDuringReindex(ctx, t, className)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	awaitReindexMidFlight(t, restURIOf(compose, 1), taskID, 60*time.Second)
	restartFn(ctx, t, compose)
	assertReindexCompleteAndConsistent(t, compose, className, taskID, awaitTimeout)
}

var sigkillFast = 1 * time.Second

// Pins weaviate/0-weaviate-issues#239 Mode 1: graceful follower restart
// mid-reindex must resume, not mark the task FAILED.
func TestMultiNode_GracefulRestartDuringReindex(t *testing.T) {
	runRestartDuringReindex(t, "RestartDuringReindex", 240*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			stopStart(ctx, t, compose, 2, nil)
		})
}

// Pins weaviate/0-weaviate-issues#239 Mode 2: graceful RAFT leader
// restart must resume, not stay STUCK indefinitely. 360 s budget
// absorbs leader re-election before resumed iteration starts.
func TestMultiNode_GracefulLeaderRestartDuringReindex(t *testing.T) {
	runRestartDuringReindex(t, "LeaderRestartDuringReindex", 360*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			leaderIdx := raftLeaderIndex(t, compose)
			t.Logf("gracefully stopping leader (node %d)", leaderIdx+1)
			stopStart(ctx, t, compose, leaderIdx, nil)
			require.Eventually(t, func() bool {
				_, err := equalCount(restURIOf(compose, 1), "LeaderRestartDuringReindex", "path", "alpha-path")
				return err == nil
			}, 60*time.Second, 1*time.Second, "cluster should be ready after leader restart")
		})
}

// Pins weaviate/0-weaviate-issues#239 Mode 3: ungraceful SIGKILL
// follower mid-reindex must resume via sentinel-aware recovery.
func TestMultiNode_CrashDuringReindex(t *testing.T) {
	runRestartDuringReindex(t, "CrashDuringReindex", 240*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			stopStart(ctx, t, compose, 2, &sigkillFast)
		})
}

func TestMultiNode_MajorityCrashDuringReindex(t *testing.T) {
	runRestartDuringReindex(t, "MajorityCrash", 360*time.Second,
		func(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
			require.NoError(t, compose.StopAt(ctx, 2, &sigkillFast))
			require.NoError(t, compose.StopAt(ctx, 1, &sigkillFast))
			// Node 2 first → restores quorum with node 1 before node 3.
			require.NoError(t, compose.StartAt(ctx, 1))
			require.NoError(t, compose.StartAt(ctx, 2))
		})
}

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
	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, "text", `{"searchable":{"algorithm":"blockmax"}}`)
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
