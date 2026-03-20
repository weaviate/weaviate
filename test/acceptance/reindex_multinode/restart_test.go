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
)

func TestMultiNode_GracefulRestartDuringReindex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "RestartDuringReindex"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Record baselines.
	baselines := make(map[string][][]string)
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		baselines[q] = results
	}

	// Submit reindex.
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Wait briefly then gracefully stop node 3.
	time.Sleep(2 * time.Second)
	t.Log("gracefully stopping node 3")
	require.NoError(t, compose.StopAt(ctx, 2, nil))

	// Restart node 3.
	t.Log("restarting node 3")
	require.NoError(t, compose.StartAt(ctx, 2))

	// Await FINISHED — scheduler will re-launch units on restarted node.
	awaitReindexFinished(t, restURI, taskID)

	// Verify queries correct on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-restart query %q results differ", q)
	}

	// Verify schema update.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should be true", i)
	}
}

func TestMultiNode_CrashDuringReindex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "CrashDuringReindex"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Record baselines.
	baselines := make(map[string][][]string)
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		baselines[q] = results
	}

	// Submit reindex.
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Wait briefly then crash node 3 ungracefully.
	time.Sleep(2 * time.Second)
	stopTimeout := 1 * time.Second
	t.Log("crashing node 3 (ungraceful stop)")
	require.NoError(t, compose.StopAt(ctx, 2, &stopTimeout))

	// Restart node 3.
	t.Log("restarting node 3")
	require.NoError(t, compose.StartAt(ctx, 2))

	// Await FINISHED — scheduler will re-launch units on restarted node.
	awaitReindexFinished(t, restURI, taskID)

	// Verify queries correct on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-crash query %q results differ", q)
	}

	// Verify schema update.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should be true", i)
	}
}

func TestMultiNode_MajorityCrashDuringReindex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "MajorityCrash"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	baselines := make(map[string][][]string)
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		baselines[q] = results
	}

	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Wait then crash majority (nodes 3 then 2).
	time.Sleep(2 * time.Second)
	stopTimeout := 1 * time.Second
	t.Log("crashing node 3")
	require.NoError(t, compose.StopAt(ctx, 2, &stopTimeout))
	t.Log("crashing node 2 (majority lost)")
	require.NoError(t, compose.StopAt(ctx, 1, &stopTimeout))

	// Restore quorum: restart node 2 first (gives 2/3 = majority with node 1).
	t.Log("restarting node 2")
	require.NoError(t, compose.StartAt(ctx, 1))
	// Then restart node 3.
	t.Log("restarting node 3")
	require.NoError(t, compose.StartAt(ctx, 2))

	// Await FINISHED with longer timeout.
	awaitReindexFinished(t, restURI, taskID)

	// Verify queries on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-majority-crash query %q results differ", q)
	}

	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should be true", i)
	}
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
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	awaitReindexFinished(t, restURI, taskID)

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

	// Final consistency check across all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
	}
}
