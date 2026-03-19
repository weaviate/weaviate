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
	debugURI := compose.GetWeaviateNode(1).DebugURI()

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
	taskID := submitReindex(t, debugURI, className, "repair-searchable", nil, "")
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

func TestMultiNode_RollingRestartAfterComplete(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "RollingRestart"
	restURI := compose.GetWeaviateNode(1).URI()
	debugURI := compose.GetWeaviateNode(1).DebugURI()

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

	// Complete a reindex.
	taskID := submitReindex(t, debugURI, className, "repair-searchable", nil, "")
	awaitReindexFinished(t, restURI, taskID)

	// Rolling restart all 3 nodes one at a time.
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		t.Logf("rolling restart: stopping node %d", nodeIdx+1)
		require.NoError(t, compose.StopAt(ctx, nodeIdx, nil))
		t.Logf("rolling restart: starting node %d", nodeIdx+1)
		require.NoError(t, compose.StartAt(ctx, nodeIdx))

		// After restart, verify queries on the restarted node.
		restartedURI := compose.GetWeaviateNode(nodeIdx + 1).URI()

		// Wait for node to be ready.
		require.Eventually(t, func() bool {
			_, err := runBM25QueryOnNode(t, restartedURI, className, "alpha")
			return err == nil
		}, 30*time.Second, 1*time.Second, "node %d should be ready after restart", nodeIdx+1)

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
