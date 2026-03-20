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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestMultiNode_QueryConsistencyDuringReindex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "ConsistencyTest"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// Record baselines on all nodes.
	baselines := make(map[string][][]string)
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		baselines[q] = results
		require.NotEmpty(t, results[0], "baseline query %q returned no results", q)
	}

	// Start 3 concurrent query loops (one per node), cycling through all queries.
	var queryFailures atomic.Int64
	var queryRuns atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		wg.Add(1)
		nodeURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
		go func(uri string, idx int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				for _, q := range testBM25Queries {
					ids, err := runBM25QueryOnNodeWithRetry(t, uri, className, q)
					queryRuns.Add(1)
					if err != nil {
						queryFailures.Add(1)
						t.Logf("node %d query %q error: %v", idx+1, q, err)
					} else if !idsMatchUnordered(baselines[q][0], ids) {
						queryFailures.Add(1)
						t.Logf("node %d query %q mismatch: expected %d got %d",
							idx+1, q, len(baselines[q][0]), len(ids))
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(nodeURI, nodeIdx)
	}

	// Submit reindex (repair-searchable — the most common type).
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	awaitReindexFinished(t, restURI, taskID)

	// Continue queries for several consecutive successful rounds after
	// completion to catch post-swap transients. We require 10 consecutive
	// successful rounds (each cycling all queries on all nodes) before stopping.
	const requiredConsecutiveSuccesses = 10
	consecutiveSuccesses := 0
	require.Eventually(t, func() bool {
		allOK := true
		for _, q := range testBM25Queries {
			for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
				nodeURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
				ids, err := runBM25QueryOnNode(t, nodeURI, className, q)
				if err != nil || !idsMatchUnordered(baselines[q][0], ids) {
					allOK = false
					break
				}
			}
			if !allOK {
				break
			}
		}
		if allOK {
			consecutiveSuccesses++
		} else {
			consecutiveSuccesses = 0
		}
		return consecutiveSuccesses >= requiredConsecutiveSuccesses
	}, 30*time.Second, 200*time.Millisecond,
		"expected %d consecutive successful query rounds after reindex completion",
		requiredConsecutiveSuccesses)

	// Stop background queries.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(),
		"zero query failures expected across all goroutines and all nodes")

	// Verify schema update on all nodes.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should be true", i)
	}

	// Final consistency check.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-migration query %q results differ from baseline", q)
	}
}
