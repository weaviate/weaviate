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
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// testDocuments for BM25 testing with distinct terms.
var testDocuments = []string{
	"alpha bravo charlie delta echo foxtrot",
	"golf hotel india juliet kilo lima",
	"mike november oscar papa quebec romeo",
	"sierra tango uniform victor whiskey xray",
	"yankee zulu alpha bravo charlie delta",
	"echo foxtrot golf hotel india juliet",
	"kilo lima mike november oscar papa",
	"quebec romeo sierra tango uniform victor",
	"whiskey xray yankee zulu alpha bravo",
	"charlie delta echo foxtrot golf hotel",
	"india juliet kilo lima mike november",
	"oscar papa quebec romeo sierra tango",
	"uniform victor whiskey xray yankee zulu",
	"alpha charlie echo golf india kilo",
	"mike oscar quebec sierra uniform whiskey",
	"yankee bravo delta foxtrot hotel juliet",
	"lima november papa romeo tango victor",
	"xray zulu alpha echo india mike",
	"oscar sierra uniform yankee charlie foxtrot",
	"hotel kilo november quebec romeo victor",
	"alpha alpha alpha bravo bravo charlie",
	"delta delta delta echo echo foxtrot",
	"golf golf golf hotel hotel india",
	"juliet juliet juliet kilo kilo lima",
	"mike mike mike november november oscar",
}

var testBM25Queries = []string{
	"alpha",
	"bravo charlie",
	"echo foxtrot golf",
	"mike november oscar",
}

// TestMultiNode_HappyPath runs all happy-path reindex tests on a single shared
// 3-node cluster. Each subtest uses a different collection name for isolation.
// This avoids spinning up a new cluster per test (~20s overhead each).
func TestMultiNode_HappyPath(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	t.Run("MapToBlockmax", func(t *testing.T) {
		testMapToBlockmax(t, compose)
	})
	t.Run("RoaringSetRefresh", func(t *testing.T) {
		testRoaringSetRefresh(t, compose)
	})
	t.Run("EnableRangeable", func(t *testing.T) {
		testEnableRangeable(t, compose)
	})
	t.Run("ChangeTokenization", func(t *testing.T) {
		testChangeTokenization(t, compose)
	})
	t.Run("QueryConsistencyDuringReindex", func(t *testing.T) {
		testQueryConsistencyDuringReindex(t, compose)
	})
}

func testMapToBlockmax(t *testing.T, compose *docker.DockerCompose) {
	className := "MultiNodeBlockmax"
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
	}

	// Start background query loops on all nodes.
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
						t.Logf("node %d query %q mismatch", idx+1, q)
					}
				}
				time.Sleep(200 * time.Millisecond)
			}
		}(nodeURI, nodeIdx)
	}

	// Submit reindex.
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// Poll until reindex is done via /indexes endpoint.
	awaitReindexViaIndexes(t, restURI, className, "text", "searchable")

	// Verify task reached FINISHED state.
	awaitReindexFinished(t, restURI, taskID)

	// Stop background queries.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "queries failed during migration")

	// Verify schema update on all nodes.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		assert.True(t, cls.InvertedIndexConfig.UsingBlockMaxWAND,
			"node %d: UsingBlockMaxWAND should be true", i)
	}

	// Final queries on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-migration query %q results differ from baseline", q)
	}
}

func testRoaringSetRefresh(t *testing.T, compose *docker.DockerCompose) {
	className := "MultiNodeRoaringSet"
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

	taskID := submitIndexUpdate(t, restURI, className, "text", `{"filterable":{"rebuild":true}}`)

	// Poll until reindex is done via /indexes endpoint.
	awaitReindexViaIndexes(t, restURI, className, "text", "filterable")

	// Verify task reached FINISHED state.
	awaitReindexFinished(t, restURI, taskID)

	// Verify filterable index is healthy on all nodes via /indexes endpoint.
	for i := 1; i <= 3; i++ {
		nodeURI := compose.GetWeaviateNode(i).URI()
		indexes := getIndexes(t, nodeURI, className)
		for _, prop := range indexes.Properties {
			if prop.Name == "text" {
				for _, idx := range prop.Indexes {
					if idx.Type == "filterable" {
						assert.Equal(t, "ready", idx.Status,
							"node %d: filterable index should be ready", i)
					}
				}
			}
		}
	}

	// Verify queries still correct on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-migration query %q results differ", q)
	}
}

func testEnableRangeable(t *testing.T, compose *docker.DockerCompose) {
	className := "MultiNodeRangeable"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}},
	})
	defer deleteCollection(t, restURI, className)

	// Import objects with score values (1..25).
	for i, text := range testDocuments {
		importObjectWithScore(t, restURI, className, text, i+1)
	}

	taskID := submitIndexUpdate(t, restURI, className, "score", `{"rangeable":{"enabled":true}}`)

	// Poll until reindex is done via /indexes endpoint.
	awaitReindexViaIndexes(t, restURI, className, "score", "rangeable")

	// Verify task reached FINISHED state.
	awaitReindexFinished(t, restURI, taskID)

	// Verify schema on all nodes.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		for _, prop := range cls.Properties {
			if prop.Name == "score" {
				require.NotNil(t, prop.IndexRangeFilters,
					"node %d: score IndexRangeFilters should be set", i)
				assert.True(t, *prop.IndexRangeFilters,
					"node %d: score IndexRangeFilters should be true", i)
			}
		}
	}

	// After reindex: verify range queries work on all nodes.
	// score > 10 should return docs with scores 11..25 (15 docs).
	gtResults := queryAllNodesRange(t, compose, className, "score", "GreaterThan", 10)
	assertQueryConsistency(t, gtResults)
	assert.Len(t, gtResults[0], 15,
		"score > 10 should match 15 documents (scores 11-25)")

	// score < 5 should return docs with scores 1..4 (4 docs).
	ltResults := queryAllNodesRange(t, compose, className, "score", "LessThan", 5)
	assertQueryConsistency(t, ltResults)
	assert.Len(t, ltResults[0], 4,
		"score < 5 should match 4 documents (scores 1-4)")
}

func testChangeTokenization(t *testing.T, compose *docker.DockerCompose) {
	className := "MultiNodeTokenize"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
	})
	defer deleteCollection(t, restURI, className)

	importObjects(t, restURI, className, testDocuments)

	// With WORD tokenization, "alpha" matches multiple docs.
	for i := 1; i <= 3; i++ {
		ids, err := runBM25QueryOnNode(t, compose.GetWeaviateNode(i).URI(), className, "alpha")
		require.NoError(t, err)
		require.Greater(t, len(ids), 1,
			"node %d: 'alpha' with WORD should match multiple docs", i)
	}

	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"tokenization":"field"}}`)

	// Poll until reindex is done via /indexes endpoint.
	awaitReindexViaIndexes(t, restURI, className, "text", "searchable")

	// Verify task reached FINISHED state.
	awaitReindexFinished(t, restURI, taskID)

	// For semantic migrations (change-tokenization), the swap phase runs
	// asynchronously via OnGroupCompleted on the next scheduler tick AFTER
	// the task reaches FINISHED. Wait for the schema to reflect the update.
	require.Eventually(t, func() bool {
		return tryGetPropertyTokenization(restURI, className, "text") == "field"
	}, 30*time.Second, 1*time.Second, "tokenization should change to field after swap phase")

	// Verify schema on all nodes.
	for i := 1; i <= 3; i++ {
		cls := getClassFromNode(t, compose.GetWeaviateNode(i).URI(), className)
		for _, prop := range cls.Properties {
			if prop.Name == "text" {
				assert.Equal(t, "field", prop.Tokenization,
					"node %d: text should have tokenization=field", i)
			}
		}
	}

	// With FIELD tokenization, "alpha" as a partial token should match no docs.
	for i := 1; i <= 3; i++ {
		nodeURI := compose.GetWeaviateNode(i).URI()
		require.Eventually(t, func() bool {
			ids, err := runBM25QueryOnNode(t, nodeURI, className, "alpha")
			return err == nil && len(ids) == 0
		}, 30*time.Second, 1*time.Second,
			"node %d: 'alpha' with FIELD should match no docs", i)
	}

	// Full text matches should work.
	for i := 1; i <= 3; i++ {
		ids, err := runBM25QueryOnNode(t, compose.GetWeaviateNode(i).URI(), className,
			"alpha bravo charlie delta echo foxtrot")
		require.NoError(t, err)
		assert.Len(t, ids, 1,
			"node %d: full text with FIELD should match exactly one doc", i)
	}
}

func testQueryConsistencyDuringReindex(t *testing.T, compose *docker.DockerCompose) {
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
	// completion to catch post-swap transients.
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

// importObjectWithScore imports an object with text and score fields.
func importObjectWithScore(t *testing.T, restURI, className, text string, score int) {
	t.Helper()

	obj := map[string]interface{}{
		"class": className,
		"properties": map[string]interface{}{
			"text":  text,
			"score": score,
		},
	}

	body, err := json.Marshal(obj)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/objects", restURI),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"import object failed: %s", string(respBody))
}

// idsMatchUnordered compares two slices of IDs without regard to order.
func idsMatchUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aSorted := make([]string, len(a))
	bSorted := make([]string, len(b))
	copy(aSorted, a)
	copy(bSorted, b)
	sort.Strings(aSorted)
	sort.Strings(bSorted)
	for i := range aSorted {
		if aSorted[i] != bSorted[i] {
			return false
		}
	}
	return true
}
