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

func TestMultiNode_MapToBlockmax(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "MultiNodeBlockmax"
	restURI := compose.GetWeaviateNode(1).URI()

	// Create collection with RF=3 and import data.
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
					ids, err := runBM25QueryOnNode(t, uri, className, q)
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

func TestMultiNode_RoaringSetRefresh(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

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

	// Verify queries still correct on all nodes.
	for _, q := range testBM25Queries {
		results := queryAllNodes(t, compose, className, q)
		assertQueryConsistency(t, results)
		assert.ElementsMatch(t, baselines[q][0], results[0],
			"post-migration query %q results differ", q)
	}
}

func TestMultiNode_EnableRangeable(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	className := "MultiNodeRangeable"
	restURI := compose.GetWeaviateNode(1).URI()

	createCollection(t, restURI, className, 3, 3, []*models.Property{
		{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		{Name: "score", DataType: []string{"int"}},
	})
	defer deleteCollection(t, restURI, className)

	// Import objects with score values.
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
}

func TestMultiNode_ChangeTokenization(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

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
		ids, err := runBM25QueryOnNode(t, compose.GetWeaviateNode(i).URI(), className, "alpha")
		require.NoError(t, err)
		assert.Empty(t, ids,
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
