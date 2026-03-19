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

package reindex_to_blockmax

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const className = "ReindexTest"

// documents contains deterministic text for BM25 testing.
// Each document has unique terms to produce stable, predictable ranking.
var documents = []struct {
	text string
}{
	{text: "alpha bravo charlie delta echo foxtrot"},
	{text: "golf hotel india juliet kilo lima"},
	{text: "mike november oscar papa quebec romeo"},
	{text: "sierra tango uniform victor whiskey xray"},
	{text: "yankee zulu alpha bravo charlie delta"},
	{text: "echo foxtrot golf hotel india juliet"},
	{text: "kilo lima mike november oscar papa"},
	{text: "quebec romeo sierra tango uniform victor"},
	{text: "whiskey xray yankee zulu alpha bravo"},
	{text: "charlie delta echo foxtrot golf hotel"},
	{text: "india juliet kilo lima mike november"},
	{text: "oscar papa quebec romeo sierra tango"},
	{text: "uniform victor whiskey xray yankee zulu"},
	{text: "alpha charlie echo golf india kilo"},
	{text: "mike oscar quebec sierra uniform whiskey"},
	{text: "yankee bravo delta foxtrot hotel juliet"},
	{text: "lima november papa romeo tango victor"},
	{text: "xray zulu alpha echo india mike"},
	{text: "oscar sierra uniform yankee charlie foxtrot"},
	{text: "hotel kilo november quebec romeo victor"},
	{text: "alpha alpha alpha bravo bravo charlie"},
	{text: "delta delta delta echo echo foxtrot"},
	{text: "golf golf golf hotel hotel india"},
	{text: "juliet juliet juliet kilo kilo lima"},
	{text: "mike mike mike november november oscar"},
}

// bm25Queries are the queries we use to test BM25 stability.
var bm25Queries = []string{
	"alpha",
	"bravo charlie",
	"echo foxtrot golf",
	"mike november oscar",
}

func TestRuntimeMigrationToBlockmax(t *testing.T) {
	ctx := context.Background()

	// Start Weaviate with DTM enabled and non-BMW default.
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	restURI := compose.GetWeaviate().URI()

	// On test failure, dump container logs for debugging.
	defer func() {
		if t.Failed() {
			reader, err := compose.GetWeaviate().Container().Logs(ctx)
			if err != nil {
				t.Logf("failed to get container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			t.Logf("=== Container logs ===\n%s", string(logs))
		}
	}()

	// 1. Create a non-BMW collection with a text property.
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{
				K1: 1.2,
				B:  0.75,
			},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// Verify UsingBlockMaxWAND is false.
	createdClass := helper.GetClass(t, className)
	require.False(t, createdClass.InvertedIndexConfig.UsingBlockMaxWAND,
		"collection should start with UsingBlockMaxWAND=false")

	// 2. Import objects.
	for i, doc := range documents {
		obj := &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"text": doc.text,
			},
		}
		err := helper.CreateObject(t, obj)
		require.NoError(t, err, "failed to create object %d", i)
	}

	// 3. Run baseline BM25 queries and record expected results.
	type queryResult struct {
		query string
		ids   []string
	}
	baselines := make([]queryResult, len(bm25Queries))
	for i, q := range bm25Queries {
		ids := runBM25Query(t, q)
		require.NotEmpty(t, ids, "baseline query %q returned no results", q)
		baselines[i] = queryResult{query: q, ids: ids}
	}
	t.Logf("baseline queries recorded: %d queries", len(baselines))

	// 4. Start background query loop.
	var (
		queryFailures atomic.Int64
		queryRuns     atomic.Int64
		stopCh        = make(chan struct{})
		wg            sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			for _, bl := range baselines {
				ids, err := runBM25QuerySafe(t, bl.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
					t.Logf("query %q error during migration: %v", bl.query, err)
				} else if !idsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
					t.Logf("query %q mismatch during migration: expected %v, got %v",
						bl.query, bl.ids, ids)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 5. Submit reindex task via PUT /v1/schema/{collection}/indexes/{property}.
	taskID := submitIndexUpdate(t, restURI, className, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	// 6. Poll until reindex is done via /indexes endpoint.
	awaitReindexViaIndexes(t, restURI, className, "text", "searchable")

	// Verify task reached FINISHED state.
	awaitReindexFinished(t, restURI, taskID)

	// 7. Stop background query loop.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "BM25 queries failed during migration")

	// 8. Verify schema: UsingBlockMaxWAND should now be true.
	updatedClass := helper.GetClass(t, className)
	t.Logf("post-migration InvertedIndexConfig: %+v", updatedClass.InvertedIndexConfig)
	require.True(t, updatedClass.InvertedIndexConfig.UsingBlockMaxWAND,
		"UsingBlockMaxWAND should be true after migration")

	// 9. Final BM25 queries — must return the same set of results.
	// The order may differ slightly for documents with identical BM25 scores
	// because BlockmaxWAND and the old WAND use different tie-breaking.
	for _, bl := range baselines {
		ids := runBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-migration query %q results differ from baseline", bl.query)
	}

	// 10. Get shard name from the nodes API.
	shardName := getFirstShardName(t, restURI, className)
	container := compose.GetWeaviate().Container()

	// 11. Restart the server. The deferred finalize (directory renames)
	// happens during startup via FinalizeCompletedMigrations.
	t.Log("restarting weaviate container")
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())

	// 12. Verify queries return correct results after restart.
	for _, bl := range baselines {
		ids := runBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-restart query %q results differ from baseline", bl.query)
	}

	// 13. Verify filesystem clean after restart (finalize renamed dirs).
	dirs := listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__blockmax_")
}

// submitIndexUpdate submits an index update via PUT /v1/schema/{collection}/indexes/{property}
// on the main API port and returns the task ID.
func submitIndexUpdate(t *testing.T, restURI, collection, property, jsonBody string) string {
	t.Helper()

	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "index update request failed")
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("index update response (status=%d): %s", resp.StatusCode, string(respBody))
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"index update endpoint returned non-202: %s", string(respBody))

	var result map[string]string
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result["taskId"]
}

type indexesResponse struct {
	Collection string `json:"collection"`
	Properties []struct {
		Name    string `json:"name"`
		Indexes []struct {
			Type               string  `json:"type"`
			Status             string  `json:"status"`
			Progress           float32 `json:"progress"`
			Tokenization       string  `json:"tokenization,omitempty"`
			TargetTokenization string  `json:"targetTokenization,omitempty"`
		} `json:"indexes"`
	} `json:"properties"`
}

func getIndexes(t *testing.T, restURI, collection string) *indexesResponse {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s/indexes", restURI, collection))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get indexes failed: %s", string(body))

	var result indexesResponse
	require.NoError(t, json.Unmarshal(body, &result))
	return &result
}

// awaitReindexViaIndexes polls GET /v1/schema/{collection}/indexes until
// the targeted property's index reaches "ready" status.
func awaitReindexViaIndexes(t *testing.T, restURI, collection, property, indexType string) {
	t.Helper()
	var lastProgress float32
	var sawIndexing bool

	require.Eventually(t, func() bool {
		resp := getIndexes(t, restURI, collection)

		for _, prop := range resp.Properties {
			if prop.Name == property {
				for _, idx := range prop.Indexes {
					if idx.Type == indexType {
						switch idx.Status {
						case "indexing":
							sawIndexing = true
							if idx.Progress < lastProgress {
								t.Logf("WARNING: progress went backwards: %f -> %f", lastProgress, idx.Progress)
							}
							lastProgress = idx.Progress
							return false
						case "ready":
							return true // Accept ready even if we never saw indexing (fast migration)
						case "pending":
							return false
						}
					}
				}
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "expected property %s index %s to reach ready status", property, indexType)

	if sawIndexing {
		t.Logf("index monitoring: saw indexing->ready transition for %s/%s (final progress: %f)", property, indexType, lastProgress)
	} else {
		t.Logf("index monitoring: task completed too fast to see indexing status for %s/%s", property, indexType)
	}
}

// awaitReindexFinished polls GET /v1/tasks until the reindex task reaches FINISHED status.
func awaitReindexFinished(t *testing.T, restURI, taskID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}

		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			return false
		}

		for _, task := range tasks["reindex"] {
			if task.ID == taskID {
				t.Logf("task %s status: %s", taskID, task.Status)
				if task.Status == "FAILED" {
					t.Fatalf("reindex task failed: %s", task.Error)
				}
				return task.Status == "FINISHED"
			}
		}
		return false
	}, 120*time.Second, 1*time.Second, "reindex task %s should reach FINISHED status", taskID)
}

// getFirstShardName retrieves the first shard name for a collection via the nodes API.
func getFirstShardName(t *testing.T, restURI, collection string) string {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var nodesResp struct {
		Nodes []struct {
			Shards []struct {
				Class string `json:"class"`
				Name  string `json:"name"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &nodesResp))

	for _, node := range nodesResp.Nodes {
		for _, shard := range node.Shards {
			if shard.Class == collection {
				return shard.Name
			}
		}
	}
	t.Fatalf("no shard found for collection %s", collection)
	return ""
}

// runBM25Query executes a BM25 query and returns the ordered list of object IDs.
// It calls t.Fatal on error, so must only be called from the test goroutine.
func runBM25Query(t *testing.T, query string) []string {
	t.Helper()
	ids, err := runBM25QuerySafe(t, query)
	require.NoError(t, err, "BM25 query %q failed", query)
	return ids
}

// runBM25QuerySafe executes a BM25 query and returns the ordered list of object IDs.
// It returns an error instead of calling t.Fatal, making it safe for goroutines.
func runBM25QuerySafe(t *testing.T, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}) {
				text
				_additional { id }
			}
		}
	}`, className, query)

	resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", gqlQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	if len(resp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", resp.Errors[0].Message)
	}

	data := make(map[string]interface{})
	for key, value := range resp.Data {
		data[key] = value
	}
	getMap := data["Get"].(map[string]interface{})
	items := getMap[className].([]interface{})

	ids := make([]string, 0, len(items))
	for _, item := range items {
		m := item.(map[string]interface{})
		additional := m["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

// listLSMDirs lists directory names under the shard's LSM path inside the container.
func listLSMDirs(ctx context.Context, t *testing.T, c testcontainers.Container, col, shard string) []string {
	t.Helper()
	path := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(col), shard)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path})
	require.NoError(t, err, "exec ls on container")
	require.Equal(t, 0, code, "ls returned non-zero exit code")
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)
	var dirs []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dirs = append(dirs, line)
		}
	}
	return dirs
}

// assertNoSuffixedBuckets checks that no LSM directory names contain the given suffix.
func assertNoSuffixedBuckets(t *testing.T, dirs []string, suffix string) {
	t.Helper()
	for _, d := range dirs {
		assert.False(t, strings.Contains(d, suffix),
			"unexpected leftover bucket directory: %s", d)
	}
}

// idsMatchUnordered compares two slices of IDs without regard to order.
// During a Map→Blockmax migration, BM25 tie-breaking can change for equal-score
// documents, so we compare result sets rather than ordered lists.
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
