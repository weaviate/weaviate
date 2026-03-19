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

package reindex_enable_rangeable

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

const className = "EnableRangeableTest"

// testObjects creates deterministic data for range + exact-match filter testing.
// 25 objects with varied numeric values.
var testObjects = func() []map[string]interface{} {
	objects := make([]map[string]interface{}, 0, 25)
	for i := 0; i < 25; i++ {
		objects = append(objects, map[string]interface{}{
			"name":  fmt.Sprintf("item_%d", i),
			"score": float64(i + 1),        // 1..25
			"price": float64(i)*3.5 + 10.0, // 10.0, 13.5, 17.0, ...
		})
	}
	return objects
}()

// filterQuery describes a filter query and its GraphQL where clause.
type filterQuery struct {
	name  string
	where string
}

var filterQueries = []filterQuery{
	{
		name:  "score_gt_15",
		where: `{path:["score"], operator:GreaterThan, valueInt:15}`,
	},
	{
		name:  "price_lte_50",
		where: `{path:["price"], operator:LessThanEqual, valueNumber:50.0}`,
	},
	{
		name: "score_gte_10_and_price_lt_80",
		where: `{operator:And, operands:[
			{path:["score"], operator:GreaterThanEqual, valueInt:10},
			{path:["price"], operator:LessThan, valueNumber:80.0}
		]}`,
	},
	{
		name:  "score_eq_5",
		where: `{path:["score"], operator:Equal, valueInt:5}`,
	},
	{
		name:  "price_eq_exact",
		where: `{path:["price"], operator:Equal, valueNumber:10.0}`,
	},
}

func TestRuntimeEnableRangeable(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
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

	// Dump container logs on failure.
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

	// 1. Create collection with numeric properties but NO indexRangeFilters.
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"text"},
			},
			{
				Name:     "score",
				DataType: []string{"int"},
			},
			{
				Name:     "price",
				DataType: []string{"number"},
			},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// 2. Import objects.
	for i, props := range testObjects {
		obj := &models.Object{
			Class:      className,
			Properties: props,
		}
		err := helper.CreateObject(t, obj)
		require.NoError(t, err, "failed to create object %d", i)
	}

	// 3. Run baseline queries.
	type queryResult struct {
		name string
		ids  []string
	}
	baselines := make([]queryResult, len(filterQueries))
	for i, fq := range filterQueries {
		ids := runFilterQuery(t, fq.where)
		require.NotEmpty(t, ids, "baseline query %q returned no results", fq.name)
		baselines[i] = queryResult{name: fq.name, ids: ids}
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
				fq := filterQueries[0]
				for _, q := range filterQueries {
					if q.name == bl.name {
						fq = q
						break
					}
				}
				ids, err := runFilterQuerySafe(t, fq.where)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
					t.Logf("query %q error during migration: %v", bl.name, err)
				} else if !idsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
					t.Logf("query %q mismatch during migration: expected %v, got %v",
						bl.name, bl.ids, ids)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 5. Submit reindex tasks sequentially — one per property.
	taskID1 := submitIndexUpdate(t, restURI, className, "score", `{"rangeable":{"enabled":true}}`)
	t.Logf("submitted reindex task for score: %s", taskID1)
	awaitReindexViaIndexes(t, restURI, className, "score", "rangeable")
	awaitReindexFinished(t, restURI, taskID1)

	taskID2 := submitIndexUpdate(t, restURI, className, "price", `{"rangeable":{"enabled":true}}`)
	t.Logf("submitted reindex task for price: %s", taskID2)
	awaitReindexViaIndexes(t, restURI, className, "price", "rangeable")
	awaitReindexFinished(t, restURI, taskID2)

	// 7. Stop background query loop.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "filter queries failed during migration")

	// 8. Final filter queries — must return same results.
	for i, bl := range baselines {
		ids := runFilterQuery(t, filterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-migration query %q results differ from baseline", bl.name)
	}

	// 9. Verify schema: IndexRangeFilters=true on score and price, NOT on name.
	updatedClass := helper.GetClass(t, className)
	for _, prop := range updatedClass.Properties {
		switch prop.Name {
		case "score", "price":
			require.NotNil(t, prop.IndexRangeFilters,
				"property %q should have IndexRangeFilters set", prop.Name)
			assert.True(t, *prop.IndexRangeFilters,
				"property %q should have IndexRangeFilters=true", prop.Name)
		case "name":
			if prop.IndexRangeFilters != nil {
				assert.False(t, *prop.IndexRangeFilters,
					"property 'name' should NOT have IndexRangeFilters=true")
			}
		}
	}

	// 10. Get shard name for filesystem checks.
	shardName := getFirstShardName(t, restURI, className)
	container := compose.GetWeaviate().Container()

	// 11. Restart container. Deferred finalize (directory renames) happens
	// during startup via FinalizeCompletedMigrations.
	t.Log("restarting weaviate container")
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())

	// 12. Post-restart: queries still correct.
	for i, bl := range baselines {
		ids := runFilterQuery(t, filterQueries[i].where)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-restart query %q results differ from baseline", bl.name)
	}

	// 13. Post-restart: filesystem clean (finalize renamed dirs).
	dirs := listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__rangeable_")
	assertBucketExists(t, dirs, "property_score_rangeable")
	assertBucketExists(t, dirs, "property_price_rangeable")
	assertBucketExists(t, dirs, "property_score")
	assertBucketExists(t, dirs, "property_price")
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

// runFilterQuery executes a filter query and returns the list of object IDs.
func runFilterQuery(t *testing.T, where string) []string {
	t.Helper()
	ids, err := runFilterQuerySafe(t, where)
	require.NoError(t, err, "filter query failed")
	return ids
}

// runFilterQuerySafe executes a filter query and returns the list of object IDs.
func runFilterQuerySafe(t *testing.T, where string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: %s) {
				name
				score
				price
				_additional { id }
			}
		}
	}`, className, where)

	resp, err := graphqlhelper.QueryGraphQL(t, nil, "", gqlQuery, nil)
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

// assertBucketExists checks that a bucket directory exists in the LSM dirs.
func assertBucketExists(t *testing.T, dirs []string, bucketName string) {
	t.Helper()
	found := false
	for _, d := range dirs {
		if d == bucketName {
			found = true
			break
		}
	}
	assert.True(t, found, "expected bucket directory %q not found in %v", bucketName, dirs)
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
