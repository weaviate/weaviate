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

package reindex_change_tokenization

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

const className = "RetokenizeTest"

// testObjects contains file-path-like strings for BM25 and filter testing.
// With WORD tokenization, paths are split into many tokens causing false positives.
// With FIELD tokenization, the entire path is a single token.
var testObjects = []map[string]interface{}{
	{"filepath": "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go", "description": "primary application source code"},
	{"filepath": "/code/github.com/weaviate/weaviate/README.md", "description": "documentation readme file"},
	{"filepath": "/code/github.com/weaviate/weaviate/main.go", "description": "entry point for the server"},
	{"filepath": "/code/github.com/other/project/main.go", "description": "alternative project launcher"},
	{"filepath": "/code/github.com/other/project/README.md", "description": "alternative project documentation"},
	{"filepath": "/code/docs/tutorial/getting_started.md", "description": "beginner tutorial guide"},
	{"filepath": "/code/docs/tutorial/advanced.md", "description": "expert level tutorial"},
	{"filepath": "/home/user/documents/report.pdf", "description": "quarterly financial report"},
	{"filepath": "/home/user/documents/notes.txt", "description": "personal meeting notes"},
	{"filepath": "/var/log/system.log", "description": "operating system log file"},
}

// bm25Query describes a BM25 query used for baseline and post-migration checks.
type bm25Query struct {
	name     string
	property string
	query    string
}

var bm25Queries = []bm25Query{
	{
		name:     "full_path_search",
		property: "filepath",
		query:    "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go",
	},
	{
		name:     "partial_token_weaviate",
		property: "filepath",
		query:    "weaviate",
	},
	{
		name:     "description_search",
		property: "description",
		query:    "tutorial",
	},
}

func TestRuntimeChangeTokenization(t *testing.T) {
	ctx := context.Background()

	// 1. Setup containers with DTM enabled.
	compose, err := docker.New().
		WithWeaviateWithDebugPort().
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
	debugURI := "http://" + compose.GetWeaviate().DebugURI()

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

	// 2. Create collection with WORD tokenization on both text properties.
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "filepath",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
			{
				Name:         "description",
				DataType:     []string{"text"},
				Tokenization: "word",
			},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	// 3. Import objects.
	for i, props := range testObjects {
		obj := &models.Object{
			Class:      className,
			Properties: props,
		}
		err := helper.CreateObject(t, obj)
		require.NoError(t, err, "failed to create object %d", i)
	}

	// 4. Baseline BM25 queries.
	type queryResult struct {
		name string
		ids  []string
	}
	baselines := make([]queryResult, len(bm25Queries))
	for i, bq := range bm25Queries {
		ids := runBM25Query(t, bq.property, bq.query)
		baselines[i] = queryResult{name: bq.name, ids: ids}
		t.Logf("baseline BM25 %q: %d results", bq.name, len(ids))
	}

	// Sanity checks for WORD tokenization BM25 baselines:
	require.Greater(t, len(baselines[0].ids), 1,
		"full_path_search with WORD should match multiple objects")
	require.Greater(t, len(baselines[1].ids), 0,
		"partial_token_weaviate with WORD should match objects")
	require.Greater(t, len(baselines[2].ids), 0,
		"description_search should match tutorial objects")

	// 4b. Baseline filter queries (test the filterable bucket).
	filterEqualWeaviateBaseline := runFilterQuery(t, "filepath", "Equal", "weaviate")
	t.Logf("baseline filter Equal 'weaviate' on filepath: %d results", len(filterEqualWeaviateBaseline))
	require.Greater(t, len(filterEqualWeaviateBaseline), 0,
		"Equal 'weaviate' with WORD should match objects with 'weaviate' token")

	filterEqualDescBaseline := runFilterQuery(t, "description", "Equal", "tutorial")
	t.Logf("baseline filter Equal 'tutorial' on description: %d results", len(filterEqualDescBaseline))
	require.Greater(t, len(filterEqualDescBaseline), 0,
		"Equal 'tutorial' with WORD should match tutorial objects")

	filterLikeBaseline := runFilterQuery(t, "filepath", "Like", "weaviate*")
	t.Logf("baseline filter Like 'weaviate*' on filepath: %d results", len(filterLikeBaseline))
	require.Greater(t, len(filterLikeBaseline), 0,
		"Like 'weaviate*' with WORD should match objects with 'weaviate' token")

	// 5. Background query loop.
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
			for i, bl := range baselines {
				bq := bm25Queries[i]
				ids, err := runBM25QuerySafe(t, bq.property, bq.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
					t.Logf("query %q error during migration: %v", bl.name, err)
					continue
				}
				// During migration, we may see old results or new results,
				// but never an inconsistent in-between state.
				// For description queries: results should be unchanged.
				if bq.property == "description" {
					if !idsMatchUnordered(bl.ids, ids) {
						queryFailures.Add(1)
						t.Logf("query %q mismatch during migration: expected %v, got %v",
							bl.name, bl.ids, ids)
					}
				}
			}
			// Also run a filter query during migration to check filterable bucket.
			ids, err := runFilterQuerySafe(t, "description", "Equal", "tutorial")
			queryRuns.Add(1)
			if err != nil {
				queryFailures.Add(1)
				t.Logf("filter query error during migration: %v", err)
			} else if !idsMatchUnordered(filterEqualDescBaseline, ids) {
				queryFailures.Add(1)
				t.Logf("filter query mismatch during migration: expected %v, got %v",
					filterEqualDescBaseline, ids)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// 6. Submit reindex task: change filepath tokenization from WORD to FIELD.
	taskID := submitReindex(t, debugURI, className, "change-tokenization",
		[]string{"filepath"}, "field")
	t.Logf("submitted reindex task: %s", taskID)

	// 7. Poll until task is FINISHED.
	awaitReindexFinished(t, restURI, taskID)

	// 8. Stop background query loop.
	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "queries failed during migration")

	// 9. Post-migration BM25 queries with new expectations.
	// After FIELD tokenization on filepath:
	// - full_path_search: only exact match (1 result)
	postFullPath := runBM25Query(t, "filepath", "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postFullPath, 1, "full_path_search with FIELD should match exactly one object")

	// - partial_token_weaviate: no matches (FIELD doesn't split)
	postPartial := runBM25Query(t, "filepath", "weaviate")
	assert.Empty(t, postPartial, "partial_token_weaviate with FIELD should match no objects")

	// - description_search: unchanged (property not retokenized)
	postDescription := runBM25Query(t, "description", "tutorial")
	assert.ElementsMatch(t, baselines[2].ids, postDescription,
		"description_search results should be unchanged")

	// 9b. Post-migration filter queries.
	postFilterEqualWeaviate := runFilterQuery(t, "filepath", "Equal", "weaviate")
	assert.Empty(t, postFilterEqualWeaviate,
		"filter Equal 'weaviate' with FIELD should match no objects")

	postFilterEqualFull := runFilterQuery(t, "filepath", "Equal",
		"/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postFilterEqualFull, 1,
		"filter Equal full path with FIELD should match exactly one object")

	postFilterLike := runFilterQuery(t, "filepath", "Like", "weaviate*")
	assert.Empty(t, postFilterLike,
		"filter Like 'weaviate*' with FIELD should match no objects")

	postFilterDesc := runFilterQuery(t, "description", "Equal", "tutorial")
	assert.ElementsMatch(t, filterEqualDescBaseline, postFilterDesc,
		"filter Equal 'tutorial' on description should be unchanged")

	// 10. Verify schema: tokenization changed to "field" on filepath, unchanged on description.
	updatedClass := helper.GetClass(t, className)
	for _, prop := range updatedClass.Properties {
		switch prop.Name {
		case "filepath":
			assert.Equal(t, "field", prop.Tokenization,
				"filepath should have tokenization=field after migration")
		case "description":
			assert.Equal(t, "word", prop.Tokenization,
				"description should still have tokenization=word")
		}
	}

	// 11. Verify filesystem: no leftover retokenize buckets.
	shardName := getFirstShardName(t, restURI, className)
	container := compose.GetWeaviate().Container()
	dirs := listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__retokenize_")
	assertNoSuffixedBuckets(t, dirs, "__filt_retokenize_")

	// Searchable and filterable buckets should still exist.
	assertBucketExists(t, dirs, "property_filepath_searchable")
	assertBucketExists(t, dirs, "property_description_searchable")
	assertBucketExists(t, dirs, "property_filepath")
	assertBucketExists(t, dirs, "property_description")

	// 12. Restart container.
	t.Log("restarting weaviate container")
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())

	// 13. Post-restart: BM25 queries still correct.
	postRestartFullPath := runBM25Query(t, "filepath", "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postRestartFullPath, 1, "post-restart: full_path_search with FIELD should match exactly one object")

	postRestartPartial := runBM25Query(t, "filepath", "weaviate")
	assert.Empty(t, postRestartPartial, "post-restart: partial_token_weaviate with FIELD should match no objects")

	postRestartDescription := runBM25Query(t, "description", "tutorial")
	assert.ElementsMatch(t, baselines[2].ids, postRestartDescription,
		"post-restart: description_search results should be unchanged")

	// 13b. Post-restart: filter queries still correct.
	postRestartFilterEqual := runFilterQuery(t, "filepath", "Equal", "weaviate")
	assert.Empty(t, postRestartFilterEqual,
		"post-restart: filter Equal 'weaviate' with FIELD should match no objects")

	postRestartFilterFull := runFilterQuery(t, "filepath", "Equal",
		"/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postRestartFilterFull, 1,
		"post-restart: filter Equal full path with FIELD should match exactly one object")

	postRestartFilterLike := runFilterQuery(t, "filepath", "Like", "weaviate*")
	assert.Empty(t, postRestartFilterLike,
		"post-restart: filter Like 'weaviate*' with FIELD should match no objects")

	postRestartFilterDesc := runFilterQuery(t, "description", "Equal", "tutorial")
	assert.ElementsMatch(t, filterEqualDescBaseline, postRestartFilterDesc,
		"post-restart: filter Equal 'tutorial' on description should be unchanged")

	// Post-restart: schema still correct.
	restartClass := helper.GetClass(t, className)
	for _, prop := range restartClass.Properties {
		if prop.Name == "filepath" {
			assert.Equal(t, "field", prop.Tokenization,
				"post-restart: filepath should have tokenization=field")
		}
	}

	// Post-restart: filesystem still clean.
	dirs = listLSMDirs(ctx, t, container, className, shardName)
	assertNoSuffixedBuckets(t, dirs, "__retokenize_")
	assertNoSuffixedBuckets(t, dirs, "__filt_retokenize_")
	assertBucketExists(t, dirs, "property_filepath_searchable")
	assertBucketExists(t, dirs, "property_description_searchable")
	assertBucketExists(t, dirs, "property_filepath")
	assertBucketExists(t, dirs, "property_description")
}

// submitReindex submits a reindex task via POST /v1/schema/{collection}/reindex
// on the debug port and returns the task ID.
func submitReindex(t *testing.T, debugURI, collection, migType string, properties []string, targetTokenization string) string {
	t.Helper()

	body := map[string]interface{}{
		"type": migType,
	}
	if len(properties) > 0 {
		body["properties"] = properties
	}
	if targetTokenization != "" {
		body["targetTokenization"] = targetTokenization
	}

	jsonBody, err := json.Marshal(body)
	require.NoError(t, err)

	url := fmt.Sprintf("%s/v1/schema/%s/reindex", debugURI, collection)
	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonBody)) //nolint:gosec
	require.NoError(t, err, "reindex submit request failed")
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	t.Logf("reindex submit response (status=%d): %s", resp.StatusCode, string(respBody))
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"reindex endpoint returned non-202: %s", string(respBody))

	var result map[string]string
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result["taskId"]
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

// runBM25Query executes a BM25 query and returns the list of object IDs.
func runBM25Query(t *testing.T, property, query string) []string {
	t.Helper()
	ids, err := runBM25QuerySafe(t, property, query)
	require.NoError(t, err, "BM25 query failed")
	return ids
}

// runBM25QuerySafe executes a BM25 query and returns the list of object IDs.
func runBM25QuerySafe(t *testing.T, property, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}) {
				filepath
				description
				_additional { id }
			}
		}
	}`, className, query, property)

	return executeGraphQLQuery(t, gqlQuery)
}

// runFilterQuery executes a where-filter query and returns the list of object IDs.
func runFilterQuery(t *testing.T, property, operator, value string) []string {
	t.Helper()
	ids, err := runFilterQuerySafe(t, property, operator, value)
	require.NoError(t, err, "filter query failed for %s %s %q", operator, property, value)
	return ids
}

// runFilterQuerySafe executes a where-filter query and returns the list of object IDs.
func runFilterQuerySafe(t *testing.T, property, operator, value string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {operator: %s, path: [%q], valueText: %q}) {
				filepath
				description
				_additional { id }
			}
		}
	}`, className, operator, property, value)

	return executeGraphQLQuery(t, gqlQuery)
}

// executeGraphQLQuery runs a GraphQL query and extracts object IDs from the response.
func executeGraphQLQuery(t *testing.T, gqlQuery string) ([]string, error) {
	t.Helper()
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
