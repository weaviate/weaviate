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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// start3NodeReindexCluster spins up a 3-node cluster with DTM enabled and the
// reindex provider automatically registered.
func start3NodeReindexCluster(ctx context.Context, t *testing.T) (*docker.DockerCompose, func()) {
	t.Helper()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		Start(ctx)
	require.NoError(t, err)

	return compose, func() { require.NoError(t, compose.Terminate(ctx)) }
}

// createCollection creates a class with the given shard count and replication factor
// via the REST API.
func createCollection(t *testing.T, restURI, className string, shardCount, rf int, properties []*models.Property) {
	t.Helper()

	class := map[string]interface{}{
		"class":      className,
		"vectorizer": "none",
		"shardingConfig": map[string]interface{}{
			"desiredCount": shardCount,
		},
		"replicationConfig": map[string]interface{}{
			"factor": rf,
		},
		"properties": properties,
	}

	body, err := json.Marshal(class)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema", restURI),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create class failed: %s", string(respBody))
}

// deleteCollection deletes a class via the REST API.
func deleteCollection(t *testing.T, restURI, className string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/v1/schema/%s", restURI, className), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

// importObjects imports objects with a text property into the collection.
func importObjects(t *testing.T, restURI, className string, texts []string) {
	t.Helper()

	for i, text := range texts {
		obj := map[string]interface{}{
			"class": className,
			"properties": map[string]interface{}{
				"text": text,
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

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"import object %d failed: %s", i, string(respBody))
	}
}

// submitReindex submits a reindex task via POST /v1/schema/{collection}/reindex
// on the debug port and returns the task ID.
func submitReindex(t *testing.T, debugURI, collection, migType string, properties []string, targetTokenization string) string {
	t.Helper()

	reqBody := map[string]interface{}{
		"type": migType,
	}
	if len(properties) > 0 {
		reqBody["properties"] = properties
	}
	if targetTokenization != "" {
		reqBody["targetTokenization"] = targetTokenization
	}

	jsonBody, err := json.Marshal(reqBody)
	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/v1/schema/%s/reindex", debugURI, collection)
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
	}, 180*time.Second, 1*time.Second, "reindex task %s should reach FINISHED status", taskID)
}

// runBM25QueryOnNode executes a BM25 query against a specific node and returns object IDs.
func runBM25QueryOnNode(t *testing.T, restURI, className, query string) ([]string, error) {
	t.Helper()

	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}) {
				text
				_additional { id }
			}
		}
	}`, className, query)

	reqBody := map[string]interface{}{
		"query": gqlQuery,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var gqlResp struct {
		Data struct {
			Get map[string][]map[string]interface{} `json:"Get"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", gqlResp.Errors[0].Message)
	}

	items := gqlResp.Data.Get[className]
	ids := make([]string, 0, len(items))
	for _, item := range items {
		additional := item["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

// queryAllNodes runs a BM25 query on all 3 nodes and returns results per node.
func queryAllNodes(t *testing.T, compose *docker.DockerCompose, className, query string) [][]string {
	t.Helper()

	results := make([][]string, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runBM25QueryOnNode(t, uri, className, query)
		require.NoError(t, err, "query on node %d failed", i+1)
		results[i] = ids
	}
	return results
}

// assertQueryConsistency verifies all nodes return the same result set.
func assertQueryConsistency(t *testing.T, results [][]string) {
	t.Helper()

	require.Len(t, results, 3, "expected results from 3 nodes")
	for i := 1; i < len(results); i++ {
		require.ElementsMatch(t, results[0], results[i],
			"node %d results differ from node 1", i+1)
	}
}

// getClassFromNode retrieves a class schema from a specific node.
func getClassFromNode(t *testing.T, restURI, className string) *models.Class {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s", restURI, className))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get class failed: %s", string(body))

	var class models.Class
	require.NoError(t, json.Unmarshal(body, &class))
	return &class
}

// dumpContainerLogs prints container logs for all nodes on test failure.
func dumpContainerLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	if !t.Failed() {
		return
	}

	for i := 1; i <= 3; i++ {
		reader, err := compose.GetWeaviateNode(i).Container().Logs(ctx)
		if err != nil {
			t.Logf("failed to get logs for node %d: %v", i, err)
			continue
		}
		logs, _ := io.ReadAll(reader)
		reader.Close()
		// Only print last 200 lines per node.
		lines := strings.Split(string(logs), "\n")
		if len(lines) > 200 {
			lines = lines[len(lines)-200:]
		}
		t.Logf("=== Node %d logs (last 200 lines) ===\n%s", i, strings.Join(lines, "\n"))
	}
}
