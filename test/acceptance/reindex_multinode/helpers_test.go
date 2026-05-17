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
	"sync"
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
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false").
		Start(ctx)
	if err != nil {
		if compose != nil {
			dumpStartupLogs(ctx, t, compose)
		}
		require.NoError(t, err)
	}

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
//
// Uses consistency_level=ALL so the POST does not return until every
// replica has applied the write. Without this, the default (single-replica
// ack) lets the next query race ahead of replication — a baseline check
// that immediately polls all three nodes can see node1=6 / node2=5 / etc.,
// failing the per-replica equality assertion. See R0 flake repro.
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
			fmt.Sprintf("http://%s/v1/objects?consistency_level=ALL", restURI),
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

// awaitReindexReachedFinalizing polls /v1/tasks until the reindex task
// transitions into FINALIZING — i.e. every unit has completed its
// reindex iteration on every node and the cluster is about to fire the
// post-completion swap + schema flip. Used by tests that need to
// trigger destructive events (rolling restart, SIGKILL) inside the
// brief FINALIZING window to exercise the post-completion ack barrier.
//
// FINALIZING is short for format-only migrations (essentially zero
// wall-clock time) and seconds for change-tokenization at moderate
// scale. We poll at 200ms which is fast enough to land inside even the
// tightest window. Returns the snapshot of the task at the moment we
// first observed FINALIZING (for forensic logging by the caller).
func awaitReindexReachedFinalizing(t *testing.T, restURI, taskID string) string {
	t.Helper()
	var observed string
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
			if task.ID != taskID {
				continue
			}
			if task.Status == "FAILED" {
				t.Fatalf("reindex task failed before reaching FINALIZING: %s", task.Error)
			}
			if task.Status == "FINALIZING" || task.Status == "FINISHED" {
				// FINISHED here means the FINALIZING window was so
				// short we missed it — the rolling restart will
				// already be too late. Return the observed status so
				// the test caller can re-tune dataset size / poll
				// cadence rather than silently passing on a stale
				// repro.
				observed = task.Status
				return true
			}
		}
		return false
	}, 240*time.Second, 200*time.Millisecond,
		"reindex task %s should reach FINALIZING (or FINISHED) within 240s", taskID)
	return observed
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
	}, 180*time.Second, 1*time.Second, "expected property %s index %s to reach ready status", property, indexType)

	if sawIndexing {
		t.Logf("index monitoring: saw indexing->ready transition for %s/%s (final progress: %f)", property, indexType, lastProgress)
	} else {
		t.Logf("index monitoring: task completed too fast to see indexing status for %s/%s", property, indexType)
	}
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

// tryImportObject attempts to import a single object and returns an error
// instead of calling t.Fatal. Useful for polling Raft write-readiness.
func tryImportObject(restURI, className, text string) error {
	obj := map[string]interface{}{
		"class": className,
		"properties": map[string]interface{}{
			"text": text,
		},
	}

	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/objects", restURI),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("import request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("import failed (status %d): %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// tryGetPropertyTokenization retrieves a property's tokenization from a node.
// Returns "" if the request fails or the property is not found.
func tryGetPropertyTokenization(restURI, className, propName string) string {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s", restURI, className))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}

	var class models.Class
	if err := json.Unmarshal(body, &class); err != nil {
		return ""
	}

	for _, prop := range class.Properties {
		if prop.Name == propName {
			return prop.Tokenization
		}
	}
	return ""
}

// runRangeQueryOnNode executes a range filter query (e.g. score > 10) against a specific node
// and returns matching object IDs.
func runRangeQueryOnNode(t *testing.T, restURI, className, propName, operator string, value int) ([]string, error) {
	t.Helper()

	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: %s, valueInt: %d}) {
				_additional { id }
			}
		}
	}`, className, propName, operator, value)

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

// queryAllNodesRange runs a range query on all 3 nodes and returns results per node.
func queryAllNodesRange(t *testing.T, compose *docker.DockerCompose, className, propName, operator string, value int) [][]string {
	t.Helper()

	results := make([][]string, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runRangeQueryOnNode(t, uri, className, propName, operator, value)
		require.NoError(t, err, "range query on node %d failed", i+1)
		results[i] = ids
	}
	return results
}

// runBM25QueryOnNodeWithRetry executes a BM25 query with one retry on transient
// errors (connection refused, timeouts). This is useful in background query loops
// where a single transient failure during node swap should not count as a test failure.
func runBM25QueryOnNodeWithRetry(t *testing.T, restURI, className, query string) ([]string, error) {
	t.Helper()

	ids, err := runBM25QueryOnNode(t, restURI, className, query)
	if err != nil {
		// Retry once after a short delay for transient errors.
		time.Sleep(200 * time.Millisecond)
		ids, err = runBM25QueryOnNode(t, restURI, className, query)
	}
	return ids, err
}

// restartCluster cycles every node serially — stop, start, wait for
// ready, move on. Used by the restart-matrix tests to verify the
// deferred-finalize design: every per-node migration tracker dir is
// consumed by FinalizeCompletedMigrations at startup, and follow-up
// migrations start from a clean state.
//
// Full-cluster simultaneous restart is intentionally NOT used here.
// Stopping all 3 nodes loses RAFT quorum, and the first node to come
// back up cannot form a leader alone — its readiness check times out.
// Serial restart keeps 2/3 nodes up at every step so RAFT continues to
// function while each node individually cycles through finalize at
// startup. This is the same shape as a Kubernetes StatefulSet rolling
// update, which is the production deployment model for Weaviate.
func restartCluster(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	rollingRestartCluster(ctx, t, compose)
}

// rollingRestartCluster stops + restarts each node ONE AT A TIME,
// waiting for the node to be ready (and for RAFT to accept writes
// again) before moving on. Mimics a Kubernetes StatefulSet rolling
// update — the failure mode that hid weaviate/weaviate#10675 in
// Frontend Claude's prod environment, where pods rolled at different
// times produced different on-disk states for the same migration.
//
// Without the readiness wait, the test would race the node's
// FinalizeCompletedMigrations + shard-init + bucket-load — queries to
// a not-yet-ready node return 0 across the board even though the
// promoted canonical dir is present on disk. That manifested as a
// per-replica `[6 6 0]`/`[0 0 0]` failure that looks identical to the
// real #10675 prod data-loss bug but is just a missing test barrier.
func rollingRestartCluster(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	for i := 1; i <= 3; i++ {
		t.Logf("rolling restart: cycling node %d", i)
		require.NoErrorf(t, compose.StopAt(ctx, i-1, nil), "stop node %d", i)
		require.NoErrorf(t, compose.StartAt(ctx, i-1), "start node %d", i)

		// Wait for this node's HTTP endpoint to respond before moving
		// on. tryGetSchema is cheap and exercises the same routing
		// path the test asserts against. 60s is generous for the
		// FinalizeCompletedMigrations + shard-init phase.
		restartedURI := compose.GetWeaviateNode(i).URI()
		require.Eventuallyf(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://%s/v1/.well-known/ready", restartedURI))
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 60*time.Second, 500*time.Millisecond,
			"node %d should be ready after rolling restart", i)
	}
}

// dumpContainerLogs prints container logs for all nodes on test failure.
func dumpContainerLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	if !t.Failed() {
		return
	}

	dumpStartupLogs(ctx, t, compose)
}

// filterMigrationLogLines returns lines from a container log that mention
// reindex / migration / swap state. Used by dumpStartupLogs to make the
// per-failure log post-mortem tractable without dropping the relevant
// events.
func filterMigrationLogLines(s string) []string {
	keywords := []string{
		"reindex", "migration", "Reindex", "Migration",
		"OnAfterLsmInit", "OnBeforeLsmInit",
		"OnGroupCompleted", "OnTaskCompleted",
		"RunSwapOnShard", "RunReindexOnlyOnShard", "RunOnShard",
		"finalize:", "FinalizeCompletedMigrations",
		"swapped.mig", "tidied.mig", "merged.mig", "prepended.mig",
		"recovered untidied", "swap INCOMPLETE", "swap complete",
		"runtime swap", "trim:",
		"distributed task", "distributedtask",
	}
	var out []string
	for _, line := range strings.Split(s, "\n") {
		for _, kw := range keywords {
			if strings.Contains(line, kw) {
				out = append(out, line)
				break
			}
		}
	}
	return out
}

// dumpStartupLogs unconditionally prints container logs for all available nodes.
// Use this when you need logs before the test has been marked as failed (e.g. on
// startup errors).
func dumpStartupLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose) {
	t.Helper()

	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		if node == nil {
			t.Logf("=== Node %d: container not available ===", i)
			continue
		}
		reader, err := node.Container().Logs(ctx)
		if err != nil {
			t.Logf("failed to get logs for node %d: %v", i, err)
			continue
		}
		logs, _ := io.ReadAll(reader)
		reader.Close()
		// Filter to lines that mention reindex / migration / swap-related
		// state. The full log is too verbose to dump per-failure, but
		// throwing away everything except the migration-relevant entries
		// keeps the post-mortem small without losing the failure context.
		// Falls back to the last 400 lines if no migration-related entries
		// matched, so we still get a tail for non-reindex failures.
		filtered := filterMigrationLogLines(string(logs))
		if len(filtered) == 0 {
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 400 {
				lines = lines[len(lines)-400:]
			}
			filtered = lines
		}
		t.Logf("=== Node %d logs (%d migration/reindex lines) ===\n%s", i, len(filtered), strings.Join(filtered, "\n"))
	}
}

// probeSample is one observation of a probe function against a node.
type probeSample struct {
	t      time.Time
	nodeID int
	count  int
	err    error
}

// probeFn is the shape of a per-node query probe. Returns (count, err).
type probeFn func(restURI, className string) (int, error)

// waitForProbeBaseline polls the given probe across all three replicas
// until counts agree AND repeat once. Returns the converged count.
//
// Why this is needed: batchImport / importObjects use the default write
// consistency, which returns to the caller after a quorum of replicas
// has acknowledged the write — but the third replica's apply leg can
// still be in flight for hundreds of ms after the POST returns. A
// baseline captured during that lag window will be smaller than the
// steady-state count by the lag amount. Subsequent FINALIZING-window
// probe samples then read the converged (larger) count and get
// classified as "out-of-range" by classifyProbeSamples, producing
// spurious failures even though the per-shard tokenization overlay is
// working correctly.
//
// Observed on PR #11323 CI run b19dd49366 / job 76404184658:
//
//	baseline captured: 1495 (lagged replica)
//	steady-state count: 1500 (all replicas converged)
//	13 OUT-OF-RANGE samples logged, all count=1500 vs valid range [0, 1495]
//
// The shape is the same one `waitForPerReplicaBaseline` (in
// round_trip_adjacent_test.go) was added for; this is the
// probeFn-generic version of the same pattern for tests that don't use
// a fixed list of BM25 query strings.
func waitForProbeBaseline(
	t *testing.T, compose *docker.DockerCompose, className string,
	probe probeFn,
) int {
	t.Helper()
	deadline := time.Now().Add(perReplicaConvergenceTimeout)
	prevAll := -1
	for time.Now().Before(deadline) {
		var counts [3]int
		ok := true
		for n := 0; n < 3; n++ {
			c, err := probe(compose.GetWeaviateNode(n+1).URI(), className)
			if err != nil {
				t.Logf("waitForProbeBaseline: probe error on node %d: %v", n+1, err)
				ok = false
				break
			}
			counts[n] = c
		}
		if ok && allEqualPositive(counts) {
			if prevAll == counts[0] {
				t.Logf("waitForProbeBaseline: converged at count=%d across all 3 replicas",
					counts[0])
				return counts[0]
			}
			prevAll = counts[0]
		} else {
			// Divergence resets the "stable" requirement so a flapping
			// count gets fully re-validated.
			prevAll = -1
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("waitForProbeBaseline: per-replica counts did not converge within %s",
		perReplicaConvergenceTimeout)
	return 0
}

// allEqualPositive reports whether every per-replica count in counts
// is the same strictly-positive integer. Used as the convergence gate
// in waitForProbeBaseline: a steady-state read must agree across all
// three replicas AND be > 0 (zero would indicate an empty bucket that
// hasn't yet been populated, not a converged baseline).
func allEqualPositive(counts [3]int) bool {
	return counts[0] > 0 && counts[0] == counts[1] && counts[1] == counts[2]
}

// runMigrationWithProbes spins up one probe goroutine per node, each
// invoking `probe` every `probeInterval`, while `migrate` runs. After
// `migrate` returns, probes continue for `tailDuration` to capture the
// post-cutover steady state, then stop. Returns the collected samples
// and the wall-clock time `migrate` started at.
//
// Shared between TestPartialResultsDuringChangeTokenization (which
// pins the looser cluster-wide cutover bound) and
// TestLiveQueriesDuringChangeTokenization (which pins the tighter
// per-shard alignment bound under the tokenization overlay) so both
// tests use identical sampling machinery — only their assertions
// differ.
func runMigrationWithProbes(
	t *testing.T,
	compose *docker.DockerCompose,
	className string,
	probeInterval, tailDuration time.Duration,
	probe probeFn,
	migrate func(),
) ([]probeSample, time.Time) {
	t.Helper()

	samplesMu := sync.Mutex{}
	samples := make([]probeSample, 0, 1024)
	record := func(s probeSample) {
		samplesMu.Lock()
		samples = append(samples, s)
		samplesMu.Unlock()
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		wg.Add(1)
		nodeURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
		idx := nodeIdx + 1
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				start := time.Now()
				count, err := probe(nodeURI, className)
				record(probeSample{t: start, nodeID: idx, count: count, err: err})
				time.Sleep(probeInterval)
			}
		}()
	}

	migrationStart := time.Now()
	migrate()

	// Let probes continue past the migration completion to capture late
	// samples and the post-cutover steady state.
	time.Sleep(tailDuration)
	close(stopCh)
	wg.Wait()

	return samples, migrationStart
}

// probeClassification summarizes a probe sample set against the two
// known steady-state counts: `baseline` (what the probe should return
// before the migration starts) and `expectedAfter` (what it should
// return after the migration commits).
//
// Pre/Post counts represent steady-state observations on either side
// of the cutover. Partial counts are samples that lie inside the
// open range (min(baseline, expectedAfter), max(baseline, expectedAfter))
// — the cross-shard cutover spread admits a brief partial window
// during the per-replica swap + cluster-wide schema flip. OutOfRange
// counts are samples OUTSIDE that range; with the per-shard
// tokenization overlay in place, no sample should be out-of-range
// because every replica's bucket content is always tokenization-
// aligned with the value the analyzer uses. Out-of-range samples
// indicate either the overlay isn't wired into a query path or the
// set/clear hooks fire at the wrong FSM transition.
type probeClassification struct {
	Pre, Post, Partial, OutOfRange, Errors int
	FirstPartial, LastPartial              time.Time
}

// classifyProbeSamples buckets each non-error sample as Pre (==
// baseline), Post (== expectedAfter), Partial (inside the open range
// between them), or OutOfRange (outside that range — the #216
// misalignment shape). Logs every partial and out-of-range sample
// for forensic visibility.
func classifyProbeSamples(t *testing.T, samples []probeSample, baseline, expectedAfter int, migrationStart time.Time) probeClassification {
	t.Helper()
	lo, hi := baseline, expectedAfter
	if lo > hi {
		lo, hi = hi, lo
	}
	var c probeClassification
	for _, s := range samples {
		switch {
		case s.err != nil:
			c.Errors++
		case s.count == baseline:
			c.Pre++
		case s.count == expectedAfter:
			c.Post++
		case s.count < lo || s.count > hi:
			c.OutOfRange++
			t.Logf("OUT-OF-RANGE @ +%v node=%d count=%d (valid range [%d, %d])",
				s.t.Sub(migrationStart).Round(time.Millisecond),
				s.nodeID, s.count, lo, hi)
		default:
			c.Partial++
			if c.FirstPartial.IsZero() {
				c.FirstPartial = s.t
			}
			c.LastPartial = s.t
			t.Logf("partial @ +%v node=%d count=%d (baseline=%d, post=%d)",
				s.t.Sub(migrationStart).Round(time.Millisecond),
				s.nodeID, s.count, baseline, expectedAfter)
		}
	}
	t.Logf("probe classification: pre=%d post=%d partial=%d out_of_range=%d err=%d",
		c.Pre, c.Post, c.Partial, c.OutOfRange, c.Errors)
	if c.Partial > 0 {
		t.Logf("partial-results window spanned %v (first @ +%v, last @ +%v)",
			c.LastPartial.Sub(c.FirstPartial).Round(time.Millisecond),
			c.FirstPartial.Sub(migrationStart).Round(time.Millisecond),
			c.LastPartial.Sub(migrationStart).Round(time.Millisecond))
	}
	return c
}

// countLatePartials returns the number of non-error samples whose
// timestamp is after `anchor` and whose count is neither baseline nor
// expectedAfter. Used by both tests as the post-window convergence
// guarantee — late partials indicate the cutover has not stabilized
// after the bounded window closed.
func countLatePartials(t *testing.T, samples []probeSample, baseline, expectedAfter int, anchor, migrationStart time.Time) int {
	t.Helper()
	var late int
	for _, s := range samples {
		if s.err != nil {
			continue
		}
		if s.t.After(anchor) && s.count != baseline && s.count != expectedAfter {
			late++
			t.Logf("late partial @ +%v node=%d count=%d (after anchor)",
				s.t.Sub(migrationStart).Round(time.Millisecond),
				s.nodeID, s.count)
		}
	}
	return late
}
