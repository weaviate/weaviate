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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// start3NodeReindexCluster spins up a 3-node cluster with DTM enabled
// and the reindex provider automatically registered. Optional
// `extraEnv` pairs (key, value, key, value, …) are applied on top so a
// test that needs e.g. USE_INVERTED_SEARCHABLE=false can opt in without
// changing the package-wide default — tests that exercise BlockMax-
// based code paths (change-tokenization, etc.) keep the production
// default.
func start3NodeReindexCluster(ctx context.Context, t *testing.T, extraEnv ...string) (*docker.DockerCompose, func()) {
	t.Helper()
	if len(extraEnv)%2 != 0 {
		t.Fatalf("start3NodeReindexCluster: extraEnv must be (key,value) pairs, got %d items", len(extraEnv))
	}

	b := docker.New().
		WithWeaviateEnv("RUNTIME_REINDEX_ENABLED", "true").
		With3NodeCluster().
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false")
	for i := 0; i < len(extraEnv); i += 2 {
		b = b.WithWeaviateEnv(extraEnv[i], extraEnv[i+1])
	}
	compose, err := b.Start(ctx)
	if err != nil {
		if compose != nil {
			dumpStartupLogs(ctx, t, compose)
		}
		require.NoError(t, err)
	}

	return compose, func() { require.NoError(t, compose.Terminate(ctx)) }
}

func textProps(names ...string) []*models.Property {
	props := make([]*models.Property, 0, len(names))
	for _, name := range names {
		props = append(props, &models.Property{
			Name:         name,
			DataType:     []string{"text"},
			Tokenization: "word",
		})
	}
	return props
}

// createCollection creates a class via the REST API, then blocks until it is
// in every node's local schema view: a lagging follower fails consistency=ALL
// writes, and retrying isn't safe (auto-UUIDs would duplicate).
func createCollection(t *testing.T, compose *docker.DockerCompose, restURI, className string, shardCount, rf int, properties []*models.Property) {
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

	// consistency:false forces a local read; the default proxies to the
	// leader, which would hide a lagging follower.
	for _, node := range compose.Containers() {
		if !strings.HasPrefix(node.Name(), "weaviate-") {
			continue
		}
		nodeURI := node.URI()
		require.Eventuallyf(t, func() bool {
			req, err := http.NewRequest(http.MethodGet,
				fmt.Sprintf("http://%s/v1/schema/%s", nodeURI, className), nil)
			if err != nil {
				return false
			}
			req.Header.Set("consistency", "false")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return false
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		}, 60*time.Second, 100*time.Millisecond,
			"class %s must be locally visible on node %s before consistency=ALL writes", className, node.Name())
	}
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

// httpGetJSON GETs url and JSON-decodes into out. Returns false on any
// step's error so it composes cleanly inside require.Eventually polls.
func httpGetJSON(url string, out any) bool {
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	return json.Unmarshal(body, out) == nil
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
		var tasks models.DistributedTasks
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/tasks", restURI), &tasks) {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			if task.Status == "FAILED" {
				t.Fatalf("reindex task failed before reaching coordination phase: %s", task.Error)
			}
			// New two-phase barrier (per weaviate/0-weaviate-issues#225 design):
			// PREPARING is the per-node PREP coordination phase; SWAPPING is
			// the post-barrier per-node swap phase. FINISHED here means
			// either window was so short we missed it — the rolling restart
			// will already be too late. Return the observed status so the
			// test caller can re-tune dataset size / poll cadence rather
			// than silently passing on a stale repro.
			if task.Status == "PREPARING" || task.Status == "SWAPPING" || task.Status == "FINISHED" {
				observed = task.Status
				return true
			}
		}
		return false
	}, 240*time.Second, 50*time.Millisecond,
		"reindex task %s should reach FINALIZING (or FINISHED) within 240s", taskID)
	return observed
}

// Fails the test if the migration ends before reaching STARTED + at
// least one IN_PROGRESS unit (weaviate/0-weaviate-issues#239
// anti-vacuous-pass). Status IN_PROGRESS — not a numeric Progress
// floor — is the signal: the DTM ThrottledRecorder (3 s window) means
// fast units may only emit one progress=0 update before COMPLETED, so
// asserting on a non-zero floor flakes on fast CI runners.
func awaitReindexMidFlight(t *testing.T, restURI, taskID string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		var tasks models.DistributedTasks
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/tasks", restURI), &tasks) {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID != taskID {
				continue
			}
			if task.Status == "FAILED" {
				t.Fatalf("reindex task %s failed before mid-flight check: %s", taskID, task.Error)
			}
			if task.Status == "FINISHED" || task.Status == "PREPARING" || task.Status == "SWAPPING" {
				t.Fatalf("reindex task %s reached %s before mid-flight check — "+
					"dataset too small for the iteration window. Bump totalObjects.",
					taskID, task.Status)
			}
			if task.Status != "STARTED" {
				return false
			}
			for _, u := range task.Units {
				if u.Status == "IN_PROGRESS" {
					return true
				}
			}
			return false
		}
		return false
	}, timeout, 50*time.Millisecond,
		"reindex task %s should have at least one IN_PROGRESS unit within %s",
		taskID, timeout)
}

func raftLeaderIndex(t *testing.T, compose *docker.DockerCompose) int {
	t.Helper()
	var leaderName string
	require.Eventually(t, func() bool {
		var stats models.ClusterStatisticsResponse
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/cluster/statistics", restURIOf(compose, 1)), &stats) {
			return false
		}
		for _, s := range stats.Statistics {
			if s.LeaderID == nil {
				continue
			}
			if name, ok := s.LeaderID.(string); ok && name != "" {
				leaderName = name
				return true
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "/v1/cluster/statistics should report a leader")
	for idx, name := range []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2} {
		if name == leaderName {
			return idx
		}
	}
	t.Fatalf("leader name %q does not match any of weaviate-{0,1,2}", leaderName)
	return -1
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

// getClassFromNode is LEADER-PROXIED (default GET consistency): fine for
// cluster-level assertions, never a per-node visibility gate — use
// reindexhelpers.AwaitTokenizationVisible for gating.
func getClassFromNode(t *testing.T, restURI, className string) *models.Class {
	t.Helper()

	class, ok := reindexhelpers.FetchClass(restURI, className, false)
	require.True(t, ok, "get class %s via %s failed", className, restURI)
	return class
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

// tryGetPropertyTokenization is LEADER-PROXIED (default GET consistency):
// fine for cluster-level assertions, never a per-node visibility gate — use
// reindexhelpers.AwaitTokenizationVisible for gating. "" = failed/not found.
func tryGetPropertyTokenization(restURI, className, propName string) string {
	class, ok := reindexhelpers.FetchClass(restURI, className, false)
	if !ok {
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
		time.Sleep(50 * time.Millisecond)
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

// cycleNodeFast restarts a node via `docker restart` and waits for
// /v1/.well-known/ready. Use compose.StopAt + StartAt instead when the
// test needs cluster membership to converge on "node X unhealthy" first
// (e.g. consistency_level=ALL writes mid-restart). See weaviate/0-weaviate-issues#254.
func cycleNodeFast(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeIdx int) {
	t.Helper()
	require.NoErrorf(t, compose.RestartAt(ctx, nodeIdx, nil),
		"cycleNodeFast: restart node at index %d", nodeIdx)
}

// cycleNodeFastKill is cycleNodeFast with SIGKILL (`docker restart -t 0`).
// For crash-path tests that need to bypass the on-shutdown bucket flush.
func cycleNodeFastKill(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodeIdx int) {
	t.Helper()
	zero := 0 * time.Second
	require.NoErrorf(t, compose.RestartAt(ctx, nodeIdx, &zero),
		"cycleNodeFastKill: restart (SIGKILL) node at index %d", nodeIdx)
}

// rollingRestartCluster stops + restarts each node ONE AT A TIME,
// waiting for the node to be ready (and for RAFT to accept writes
// again) before moving on. Mimics a Kubernetes StatefulSet rolling
// update — the failure mode that hid https://github.com/weaviate/weaviate/issues/10675 in
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
		cycleNodeFast(ctx, t, compose, i-1)

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
		}, 60*time.Second, 50*time.Millisecond,
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
		// raft leadership lines: correlate with the FINISHED vs
		// local-schema race (see AwaitTokenizationVisible).
		"entering follower state", "entering candidate state",
		"entering leader state", "election won", "leadership",
		"raft_node_state",
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

// forensicCaptureByteBudget caps the bytes one capture may write, so a
// pathological on-disk state cannot fill the CI runner's disk. It is shared by
// every node in one capture, but each capture gets a fresh one: a test that
// asserts at several phases can spend it once per failing assert.
const forensicCaptureByteBudget = 512 << 20 // 512 MiB

// forensicFilesPerNodeCap caps how many files a single node contributes, so one
// node with a runaway directory cannot consume the whole byte budget.
const forensicFilesPerNodeCap = 4000

// forensicExecOutputCap caps how much of one in-container command's output is
// read into memory. The byte budget and the file cap only apply once the file
// list exists, so without this a runaway directory is read whole first.
const forensicExecOutputCap = 1 << 20 // 1 MiB

// forensicCaptureWindow bounds how long a capture may take. It does not reach
// into container.Exec; see execCollect for what bounds that.
const forensicCaptureWindow = 90 * time.Second

// captureManifestName is the record of what the capture did, written inside the
// artifact. The artifact zip and the CI job log are separate downloads, so
// anything that explains a missing or short file has to be in both.
const captureManifestName = "CAPTURE-MANIFEST.txt"

// Suffixes for a captured file that is not a faithful copy of the container's.
// They also take the file out of its original extension, so a tool pointed at
// the artifact cannot parse a cut segment as if it were whole.
const (
	truncatedSuffix = ".TRUNCATED"
	partialSuffix   = ".PARTIAL"
)

// captureAllowance is the decision for one candidate file: whether the capture
// should stop, and if not, the most bytes that file may consume.
type captureAllowance struct {
	stop   bool
	reason string
	limit  int64
}

// allowNextFile applies both bounds before a file is copied. copiedTotal is the
// running cross-node byte total and filesThisNode the count already taken from
// the current node, so the byte budget is global while the file cap is per node.
// A non-stopping allowance always carries a limit above zero, which is what lets
// the caller pass it straight to io.CopyN.
func allowNextFile(copiedTotal int64, filesThisNode int) captureAllowance {
	if filesThisNode >= forensicFilesPerNodeCap {
		return captureAllowance{
			stop:   true,
			reason: fmt.Sprintf("per-node file cap (%d) reached", forensicFilesPerNodeCap),
		}
	}
	remaining := int64(forensicCaptureByteBudget) - copiedTotal
	if remaining <= 0 {
		return captureAllowance{
			stop:   true,
			reason: fmt.Sprintf("byte budget (%d) exhausted", forensicCaptureByteBudget),
		}
	}
	return captureAllowance{limit: remaining}
}

// captureLog records what a capture did, to the test log and to a file inside
// the artifact. Both matter: whoever downloads the zip may not have the job log
// open, and a line that explains a missing or short file is useless in the one
// place the reader is not looking.
type captureLog struct {
	t      *testing.T
	lines  []string
	prefix string
}

func (c *captureLog) recordf(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	c.lines = append(c.lines, line)
	c.t.Logf("%s: %s", c.prefix, line)
}

// captureManifestTerminator ends a complete manifest. os.WriteFile leaves the
// bytes it managed to write behind when it fails, so without a terminator a
// manifest cut short by a full disk reads exactly like a complete one.
const captureManifestTerminator = "=== END OF CAPTURE MANIFEST, %d records ==="

// writeTo drops the record into the artifact root under captureManifestName.
// A capture whose manifest cannot be written is still a valid artifact, so this
// reports and returns rather than failing the capture.
func (c *captureLog) writeTo(root string) {
	dest := filepath.Join(root, captureManifestName)
	body := strings.Join(c.lines, "\n") + "\n" +
		fmt.Sprintf(captureManifestTerminator, len(c.lines)) + "\n"
	if err := os.WriteFile(dest, []byte(body), 0o644); err != nil {
		// Recording this through recordf would write it into the file that just
		// failed to be written.
		c.t.Logf("%s: writing %s failed, so the artifact carries no manifest or a cut one: %v",
			c.prefix, dest, err)
	}
}

// forensicCapture is one capture in progress: where files land, how much of the
// shared byte budget is spent, and the record being built. dataDir is the
// container path the class dirs live under, which is what PERSISTENCE_DATA_PATH
// sets on the node.
type forensicCapture struct {
	log         *captureLog
	root        string
	dataDir     string
	classDir    string
	bucketMatch string
	copied      int64
}

// captureRangeableDataDirsOnFailure is a best-effort dump of the rangeable
// bucket's on-disk state (weaviate/0-weaviate-issues#335), for offline
// post-mortem on a range-count failure. Capture errors are logged, never fatal.
func captureRangeableDataDirsOnFailure(t *testing.T, compose *docker.DockerCompose, className, propName, phase string) {
	t.Helper()
	// The test's own ctx may already be cancelling, so capture gets its own.
	ctx, cancel := context.WithTimeout(context.Background(), forensicCaptureWindow)
	defer cancel()

	log := &captureLog{t: t, prefix: fmt.Sprintf("range-count forensic capture [%s]", phase)}
	root, err := forensicArtifactRoot(t, className, phase)
	if err != nil {
		// Without a root every copy below would fail, one log line per file.
		t.Logf("%s: no artifact dir (%v); nothing captured", log.prefix, err)
		return
	}

	c := &forensicCapture{
		log:      log,
		root:     root,
		dataDir:  "/data",
		classDir: strings.ToLower(className),
		// Prefix shared by the canonical bucket dir and all its sidecar generations.
		bucketMatch: fmt.Sprintf("property_%s_rangeable", propName),
	}
	defer c.log.writeTo(root)
	c.collectAll(ctx, weaviateContainers(compose))
}

// weaviateContainers lists the compose's weaviate nodes in node order, walking
// until the compose runs out of nodes rather than assuming three. The lookup is
// an in-memory scan, so listing them all up front costs nothing.
func weaviateContainers(compose *docker.DockerCompose) []testcontainers.Container {
	var containers []testcontainers.Container
	for nodeIdx := 1; ; nodeIdx++ {
		node := compose.GetWeaviateNode(nodeIdx)
		if node == nil {
			return containers
		}
		containers = append(containers, node.Container())
	}
}

// collectAll captures every node in turn, opening the record with what the
// artifact is and what it is not.
func (c *forensicCapture) collectAll(ctx context.Context, containers []testcontainers.Container) {
	c.log.recordf("artifact root = %s", c.root)
	c.recordHowToRead()
	if len(containers) == 0 {
		c.log.recordf("no weaviate containers found, nothing captured")
	}

	visited := 0
	for i, container := range containers {
		if err := ctx.Err(); err != nil {
			c.log.recordf("capture window closed (%v) after %d of %d nodes; the rest were not visited",
				err, visited, len(containers))
			break
		}
		visited++
		c.collectNode(ctx, container, i+1)
	}
	c.log.recordf("copied ~%d bytes from %d of %d nodes into %s",
		c.copied, visited, len(containers), c.root)
}

// recordHowToRead states the one thing a file name cannot: the copy was taken
// from a live cluster. Every other way a file here can be wrong is either
// marked on the file or named in this record, so without this line an
// investigator reads mutually inconsistent segments as product corruption.
func (c *forensicCapture) recordHowToRead() {
	c.log.recordf("how to read this artifact:\n"+
		"  the nodes were running and serving while these files were copied, so this is not a point-in-time snapshot\n"+
		"  each file is copied on its own: two segments here can be from different instants, and a .wal can have a torn tail\n"+
		"  files in that state keep their original names, because from the copier's side nothing went wrong\n"+
		"  the assertion had already polled for %s before this ran, so what is on disk here can be that much newer than the first bad count",
		rangeCountConvergenceWindow)
}

// collectNode records one node's rangeable bucket manifest and copies the
// matching files under <root>/node<nodeIdx>/.
func (c *forensicCapture) collectNode(ctx context.Context, container testcontainers.Container, nodeIdx int) {
	// A command that failed and a command whose output was cut are separate
	// facts. Reported as one choice, a manifest that hit both says only that
	// the command failed and never mentions the cap.
	manifest, cut, err := execCollect(ctx, container, []string{"sh", "-c", c.manifestScript()})
	if err != nil {
		c.log.recordf("node %d manifest command failed (%v); what it printed before that is below", nodeIdx, err)
	}
	if cut {
		c.log.recordf("node %d manifest output was cut at %d bytes%s",
			nodeIdx, forensicExecOutputCap, nothingSurvivedNote(manifest))
	}
	c.log.recordf("node %d rangeable bucket manifest:\n%s", nodeIdx, strings.TrimSpace(manifest))

	fileList, cut, err := execCollect(ctx, container, []string{"sh", "-c", c.fileListScript()})
	if err != nil {
		// Whatever paths did arrive are still worth copying, so this reports
		// rather than returns.
		c.log.recordf("node %d file list command failed (%v); copying what it did list", nodeIdx, err)
	}
	if cut {
		c.log.recordf("node %d file list was cut at %d bytes; the files past the cut were never offered to the copy%s",
			nodeIdx, forensicExecOutputCap, nothingSurvivedNote(fileList))
	}
	c.copyFiles(ctx, container, nodeIdx, fileList)
}

// nothingSurvivedNote explains output that the cap left empty. capExecOutput
// keeps whole lines only, so a cut that found no line break keeps nothing, and
// the bare word "cut" would otherwise read as if something was shown.
func nothingSurvivedNote(out string) string {
	if strings.TrimSpace(out) != "" {
		return ""
	}
	return "; no whole line survived the cut, so none of it is shown"
}

// manifestScript lists the rangeable bucket dirs on one node. Every branch
// prints something: an empty manifest reads the same whether the data is
// genuinely absent, the path is wrong, or the collector broke. The find keeps
// its stderr and its exit status for the same reason, so a search that could
// not read a directory is not reported as a search that found nothing.
func (c *forensicCapture) manifestScript() string {
	return fmt.Sprintf(
		`d="%[1]s/%[2]s"; `+
			`if [ ! -d "$d" ]; then echo "(no $d dir; %[1]s listing:)"; ls -la "%[1]s"; exit 0; fi; `+
			`found=0; `+
			`for lsm in "$d"/*/lsm; do [ -d "$lsm" ] || continue; found=1; echo "### $lsm"; `+
			`m=$(find "$lsm" -path "*%[3]s*"); rc=$?; `+
			`[ "$rc" = 0 ] || echo "(find under $lsm exited $rc; what it listed may be incomplete)"; `+
			`if [ -n "$m" ]; then echo "$m" | while IFS= read -r p; do ls -ld "$p"; done; `+
			`elif [ "$rc" = 0 ]; then echo "(no %[3]s* bucket dirs found; full lsm listing:)"; ls -la "$lsm"; `+
			`else ls -la "$lsm"; fi; done; `+
			`[ "$found" = 1 ] || { echo "(no */lsm dirs under $d; listing:)"; ls -la "$d"; }`,
		c.dataDir, c.classDir, c.bucketMatch)
}

func (c *forensicCapture) fileListScript() string {
	return fmt.Sprintf(`find "%s/%s" -path "*%s*" -type f 2>/dev/null`, c.dataDir, c.classDir, c.bucketMatch)
}

// copyFiles copies each listed container path under <root>/node<nodeIdx>/,
// stopping at the first bound that trips.
func (c *forensicCapture) copyFiles(
	ctx context.Context, container testcontainers.Container, nodeIdx int, newlineSeparatedPaths string,
) {
	paths := splitPaths(newlineSeparatedPaths)
	files, whole, bytesBefore := 0, 0, c.copied
	// Without this, a node that listed nothing and a node that copied everything
	// cleanly leave the same trace: none.
	defer func() {
		c.log.recordf("node %d: listed %d files, attempted %d, copied %d whole, %d bytes",
			nodeIdx, len(paths), files, whole, c.copied-bytesBefore)
	}()
	for i, p := range paths {
		// Once the capture window closes every remaining copy fails instantly,
		// which would otherwise emit one log line per remaining path.
		if err := ctx.Err(); err != nil {
			c.log.recordf("node %d capture window closed (%v); %d of %d files not attempted",
				nodeIdx, err, len(paths)-i, len(paths))
			return
		}
		allowance := allowNextFile(c.copied, files)
		if allowance.stop {
			c.log.recordf("node %d %s; %d of %d files not attempted",
				nodeIdx, allowance.reason, len(paths)-i, len(paths))
			return
		}
		files++
		res := c.copyOneFile(ctx, container, nodeIdx, p, allowance.limit)
		// A copy that broke mid-stream still left its bytes on disk, so they
		// count against the budget the same as a copy that finished.
		c.copied += res.written
		if res.err == nil && !res.truncated {
			whole++
		}
		c.recordCopy(nodeIdx, p, c.markIncomplete(nodeIdx, res))
	}
}

// markIncomplete renames a file that is not a faithful copy, and reports the
// result with rel pointing at whatever name the file ended up under.
func (c *forensicCapture) markIncomplete(nodeIdx int, res copyResult) copyResult {
	suffix := incompleteSuffix(res.truncated, res.err)
	if suffix == "" || res.rel == "" {
		return res
	}
	if err := os.Rename(filepath.Join(c.root, res.rel), filepath.Join(c.root, res.rel+suffix)); err != nil {
		c.log.recordf("node %d could not mark %s as %s; it stays in the artifact under its original name: %v",
			nodeIdx, res.rel, suffix, err)
		return res
	}
	res.rel += suffix
	return res
}

func (c *forensicCapture) recordCopy(nodeIdx int, containerPath string, res copyResult) {
	switch {
	case res.err != nil && res.rel == "":
		c.log.recordf("node %d copy %s failed, no file written: %v", nodeIdx, containerPath, res.err)
	case res.err != nil:
		c.log.recordf("node %d copy %s broke after %d bytes; kept as %s: %v",
			nodeIdx, containerPath, res.written, res.rel, res.err)
	case res.truncated:
		c.log.recordf("node %d truncated %s at %d bytes to stay inside the budget; kept as %s",
			nodeIdx, containerPath, res.written, res.rel)
	}
}

// incompleteSuffix names what is wrong with a captured file, or "" when the
// file is a faithful copy.
func incompleteSuffix(truncated bool, err error) string {
	switch {
	case err != nil:
		return partialSuffix
	case truncated:
		return truncatedSuffix
	default:
		return ""
	}
}

// splitPaths turns a find(1) listing into the paths it names.
func splitPaths(newlineSeparated string) []string {
	var paths []string
	for _, p := range strings.Split(newlineSeparated, "\n") {
		if p = strings.TrimSpace(p); p != "" {
			paths = append(paths, p)
		}
	}
	return paths
}

// forensicArtifactRoot creates a unique host dir for one capture: under
// REINDEX_FORENSICS_DIR in CI, an OS temp dir locally. Unique per
// test+phase+timestamp so repeated captures in the same run don't collide.
func forensicArtifactRoot(t *testing.T, className, phase string) (string, error) {
	t.Helper()
	base := os.Getenv("REINDEX_FORENSICS_DIR")
	if base == "" {
		base = filepath.Join(os.TempDir(), "reindex-forensics")
	}
	name := strings.ReplaceAll(t.Name(), "/", "_")
	root := filepath.Join(base, fmt.Sprintf("%s_%s_%s_%d", name, className, phase, time.Now().UnixNano()))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	return root, nil
}

// execCollect runs cmd in the container and returns its combined stdout+stderr,
// whether that output was cut at forensicExecOutputCap, and what went wrong.
//
// ctx does not bound container.Exec. With tcexec.Multiplexed it blocks draining
// a hijacked connection that nothing closes when ctx expires, so a wedged
// container — the state this code exists for — would block until the go test
// timeout fires and buries the real failure. Handing the call its own goroutine
// is what actually bounds the caller's wait. A wedged exec parks that goroutine
// until the test binary exits; it only ever writes to a buffered channel, never
// to t, so it cannot log after the test has finished.
func execCollect(ctx context.Context, container testcontainers.Container, cmd []string) (string, bool, error) {
	type collected struct {
		out string
		err error
	}
	done := make(chan collected, 1)
	go func() {
		out, err := execCollectBlocking(ctx, container, cmd)
		done <- collected{out: out, err: err}
	}()

	select {
	case r := <-done:
		out, cut := capExecOutput(r.out)
		return out, cut, r.err
	case <-ctx.Done():
		return "", false, fmt.Errorf("command did not return inside the capture window: %w", ctx.Err())
	}
}

// execCollectBlocking is the part that can hang. tcexec.Multiplexed strips
// Docker's stream-framing headers so the output is plain text.
func execCollectBlocking(ctx context.Context, container testcontainers.Container, cmd []string) (string, error) {
	code, reader, err := container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	buf := new(strings.Builder)
	if reader != nil {
		// One byte past the cap is enough for capExecOutput to see it was cut.
		if _, err := io.Copy(buf, io.LimitReader(reader, forensicExecOutputCap+1)); err != nil {
			return buf.String(), fmt.Errorf("reading output: %w", err)
		}
	}
	if code != 0 {
		return buf.String(), fmt.Errorf("exit code %d", code)
	}
	return buf.String(), nil
}

// capExecOutput cuts s to forensicExecOutputCap at a line boundary, so a cut
// list of paths never ends in half a path, and reports whether it cut anything.
func capExecOutput(s string) (string, bool) {
	if len(s) <= forensicExecOutputCap {
		return s, false
	}
	s = s[:forensicExecOutputCap]
	// No newline at all means no whole line survived the cut.
	i := strings.LastIndexByte(s, '\n')
	if i < 0 {
		return "", true
	}
	return s[:i+1], true
}

// copyResult is what one file copy left behind. rel is the file's path inside
// the artifact root, empty when no file was created.
type copyResult struct {
	rel       string
	written   int64
	truncated bool
	err       error
}

// copyOneFile mirrors containerPath's dataDir-relative path under
// <root>/node<nodeIdx>/, writing at most limit bytes.
func (c *forensicCapture) copyOneFile(
	ctx context.Context, container testcontainers.Container, nodeIdx int,
	containerPath string, limit int64,
) copyResult {
	rc, err := container.CopyFileFromContainer(ctx, containerPath)
	if err != nil {
		return copyResult{err: err}
	}
	defer rc.Close()
	rel := filepath.Join(fmt.Sprintf("node%d", nodeIdx),
		filepath.FromSlash(strings.TrimPrefix(containerPath, c.dataDir+"/")))
	dest := filepath.Join(c.root, rel)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return copyResult{err: err}
	}
	f, err := os.Create(dest)
	if err != nil {
		return copyResult{err: err}
	}
	written, truncated, err := copyBounded(f, rc, limit)
	// A close-time write error leaves behind another silently short file.
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return copyResult{rel: rel, written: written, truncated: truncated, err: err}
}

// copyBounded writes at most limit bytes from src to dst, reporting how much it
// wrote and whether src still had data left at that point. Bounded rather than
// io.Copy because the byte budget is only checked between files, so one
// pathological segment would otherwise carry the capture past it.
func copyBounded(dst io.Writer, src io.Reader, limit int64) (written int64, truncated bool, err error) {
	written, err = io.CopyN(dst, src, limit)
	if errors.Is(err, io.EOF) {
		return written, false, nil
	}
	if err != nil {
		return written, false, err
	}
	// CopyN stops at limit without reading beyond it, so a file of exactly
	// limit bytes looks the same as a larger one. Probe for one more byte
	// rather than reporting a truncation that did not happen.
	var probe [1]byte
	n, probeErr := src.Read(probe[:])
	if n > 0 {
		return written, true, nil
	}
	if probeErr != nil && !errors.Is(probeErr, io.EOF) {
		return written, false, probeErr
	}
	return written, false, nil
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
// Observed on PR https://github.com/weaviate/weaviate/pull/11323 CI run b19dd49366 / job 76404184658:
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
	// Sample at 50ms via an explicit ticker. The two-consecutive-stable
	// requirement (prevAll) is a sampling property of this baseline gate,
	// not a simple wait-for-condition, so it is preserved verbatim rather
	// than collapsed into require.Eventually (which would return on the
	// first satisfying read and drop the stability check).
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
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
		<-ticker.C
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
		defer samplesMu.Unlock()
		samples = append(samples, s)
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for nodeIdx := 0; nodeIdx < 3; nodeIdx++ {
		wg.Add(1)
		nodeURI := compose.GetWeaviateNode(nodeIdx + 1).URI()
		idx := nodeIdx + 1
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(probeInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
				}
				start := time.Now()
				count, err := probe(nodeURI, className)
				record(probeSample{t: start, nodeID: idx, count: count, err: err})
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
			// min/max by timestamp, not arrival order: per-node probe goroutines interleave.
			if c.FirstPartial.IsZero() || s.t.Before(c.FirstPartial) {
				c.FirstPartial = s.t
			}
			if s.t.After(c.LastPartial) {
				c.LastPartial = s.t
			}
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

// awaitRangeCountSettledNoFallback polls until the range count converges,
// then asserts the disk-fallback WARN never appeared in the container logs.
func awaitRangeCountSettledNoFallback(
	ctx context.Context, t *testing.T,
	container interface {
		Logs(context.Context) (io.ReadCloser, error)
	},
	restURI, className string, lo, hi, expected int,
	countFailMsgFmt string, countFailArg interface{},
	warnFailMsg string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		c, e := rangeCount(restURI, className, "score", lo, hi)
		return e == nil && c == expected
	}, 60*time.Second, 200*time.Millisecond, countFailMsgFmt, countFailArg)

	for i := 0; i < 5; i++ {
		_, _ = rangeCount(restURI, className, "score", lo, hi)
	}
	time.Sleep(2 * time.Second) // let the container flush stdout

	assert.Zero(t, countInLogs(ctx, t, container, fallbackWARNSubstr), warnFailMsg)
}

// rangeCountProbeCounters aggregates the results of a background polling
// loop started by startRangeCountPolling.
type rangeCountProbeCounters struct {
	wrongCounts atomic.Int64
	queryRuns   atomic.Int64
	queryErrors atomic.Int64
}

// startRangeCountPolling launches one goroutine per node, firing a
// range-count query every 50ms until stopCh closes. onWrong/onError run
// concurrently and must be goroutine-safe.
func startRangeCountPolling(
	compose *docker.DockerCompose, className string, lo, hi, expected, nodeCount int,
	onWrong func(nodeIdx, got int), onError func(nodeIdx int, err error),
) (counters *rangeCountProbeCounters, stopCh chan struct{}, wg *sync.WaitGroup) {
	counters = &rangeCountProbeCounters{}
	stopCh = make(chan struct{})
	wg = &sync.WaitGroup{}
	for nodeIdx := 1; nodeIdx <= nodeCount; nodeIdx++ {
		wg.Add(1)
		uri := restURIOf(compose, nodeIdx)
		idx := nodeIdx
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
				}
				count, err := rangeCount(uri, className, "score", lo, hi)
				counters.queryRuns.Add(1)
				if err != nil {
					counters.queryErrors.Add(1)
					if onError != nil {
						onError(idx, err)
					}
					continue
				}
				if count != expected {
					counters.wrongCounts.Add(1)
					if onWrong != nil {
						onWrong(idx, count)
					}
				}
			}
		}()
	}
	return counters, stopCh, wg
}

// Pins a false positive: out-of-order partial timestamps must not regress LastPartial.
func TestClassifyProbeSamples_OutOfOrderPartials(t *testing.T) {
	start := time.Now()
	samples := []probeSample{
		{t: start.Add(2092 * time.Millisecond), nodeID: 1, count: 970}, // partial, arrives first
		{t: start.Add(1990 * time.Millisecond), nodeID: 3, count: 970}, // partial, earlier timestamp
		{t: start.Add(3 * time.Second), nodeID: 2, count: 0},           // post
	}
	c := classifyProbeSamples(t, samples, 1500, 0, start)

	require.Equal(t, 2, c.Partial)
	require.Equal(t, start.Add(1990*time.Millisecond), c.FirstPartial, "FirstPartial must be the earliest timestamp")
	require.Equal(t, start.Add(2092*time.Millisecond), c.LastPartial, "LastPartial must be the latest timestamp")
	require.False(t, c.LastPartial.Before(c.FirstPartial), "window must never be negative")

	// with a correct anchor no partial can post-date it
	anchor := c.LastPartial.Add(100 * time.Millisecond)
	require.Zero(t, countLatePartials(t, samples, 1500, 0, anchor, start))
}
