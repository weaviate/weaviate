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
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_CancelClearsAcrossReplicas pins R5/R6 cluster-wide
// for the c133b1fd0d `defer wg.Wait()` fix. Single-node tests can't
// reproduce the processUnits limiter.Acquire-vs-ctx-cancel race —
// it needs ≥3 nodes, ≥2 shards/node, and cancel inside ~1s of
// STARTED so Acquire is still blocked. Setup: 3×3×RF=3, word→field
// change-tokenization, cancel <1s. Asserts: tracker dirs drain
// cluster-wide (R5a), backup succeeds (R5b: canCommit clears), and
// DELETE class removes `<root>/<class-lower>/` on every node (R6).
func TestMultiNode_CancelClearsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	// S3/MinIO required: filesystem backend is per-node and the API
	// refuses it for ≥2 nodes. REINDEX_CONCURRENCY=1 is the
	// determinism knob — at default 2 the race window narrows and
	// the test flakes on fast hardware.
	const backupBucket = "cancel-clears-bucket"
	compose, err := docker.New().
		With3NodeCluster().
		WithBackendS3(backupBucket, "us-east-1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false").
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		WithWeaviateEnv("REINDEX_CONCURRENCY", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className = "CancelClearsAcrossReplicas"
		propName  = "body"
		// 200k keeps the change-tokenization iteration alive for
		// >3 s even on a fast laptop with 3 shards × concurrency=2;
		// at 50k the migration finished before our cancel HTTP
		// hit the network, and the canonical PUT returned 404.
		dataset       = 200_000
		cancelTimeout = 30 * time.Second
	)
	classDirLower := strings.ToLower(className)

	uri := restURIOf(compose, 1)
	trueVal := true
	createCollection(t, uri, className, 3, 3, []*models.Property{
		{
			Name:            propName,
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})

	paths := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	batchImportMultiProp(t, uri, className, dataset, func(i int) map[string]interface{} {
		return map[string]interface{}{propName: paths[i%len(paths)]}
	})

	// Submit word→field. Tokenization-changing migration → both
	// searchable + filterable trackers per (shard, replica).
	taskID := reindexhelpers.SubmitIndexUpdate(t, uri, className, propName,
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task: %s", taskID)

	// Cancel within ~1 s of STARTED. We poll for the status flip and
	// cancel immediately — network + dispatch latency alone gives the
	// per-unit goroutines enough time to enter limiter.Acquire-blocked
	// or first-pass iteration. Adding a sleep here makes the race
	// stop reproducing on fast hardware (migration finishes first).
	awaitTaskStartedFast(t, uri, taskID, 30*time.Second)

	allShards := collectShardNamesForClass(t, uri, className)
	require.GreaterOrEqual(t, len(allShards), 3,
		"sanity: expected ≥3 shards on a 3-shard class; got %v", allShards)

	// QA Claude's 20:01Z review on PR #11327: pre-cancel positive
	// observation. The post-cancel assertions all trivially PASS if
	// the migration finishes before our cancel HTTP arrives — no
	// `.migrations/` dirs, backup succeeds, DELETE succeeds, and the
	// `defer wg.Wait()` fix isn't even exercised. Pinning ≥1 mid-flight
	// tracker on disk before the cancel turns "test passes for the
	// right reason" into an enforceable invariant. A future runner
	// that closes the race window from the data side (e.g. compiler
	// optimizations on a much faster CPU) will fail loudly here
	// instead of silently shipping a no-op test.
	preCancelSurvivors := scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName)
	require.NotEmptyf(t, preCancelSurvivors,
		"sanity: expected at least 1 .migrations/*_%s_* dir on disk mid-flight before cancel; got 0 — migration likely finished before cancel reached the cluster, so the post-cancel R5/R6 assertions would PASS trivially without exercising the fix",
		propName)

	cancelReindexProperty(t, uri, className, propName, "searchable")
	t.Logf("issued cancel for searchable migration on %s/%s (pre-cancel survivors: %d)",
		className, propName, len(preCancelSurvivors))

	// R5a: every replica on every node drains its `.migrations/*_body_*`
	// dirs within `cancelTimeout`.

	deadline := time.Now().Add(cancelTimeout)
	for {
		survivors := scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName)
		if len(survivors) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("R5a — cancel-cleanup left .migrations/*_%s_* dirs on %d replica slots after %s:\n  %s",
				propName, len(survivors), cancelTimeout, strings.Join(survivors, "\n  "))
		}
		time.Sleep(500 * time.Millisecond)
	}

	// R5b: filesystem backup succeeds end-to-end. canCommit refuses
	// the create while any in-flight tracker is present, so a green
	// backup here is the load-bearing assertion that the inflight
	// registration was cleared on every node.
	backupID := "cancel-clears-backup"
	require.NoError(t, createS3Backup(t, uri, className, backupID, backupBucket), "R5b — backup must succeed after cancel-cleanup drains")

	// R6: DELETE class succeeds, and the on-disk class dir is gone on
	// every node within `cancelTimeout`. With the
	// MutationGuard treating CANCELLED tasks as not-in-flight, this
	// returns 200 immediately.
	require.NoError(t, deleteClassExpectOK(t, uri, className), "R6 — DELETE class must succeed post-cancel")

	deleteDeadline := time.Now().Add(cancelTimeout)
	for {
		survivors := scanClassDirAllNodes(ctx, t, compose, classDirLower)
		if len(survivors) == 0 {
			break
		}
		if time.Now().After(deleteDeadline) {
			t.Fatalf("R6 — DELETE class left /data/%s on %d node(s) after %s: %v",
				classDirLower, len(survivors), cancelTimeout, survivors)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// awaitTaskStartedFast polls /v1/tasks until the named task reaches
// STARTED. Tight timeout because the cancel race needs the cancel to
// land within ~1 s of STARTED — if we wait longer the iteration may
// flush enough through its limiter window that the race no longer
// triggers.
func awaitTaskStartedFast(t *testing.T, restURI, taskID string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		var tasks models.DistributedTasks
		if err := json.Unmarshal(body, &tasks); err != nil {
			return false
		}
		for _, task := range tasks["reindex"] {
			if task.ID == taskID && task.Status == "STARTED" {
				return true
			}
		}
		return false
	}, timeout, 100*time.Millisecond, "task %s should reach STARTED", taskID)
}

// cancelReindexProperty sends `{<indexType>: {cancel: true}}` to the
// canonical PUT /v1/schema/<class>/indexes/<prop> endpoint and
// requires a 202.
func cancelReindexProperty(t *testing.T, restURI, className, propName, indexType string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName)
	body := fmt.Sprintf(`{%q:{"cancel":true}}`, indexType)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusAccepted, resp.StatusCode,
		"cancel returned %d: %s", resp.StatusCode, string(respBody))
}

// collectShardNamesForClass returns every distinct shard name owned by
// the given class across all nodes, via /v1/nodes?output=verbose. With
// RF=3 every shard appears on every node, so the set is just the
// shard partition (3 entries for a 3-shard class).
func collectShardNamesForClass(t *testing.T, restURI, className string) []string {
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

	seen := map[string]bool{}
	for _, node := range nodesResp.Nodes {
		for _, sh := range node.Shards {
			if sh.Class == className {
				seen[sh.Name] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	return out
}

// scanBodyMigrationsAllReplicas returns "<nodeIdx>:<shard>" slot
// identifiers for every (node, shard) replica that still has at least
// one `.migrations/*_<propName>_*` directory on disk. Empty slice ==
// every replica is clean.
func scanBodyMigrationsAllReplicas(
	ctx context.Context, t *testing.T, compose *docker.DockerCompose,
	classDirLower string, shards []string, propName string,
) []string {
	t.Helper()
	var survivors []string
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		container := compose.GetWeaviateNode(nodeIdx).Container()
		for _, shard := range shards {
			migsPath := fmt.Sprintf("/data/%s/%s/lsm/.migrations", classDirLower, shard)
			cmd := []string{
				"sh", "-c",
				fmt.Sprintf(`ls -1 %s 2>/dev/null | grep -E '_%s($|_)' | head -10`,
					migsPath, propName),
			}
			code, reader, err := container.Exec(ctx, cmd)
			require.NoError(t, err, "exec on node %d for shard %s", nodeIdx, shard)
			out := new(strings.Builder)
			if reader != nil {
				_, _ = io.Copy(out, reader)
			}
			matches := strings.TrimSpace(out.String())
			if code == 0 && matches != "" {
				survivors = append(survivors, fmt.Sprintf("node%d:%s [%s]", nodeIdx, shard, matches))
			}
		}
	}
	return survivors
}

// scanClassDirAllNodes returns node indexes (1-based) where
// `/data/<classDirLower>` still exists. Empty slice == every node
// cleaned. Used for the R6 invariant after DELETE class.
func scanClassDirAllNodes(
	ctx context.Context, t *testing.T, compose *docker.DockerCompose, classDirLower string,
) []int {
	t.Helper()
	var survivors []int
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		container := compose.GetWeaviateNode(nodeIdx).Container()
		code, _, err := container.Exec(ctx, []string{"test", "-d", fmt.Sprintf("/data/%s", classDirLower)})
		require.NoError(t, err, "exec on node %d", nodeIdx)
		if code == 0 {
			survivors = append(survivors, nodeIdx)
		}
	}
	return survivors
}

// createS3Backup posts to /v1/backups/s3 with the
// supplied id + className and waits up to 60 s for a SUCCESS terminal
// status. canCommit returns 422 with the structured "backup blocked"
// body if any local shard has an in-flight tracker — that's the R5b
// invariant we want to assert flips to clean after cancel-cleanup.
func createS3Backup(t *testing.T, restURI, className, backupID, bucket string) error {
	t.Helper()
	body := map[string]interface{}{
		"id":      backupID,
		"include": []string{className},
		"config":  map[string]interface{}{"Bucket": bucket},
	}
	reqBody, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/backups/s3", restURI),
		"application/json",
		bytes.NewReader(reqBody),
	)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backup create returned %d: %s", resp.StatusCode, string(respBody))
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		r, err := http.Get(fmt.Sprintf("http://%s/v1/backups/s3/%s", restURI, backupID))
		if err != nil {
			return fmt.Errorf("backup status: %w", err)
		}
		statusBody, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		var status struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		_ = json.Unmarshal(statusBody, &status)
		switch status.Status {
		case "SUCCESS":
			return nil
		case "FAILED":
			return fmt.Errorf("backup FAILED: %s", status.Error)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("backup did not reach SUCCESS/FAILED in 60s; last status: %s", status.Status)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// deleteClassExpectOK sends DELETE /v1/schema/<class> and requires a
// 200. Wraps the inline pattern so the test stays readable.
func deleteClassExpectOK(t *testing.T, restURI, className string) error {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("http://%s/v1/schema/%s", restURI, className), nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE class returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
