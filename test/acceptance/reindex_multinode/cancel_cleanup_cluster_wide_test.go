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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

// TestMultiNode_CancelClearsAcrossReplicas asserts that cancel-cleanup
// drains in-flight reindex trackers on every replica, that a subsequent
// backup succeeds, and that DELETE class removes the class dir on every
// node. Requires ≥3 nodes and cancel within ~1s of STARTED to exercise
// the limiter.Acquire-vs-ctx-cancel path.
func TestMultiNode_CancelClearsAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	// S3/MinIO required: filesystem backend is per-node and refused by
	// the API for ≥2 nodes. REINDEX_CONCURRENCY=1 keeps the race window
	// wide enough to reproduce reliably on fast hardware.
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
		// 200k keeps the change-tokenization iteration alive long enough
		// that the cancel HTTP call lands mid-flight. Smaller values let
		// the migration finish first, and the cancel hits 404.
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

	// Tokenization-changing migration creates both searchable and
	// filterable trackers per (shard, replica).
	taskID := reindexhelpers.SubmitIndexUpdate(t, uri, className, propName,
		`{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted change-tokenization task: %s", taskID)

	// Cancel as soon as the task hits STARTED. Inserting a sleep here
	// would let the migration finish on fast hardware and stop
	// reproducing the race.
	awaitTaskStartedFast(t, uri, taskID, 30*time.Second)

	allShards := collectShardNamesForClass(t, uri, className)
	require.GreaterOrEqual(t, len(allShards), 3,
		"sanity: expected ≥3 shards on a 3-shard class; got %v", allShards)

	// Sanity: must observe at least one mid-flight tracker dir before
	// the cancel. Otherwise the migration finished early and the
	// post-cancel assertions pass trivially without exercising the fix.
	preCancelSurvivors := scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName)
	require.NotEmptyf(t, preCancelSurvivors,
		"sanity: expected at least 1 .migrations/*_%s_* dir on disk mid-flight before cancel; got 0 — migration likely finished before cancel reached the cluster, so the post-cancel R5/R6 assertions would PASS trivially without exercising the fix",
		propName)

	cancelReindexProperty(t, uri, className, propName, "searchable")
	t.Logf("issued cancel for searchable migration on %s/%s (pre-cancel survivors: %d)",
		className, propName, len(preCancelSurvivors))

	// Every replica on every node must drain its .migrations/*_body_*
	// dirs within cancelTimeout.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		survivors := scanBodyMigrationsAllReplicas(ctx, t, compose, classDirLower, allShards, propName)
		assert.Emptyf(c, survivors,
			"cancel-cleanup left .migrations/*_%s_* dirs on %d replica slots:\n  %s",
			propName, len(survivors), strings.Join(survivors, "\n  "))
	}, cancelTimeout, 50*time.Millisecond)

	// Backup must succeed. canCommit refuses while any in-flight tracker
	// is present, so a green backup here proves the inflight registration
	// was cleared on every node.
	backupID := "cancel-clears-backup"
	require.NoError(t, createS3Backup(t, uri, className, backupID, backupBucket), "backup must succeed after cancel-cleanup drains")

	// DELETE class must succeed (MutationGuard treats CANCELLED tasks as
	// not-in-flight) and the on-disk class dir must disappear on every node.
	require.NoError(t, deleteClassExpectOK(t, uri, className), "DELETE class must succeed post-cancel")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		survivors := scanClassDirAllNodes(ctx, t, compose, classDirLower)
		assert.Emptyf(c, survivors,
			"DELETE class left /data/%s on %d node(s): %v",
			classDirLower, len(survivors), survivors)
	}, cancelTimeout, 50*time.Millisecond)
}

// awaitTaskStartedFast polls /v1/tasks until the named task reaches
// STARTED. Tight tick interval because the cancel must land within ~1s
// of STARTED to reproduce the limiter race.
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
	}, timeout, 50*time.Millisecond, "task %s should reach STARTED", taskID)
}

// cancelReindexProperty sends {<indexType>: {cancel: true}} to
// PUT /v1/schema/<class>/indexes/<prop> and requires a 202.
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
// the given class across all nodes.
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

// scanBodyMigrationsAllReplicas returns "<nodeIdx>:<shard>" identifiers
// for every replica that still has a .migrations/*_<propName>_* dir on
// disk. Empty slice means every replica is clean.
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

// scanClassDirAllNodes returns the 1-based node indexes where
// /data/<classDirLower> still exists. Empty slice means every node
// is clean.
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

// createS3Backup posts to /v1/backups/s3 and waits up to 120s for a
// SUCCESS terminal status.
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

	deadline := time.Now().Add(120 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
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
			return fmt.Errorf("backup did not reach SUCCESS/FAILED in 120s; last status: %s", status.Status)
		}
		<-ticker.C
	}
}

// deleteClassExpectOK sends DELETE /v1/schema/<class> and requires 200.
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
