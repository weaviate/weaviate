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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

func TestMultiNode_RestartInsideMergedBarrier_CommitsAndServes(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const (
		className    = "MergedBarrierRestart"
		totalObjects = 50_000
	)
	paths := []string{"alpha-path", "beta-path", "gamma-path", "delta-path"}
	const expectedPerPath = totalObjects / 4

	trueVal := true
	createCollection(t, compose, restURIOf(compose, 1), className, 3, 3, []*models.Property{
		{
			Name:            "path",
			DataType:        []string{"text"},
			IndexFilterable: &trueVal,
			Tokenization:    "word",
		},
	})
	defer func() { deleteCollection(t, restURIOf(compose, 1), className) }()

	batchImportMultiProp(t, restURIOf(compose, 1), className, totalObjects, func(i int) map[string]interface{} {
		return map[string]interface{}{"path": paths[i%len(paths)]}
	})

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "pre-migration")

	// Pause "lagging" before "holder": once holder is paused the swap waits on its
	// prep ack, so the task must not reach PREPARING earlier. Followers only: a
	// paused leader stalls the cluster until re-election.
	restartedNode := raftLeaderIndex(t, compose) + 1
	lagging, holder := restartedNode%3+1, (restartedNode+1)%3+1
	resumeLagging := pauseNode(ctx, t, compose, lagging)
	defer resumeLagging()

	uri := restURIOf(compose, restartedNode)
	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, className, "path", "searchable",
		`{"tokenization":"field"}`)
	t.Logf("submitted change-tokenization task %s with node %d paused", taskID, lagging)

	awaitNodeUnitsCompleted(t, uri, taskID, holder)
	resumeHolder := pauseNode(ctx, t, compose, holder)
	defer resumeHolder()
	resumeLagging()

	// One record per shard for each of the searchable and filterable indexes.
	awaitNodeRecordsMerged(ctx, t, compose, restartedNode, strings.ToLower(className), 6)
	require.Equal(t, "PREPARING", taskStatus(t, uri, taskID),
		"node %d is paused before its prep ack, so the task must still be PREPARING", holder)

	// docker kill, not StopAt: StopAt polls a survivor that may be the paused node.
	dockerOnNode(ctx, t, compose, restartedNode, "kill")
	t.Logf("node %d killed inside the merged window", restartedNode)
	resumeHolder()
	require.NoError(t, compose.StartAt(ctx, restartedNode-1))

	reindexhelpers.AwaitReindexFinished(t, restURIOf(compose, 1), taskID,
		reindexhelpers.WithTimeout(240*time.Second))

	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after the task finished")

	rollingRestartCluster(ctx, t, compose)
	requireEveryReplicaServes(t, compose, className, "path", paths[0], expectedPerPath, "after a second restart")

	awaitTokenizationOnAllNodes(t, compose, className, "path", "field")
}

func requireEveryReplicaServes(t *testing.T, compose *docker.DockerCompose,
	className, propName, value string, want int, phase string,
) {
	t.Helper()
	for nodeIdx := 1; nodeIdx <= 3; nodeIdx++ {
		node := nodeIdx
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			got, err := equalCount(restURIOf(compose, node), className, propName, value)
			if !assert.NoErrorf(ct, err, "node %d could not be queried", node) {
				return
			}
			assert.Equalf(ct, want, got, "node %d serves the wrong count for %s=%q", node, propName, value)
		}, 90*time.Second, 200*time.Millisecond,
			"node %d must serve %d objects for %s=%q %s", node, want, propName, value, phase)
	}
}

// The returned resume func is idempotent so it can be both deferred and called early.
func pauseNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, node int) func() {
	t.Helper()
	dockerOnNode(ctx, t, compose, node, "pause")
	paused := true
	return func() {
		if paused {
			paused = false
			dockerOnNode(ctx, t, compose, node, "unpause")
		}
	}
}

func dockerOnNode(ctx context.Context, t *testing.T, compose *docker.DockerCompose, node int, action string) {
	t.Helper()
	id := compose.GetWeaviateNode(node).Container().GetContainerID()
	out, err := exec.CommandContext(ctx, "docker", action, id).CombinedOutput()
	require.NoErrorf(t, err, "docker %s node %d: %s", action, node, out)
}

// Short timeout: a request routed through a paused node would otherwise hang.
var pausedClusterClient = &http.Client{Timeout: 5 * time.Second}

func fetchTask(restURI, taskID string) (*models.DistributedTask, bool) {
	resp, err := pausedClusterClient.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var tasks models.DistributedTasks
	if json.Unmarshal(body, &tasks) != nil {
		return nil, false
	}
	for _, task := range tasks["reindex"] {
		if task.ID == taskID {
			return &task, true
		}
	}
	return nil, false
}

func taskStatus(t *testing.T, restURI, taskID string) string {
	t.Helper()
	var status string
	require.Eventually(t, func() bool {
		task, ok := fetchTask(restURI, taskID)
		if ok {
			status = task.Status
		}
		return ok
	}, 30*time.Second, 100*time.Millisecond, "task %s must be listed", taskID)
	return status
}

func awaitNodeUnitsCompleted(t *testing.T, restURI, taskID string, node int) {
	t.Helper()
	nodeName := fmt.Sprintf("weaviate-%d", node-1)
	require.EventuallyWithTf(t, func(ct *assert.CollectT) {
		task, ok := fetchTask(restURI, taskID)
		if !assert.True(ct, ok, "task %s not listed", taskID) {
			return
		}
		seen := 0
		for _, u := range task.Units {
			if u.NodeID == nodeName {
				seen++
				assert.Equalf(ct, "COMPLETED", u.Status, "unit %s (task %s: %s)", u.ID, task.Status, task.Error)
			}
		}
		assert.Positive(ct, seen, "node %s owns no unit", nodeName)
	}, 240*time.Second, 100*time.Millisecond, "every unit on node %d must complete", node)
}

func awaitNodeRecordsMerged(ctx context.Context, t *testing.T, compose *docker.DockerCompose,
	node int, classDirLower string, wantRecords int,
) {
	t.Helper()
	cmd := []string{"sh", "-c", fmt.Sprintf(
		`for f in $(find /data/%s -path '*/.migrations/records/*.json'); do grep -o '"state": *"[a-z]*"' "$f"; done; exit 0`,
		classDirLower)}
	require.EventuallyWithTf(t, func(ct *assert.CollectT) {
		code, reader, err := compose.GetWeaviateNode(node).Container().Exec(ctx, cmd, tcexec.Multiplexed())
		if !assert.NoError(ct, err) || !assert.Zero(ct, code) || !assert.NotNil(ct, reader) {
			return
		}
		out, _ := io.ReadAll(reader)
		states := strings.Fields(strings.ReplaceAll(string(out), ": ", ":"))
		assert.Len(ct, states, wantRecords, "records: %q", out)
		for _, state := range states {
			assert.Equal(ct, `"state":"merged"`, state)
		}
	}, 60*time.Second, 100*time.Millisecond, "node %d records must all read merged", node)
}
