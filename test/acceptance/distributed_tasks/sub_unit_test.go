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

package distributed_tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

func TestSubUnitTaskLifecycle_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "sub-unit-success-test"
	addTask(t, debugURI, taskID, "sub_units=su-1,su-2,su-3")
	awaitTaskStatus(t, restURI, taskID, "FINISHED")

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	require.NotNil(t, task.SubUnits)
	assert.Len(t, task.SubUnits, 3)

	for _, su := range task.SubUnits {
		assert.Equal(t, "COMPLETED", su.Status, "sub-unit %s should be completed", su.ID)
		assert.Equal(t, float32(1.0), su.Progress, "sub-unit %s should have progress 1.0", su.ID)
	}
}

func TestSubUnitTaskLifecycle_Failure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "sub-unit-failure-test"
	addTask(t, debugURI, taskID, "sub_units=su-1,su-2,su-3&fail_sub_unit=su-2")
	awaitTaskStatus(t, restURI, taskID, "FAILED")

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FAILED", task.Status)
	assert.Contains(t, task.Error, "dummy failure")
	require.NotNil(t, task.SubUnits)

	var failedSU *models.DistributedTaskSubUnit
	for _, su := range task.SubUnits {
		if su.ID == "su-2" {
			failedSU = su
			break
		}
	}
	require.NotNil(t, failedSU)
	assert.Equal(t, "FAILED", failedSU.Status)
	assert.Contains(t, failedSU.Error, "dummy failure")
}

func TestLegacyTask_NoSubUnits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "legacy-test"
	addTask(t, debugURI, taskID, "")
	awaitTaskStatus(t, restURI, taskID, "FINISHED")

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Nil(t, task.SubUnits)
}

func TestSubUnitTask_PerShardFinalize_MoreSubUnitsThanNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-more-sub-units"
	subUnits := []string{"su-1", "su-2", "su-3", "su-4", "su-5", "su-6"}
	debugURI1 := compose.GetWeaviate().DebugURI()
	addTask(t, debugURI1, taskID, "sub_units="+strings.Join(subUnits, ","))
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	// Verify all 6 sub-units have finalization marker files across the cluster
	allFinalized := collectFinalizedSubUnitsFromCluster(t, ctx, compose, taskID)
	sort.Strings(allFinalized)
	sort.Strings(subUnits)
	assert.Equal(t, subUnits, allFinalized, "all sub-units should be finalized across the cluster")

	// Verify OnTaskCompleted fired on at least one node via debug status endpoint
	verifyTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_FewerSubUnitsThanNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-fewer-sub-units"
	subUnits := []string{"su-1", "su-2"}
	debugURI1 := compose.GetWeaviate().DebugURI()
	addTask(t, debugURI1, taskID, "sub_units="+strings.Join(subUnits, ","))
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	// Only 2 marker files should exist across the entire cluster
	allFinalized := collectFinalizedSubUnitsFromCluster(t, ctx, compose, taskID)
	sort.Strings(allFinalized)
	sort.Strings(subUnits)
	assert.Equal(t, subUnits, allFinalized, "exactly 2 sub-units should be finalized")

	verifyTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_OneSubUnit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-one-sub-unit"
	debugURI1 := compose.GetWeaviate().DebugURI()
	addTask(t, debugURI1, taskID, "sub_units=su-only")
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	// Exactly 1 marker file should exist across the cluster
	allFinalized := collectFinalizedSubUnitsFromCluster(t, ctx, compose, taskID)
	assert.Equal(t, []string{"su-only"}, allFinalized, "exactly 1 sub-unit should be finalized")

	verifyTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_OnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	// Use a single sub-unit that will fail. This ensures the sub-unit is claimed
	// (progress reported) before failure, so OnSubUnitsCompleted receives it.
	taskID := "finalize-on-failure"
	debugURI1 := compose.GetWeaviate().DebugURI()
	addTask(t, debugURI1, taskID, "sub_units=su-1&fail_sub_unit=su-1")
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FAILED")

	// Wait for callbacks to fire (scheduler tick interval is 1s)
	time.Sleep(3 * time.Second)

	// OnSubUnitsCompleted should still fire on failure — the sub-unit was claimed
	// (progress was reported at 50%) before it failed, so it has a NodeID.
	allFinalized := collectFinalizedSubUnitsFromCluster(t, ctx, compose, taskID)
	assert.Equal(t, []string{"su-1"}, allFinalized, "OnSubUnitsCompleted should fire even on failure")

	// OnTaskCompleted should fire on failure too
	verifyTaskCompletedOnAnyNode(t, compose, taskID)
}

// startDTMCluster spins up a single-node Weaviate with DTM and the shard-noop provider enabled.
func startDTMCluster(ctx context.Context, t *testing.T) (restURI, debugURI string, cleanup func()) {
	t.Helper()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("SHARD_NOOP_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)

	return compose.GetWeaviate().URI(),
		compose.GetWeaviate().DebugURI(),
		func() { require.NoError(t, compose.Terminate(ctx)) }
}

// start3NodeDTMCluster spins up a 3-node Weaviate cluster with DTM and the shard-noop provider.
func start3NodeDTMCluster(ctx context.Context, t *testing.T) (*docker.DockerCompose, func()) {
	t.Helper()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("SHARD_NOOP_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)

	return compose, func() { require.NoError(t, compose.Terminate(ctx)) }
}

// addTask creates a task via the debug endpoint. params is the query string after "id=<taskID>&"
// (e.g. "sub_units=su-1,su-2" or "" for a legacy task).
func addTask(t *testing.T, debugURI, taskID, params string) {
	t.Helper()

	url := fmt.Sprintf("http://%s/debug/distributed-tasks/add?id=%s", debugURI, taskID)
	if params != "" {
		url += "&" + params
	}

	resp, err := http.Post(url, "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()
}

// awaitTaskStatus polls GET /v1/tasks until the given task reaches the expected status.
func awaitTaskStatus(t *testing.T, restURI, taskID, expectedStatus string) {
	t.Helper()

	require.Eventually(t, func() bool {
		tasks := listTasks(t, restURI)
		for _, task := range tasks["shard-noop"] {
			if task.ID == taskID && task.Status == expectedStatus {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "task %s should reach %s status", taskID, expectedStatus)
}

// findTask retrieves a specific task by ID from the REST API.
func findTask(t *testing.T, restURI, taskID string) *models.DistributedTask {
	t.Helper()

	tasks := listTasks(t, restURI)
	for i := range tasks["shard-noop"] {
		if tasks["shard-noop"][i].ID == taskID {
			return &tasks["shard-noop"][i]
		}
	}
	t.Fatalf("task %s not found", taskID)
	return nil
}

func listTasks(t *testing.T, restURI string) models.DistributedTasks {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Logf("GET /v1/tasks returned %d: %s", resp.StatusCode, string(body))
		return nil
	}

	var tasks models.DistributedTasks
	require.NoError(t, json.Unmarshal(body, &tasks))
	return tasks
}

// listMarkerFiles lists finalization marker files for a task inside a container.
func listMarkerFiles(ctx context.Context, c testcontainers.Container, taskID string) []string {
	path := fmt.Sprintf("/tmp/dtm-finalize/%s", taskID)
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", path}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		return nil
	}
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil
	}
	var files []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			files = append(files, line)
		}
	}
	return files
}

// collectFinalizedSubUnitsFromCluster collects all finalized sub-unit marker files from all 3 nodes.
func collectFinalizedSubUnitsFromCluster(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string) []string {
	t.Helper()

	var all []string
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		files := listMarkerFiles(ctx, node.Container(), taskID)
		all = append(all, files...)
	}
	return all
}

type debugStatus struct {
	TaskCompleted     bool     `json:"taskCompleted"`
	FinalizedSubUnits []string `json:"finalizedSubUnits"`
}

// getDebugStatus queries the debug status endpoint on a node.
func getDebugStatus(t *testing.T, debugURI, taskID string) debugStatus {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s/debug/distributed-tasks/status?id=%s", debugURI, taskID))
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var status debugStatus
	require.NoError(t, json.Unmarshal(body, &status))
	return status
}

// verifyTaskCompletedOnAnyNode checks that OnTaskCompleted fired on at least one node.
func verifyTaskCompletedOnAnyNode(t *testing.T, compose *docker.DockerCompose, taskID string) {
	t.Helper()

	var anyCompleted bool
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		status := getDebugStatus(t, node.DebugURI(), taskID)
		if status.TaskCompleted {
			anyCompleted = true
			break
		}
	}
	assert.True(t, anyCompleted, "OnTaskCompleted should fire on at least one node")
}
