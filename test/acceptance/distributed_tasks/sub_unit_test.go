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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// startDTMCluster spins up a single-node Weaviate with DTM and the dummy test provider enabled.
func startDTMCluster(ctx context.Context, t *testing.T) (restURI, debugURI string, cleanup func()) {
	t.Helper()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_TEST_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)

	return compose.GetWeaviate().URI(),
		compose.GetWeaviate().DebugURI(),
		func() { require.NoError(t, compose.Terminate(ctx)) }
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
		for _, task := range tasks["dummy-test"] {
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
	for i := range tasks["dummy-test"] {
		if tasks["dummy-test"][i].ID == taskID {
			return &tasks["dummy-test"][i]
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
