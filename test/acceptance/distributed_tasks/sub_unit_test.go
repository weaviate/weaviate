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

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_TEST_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	restURI := compose.GetWeaviate().URI()
	debugURI := compose.GetWeaviate().DebugURI()

	// Create a task with 3 sub-units via the debug endpoint
	taskID := "sub-unit-success-test"
	resp, err := http.Post(
		fmt.Sprintf("http://%s/debug/distributed-tasks/add?id=%s&sub_units=su-1,su-2,su-3", debugURI, taskID),
		"application/json", nil,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	// Poll GET /v1/tasks until the task reaches FINISHED status
	require.Eventually(t, func() bool {
		tasks := listTasks(t, restURI)
		namespaceTasks, ok := tasks["dummy-test"]
		if !ok || len(namespaceTasks) == 0 {
			return false
		}
		for _, task := range namespaceTasks {
			if task.ID == taskID && task.Status == "FINISHED" {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "task should reach FINISHED status")

	// Verify sub-unit details
	tasks := listTasks(t, restURI)
	var task *models.DistributedTask
	for i := range tasks["dummy-test"] {
		if tasks["dummy-test"][i].ID == taskID {
			task = &tasks["dummy-test"][i]
			break
		}
	}
	require.NotNil(t, task)

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

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_TEST_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	restURI := compose.GetWeaviate().URI()
	debugURI := compose.GetWeaviate().DebugURI()

	// Create a task with 3 sub-units, one configured to fail
	taskID := "sub-unit-failure-test"
	resp, err := http.Post(
		fmt.Sprintf("http://%s/debug/distributed-tasks/add?id=%s&sub_units=su-1,su-2,su-3&fail_sub_unit=su-2", debugURI, taskID),
		"application/json", nil,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	// Poll until the task reaches FAILED status
	require.Eventually(t, func() bool {
		tasks := listTasks(t, restURI)
		namespaceTasks, ok := tasks["dummy-test"]
		if !ok || len(namespaceTasks) == 0 {
			return false
		}
		for _, task := range namespaceTasks {
			if task.ID == taskID && task.Status == "FAILED" {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "task should reach FAILED status")

	// Verify task error contains sub-unit failure info
	tasks := listTasks(t, restURI)
	var task *models.DistributedTask
	for i := range tasks["dummy-test"] {
		if tasks["dummy-test"][i].ID == taskID {
			task = &tasks["dummy-test"][i]
			break
		}
	}
	require.NotNil(t, task)

	assert.Equal(t, "FAILED", task.Status)
	assert.Contains(t, task.Error, "dummy failure")
	require.NotNil(t, task.SubUnits)

	// Find the failed sub-unit
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

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("DISTRIBUTED_TASKS_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS", "1").
		WithWeaviateEnv("DISTRIBUTED_TASKS_TEST_PROVIDER_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	restURI := compose.GetWeaviate().URI()
	debugURI := compose.GetWeaviate().DebugURI()

	// Create a legacy task (no sub-units)
	taskID := "legacy-test"
	resp, err := http.Post(
		fmt.Sprintf("http://%s/debug/distributed-tasks/add?id=%s", debugURI, taskID),
		"application/json", nil,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	// Poll until the task reaches FINISHED status
	require.Eventually(t, func() bool {
		tasks := listTasks(t, restURI)
		namespaceTasks, ok := tasks["dummy-test"]
		if !ok || len(namespaceTasks) == 0 {
			return false
		}
		for _, task := range namespaceTasks {
			if task.ID == taskID && task.Status == "FINISHED" {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "legacy task should reach FINISHED status")

	// Verify no sub-units
	tasks := listTasks(t, restURI)
	var task *models.DistributedTask
	for i := range tasks["dummy-test"] {
		if tasks["dummy-test"][i].ID == taskID {
			task = &tasks["dummy-test"][i]
			break
		}
	}
	require.NotNil(t, task)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Nil(t, task.SubUnits)
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
