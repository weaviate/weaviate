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

// TestCrashRecovery_SingleNodeRestart verifies that a task completes after one
// non-leader node crashes and restarts. The crashed node's IN_PROGRESS sub-units
// are re-launched by the Scheduler on restart.
func TestCrashRecovery_SingleNodeRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	taskID, className, subUnitIDs := setupCrashRecoveryTask(t, compose, "CrashSingleNode", "crash-single-node", 2000)

	// Wait until at least one sub-unit is in progress before crashing.
	awaitAnySubUnitInProgress(t, restURI, taskID)

	// Kill node 2 (index 1, non-leader).
	require.NoError(t, compose.StopAt(ctx, 1, nil))

	// Let the surviving nodes finish their sub-units.
	time.Sleep(5 * time.Second)

	// Restart node 2.
	require.NoError(t, compose.StartAt(ctx, 1))

	// Task must complete — the restarted node re-processes its sub-units.
	if !awaitTaskStatusWithTimeout(t, restURI, taskID, "FINISHED", 120*time.Second) {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, 9)

	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)

	deleteCollection(t, restURI, className)
}

// TestCrashRecovery_MajorityRestart verifies that a task completes after a
// majority of nodes (2 of 3) crash and restart while one node stays up.
// Both crashed nodes re-launch their own sub-units on restart.
func TestCrashRecovery_MajorityRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	taskID, className, subUnitIDs := setupCrashRecoveryTask(t, compose, "CrashMajority", "crash-majority", 2000)

	// Wait until at least one sub-unit is in progress.
	awaitAnySubUnitInProgress(t, restURI, taskID)

	// Kill nodes 1 and 2 (indices 1 and 2), leaving node 0 (leader) alive.
	require.NoError(t, compose.StopAt(ctx, 1, nil))
	require.NoError(t, compose.StopAt(ctx, 2, nil))

	// Wait for surviving node to finish its sub-units.
	time.Sleep(8 * time.Second)

	// Restart the crashed nodes.
	require.NoError(t, compose.StartAt(ctx, 1))
	require.NoError(t, compose.StartAt(ctx, 2))

	if !awaitTaskStatusWithTimeout(t, restURI, taskID, "FINISHED", 120*time.Second) {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, 9)

	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)

	deleteCollection(t, restURI, className)
}

// TestCrashRecovery_RollingRestart verifies that a task completes despite
// rolling restarts where nodes are cycled one after another, with overlapping
// down-times.
func TestCrashRecovery_RollingRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	taskID, className, subUnitIDs := setupCrashRecoveryTask(t, compose, "CrashRolling", "crash-rolling", 3000)

	// Wait for processing to start.
	awaitAnySubUnitInProgress(t, restURI, taskID)

	// Rolling restart: crash node 1, restart it, then crash node 2, restart it.
	// Node 0 stays up throughout, maintaining Raft quorum.
	require.NoError(t, compose.StopAt(ctx, 1, nil))
	require.NoError(t, compose.StartAt(ctx, 1))

	// Second crash cycle while node 1 is still re-processing.
	require.NoError(t, compose.StopAt(ctx, 2, nil))
	require.NoError(t, compose.StartAt(ctx, 2))

	// URI may have changed after restart.
	restURI = compose.GetWeaviate().URI()

	if !awaitTaskStatusWithTimeout(t, restURI, taskID, "FINISHED", 120*time.Second) {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, 9)

	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)

	deleteCollection(t, restURI, className)
}

// TestCrashRecovery_CrashBeforeAnyProgress verifies that a task completes when
// a node crashes before reporting any progress. The node's sub-units remain
// PENDING and are re-claimed on restart.
func TestCrashRecovery_CrashBeforeAnyProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	taskID, className, subUnitIDs := setupCrashRecoveryTask(t, compose, "CrashBeforeProgress", "crash-before-progress", 5000)

	// Immediately kill node 2 before it reports any progress.
	require.NoError(t, compose.StopAt(ctx, 1, nil))

	// Let the other nodes finish their sub-units.
	time.Sleep(10 * time.Second)

	// Restart node 2.
	require.NoError(t, compose.StartAt(ctx, 1))

	if !awaitTaskStatusWithTimeout(t, restURI, taskID, "FINISHED", 120*time.Second) {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, 9)

	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)

	deleteCollection(t, restURI, className)
}

// ---------------------------------------------------------------------------
// Crash recovery helpers
// ---------------------------------------------------------------------------

// setupCrashRecoveryTask creates an RF3/3-shard collection and adds a task with 9 per-replica sub-units.
func setupCrashRecoveryTask(t *testing.T, compose *docker.DockerCompose, className, taskID string, processingDelayMs int) (string, string, []string) {
	t.Helper()

	restURI := compose.GetWeaviate().URI()
	createCollection(t, restURI, className, 3, 3)

	placements := getShardPlacement(t, restURI, className, 9)
	subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
	require.Len(t, subUnitIDs, 9)

	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:                taskID,
		SubUnits:          subUnitIDs,
		Collection:        className,
		SubUnitToShard:    subUnitToShard,
		SubUnitToNode:     subUnitToNode,
		ProcessingDelayMs: processingDelayMs,
	})

	return taskID, className, subUnitIDs
}

// awaitAnySubUnitInProgress polls until at least one sub-unit has IN_PROGRESS status.
func awaitAnySubUnitInProgress(t *testing.T, restURI, taskID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		task := findTaskSafe(t, restURI, taskID)
		if task == nil {
			return false
		}
		for _, su := range task.SubUnits {
			if su.Status == "IN_PROGRESS" {
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "expected at least one sub-unit to be IN_PROGRESS")
}

// awaitTaskStatusWithTimeout is like awaitTaskStatusOK but with a configurable timeout.
func awaitTaskStatusWithTimeout(t *testing.T, restURI, taskID, expectedStatus string, timeout time.Duration) bool {
	t.Helper()

	return assert.Eventually(t, func() bool {
		tasks := listTasksSafe(restURI)
		if tasks == nil {
			return false
		}
		for _, task := range tasks[shardNoopNamespace] {
			if task.ID == taskID && task.Status == expectedStatus {
				return true
			}
		}
		return false
	}, timeout, 1*time.Second, "task %s should reach %s status", taskID, expectedStatus)
}

// findTaskSafe retrieves a task by ID, returning nil if the node is unreachable.
func findTaskSafe(t *testing.T, restURI, taskID string) *models.DistributedTask {
	t.Helper()

	tasks := listTasksSafe(restURI)
	if tasks == nil {
		return nil
	}
	for i := range tasks[shardNoopNamespace] {
		if tasks[shardNoopNamespace][i].ID == taskID {
			return &tasks[shardNoopNamespace][i]
		}
	}
	return nil
}

// listTasksSafe is like listTasks but returns nil on any error instead of failing.
func listTasksSafe(restURI string) models.DistributedTasks {
	resp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var tasks models.DistributedTasks
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil
	}
	return tasks
}
