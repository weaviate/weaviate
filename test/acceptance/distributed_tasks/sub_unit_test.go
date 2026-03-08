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
	"bytes"
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

// ---------------------------------------------------------------------------
// Single-node tests (synthetic sub-units)
// ---------------------------------------------------------------------------

func TestSubUnitTaskLifecycle_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "sub-unit-success-test"
	addTaskJSON(t, debugURI, addTaskRequest{
		ID:       taskID,
		SubUnits: []string{"su-1", "su-2", "su-3"},
	})
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
	addTaskJSON(t, debugURI, addTaskRequest{
		ID:          taskID,
		SubUnits:    []string{"su-1", "su-2", "su-3"},
		FailSubUnit: "su-2",
	})
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
	addTaskJSON(t, debugURI, addTaskRequest{ID: taskID})
	awaitTaskStatus(t, restURI, taskID, "FINISHED")

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Nil(t, task.SubUnits)
}

// ---------------------------------------------------------------------------
// 3-node synthetic sub-unit tests (finalization)
// ---------------------------------------------------------------------------

func TestSubUnitTask_PerShardFinalize_MoreSubUnitsThanNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-more-sub-units"
	subUnits := []string{"su-1", "su-2", "su-3", "su-4", "su-5", "su-6"}
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:       taskID,
		SubUnits: subUnits,
	})
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnits)
	awaitTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_FewerSubUnitsThanNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-fewer-sub-units"
	subUnits := []string{"su-1", "su-2"}
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:       taskID,
		SubUnits: subUnits,
	})
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnits)
	awaitTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_OneSubUnit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-one-sub-unit"
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:       taskID,
		SubUnits: []string{"su-only"},
	})
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FINISHED")

	awaitFinalizedSubUnits(t, ctx, compose, taskID, []string{"su-only"})
	awaitTaskCompletedOnAnyNode(t, compose, taskID)
}

func TestSubUnitTask_PerShardFinalize_OnFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	taskID := "finalize-on-failure"
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:          taskID,
		SubUnits:    []string{"su-1"},
		FailSubUnit: "su-1",
	})
	awaitTaskStatus(t, compose.GetWeaviate().URI(), taskID, "FAILED")

	awaitFinalizedSubUnits(t, ctx, compose, taskID, []string{"su-1"})
	awaitTaskCompletedOnAnyNode(t, compose, taskID)
}

// ---------------------------------------------------------------------------
// Standalone real-collection tests (fast feedback)
// ---------------------------------------------------------------------------

func TestRealCollection_RF1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	className := "DTMTestRF1"
	createCollection(t, restURI, className, 3, 1)

	placements := getShardPlacement(t, restURI, className, 3)
	t.Logf("placements: %v", placements)

	subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)

	taskID := "real-collection-rf1"
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:                taskID,
		SubUnits:          subUnitIDs,
		Collection:        className,
		SubUnitToShard:    subUnitToShard,
		SubUnitToNode:     subUnitToNode,
		ProcessingDelayMs: 10,
	})

	if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	require.NotNil(t, task.SubUnits)
	assert.Len(t, task.SubUnits, 3)
	for _, su := range task.SubUnits {
		assert.Equal(t, "COMPLETED", su.Status, "sub-unit %s should be completed", su.ID)
	}

	// Verify all 3 phases of markers
	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)
}

func TestMultiTenant_RF1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	className := "DTMTestMTRF1"
	createMTCollection(t, restURI, className, 1)
	createTenants(t, restURI, className, 10)

	placements := getShardPlacement(t, restURI, className, 10)
	t.Logf("placements (%d): %v", len(placements), placements)

	subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)

	taskID := "mt-rf1-10"
	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:                taskID,
		SubUnits:          subUnitIDs,
		Collection:        className,
		SubUnitToShard:    subUnitToShard,
		SubUnitToNode:     subUnitToNode,
		ProcessingDelayMs: 10,
	})

	if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
		dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	require.NotNil(t, task.SubUnits)
	assert.Len(t, task.SubUnits, 10)

	awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
	awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
	awaitCompletionMarkers(t, ctx, compose, taskID, 3)
}

// ---------------------------------------------------------------------------
// Real-collection edge case suite (shared cluster)
// ---------------------------------------------------------------------------

func TestRealCollectionSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()

	t.Run("RF2_Shards3", func(t *testing.T) {
		className := "DTMSuiteRF2S3"
		createCollection(t, restURI, className, 3, 2)

		placements := getShardPlacement(t, restURI, className, 6) // 3 shards * RF2
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 6)

		taskID := "suite-rf2-shards3"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 6)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_Shards1", func(t *testing.T) {
		className := "DTMSuiteRF3S1"
		createCollection(t, restURI, className, 1, 3)

		placements := getShardPlacement(t, restURI, className, 3) // 1 shard * RF3
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 3)

		taskID := "suite-rf3-shards1"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 3)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_Shards3", func(t *testing.T) {
		className := "DTMSuiteRF3S3"
		createCollection(t, restURI, className, 3, 3)

		placements := getShardPlacement(t, restURI, className, 9) // 3 shards * RF3
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 9)

		taskID := "suite-rf3-shards3"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
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
	})

	t.Run("RF2_Shards24", func(t *testing.T) {
		className := "DTMSuiteRF2S24"
		createCollection(t, restURI, className, 24, 2)

		placements := getShardPlacement(t, restURI, className, 48) // 24 shards * RF2
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 48)

		taskID := "suite-rf2-shards24"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 48)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF1_Shards2_IdleNode", func(t *testing.T) {
		className := "DTMSuiteRF1S2Idle"
		createCollection(t, restURI, className, 2, 1)

		placements := getShardPlacement(t, restURI, className, 2) // 2 shards * RF1
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 2)

		taskID := "suite-rf1-shards2-idle"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 2)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		// All 3 nodes get OnTaskCompleted even if one is idle
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF1_Failure", func(t *testing.T) {
		className := "DTMSuiteRF1Fail"
		createCollection(t, restURI, className, 3, 1)

		placements := getShardPlacement(t, restURI, className, 3)
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		failSubUnit := subUnitIDs[0]

		taskID := "suite-rf1-failure"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			FailSubUnit:       failSubUnit,
			ProcessingDelayMs: 10,
		})

		awaitTaskStatus(t, restURI, taskID, "FAILED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FAILED", task.Status)
		assert.Contains(t, task.Error, "dummy failure")

		awaitTaskCompletedOnAnyNode(t, compose, taskID)

		deleteCollection(t, restURI, className)
	})

	t.Run("PartialTargeting", func(t *testing.T) {
		className := "DTMSuitePartial"
		createCollection(t, restURI, className, 5, 1)

		allPlacements := getShardPlacement(t, restURI, className, 5)

		// Target only the first 3 shards
		targetPlacements := allPlacements[:3]
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(targetPlacements)
		require.Len(t, subUnitIDs, 3)

		taskID := "suite-partial-targeting"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 3)

		// Verify only targeted sub-units have processing markers
		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)

		deleteCollection(t, restURI, className)
	})

	t.Run("TemporalOrdering", func(t *testing.T) {
		className := "DTMSuiteTemporal"
		createCollection(t, restURI, className, 3, 1)

		placements := getShardPlacement(t, restURI, className, 3)
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)

		// Make the first sub-unit slow
		slowSubUnit := subUnitIDs[0]

		taskID := "suite-temporal-ordering"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			SlowSubUnit:       slowSubUnit,
			SlowDelayMs:       5000,
			ProcessingDelayMs: 10,
		})

		// Fast sub-units should complete quickly. Wait until at least one
		// non-slow sub-unit is COMPLETED in the task API.
		fastSubUnits := subUnitIDs[1:]
		require.Eventually(t, func() bool {
			task := findTask(t, restURI, taskID)
			if task == nil {
				return false
			}
			for _, su := range task.SubUnits {
				for _, fsu := range fastSubUnits {
					if su.ID == fsu && su.Status == "COMPLETED" {
						return true
					}
				}
			}
			return false
		}, 15*time.Second, 500*time.Millisecond, "fast sub-units should complete before slow one")

		// At this point, NO finalization markers should exist yet because the
		// slow sub-unit hasn't completed → the per-shard barrier is not met.
		finMarkers := collectMarkersFromCluster(t, ctx, compose, fmt.Sprintf("/tmp/dtm-finalize/%s", taskID))
		t.Logf("finalization markers during slow phase: %v", finMarkers)
		// Note: we can't strictly assert zero here because the slow sub-unit
		// might be on a different node and that node's fast sub-units might
		// already have finalization. We verify the full set after completion.

		// Wait for the task to finish (slow sub-unit completes)
		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		// Now all markers should appear
		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})
}

// ---------------------------------------------------------------------------
// Multi-tenant edge case suite (shared cluster)
// ---------------------------------------------------------------------------

func TestMultiTenantSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()

	t.Run("RF1_100Tenants", func(t *testing.T) {
		className := "DTMMTRF1T100"
		createMTCollection(t, restURI, className, 1)
		createTenants(t, restURI, className, 100)

		placements := getShardPlacement(t, restURI, className, 100)
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 100)

		taskID := "mt-rf1-100"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 100)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_10Tenants", func(t *testing.T) {
		className := "DTMMTRF3T10"
		createMTCollection(t, restURI, className, 3)
		createTenants(t, restURI, className, 10)

		placements := getShardPlacement(t, restURI, className, 30) // 10 tenants * RF3
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 30)

		taskID := "mt-rf3-10"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 30)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_100Tenants", func(t *testing.T) {
		className := "DTMMTRF3T100"
		createMTCollection(t, restURI, className, 3)
		createTenants(t, restURI, className, 100)

		placements := getShardPlacement(t, restURI, className, 300) // 100 tenants * RF3
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(placements)
		require.Len(t, subUnitIDs, 300)

		taskID := "mt-rf3-100"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 300)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)
		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnitIDs)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF1_PartialTargeting", func(t *testing.T) {
		className := "DTMMTPartialRF1"
		createMTCollection(t, restURI, className, 1)
		createTenants(t, restURI, className, 10)

		allPlacements := getShardPlacement(t, restURI, className, 10)
		targetPlacements := allPlacements[:5]
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(targetPlacements)
		require.Len(t, subUnitIDs, 5)

		taskID := "mt-rf1-partial"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 5)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_PartialTargeting", func(t *testing.T) {
		className := "DTMMTPartialRF3"
		createMTCollection(t, restURI, className, 3)
		createTenants(t, restURI, className, 10)

		allPlacements := getShardPlacement(t, restURI, className, 30) // 10 * RF3

		// Target 5 tenants → 15 sub-units (5 tenants * RF3)
		// Group by shard name, pick first 5 unique shards
		shardSeen := map[string]bool{}
		var targetPlacements []shardPlacement
		for _, p := range allPlacements {
			if len(shardSeen) < 5 || shardSeen[p.ShardName] {
				shardSeen[p.ShardName] = true
				targetPlacements = append(targetPlacements, p)
			}
		}
		subUnitIDs, subUnitToShard, subUnitToNode := buildPerReplicaSubUnits(targetPlacements)
		require.Len(t, subUnitIDs, 15)

		taskID := "mt-rf3-partial"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			SubUnits:          subUnitIDs,
			Collection:        className,
			SubUnitToShard:    subUnitToShard,
			SubUnitToNode:     subUnitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 15)

		awaitProcessingMarkers(t, ctx, compose, taskID, subUnitIDs)

		deleteCollection(t, restURI, className)
	})
}

// ---------------------------------------------------------------------------
// Helper types and functions
// ---------------------------------------------------------------------------

type shardPlacement struct {
	ShardName string
	NodeName  string
}

type addTaskRequest struct {
	ID                string            `json:"id"`
	SubUnits          []string          `json:"subUnits,omitempty"`
	FailSubUnit       string            `json:"failSubUnit,omitempty"`
	Collection        string            `json:"collection,omitempty"`
	SubUnitToShard    map[string]string `json:"subUnitToShard,omitempty"`
	SubUnitToNode     map[string]string `json:"subUnitToNode,omitempty"`
	SlowSubUnit       string            `json:"slowSubUnit,omitempty"`
	SlowDelayMs       int               `json:"slowDelayMs,omitempty"`
	ProcessingDelayMs int               `json:"processingDelayMs,omitempty"`
}

// addTaskJSON sends a JSON body to the debug add endpoint.
func addTaskJSON(t *testing.T, debugURI string, req addTaskRequest) {
	t.Helper()

	body, err := json.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/debug/distributed-tasks/add", debugURI),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "addTask failed: %s", string(respBody))
}

// getShardPlacement returns (shardName, nodeName) pairs for a collection.
func getShardPlacement(t *testing.T, restURI, className string, expectedPairs int) []shardPlacement {
	t.Helper()

	var placements []shardPlacement
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI))
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false
		}

		var nodesResp struct {
			Nodes []struct {
				Name   string `json:"name"`
				Shards []struct {
					Class string `json:"class"`
					Name  string `json:"name"`
				} `json:"shards"`
			} `json:"nodes"`
		}
		if err := json.Unmarshal(body, &nodesResp); err != nil {
			return false
		}

		placements = nil
		for _, node := range nodesResp.Nodes {
			for _, shard := range node.Shards {
				if shard.Class == className {
					placements = append(placements, shardPlacement{
						ShardName: shard.Name,
						NodeName:  node.Name,
					})
				}
			}
		}

		return len(placements) >= expectedPairs
	}, 30*time.Second, 500*time.Millisecond,
		"expected %d shard-node pairs for %s, got %d", expectedPairs, className, len(placements))

	sort.Slice(placements, func(i, j int) bool {
		if placements[i].ShardName == placements[j].ShardName {
			return placements[i].NodeName < placements[j].NodeName
		}
		return placements[i].ShardName < placements[j].ShardName
	})

	return placements
}

// buildPerReplicaSubUnits creates sub-unit IDs, SubUnitToShard, and SubUnitToNode maps from placement.
func buildPerReplicaSubUnits(placements []shardPlacement) (subUnitIDs []string, shardMap, nodeMap map[string]string) {
	shardMap = make(map[string]string, len(placements))
	nodeMap = make(map[string]string, len(placements))
	for _, p := range placements {
		suID := fmt.Sprintf("%s__%s", p.ShardName, p.NodeName)
		subUnitIDs = append(subUnitIDs, suID)
		shardMap[suID] = p.ShardName
		nodeMap[suID] = p.NodeName
	}
	sort.Strings(subUnitIDs)
	return subUnitIDs, shardMap, nodeMap
}

// createCollection creates a class with the given shard count and replication factor.
func createCollection(t *testing.T, restURI, className string, shardCount, rf int) {
	t.Helper()

	body := fmt.Sprintf(`{
		"class": %q,
		"vectorizer": "none",
		"shardingConfig": {"desiredCount": %d},
		"replicationConfig": {"factor": %d}
	}`, className, shardCount, rf)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema", restURI),
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create class failed: %s", string(respBody))
}

// createMTCollection creates a multi-tenant class.
func createMTCollection(t *testing.T, restURI, className string, rf int) {
	t.Helper()

	body := fmt.Sprintf(`{
		"class": %q,
		"vectorizer": "none",
		"multiTenancyConfig": {"enabled": true},
		"replicationConfig": {"factor": %d}
	}`, className, rf)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema", restURI),
		"application/json",
		strings.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create MT class failed: %s", string(respBody))
}

// createTenants creates N tenants named "t-0" through "t-{N-1}".
func createTenants(t *testing.T, restURI, className string, count int) {
	t.Helper()

	tenants := make([]map[string]string, count)
	for i := 0; i < count; i++ {
		tenants[i] = map[string]string{"name": fmt.Sprintf("t-%d", i)}
	}

	body, err := json.Marshal(tenants)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema/%s/tenants", restURI, className),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create tenants failed: %s", string(respBody))
}

// deleteCollection deletes a class.
func deleteCollection(t *testing.T, restURI, className string) {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/v1/schema/%s", restURI, className), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// listMarkerFilesInDir lists files in a container directory.
func listMarkerFilesInDir(ctx context.Context, c testcontainers.Container, dir string) []string {
	code, reader, err := c.Exec(ctx, []string{"ls", "-1", dir}, tcexec.Multiplexed())
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

// collectMarkersFromCluster collects marker files from a directory across all 3 nodes.
func collectMarkersFromCluster(t *testing.T, ctx context.Context, compose *docker.DockerCompose, dir string) []string {
	t.Helper()

	var all []string
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		files := listMarkerFilesInDir(ctx, node.Container(), dir)
		all = append(all, files...)
	}
	return all
}

// awaitProcessingMarkers polls until expected processing marker files appear across the cluster.
func awaitProcessingMarkers(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expected []string) {
	t.Helper()

	dir := fmt.Sprintf("/tmp/dtm-process/%s", taskID)
	sort.Strings(expected)
	require.Eventually(t, func() bool {
		all := collectMarkersFromCluster(t, ctx, compose, dir)
		sort.Strings(all)
		return fmt.Sprintf("%v", all) == fmt.Sprintf("%v", expected)
	}, 60*time.Second, 500*time.Millisecond, "expected processing markers %v", expected)
}

// awaitFinalizedSubUnits polls until expected finalization marker files appear across the cluster.
func awaitFinalizedSubUnits(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expected []string) {
	t.Helper()

	dir := fmt.Sprintf("/tmp/dtm-finalize/%s", taskID)
	sort.Strings(expected)
	require.Eventually(t, func() bool {
		all := collectMarkersFromCluster(t, ctx, compose, dir)
		sort.Strings(all)
		return fmt.Sprintf("%v", all) == fmt.Sprintf("%v", expected)
	}, 60*time.Second, 500*time.Millisecond, "expected finalized sub-units %v", expected)
}

// awaitCompletionMarkers polls until the expected number of completion markers appear across the cluster.
func awaitCompletionMarkers(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expectedCount int) {
	t.Helper()

	dir := fmt.Sprintf("/tmp/dtm-complete/%s", taskID)
	require.Eventually(t, func() bool {
		all := collectMarkersFromCluster(t, ctx, compose, dir)
		return len(all) >= expectedCount
	}, 60*time.Second, 500*time.Millisecond, "expected %d completion markers", expectedCount)
}

// awaitTaskCompletedOnAnyNode polls until OnTaskCompleted has fired on at least one node.
func awaitTaskCompletedOnAnyNode(t *testing.T, compose *docker.DockerCompose, taskID string) {
	t.Helper()

	require.Eventually(t, func() bool {
		for i := 1; i <= 3; i++ {
			node := compose.GetWeaviateNode(i)
			status := getDebugStatus(t, node.DebugURI(), taskID)
			if status.TaskCompleted {
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "OnTaskCompleted should fire on at least one node")
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
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		Start(ctx)
	require.NoError(t, err)

	return compose, func() { require.NoError(t, compose.Terminate(ctx)) }
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

// awaitTaskStatusOK is like awaitTaskStatus but returns false instead of failing the test.
func awaitTaskStatusOK(t *testing.T, restURI, taskID, expectedStatus string) bool {
	t.Helper()

	return assert.Eventually(t, func() bool {
		tasks := listTasks(t, restURI)
		for _, task := range tasks["shard-noop"] {
			if task.ID == taskID && task.Status == expectedStatus {
				return true
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "task %s should reach %s status", taskID, expectedStatus)
}

// dumpTaskAndLogs prints the task state and relevant container logs for debugging.
func dumpTaskAndLogs(t *testing.T, ctx context.Context, compose *docker.DockerCompose, restURI, taskID string) {
	t.Helper()

	task := findTask(t, restURI, taskID)
	taskJSON, _ := json.MarshalIndent(task, "", "  ")
	t.Logf("task state on failure:\n%s", taskJSON)

	for i := 1; i <= 3; i++ {
		logs, err := compose.GetWeaviateNode(i).Container().Logs(ctx)
		if err != nil {
			t.Logf("node%d: failed to get logs: %v", i, err)
			continue
		}
		buf, _ := io.ReadAll(logs)
		logs.Close()
		for _, line := range strings.Split(string(buf), "\n") {
			if strings.Contains(line, "shard-noop") || strings.Contains(line, "distributed") {
				t.Logf("node%d: %s", i, line)
			}
		}
	}
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
