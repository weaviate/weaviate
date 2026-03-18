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

const shardNoopNamespace = "shard-noop"

// ---------------------------------------------------------------------------
// Single-node tests (synthetic units)
// ---------------------------------------------------------------------------

func TestUnitTaskLifecycle_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "unit-success-test"
	addTaskJSON(t, debugURI, addTaskRequest{
		ID:    taskID,
		Units: []string{"su-1", "su-2", "su-3"},
	})
	awaitTaskStatus(t, restURI, taskID, "FINISHED")

	task := findTask(t, restURI, taskID)
	assert.Equal(t, "FINISHED", task.Status)
	require.NotNil(t, task.SubUnits)
	assert.Len(t, task.SubUnits, 3)

	for _, su := range task.SubUnits {
		assert.Equal(t, "COMPLETED", su.Status, "unit %s should be completed", su.ID)
		assert.Equal(t, float32(1.0), su.Progress, "unit %s should have progress 1.0", su.ID)
	}
}

func TestUnitTaskLifecycle_Failure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	restURI, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	taskID := "unit-failure-test"
	addTaskJSON(t, debugURI, addTaskRequest{
		ID:       taskID,
		Units:    []string{"su-1", "su-2", "su-3"},
		FailUnit: "su-2",
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

func TestTask_NoUnits_Rejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, debugURI, cleanup := startDTMCluster(ctx, t)
	defer cleanup()

	// Tasks must have at least one unit; zero-unit (legacy) tasks are rejected.
	body, err := json.Marshal(addTaskRequest{ID: "no-units"})
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/debug/distributed-tasks/add", debugURI),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	respBody, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(respBody), "must have at least one unit")
}

// ---------------------------------------------------------------------------
// 3-node synthetic unit tests (finalization)
// ---------------------------------------------------------------------------

func TestUnitTask_PerShardFinalize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	tests := []struct {
		name           string
		taskID         string
		units          []string
		failUnit       string
		expectedStatus string
	}{
		{
			name:           "MoreSubUnitsThanNodes",
			taskID:         "finalize-more-units",
			units:          []string{"su-1", "su-2", "su-3", "su-4", "su-5", "su-6"},
			expectedStatus: "FINISHED",
		},
		{
			name:           "FewerSubUnitsThanNodes",
			taskID:         "finalize-fewer-units",
			units:          []string{"su-1", "su-2"},
			expectedStatus: "FINISHED",
		},
		{
			name:           "OneSubUnit",
			taskID:         "finalize-one-unit",
			units:          []string{"su-only"},
			expectedStatus: "FINISHED",
		},
		{
			name:           "OnFailure",
			taskID:         "finalize-on-failure",
			units:          []string{"su-1"},
			failUnit:       "su-1",
			expectedStatus: "FAILED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
				ID:       tc.taskID,
				Units:    tc.units,
				FailUnit: tc.failUnit,
			})
			awaitTaskStatus(t, compose.GetWeaviate().URI(), tc.taskID, tc.expectedStatus)

			awaitFinalizedUnits(t, ctx, compose, tc.taskID, tc.units)
			awaitTaskCompletedOnAnyNode(t, compose, tc.taskID)
		})
	}
}

// ---------------------------------------------------------------------------
// Standalone real-collection tests (fast feedback)
// ---------------------------------------------------------------------------

func TestRealCollection_RF1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	runStandardCollectionTest(t, ctx, compose, collectionTestCase{
		name:          "RF1_Shards3",
		className:     "DTMTestRF1",
		shardCount:    3,
		rf:            1,
		expectedUnits: 3,
		taskID:        "real-collection-rf1",
	})
}

func TestMultiTenant_RF1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	runStandardMTTest(t, ctx, compose, mtTestCase{
		name:          "RF1_10Tenants",
		className:     "DTMTestMTRF1",
		rf:            1,
		tenantCount:   10,
		expectedUnits: 10,
		taskID:        "mt-rf1-10",
	})
}

// ---------------------------------------------------------------------------
// Real-collection edge case suite (shared cluster)
// ---------------------------------------------------------------------------

func TestRealCollectionSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	standardCases := []collectionTestCase{
		{name: "RF2_Shards3", className: "DTMSuiteRF2S3", shardCount: 3, rf: 2, expectedUnits: 6, taskID: "suite-rf2-shards3"},
		{name: "RF3_Shards1", className: "DTMSuiteRF3S1", shardCount: 1, rf: 3, expectedUnits: 3, taskID: "suite-rf3-shards1"},
		{name: "RF3_Shards3", className: "DTMSuiteRF3S3", shardCount: 3, rf: 3, expectedUnits: 9, taskID: "suite-rf3-shards3"},
		{name: "RF2_Shards24", className: "DTMSuiteRF2S24", shardCount: 24, rf: 2, expectedUnits: 48, taskID: "suite-rf2-shards24"},
		{name: "RF1_Shards2_IdleNode", className: "DTMSuiteRF1S2Idle", shardCount: 2, rf: 1, expectedUnits: 2, taskID: "suite-rf1-shards2-idle"},
	}
	for _, tc := range standardCases {
		t.Run(tc.name, func(t *testing.T) {
			runStandardCollectionTest(t, ctx, compose, tc)
		})
	}

	restURI := compose.GetWeaviate().URI()

	t.Run("RF1_Failure", func(t *testing.T) {
		className := "DTMSuiteRF1Fail"
		createCollection(t, restURI, className, 3, 1)

		placements := getShardPlacement(t, restURI, className, 3)
		unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(placements)
		failUnit := unitIDs[0]

		taskID := "suite-rf1-failure"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			Units:             unitIDs,
			Collection:        className,
			UnitToShard:       unitToShard,
			UnitToNode:        unitToNode,
			FailUnit:          failUnit,
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
		unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(targetPlacements)
		require.Len(t, unitIDs, 3)

		taskID := "suite-partial-targeting"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			Units:             unitIDs,
			Collection:        className,
			UnitToShard:       unitToShard,
			UnitToNode:        unitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 3)

		// Verify only targeted units have processing markers
		awaitProcessingMarkers(t, ctx, compose, taskID, unitIDs, className)

		deleteCollection(t, restURI, className)
	})

	t.Run("TemporalOrdering", func(t *testing.T) {
		className := "DTMSuiteTemporal"
		createCollection(t, restURI, className, 3, 1)

		placements := getShardPlacement(t, restURI, className, 3)
		unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(placements)

		// Make the first unit slow
		slowUnit := unitIDs[0]

		taskID := "suite-temporal-ordering"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			Units:             unitIDs,
			Collection:        className,
			UnitToShard:       unitToShard,
			UnitToNode:        unitToNode,
			SlowUnit:          slowUnit,
			SlowDelayMs:       5000,
			ProcessingDelayMs: 10,
		})

		// Fast units should complete quickly. Wait until at least one
		// non-slow unit is COMPLETED in the task API.
		fastUnits := unitIDs[1:]
		require.Eventually(t, func() bool {
			task := findTask(t, restURI, taskID)
			if task == nil {
				return false
			}
			for _, su := range task.SubUnits {
				for _, fsu := range fastUnits {
					if su.ID == fsu && su.Status == "COMPLETED" {
						return true
					}
				}
			}
			return false
		}, 15*time.Second, 500*time.Millisecond, "fast units should complete before slow one")

		// At this point, NO finalization markers should exist yet because the
		// slow unit hasn't completed → the per-shard barrier is not met.
		baseDir := collectionMarkerBaseDir(className)
		finPattern := fmt.Sprintf(".dtm-finalize--%s--*", taskID)
		finMarkers := collectDotMarkersFromCluster(t, ctx, compose, baseDir, finPattern)
		t.Logf("finalization markers during slow phase: %v", finMarkers)
		// Note: we can't strictly assert zero here because the slow unit
		// might be on a different node and that node's fast units might
		// already have finalization. We verify the full set after completion.

		// Wait for the task to finish (slow unit completes)
		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		// Now all markers should appear
		awaitProcessingMarkers(t, ctx, compose, taskID, unitIDs, className)
		awaitFinalizedUnits(t, ctx, compose, taskID, unitIDs, className)
		awaitCompletionMarkers(t, ctx, compose, taskID, 3, className)

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

	standardCases := []mtTestCase{
		{name: "RF1_100Tenants", className: "DTMMTRF1T100", rf: 1, tenantCount: 100, expectedUnits: 100, taskID: "mt-rf1-100"},
		{name: "RF3_10Tenants", className: "DTMMTRF3T10", rf: 3, tenantCount: 10, expectedUnits: 30, taskID: "mt-rf3-10"},
		{name: "RF3_100Tenants", className: "DTMMTRF3T100", rf: 3, tenantCount: 100, expectedUnits: 300, taskID: "mt-rf3-100"},
	}
	for _, tc := range standardCases {
		t.Run(tc.name, func(t *testing.T) {
			runStandardMTTest(t, ctx, compose, tc)
		})
	}

	restURI := compose.GetWeaviate().URI()

	t.Run("RF1_PartialTargeting", func(t *testing.T) {
		className := "DTMMTPartialRF1"
		createMTCollection(t, restURI, className, 1)
		createTenants(t, restURI, className, 10)

		allPlacements := getShardPlacement(t, restURI, className, 10)
		targetPlacements := allPlacements[:5]
		unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(targetPlacements)
		require.Len(t, unitIDs, 5)

		taskID := "mt-rf1-partial"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			Units:             unitIDs,
			Collection:        className,
			UnitToShard:       unitToShard,
			UnitToNode:        unitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 5)

		awaitProcessingMarkers(t, ctx, compose, taskID, unitIDs, className)

		deleteCollection(t, restURI, className)
	})

	t.Run("RF3_PartialTargeting", func(t *testing.T) {
		className := "DTMMTPartialRF3"
		createMTCollection(t, restURI, className, 3)
		createTenants(t, restURI, className, 10)

		allPlacements := getShardPlacement(t, restURI, className, 30) // 10 * RF3

		// Target 5 tenants → 15 units (5 tenants * RF3)
		// Group by shard name, pick first 5 unique shards
		shardSeen := map[string]bool{}
		var targetPlacements []shardPlacement
		for _, p := range allPlacements {
			if len(shardSeen) < 5 || shardSeen[p.ShardName] {
				shardSeen[p.ShardName] = true
				targetPlacements = append(targetPlacements, p)
			}
		}
		unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(targetPlacements)
		require.Len(t, unitIDs, 15)

		taskID := "mt-rf3-partial"
		addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
			ID:                taskID,
			Units:             unitIDs,
			Collection:        className,
			UnitToShard:       unitToShard,
			UnitToNode:        unitToNode,
			ProcessingDelayMs: 10,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		assert.Len(t, task.SubUnits, 15)

		awaitProcessingMarkers(t, ctx, compose, taskID, unitIDs, className)

		deleteCollection(t, restURI, className)
	})
}

// ---------------------------------------------------------------------------
// Table-driven test helpers
// ---------------------------------------------------------------------------

type collectionTestCase struct {
	name          string
	className     string
	shardCount    int
	rf            int
	expectedUnits int
	taskID        string
}

// runStandardCollectionTest runs the standard create-collection → add-task → await → verify → cleanup flow.
func runStandardCollectionTest(t *testing.T, ctx context.Context, compose *docker.DockerCompose, tc collectionTestCase) {
	t.Helper()

	restURI := compose.GetWeaviate().URI()
	createCollection(t, restURI, tc.className, tc.shardCount, tc.rf)

	placements := getShardPlacement(t, restURI, tc.className, tc.expectedUnits)
	unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(placements)
	require.Len(t, unitIDs, tc.expectedUnits)

	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:                tc.taskID,
		Units:             unitIDs,
		Collection:        tc.className,
		UnitToShard:       unitToShard,
		UnitToNode:        unitToNode,
		ProcessingDelayMs: 10,
	})

	if !awaitTaskStatusOK(t, restURI, tc.taskID, "FINISHED") {
		dumpTaskAndLogs(t, ctx, compose, restURI, tc.taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, tc.taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, tc.expectedUnits)

	awaitProcessingMarkers(t, ctx, compose, tc.taskID, unitIDs, tc.className)
	awaitFinalizedUnits(t, ctx, compose, tc.taskID, unitIDs, tc.className)
	awaitCompletionMarkers(t, ctx, compose, tc.taskID, 3, tc.className)

	deleteCollection(t, restURI, tc.className)
}

type mtTestCase struct {
	name          string
	className     string
	rf            int
	tenantCount   int
	expectedUnits int
	taskID        string
}

// runStandardMTTest runs the standard create-MT-collection → create-tenants → add-task → await → verify → cleanup flow.
func runStandardMTTest(t *testing.T, ctx context.Context, compose *docker.DockerCompose, tc mtTestCase) {
	t.Helper()

	restURI := compose.GetWeaviate().URI()
	createMTCollection(t, restURI, tc.className, tc.rf)
	createTenants(t, restURI, tc.className, tc.tenantCount)

	placements := getShardPlacement(t, restURI, tc.className, tc.expectedUnits)
	unitIDs, unitToShard, unitToNode := buildPerReplicaUnits(placements)
	require.Len(t, unitIDs, tc.expectedUnits)

	addTaskJSON(t, compose.GetWeaviate().DebugURI(), addTaskRequest{
		ID:                tc.taskID,
		Units:             unitIDs,
		Collection:        tc.className,
		UnitToShard:       unitToShard,
		UnitToNode:        unitToNode,
		ProcessingDelayMs: 10,
	})

	if !awaitTaskStatusOK(t, restURI, tc.taskID, "FINISHED") {
		dumpTaskAndLogs(t, ctx, compose, restURI, tc.taskID)
		t.FailNow()
	}

	task := findTask(t, restURI, tc.taskID)
	assert.Equal(t, "FINISHED", task.Status)
	assert.Len(t, task.SubUnits, tc.expectedUnits)

	awaitProcessingMarkers(t, ctx, compose, tc.taskID, unitIDs, tc.className)
	awaitFinalizedUnits(t, ctx, compose, tc.taskID, unitIDs, tc.className)
	awaitCompletionMarkers(t, ctx, compose, tc.taskID, 3, tc.className)

	deleteCollection(t, restURI, tc.className)
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
	Units             []string          `json:"units,omitempty"`
	UnitGroups        map[string]string `json:"unitGroups,omitempty"` // unitID → groupID
	FailUnit          string            `json:"failUnit,omitempty"`
	Collection        string            `json:"collection,omitempty"`
	UnitToShard       map[string]string `json:"unitToShard,omitempty"`
	UnitToNode        map[string]string `json:"unitToNode,omitempty"`
	SlowUnit          string            `json:"slowUnit,omitempty"`
	SlowDelayMs       int               `json:"slowDelayMs,omitempty"`
	ProcessingDelayMs int               `json:"processingDelayMs,omitempty"`
	MaxConcurrency    int               `json:"maxConcurrency,omitempty"`
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

// buildPerReplicaUnits creates unit IDs, UnitToShard, and UnitToNode maps from placement.
func buildPerReplicaUnits(placements []shardPlacement) (unitIDs []string, shardMap, nodeMap map[string]string) {
	shardMap = make(map[string]string, len(placements))
	nodeMap = make(map[string]string, len(placements))
	for _, p := range placements {
		suID := fmt.Sprintf("%s__%s", p.ShardName, p.NodeName)
		unitIDs = append(unitIDs, suID)
		shardMap[suID] = p.ShardName
		nodeMap[suID] = p.NodeName
	}
	sort.Strings(unitIDs)
	return unitIDs, shardMap, nodeMap
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

// listFilesInDir lists files in a container directory.
func listFilesInDir(ctx context.Context, c testcontainers.Container, dir string) []string {
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

// findDotMarkers searches for hidden marker files matching a name pattern under baseDir
// inside a container. Returns the base file names of all matches.
func findDotMarkers(ctx context.Context, c testcontainers.Container, baseDir, namePattern string) []string {
	code, reader, err := c.Exec(ctx, []string{
		"find", baseDir, "-name", namePattern, "-type", "f",
	}, tcexec.Multiplexed())
	if err != nil || code != 0 {
		return nil
	}
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, reader); err != nil {
		return nil
	}
	var names []string
	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			// Extract base name from full path
			parts := strings.Split(line, "/")
			names = append(names, parts[len(parts)-1])
		}
	}
	return names
}

// collectDotMarkersFromCluster finds marker files matching a pattern under baseDir across all 3 nodes.
func collectDotMarkersFromCluster(t *testing.T, ctx context.Context, compose *docker.DockerCompose, baseDir, namePattern string) []string {
	t.Helper()

	var all []string
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		names := findDotMarkers(ctx, node.Container(), baseDir, namePattern)
		all = append(all, names...)
	}
	return all
}

// extractUnitIDsFromMarkers extracts unit IDs from marker filenames.
// Marker names use "--" as separator: .dtm-{type}--{taskID}--{suID} (or with groupID).
// The unit ID is always the last "--"-delimited segment.
func extractUnitIDsFromMarkers(markerNames []string) []string {
	var ids []string
	for _, name := range markerNames {
		parts := strings.Split(name, "--")
		if len(parts) >= 3 {
			ids = append(ids, parts[len(parts)-1])
		}
	}
	return ids
}

// collectSyntheticMarkersFromCluster collects marker files from a directory under
// ./data/.dtm/ across all 3 nodes.
func collectSyntheticMarkersFromCluster(t *testing.T, ctx context.Context, compose *docker.DockerCompose, dir string) []string {
	t.Helper()

	var all []string
	for i := 1; i <= 3; i++ {
		node := compose.GetWeaviateNode(i)
		files := listFilesInDir(ctx, node.Container(), dir)
		all = append(all, files...)
	}
	return all
}

// awaitSyntheticMarkers polls until expected marker files appear under ./data/.dtm/
// in the given subdirectory across the cluster.
func awaitSyntheticMarkers(t *testing.T, ctx context.Context, compose *docker.DockerCompose, subdir, taskID string, expected []string) {
	t.Helper()

	dir := fmt.Sprintf("./data/.dtm/%s/%s", subdir, taskID)
	sort.Strings(expected)
	require.Eventually(t, func() bool {
		all := collectSyntheticMarkersFromCluster(t, ctx, compose, dir)
		sort.Strings(all)
		return fmt.Sprintf("%v", all) == fmt.Sprintf("%v", expected)
	}, 60*time.Second, 500*time.Millisecond, "expected %s markers %v", subdir, expected)
}

// collectionMarkerBaseDir returns the base directory for collection-aware markers
// inside a container: ./data/{lowercase(collection)}.
func collectionMarkerBaseDir(collection string) string {
	return fmt.Sprintf("./data/%s", strings.ToLower(collection))
}

// awaitProcessingMarkers polls until expected processing marker files appear across the cluster.
// When collection is non-empty, searches for dot-files inside shard directories.
// When collection is empty (synthetic mode), searches under ./data/.dtm/.
func awaitProcessingMarkers(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expected []string, collection ...string) {
	t.Helper()
	sort.Strings(expected)

	coll := ""
	if len(collection) > 0 {
		coll = collection[0]
	}

	if coll != "" {
		baseDir := collectionMarkerBaseDir(coll)
		pattern := fmt.Sprintf(".dtm-process--%s--*", taskID)
		require.Eventually(t, func() bool {
			markers := collectDotMarkersFromCluster(t, ctx, compose, baseDir, pattern)
			ids := extractUnitIDsFromMarkers(markers)
			sort.Strings(ids)
			return fmt.Sprintf("%v", ids) == fmt.Sprintf("%v", expected)
		}, 60*time.Second, 500*time.Millisecond, "expected processing markers for %v", expected)
	} else {
		awaitSyntheticMarkers(t, ctx, compose, "dtm-process", taskID, expected)
	}
}

// awaitFinalizedUnits polls until expected finalization marker files appear across the cluster.
// When collection is non-empty, searches for dot-files inside shard directories.
// When collection is empty (synthetic mode), searches under ./data/.dtm/.
func awaitFinalizedUnits(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expected []string, collection ...string) {
	t.Helper()
	sort.Strings(expected)

	coll := ""
	if len(collection) > 0 {
		coll = collection[0]
	}

	if coll != "" {
		baseDir := collectionMarkerBaseDir(coll)
		pattern := fmt.Sprintf(".dtm-finalize--%s--*", taskID)
		require.Eventually(t, func() bool {
			markers := collectDotMarkersFromCluster(t, ctx, compose, baseDir, pattern)
			ids := extractUnitIDsFromMarkers(markers)
			sort.Strings(ids)
			return fmt.Sprintf("%v", ids) == fmt.Sprintf("%v", expected)
		}, 60*time.Second, 500*time.Millisecond, "expected finalization markers for %v", expected)
	} else {
		awaitSyntheticMarkers(t, ctx, compose, "dtm-finalize", taskID, expected)
	}
}

// awaitCompletionMarkers polls until the expected number of completion markers appear across the cluster.
// When collection is non-empty, searches for dot-files in the collection's index directory.
// When collection is empty (synthetic mode), searches under ./data/.dtm/.
func awaitCompletionMarkers(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, expectedCount int, collection ...string) {
	t.Helper()

	coll := ""
	if len(collection) > 0 {
		coll = collection[0]
	}

	if coll != "" {
		baseDir := collectionMarkerBaseDir(coll)
		pattern := fmt.Sprintf(".dtm-complete--%s--*", taskID)
		require.Eventually(t, func() bool {
			markers := collectDotMarkersFromCluster(t, ctx, compose, baseDir, pattern)
			return len(markers) >= expectedCount
		}, 60*time.Second, 500*time.Millisecond, "expected %d completion markers", expectedCount)
	} else {
		dir := fmt.Sprintf("./data/.dtm/dtm-complete/%s", taskID)
		require.Eventually(t, func() bool {
			all := collectSyntheticMarkersFromCluster(t, ctx, compose, dir)
			return len(all) >= expectedCount
		}, 60*time.Second, 500*time.Millisecond, "expected %d completion markers", expectedCount)
	}
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
		for _, task := range tasks[shardNoopNamespace] {
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
		for _, task := range tasks[shardNoopNamespace] {
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
	for i := range tasks[shardNoopNamespace] {
		if tasks[shardNoopNamespace][i].ID == taskID {
			return &tasks[shardNoopNamespace][i]
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
