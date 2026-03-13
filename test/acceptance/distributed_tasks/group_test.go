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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
)

// ---------------------------------------------------------------------------
// Group finalization acceptance tests (shared cluster)
// ---------------------------------------------------------------------------

func TestGroupFinalizationSuite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, cleanup := start3NodeDTMCluster(ctx, t)
	defer cleanup()

	restURI := compose.GetWeaviate().URI()
	debugURI := compose.GetWeaviate().DebugURI()

	// IndependentGroups verifies that OnGroupCompleted fires per-group as each
	// group's sub-units all complete. Two groups with 3 sub-units each, all succeed.
	t.Run("IndependentGroups", func(t *testing.T) {
		taskID := "group-independent"
		subUnits := []string{"g1-su1", "g1-su2", "g1-su3", "g2-su1", "g2-su2", "g2-su3"}
		subUnitGroups := map[string]string{
			"g1-su1": "group-A",
			"g1-su2": "group-A",
			"g1-su3": "group-A",
			"g2-su1": "group-B",
			"g2-su2": "group-B",
			"g2-su3": "group-B",
		}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:            taskID,
			SubUnits:      subUnits,
			SubUnitGroups: subUnitGroups,
		})

		awaitTaskStatus(t, restURI, taskID, "FINISHED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, 6)

		for _, su := range task.SubUnits {
			assert.Equal(t, "COMPLETED", su.Status, "sub-unit %s should be completed", su.ID)
		}

		awaitGroupFinalizedSubUnits(t, ctx, compose, taskID, "group-A", []string{"g1-su1", "g1-su2", "g1-su3"})
		awaitGroupFinalizedSubUnits(t, ctx, compose, taskID, "group-B", []string{"g2-su1", "g2-su2", "g2-su3"})
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// OneGroupFails verifies that when one group's sub-unit fails (task → FAILED),
	// OnTaskCompleted still fires with FAILED status.
	t.Run("OneGroupFails", func(t *testing.T) {
		taskID := "group-one-fails"
		subUnits := []string{"g1-su1", "g1-su2", "g2-su1", "g2-su2"}
		subUnitGroups := map[string]string{
			"g1-su1": "group-A",
			"g1-su2": "group-A",
			"g2-su1": "group-B",
			"g2-su2": "group-B",
		}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:            taskID,
			SubUnits:      subUnits,
			SubUnitGroups: subUnitGroups,
			FailSubUnit:   "g2-su1",
		})

		awaitTaskStatus(t, restURI, taskID, "FAILED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FAILED", task.Status)
		assert.Contains(t, task.Error, "dummy failure")

		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// ManyGroups verifies group finalization at scale: 20 groups with 2 sub-units
	// each, MaxConcurrency=4.
	t.Run("ManyGroups", func(t *testing.T) {
		const numGroups = 20
		const subUnitsPerGroup = 2

		taskID := "group-many"
		subUnits, subUnitGroups := buildGroupSubUnits(numGroups, subUnitsPerGroup)

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:             taskID,
			SubUnits:       subUnits,
			SubUnitGroups:  subUnitGroups,
			MaxConcurrency: 4,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, numGroups*subUnitsPerGroup)

		for _, su := range task.SubUnits {
			assert.Equal(t, "COMPLETED", su.Status, "sub-unit %s should be completed", su.ID)
		}

		spotCheckGroupFinalization(t, ctx, compose, taskID, subUnitsPerGroup, 0, 9, 19)
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// DefaultGroup verifies that tasks without explicit groups (default group "")
	// preserve old behavior: finalization markers appear directly under
	// /tmp/dtm-finalize/{taskID}/ (no group subdirectory).
	t.Run("DefaultGroup", func(t *testing.T) {
		taskID := "group-default"
		subUnits := []string{"su-1", "su-2", "su-3"}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:       taskID,
			SubUnits: subUnits,
		})

		awaitTaskStatus(t, restURI, taskID, "FINISHED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, 3)

		awaitFinalizedSubUnits(t, ctx, compose, taskID, subUnits)
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})
}

// ---------------------------------------------------------------------------
// Group-specific helpers
// ---------------------------------------------------------------------------

// buildGroupSubUnits generates sub-unit IDs and group assignments for numGroups groups,
// each with subUnitsPerGroup sub-units.
func buildGroupSubUnits(numGroups, subUnitsPerGroup int) ([]string, map[string]string) {
	var subUnits []string
	subUnitGroups := make(map[string]string)
	for g := 0; g < numGroups; g++ {
		groupID := fmt.Sprintf("grp-%03d", g)
		for s := 0; s < subUnitsPerGroup; s++ {
			suID := fmt.Sprintf("%s-su%d", groupID, s)
			subUnits = append(subUnits, suID)
			subUnitGroups[suID] = groupID
		}
	}
	return subUnits, subUnitGroups
}

// spotCheckGroupFinalization verifies finalization markers for a few selected groups.
func spotCheckGroupFinalization(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, subUnitsPerGroup int, groups ...int) {
	t.Helper()
	for _, g := range groups {
		groupID := fmt.Sprintf("grp-%03d", g)
		var expected []string
		for s := 0; s < subUnitsPerGroup; s++ {
			expected = append(expected, fmt.Sprintf("%s-su%d", groupID, s))
		}
		awaitGroupFinalizedSubUnits(t, ctx, compose, taskID, groupID, expected)
	}
}

// awaitGroupFinalizedSubUnits polls until expected finalization markers appear
// under /tmp/dtm-finalize/{taskID}/{groupID}/ across all cluster nodes.
func awaitGroupFinalizedSubUnits(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID, groupID string, expected []string) {
	t.Helper()
	awaitMarkers(t, ctx, compose, fmt.Sprintf("dtm-finalize/%s", taskID), groupID, expected)
}
