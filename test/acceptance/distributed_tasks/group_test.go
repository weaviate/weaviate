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
	"sort"
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
	// group's units all complete. Two groups with 3 units each, all succeed.
	t.Run("IndependentGroups", func(t *testing.T) {
		taskID := "group-independent"
		units := []string{"g1-su1", "g1-su2", "g1-su3", "g2-su1", "g2-su2", "g2-su3"}
		unitGroups := map[string]string{
			"g1-su1": "group-A",
			"g1-su2": "group-A",
			"g1-su3": "group-A",
			"g2-su1": "group-B",
			"g2-su2": "group-B",
			"g2-su3": "group-B",
		}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:         taskID,
			Units:      units,
			UnitGroups: unitGroups,
		})

		awaitTaskStatus(t, restURI, taskID, "FINISHED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, 6)

		for _, su := range task.SubUnits {
			assert.Equal(t, "COMPLETED", su.Status, "unit %s should be completed", su.ID)
		}

		awaitGroupFinalizedUnits(t, ctx, compose, taskID, "group-A", []string{"g1-su1", "g1-su2", "g1-su3"})
		awaitGroupFinalizedUnits(t, ctx, compose, taskID, "group-B", []string{"g2-su1", "g2-su2", "g2-su3"})
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// OneGroupFails verifies that when one group's unit fails (task → FAILED),
	// OnTaskCompleted still fires with FAILED status.
	t.Run("OneGroupFails", func(t *testing.T) {
		taskID := "group-one-fails"
		units := []string{"g1-su1", "g1-su2", "g2-su1", "g2-su2"}
		unitGroups := map[string]string{
			"g1-su1": "group-A",
			"g1-su2": "group-A",
			"g2-su1": "group-B",
			"g2-su2": "group-B",
		}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:         taskID,
			Units:      units,
			UnitGroups: unitGroups,
			FailUnit:   "g2-su1",
		})

		awaitTaskStatus(t, restURI, taskID, "FAILED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FAILED", task.Status)
		assert.Contains(t, task.Error, "dummy failure")

		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// ManyGroups verifies group finalization at scale: 20 groups with 2 units
	// each, MaxConcurrency=4.
	t.Run("ManyGroups", func(t *testing.T) {
		const numGroups = 20
		const unitsPerGroup = 2

		taskID := "group-many"
		units, unitGroups := buildGroupUnits(numGroups, unitsPerGroup)

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:             taskID,
			Units:          units,
			UnitGroups:     unitGroups,
			MaxConcurrency: 4,
		})

		if !awaitTaskStatusOK(t, restURI, taskID, "FINISHED") {
			dumpTaskAndLogs(t, ctx, compose, restURI, taskID)
			t.FailNow()
		}

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, numGroups*unitsPerGroup)

		for _, su := range task.SubUnits {
			assert.Equal(t, "COMPLETED", su.Status, "unit %s should be completed", su.ID)
		}

		spotCheckGroupFinalization(t, ctx, compose, taskID, unitsPerGroup, 0, 9, 19)
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})

	// DefaultGroup verifies that tasks without explicit groups (default group "")
	// preserve old behavior: finalization markers appear directly under
	// ./data/.dtm/dtm-finalize/{taskID}/ (no group subdirectory).
	t.Run("DefaultGroup", func(t *testing.T) {
		taskID := "group-default"
		units := []string{"su-1", "su-2", "su-3"}

		addTaskJSON(t, debugURI, addTaskRequest{
			ID:    taskID,
			Units: units,
		})

		awaitTaskStatus(t, restURI, taskID, "FINISHED")

		task := findTask(t, restURI, taskID)
		assert.Equal(t, "FINISHED", task.Status)
		require.NotNil(t, task.SubUnits)
		assert.Len(t, task.SubUnits, 3)

		awaitFinalizedUnits(t, ctx, compose, taskID, units)
		awaitTaskCompletedOnAnyNode(t, compose, taskID)
	})
}

// ---------------------------------------------------------------------------
// Group-specific helpers
// ---------------------------------------------------------------------------

// buildGroupUnits generates unit IDs and group assignments for numGroups groups,
// each with unitsPerGroup units.
func buildGroupUnits(numGroups, unitsPerGroup int) ([]string, map[string]string) {
	var units []string
	unitGroups := make(map[string]string)
	for g := 0; g < numGroups; g++ {
		groupID := fmt.Sprintf("grp-%03d", g)
		for s := 0; s < unitsPerGroup; s++ {
			suID := fmt.Sprintf("%s-su%d", groupID, s)
			units = append(units, suID)
			unitGroups[suID] = groupID
		}
	}
	return units, unitGroups
}

// spotCheckGroupFinalization verifies finalization markers for a few selected groups.
func spotCheckGroupFinalization(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID string, unitsPerGroup int, groups ...int) {
	t.Helper()
	for _, g := range groups {
		groupID := fmt.Sprintf("grp-%03d", g)
		var expected []string
		for s := 0; s < unitsPerGroup; s++ {
			expected = append(expected, fmt.Sprintf("%s-su%d", groupID, s))
		}
		awaitGroupFinalizedUnits(t, ctx, compose, taskID, groupID, expected)
	}
}

// awaitGroupFinalizedUnits polls until expected finalization markers appear
// under ./data/.dtm/dtm-finalize/{taskID}/{groupID}/ across all cluster nodes.
func awaitGroupFinalizedUnits(t *testing.T, ctx context.Context, compose *docker.DockerCompose, taskID, groupID string, expected []string) {
	t.Helper()

	dir := fmt.Sprintf("./data/.dtm/dtm-finalize/%s/%s", taskID, groupID)
	sort.Strings(expected)
	require.Eventually(t, func() bool {
		all := collectSyntheticMarkersFromCluster(t, ctx, compose, dir)
		sort.Strings(all)
		return fmt.Sprintf("%v", all) == fmt.Sprintf("%v", expected)
	}, 60*time.Second, 500*time.Millisecond, "expected group %s finalization markers %v", groupID, expected)
}
