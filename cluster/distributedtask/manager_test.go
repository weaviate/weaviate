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

package distributedtask

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestManager_AddTask_Failures(t *testing.T) {
	t.Run("add duplicate task", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             "test",
				Id:                    "1",
				SubmittedAtUnixMillis: time.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(c, version)
		require.NoError(t, err)

		err = h.manager.AddTask(c, version)
		require.ErrorContains(t, err, "already running")
	})

	t.Run("add task with the same version as already finished one", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace  = "test"
			taskID     = "1"
			addTaskCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		completeUnit(t, h, namespace, taskID, version, "local-node", "su-1")

		err = h.manager.AddTask(addTaskCmd, version)
		require.ErrorContains(t, err, "already finished with version")
	})

	t.Run("add task with a lower version as already finished one", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace  = "test"
			taskID     = "1"
			addTaskCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		completeUnit(t, h, namespace, taskID, version, "local-node", "su-1")

		err = h.manager.AddTask(addTaskCmd, version-10)
		require.ErrorContains(t, err, "already finished with version")
	})

	t.Run("add task without units", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		})

		err := h.manager.AddTask(c, 100)
		require.ErrorContains(t, err, "must have at least one unit")
	})
}

func TestManager_CancelTask_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace:             "test",
				Id:                    "1",
				Version:               1,
				CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.CancelTask(c)
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task with the given version does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				Version:               version - 1,
				CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CancelTask(cancelCmd)
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task is already cancelled", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"su-1"},
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace:             "test",
				Id:                    "1",
				Version:               version,
				CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CancelTask(cancelCmd)
		require.NoError(t, err)

		err = h.manager.CancelTask(cancelCmd)
		require.ErrorContains(t, err, "no longer running")
	})
}

func TestManager_CleanUpTask_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: "test",
				Id:        "1",
				Version:   1,
			})
		)

		err := h.manager.CleanUpTask(c)
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task with the given version does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * h.completedTaskTTL).UnixMilli(),
				UnitIds:               []string{"su-1"},
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: namespace,
				Id:        taskID,
				Version:   version - 1,
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CleanUpTask(cleanUpCmd)
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task is still running", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * h.completedTaskTTL).UnixMilli(),
				UnitIds:               []string{"su-1"},
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: namespace,
				Id:        taskID,
				Version:   version,
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CleanUpTask(cleanUpCmd)
		require.ErrorContains(t, err, "still running")
	})

	t.Run("completed task TTL did not pass yet", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * h.completedTaskTTL).UnixMilli(),
				UnitIds:               []string{"su-1"},
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				Version:               version,
				CancelledAtUnixMillis: h.clock.Now().Add(-h.completedTaskTTL).Add(time.Minute).UnixMilli(),
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: namespace,
				Id:        taskID,
				Version:   version,
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CancelTask(cancelCmd)
		require.NoError(t, err)

		err = h.manager.CleanUpTask(cleanUpCmd)
		require.ErrorContains(t, err, "too fresh")
	})
}

func TestManager_ListDistributedTasksPayload(t *testing.T) {
	var (
		h   = newTestHarness(t).init(t)
		now = h.clock.Now().Local().Truncate(time.Millisecond)
	)

	expectedTasks := ingestSampleTasks(t, h.manager, now)

	payload, err := h.manager.ListDistributedTasksPayload(context.Background())
	require.NoError(t, err)

	var resp ListDistributedTasksResponse
	require.NoError(t, json.Unmarshal(payload, &resp))

	assertTasks(t, expectedTasks, resp.Tasks)
}

func TestManager_SnapshotRestore(t *testing.T) {
	var (
		h   = newTestHarness(t).init(t)
		now = h.clock.Now().Truncate(time.Millisecond)
	)

	expectedTasks := ingestSampleTasks(t, h.manager, now)

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	h = newTestHarness(t).init(t)
	require.NoError(t, h.manager.Restore(snap))

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	assertTasks(t, expectedTasks, tasks)
}

func ingestSampleTasks(t *testing.T, m *Manager, now time.Time) map[string][]*Task {
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns1",
		Id:                    "task1",
		Payload:               []byte("test1"),
		SubmittedAtUnixMillis: now.UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))

	require.NoError(t, m.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             "ns1",
		Id:                    "task1",
		Version:               10,
		CancelledAtUnixMillis: now.Add(time.Minute).UnixMilli(),
	})))

	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns1",
		Id:                    "task2",
		Payload:               []byte("test2"),
		SubmittedAtUnixMillis: now.UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 13))

	require.NoError(t, m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            "ns1",
		Id:                   "task2",
		Version:              13,
		NodeId:               "local-node",
		UnitId:               "su-1",
		FinishedAtUnixMillis: now.Add(time.Minute).UnixMilli(),
	})))

	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns2",
		Id:                    "task3",
		Payload:               []byte("test3"),
		SubmittedAtUnixMillis: now.UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 15))

	return map[string][]*Task{
		"ns1": {
			{
				Namespace: "ns1",
				TaskDescriptor: TaskDescriptor{
					ID:      "task1",
					Version: 10,
				},
				Payload:    []byte("test1"),
				Status:     TaskStatusCancelled,
				StartedAt:  now,
				FinishedAt: now.Add(time.Minute),
				Units: map[string]*Unit{
					"su-1": {ID: "su-1", Status: UnitStatusPending},
				},
			},
			{
				Namespace: "ns1",
				TaskDescriptor: TaskDescriptor{
					ID:      "task2",
					Version: 13,
				},
				Payload:    []byte("test2"),
				Status:     TaskStatusFinished,
				StartedAt:  now,
				FinishedAt: now.Add(time.Minute),
				Units: map[string]*Unit{
					"su-1": {ID: "su-1", Status: UnitStatusCompleted, Progress: 1.0},
				},
			},
		},
		"ns2": {
			{
				Namespace: "ns2",
				TaskDescriptor: TaskDescriptor{
					ID:      "task3",
					Version: 15,
				},
				Payload:   []byte("test3"),
				Status:    TaskStatusStarted,
				StartedAt: now,
				Units: map[string]*Unit{
					"su-1": {ID: "su-1", Status: UnitStatusPending},
				},
			},
		},
	}
}

func assertTasks(t *testing.T, expected, actual map[string][]*Task) {
	require.Equal(t, len(expected), len(actual))
	for namespace := range expected {
		expectedTasks, ok := expected[namespace]
		require.True(t, ok)

		actualTasks, ok := actual[namespace]
		require.True(t, ok)

		require.Equal(t, len(expectedTasks), len(actualTasks))
		sortTasks := func(tasks []*Task) {
			sort.Slice(tasks, func(i, j int) bool {
				return tasks[i].ID < tasks[j].ID
			})
		}
		sortTasks(expectedTasks)
		sortTasks(actualTasks)

		for i := range expectedTasks {
			assertTask(t, expectedTasks[i], actualTasks[i])
		}
	}
}

func assertTask(t *testing.T, expected, actual *Task) {
	assert.Equal(t, expected.Namespace, actual.Namespace)
	assert.Equal(t, expected.TaskDescriptor, actual.TaskDescriptor)
	assert.Equal(t, expected.Payload, actual.Payload)
	assert.Equal(t, expected.Status, actual.Status)
	assert.Equal(t, expected.StartedAt.UTC(), actual.StartedAt.UTC())
	assert.Equal(t, expected.FinishedAt.UTC(), actual.FinishedAt.UTC())
	assert.Equal(t, expected.Error, actual.Error)
	assert.Equal(t, len(expected.Units), len(actual.Units))
	for id, expectedSU := range expected.Units {
		actualSU, ok := actual.Units[id]
		assert.True(t, ok, "unit %s should exist", id)
		if ok {
			assert.Equal(t, expectedSU.Status, actualSU.Status)
			assert.Equal(t, expectedSU.Progress, actualSU.Progress)
			assert.Equal(t, expectedSU.Error, actualSU.Error)
		}
	}
}

// addTaskWithUnits is a test helper that creates a task with the given units.
func addTaskWithUnits(t *testing.T, h *testHarness, ns, id string, version uint64, units []string) {
	t.Helper()
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             ns,
		Id:                    id,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               units,
	}), version)
	require.NoError(t, err)
}

// completeUnit records a successful unit completion.
func completeUnit(t *testing.T, h *testHarness, ns, id string, version uint64, node, unitID string) {
	t.Helper()
	err := h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		UnitId:               unitID,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

// failUnit records a failed unit completion.
func failUnit(t *testing.T, h *testHarness, ns, id string, version uint64, node, unitID, errMsg string) {
	t.Helper()
	err := h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		UnitId:               unitID,
		Error:                errMsg,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

// updateProgress updates a unit's progress value.
func updateProgress(t *testing.T, h *testHarness, ns, id string, version uint64, node, unitID string, progress float32) {
	t.Helper()
	err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
		Namespace:           ns,
		Id:                  id,
		Version:             version,
		NodeId:              node,
		UnitId:              unitID,
		Progress:            progress,
		UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

func TestManager_AddTask_WithUnits(t *testing.T) {
	h := newTestHarness(t).init(t)

	var version uint64 = 10
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "task1",
		Payload:               []byte("test"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1", "su-2", "su-3"},
	}), version)
	require.NoError(t, err)

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.NotNil(t, task.Units)
	require.Len(t, task.Units, 3)
	for _, id := range []string{"su-1", "su-2", "su-3"} {
		u, ok := task.Units[id]
		require.True(t, ok, "unit %s should exist", id)
		assert.Equal(t, UnitStatusPending, u.Status)
		assert.Equal(t, id, u.ID)
	}
}

func TestManager_RecordUnitCompletion_Success(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})

	// Complete first unit
	completeUnit(t, h, "ns", "task1", version, "node-1", "su-1")

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	assert.Equal(t, TaskStatusStarted, task.Status)
	assert.Equal(t, UnitStatusCompleted, task.Units["su-1"].Status)
	assert.Equal(t, float32(1.0), task.Units["su-1"].Progress)
	assert.Equal(t, UnitStatusPending, task.Units["su-2"].Status)

	// Complete second unit → task should finish
	completeUnit(t, h, "ns", "task1", version, "node-2", "su-2")

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task = tasks["ns"][0]
	assert.Equal(t, TaskStatusFinished, task.Status)
}

func TestManager_RecordUnitCompletion_WithError(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})

	// Fail first unit → task should immediately fail
	failUnit(t, h, "ns", "task1", version, "node-1", "su-1", "disk full")

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	assert.Equal(t, TaskStatusFailed, task.Status)
	assert.Contains(t, task.Error, "disk full")
	assert.Equal(t, UnitStatusFailed, task.Units["su-1"].Status)
}

func TestManager_RecordUnitCompletion_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		err := h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: "ns", Id: "nonexistent", Version: 1,
			NodeId: "node-1", UnitId: "su-1", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("unit does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		err := h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", UnitId: "su-nonexistent", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("unit already terminal", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
		completeUnit(t, h, "ns", "task1", version, "node-1", "su-1")

		err := h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", UnitId: "su-1", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "already terminal")
	})
}

func TestManager_UpdateUnitProgress(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

	// Update progress → should transition from PENDING to IN_PROGRESS
	updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.5)

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	u := tasks["ns"][0].Units["su-1"]
	assert.Equal(t, UnitStatusInProgress, u.Status)
	assert.Equal(t, float32(0.5), u.Progress)
	assert.Equal(t, "node-1", u.NodeID)
}

func TestManager_UpdateUnitProgress_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: "ns", Id: "nonexistent", Version: 1,
			NodeId: "node-1", UnitId: "su-1", Progress: 0.5, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("terminal unit ignored", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
		completeUnit(t, h, "ns", "task1", version, "node-1", "su-1")

		// Progress update should be silently ignored
		err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", UnitId: "su-1", Progress: 0.5, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)
	})
}

func TestManager_UpdateUnitProgress_InvalidValues(t *testing.T) {
	tests := []struct {
		name     string
		progress float32
	}{
		{"negative", -0.1},
		{"greater than 1", 1.1},
		{"large negative", -100},
		{"large positive", 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			var version uint64 = 10
			addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

			err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
				Namespace: "ns", Id: "task1", Version: version,
				NodeId: "node-1", UnitId: "su-1", Progress: tc.progress, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
			}))
			require.ErrorContains(t, err, "between 0.0 and 1.0")
		})
	}

	t.Run("boundary values accepted", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		for _, progress := range []float32{0.0, 0.5, 1.0} {
			updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", progress)
		}
	})
}

func TestManager_SnapshotRestore_WithUnits(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
	updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.7)

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	h2 := newTestHarness(t).init(t)
	require.NoError(t, h2.manager.Restore(snap))

	tasks, err := h2.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.NotNil(t, task.Units)
	require.Len(t, task.Units, 2)
	assert.Equal(t, UnitStatusInProgress, task.Units["su-1"].Status)
	assert.Equal(t, float32(0.7), task.Units["su-1"].Progress)
	assert.Equal(t, UnitStatusPending, task.Units["su-2"].Status)
}

func TestManager_AddTask_WithUnitSpecs(t *testing.T) {
	h := newTestHarness(t).init(t)

	var version uint64 = 10
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "task1",
		Payload:               []byte("test"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "su-1", GroupId: "groupA"},
			{Id: "su-2", GroupId: "groupA"},
			{Id: "su-3", GroupId: "groupB"},
		},
	}), version)
	require.NoError(t, err)

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.NotNil(t, task.Units)
	require.Len(t, task.Units, 3)

	assert.Equal(t, "groupA", task.Units["su-1"].GroupID)
	assert.Equal(t, "groupA", task.Units["su-2"].GroupID)
	assert.Equal(t, "groupB", task.Units["su-3"].GroupID)

	// Test helper methods
	groups := task.Groups()
	assert.Len(t, groups, 2)
	assert.Contains(t, groups, "groupA")
	assert.Contains(t, groups, "groupB")

	assert.False(t, task.AllGroupUnitsTerminal("groupA"))
	assert.False(t, task.AllGroupUnitsTerminal("groupB"))
}

func TestManager_UnitSpecs_TakesPrecedenceOverUnitIds(t *testing.T) {
	h := newTestHarness(t).init(t)

	var version uint64 = 10
	// When both UnitSpecs and UnitIds are set, UnitSpecs wins
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "task1",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"old-1", "old-2"},
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "new-1", GroupId: "g1"},
		},
	}), version)
	require.NoError(t, err)

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Len(t, task.Units, 1)
	assert.Contains(t, task.Units, "new-1")
	assert.Equal(t, "g1", task.Units["new-1"].GroupID)
}

func TestManager_SnapshotRestore_WithGroups(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "task1",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{
			{Id: "su-1", GroupId: "g1"},
			{Id: "su-2", GroupId: "g2"},
		},
	}), version))

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	h2 := newTestHarness(t).init(t)
	require.NoError(t, h2.manager.Restore(snap))

	tasks, err := h2.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.NotNil(t, task.Units)
	assert.Equal(t, "g1", task.Units["su-1"].GroupID)
	assert.Equal(t, "g2", task.Units["su-2"].GroupID)
}
