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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// fakeConflictDetector lets the manager-side conflict-hook test pin
// every branch of [Manager.AddTask]'s ConflictDetector invocation
// without standing up the real reindex provider.
type fakeConflictDetector struct {
	called     int
	rejectWith error
}

func (f *fakeConflictDetector) SetCompletionRecorder(_ TaskCompletionRecorder) {
}
func (f *fakeConflictDetector) GetLocalTasks() []TaskDescriptor    { return nil }
func (f *fakeConflictDetector) CleanupTask(_ TaskDescriptor) error { return nil }
func (f *fakeConflictDetector) StartTask(_ *Task) (TaskHandle, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeConflictDetector) CheckConflict(_ []byte, _ []*Task) error {
	f.called++
	return f.rejectWith
}

// TestManager_AddTask_ConflictDetector pins the cluster-wide
// conflict-rejection hook integrated into [Manager.AddTask]. Closes
// the multi-node parallel-submit race the REST handler's per-node
// submit lock cannot cover (parallel-migration bug #54).
func TestManager_AddTask_ConflictDetector(t *testing.T) {
	t.Run("hook NOT called when no detector registered for the namespace", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeConflictDetector{rejectWith: fmt.Errorf("would reject")}
		// Register the detector under a DIFFERENT namespace.
		h.manager.SetConflictDetectors(map[string]ConflictDetector{
			"other-namespace": detector,
		})

		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})

		require.NoError(t, h.manager.AddTask(c, 100))
		require.Zero(t, detector.called,
			"detector for a different namespace MUST NOT be consulted")
	})

	t.Run("hook called and accepts on no-conflict", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeConflictDetector{rejectWith: nil}
		h.manager.SetConflictDetectors(map[string]ConflictDetector{
			"test": detector,
		})

		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})

		require.NoError(t, h.manager.AddTask(c, 100))
		require.Equal(t, 1, detector.called,
			"detector MUST be consulted exactly once per AddTask")
	})

	t.Run("hook rejects → AddTask returns error and task is NOT added", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeConflictDetector{
			rejectWith: fmt.Errorf("simulated conflict: parallel migration on same prop"),
		}
		h.manager.SetConflictDetectors(map[string]ConflictDetector{
			"test": detector,
		})

		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})

		err := h.manager.AddTask(c, 100)
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated conflict")

		// Confirm the task was NOT registered.
		tasks, err2 := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err2)
		require.Empty(t, tasks["test"],
			"rejected task MUST NOT appear in the FSM-stored task list")
	})

	t.Run("hook nil-safe: SetConflictDetectors(nil) is a no-op", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		h.manager.SetConflictDetectors(nil)

		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})
		require.NoError(t, h.manager.AddTask(c, 100))
	})
}

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
				Payload: []byte("test2"),
				// Manager.RecordUnitCompletion alone takes a successful
				// task to FINALIZING; the FINISHED transition is committed
				// by [Manager.MarkTaskFinalized] which the [Scheduler]
				// issues after every node's [Provider.OnTaskCompleted]
				// returns. This helper exercises RecordUnitCompletion in
				// isolation, so FINALIZING is the expected end state.
				Status:     TaskStatusSwapping,
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

	// Complete second unit → task should transition to FINALIZING.
	// FINISHED only after [Manager.MarkTaskFinalized], which the
	// scheduler issues post-OnTaskCompleted; this test exercises the
	// manager FSM in isolation so it stops at FINALIZING.
	completeUnit(t, h, "ns", "task1", version, "node-2", "su-2")

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task = tasks["ns"][0]
	assert.Equal(t, TaskStatusSwapping, task.Status)
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

// Pins receiver-side monotonic clamp on Unit.Progress with NodeID+UpdatedAt still
// flowing through on regressions. See weaviate/0-weaviate-issues#232.
func TestManager_UpdateUnitProgress_MonotonicityGuard(t *testing.T) {
	getUnit := func(t *testing.T, h *testHarness, ns, id, unitID string) *Unit {
		t.Helper()
		tasks, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		require.Contains(t, tasks, ns)
		require.NotEmpty(t, tasks[ns])
		task := tasks[ns][0]
		require.NotNil(t, task.Units)
		u, ok := task.Units[unitID]
		require.True(t, ok, "unit %s should exist", unitID)
		return u
	}

	t.Run("smaller progress is clamped but UpdatedAt still advances", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.51)
		first := getUnit(t, h, "ns", "task1", "su-1")
		require.Equal(t, float32(0.51), first.Progress)
		require.Equal(t, "node-1", first.NodeID)
		firstUpdatedAt := first.UpdatedAt

		h.clock.Advance(time.Second)

		err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace:           "ns",
			Id:                  "task1",
			Version:             version,
			NodeId:              "node-1",
			UnitId:              "su-1",
			Progress:            0.48,
			UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		got := getUnit(t, h, "ns", "task1", "su-1")
		assert.Equal(t, float32(0.51), got.Progress, "progress must be clamped to monotonic floor")
		assert.Equal(t, "node-1", got.NodeID)
		assert.True(t, got.UpdatedAt.After(firstUpdatedAt), "UpdatedAt must still advance on regression")
	})

	t.Run("monotonic-up sequence accepted in order", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		for _, p := range []float32{0.0, 0.1, 0.25, 0.5, 0.9, 1.0} {
			updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", p)
			got := getUnit(t, h, "ns", "task1", "su-1")
			assert.Equal(t, p, got.Progress)
		}
	})

	t.Run("equal progress is a no-op for the Progress field", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.42)
		before := getUnit(t, h, "ns", "task1", "su-1")
		require.Equal(t, float32(0.42), before.Progress)
		beforeUpdatedAt := before.UpdatedAt

		h.clock.Advance(time.Second)

		err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace:           "ns",
			Id:                  "task1",
			Version:             version,
			NodeId:              "node-1",
			UnitId:              "su-1",
			Progress:            0.42,
			UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		got := getUnit(t, h, "ns", "task1", "su-1")
		assert.Equal(t, float32(0.42), got.Progress)
		assert.True(t, got.UpdatedAt.After(beforeUpdatedAt))
	})

	t.Run("interleaved regression in long sequence preserves max", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		seq := []struct {
			send float32
			want float32
		}{
			{0.10, 0.10},
			{0.30, 0.30},
			{0.51, 0.51},
			{0.48, 0.51},
			{0.52, 0.52},
			{0.50, 0.52},
			{0.75, 0.75},
			{1.00, 1.00},
		}
		for _, step := range seq {
			updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", step.send)
			got := getUnit(t, h, "ns", "task1", "su-1")
			assert.Equal(t, step.want, got.Progress, "after send=%v", step.send)
		}
	})

	t.Run("regression to 0.0 from a positive floor is clamped", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.6)
		updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.0)

		got := getUnit(t, h, "ns", "task1", "su-1")
		assert.Equal(t, float32(0.6), got.Progress, "0.0 must be clamped against 0.6 floor")
	})

	t.Run("invalid range still rejected even when smaller than floor", func(t *testing.T) {
		// Range validation runs before the monotonicity clamp, so a bad input
		// surfaces as an error even when clamping would have silently swallowed it.
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithUnits(t, h, "ns", "task1", version, []string{"su-1"})

		updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.7)

		err := h.manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace:           "ns",
			Id:                  "task1",
			Version:             version,
			NodeId:              "node-1",
			UnitId:              "su-1",
			Progress:            -0.5,
			UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "between 0.0 and 1.0")

		got := getUnit(t, h, "ns", "task1", "su-1")
		assert.Equal(t, float32(0.7), got.Progress)
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

// TestManager_RecordPostCompletionAck_Success pins the happy path of
// the per-node ack barrier introduced in https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A:
// every node's success ack is recorded, the task stays FINALIZING (no
// status change from a success ack), and the AckedAt timestamp lands
// on the persisted record.
func TestManager_RecordPostCompletionAck_Success(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1", "u-node-2", "u-node-3"})

	// Claim units and complete every unit so the FSM transitions to
	// FINALIZING (the only state in which acks are meaningful).
	for _, n := range []string{"node-1", "node-2", "node-3"} {
		updateProgress(t, h, "ns", "task1", version, n, "u-"+n, 0.1)
	}
	for _, n := range []string{"node-1", "node-2", "node-3"} {
		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            "ns",
			Id:                   "task1",
			Version:              version,
			NodeId:               n,
			UnitId:               "u-" + n,
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status)

	for _, n := range []string{"node-1", "node-2", "node-3"} {
		require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
			Namespace:         "ns",
			Id:                "task1",
			Version:           version,
			NodeId:            n,
			Success:           true,
			AckedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Equal(t, TaskStatusSwapping, task.Status,
		"success acks must NOT transition status — the scheduler issues MarkTaskFinalized once acks are present")
	require.Len(t, task.PostCompletionAcks, 3)
	for _, n := range []string{"node-1", "node-2", "node-3"} {
		ack, ok := task.PostCompletionAcks[n]
		require.True(t, ok, "ack for %s must be recorded", n)
		require.True(t, ack.Success)
		require.Empty(t, ack.Error)
		require.False(t, ack.AckedAt.IsZero(), "AckedAt must be set on apply")
	}
	require.Empty(t, task.MissingPostCompletionAckNodes())
	require.False(t, task.AnyPostCompletionAckFailed())
}

// TestManager_RecordPostCompletionAck_FailureTransitionsToFailed
// pins the failure-path of https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A: when a node
// reports a failure ack, the FSM transitions the task to FAILED
// immediately AND records the error on Task.Error for forensics. The
// cluster-wide schema flip will see FAILED and skip the flip.
func TestManager_RecordPostCompletionAck_FailureTransitionsToFailed(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1", "u-node-2"})

	for _, n := range []string{"node-1", "node-2"} {
		updateProgress(t, h, "ns", "task1", version, n, "u-"+n, 0.1)
		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            "ns",
			Id:                   "task1",
			Version:              version,
			NodeId:               n,
			UnitId:               "u-" + n,
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}

	// node-1 acks success.
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status)

	// node-2 acks failure — the apply path must immediately transition
	// the task to FAILED, with the error message captured on Task.Error.
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-2",
		Success:           false,
		Error:             "synthetic swap failure",
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Equal(t, TaskStatusFailed, task.Status,
		"any failure ack must transition the task to FAILED")
	require.Contains(t, task.Error, "post-completion swap failed on node node-2")
	require.Contains(t, task.Error, "synthetic swap failure")
	require.True(t, task.AnyPostCompletionAckFailed())
}

// TestManager_RecordPostCompletionAck_Idempotent pins the duplicate-ack
// behavior: the first ack per (task, node) wins. Subsequent acks for
// the same node are silently discarded — even when the duplicate would
// flip a recorded success to failure (or vice versa). Without this,
// retries on the scheduler's wake/tick loop would oscillate task
// status under apply-RPC flakes.
func TestManager_RecordPostCompletionAck_Idempotent(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"u"})
	updateProgress(t, h, "ns", "task1", version, "node-1", "u", 0.1)
	require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            "ns",
		Id:                   "task1",
		Version:              version,
		NodeId:               "node-1",
		UnitId:               "u",
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	// First ack: success.
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.True(t, tasks["ns"][0].PostCompletionAcks["node-1"].Success)
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status)

	// Duplicate ack from same node with FAILURE — must be ignored, the
	// status MUST stay FINALIZING (not flip to FAILED).
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           false,
		Error:             "stale retry that must NOT flip success",
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	require.True(t, tasks["ns"][0].PostCompletionAcks["node-1"].Success,
		"first ack wins — duplicate must not flip success to failure")
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status,
		"duplicate failure ack on idempotent path must not transition to FAILED")
}

// TestManager_RecordPostCompletionAck_DropsAcksForTerminalStatus pins
// the late-ack drop behavior: an ack arriving for a FAILED / FINISHED /
// CANCELLED task is silently ignored. This is required because every
// node's scheduler may re-emit acks on retry; once the cluster has
// decided the task's outcome, late acks are noise.
func TestManager_RecordPostCompletionAck_DropsAcksForTerminalStatus(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"u"})
	updateProgress(t, h, "ns", "task1", version, "node-1", "u", 0.1)
	require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            "ns",
		Id:                   "task1",
		Version:              version,
		NodeId:               "node-1",
		UnitId:               "u",
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	// Drive the task to FINISHED via MarkTaskFinalized.
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
		Namespace:             "ns",
		Id:                    "task1",
		Version:               version,
		FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusFinished, tasks["ns"][0].Status)
	preAckCount := len(tasks["ns"][0].PostCompletionAcks)

	// A stale retry from "node-2" arrives — there's no node-2 unit in
	// this task, but real cluster retries can hit FINISHED tasks. The
	// apply must silently no-op (no error returned, no map mutation).
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-2",
		Success:           false,
		Error:             "stale ack after FINISHED",
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusFinished, tasks["ns"][0].Status,
		"late ack must not perturb FINISHED status")
	require.Equal(t, preAckCount, len(tasks["ns"][0].PostCompletionAcks),
		"late ack must not be recorded on a terminal task")
}

// TestManager_SnapshotRestore_WithPostCompletionAcks pins the
// crash-safety property the https://github.com/weaviate/0-weaviate-issues/issues/214 Gap A fix relies on:
// the per-node acks survive RAFT snapshot/restore. Without this, a
// follower installing a snapshot mid-FINALIZING would lose the ack
// barrier state and the scheduler couldn't gate MarkTaskFinalized
// correctly post-restore.
func TestManager_SnapshotRestore_WithPostCompletionAcks(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1", "u-node-2"})
	for _, n := range []string{"node-1", "node-2"} {
		updateProgress(t, h, "ns", "task1", version, n, "u-"+n, 0.1)
		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            "ns",
			Id:                   "task1",
			Version:              version,
			NodeId:               n,
			UnitId:               "u-" + n,
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	h2 := newTestHarness(t).init(t)
	require.NoError(t, h2.manager.Restore(snap))

	tasks, _ := h2.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Equal(t, TaskStatusSwapping, task.Status,
		"task status must survive snapshot/restore")
	require.Len(t, task.PostCompletionAcks, 1,
		"the partial ack set must survive snapshot/restore")
	require.True(t, task.PostCompletionAcks["node-1"].Success)
	require.ElementsMatch(t, []string{"node-2"}, task.MissingPostCompletionAckNodes(),
		"post-restore the missing-acks predicate must keep gating MarkTaskFinalized")
}

// fakeSchemaMutationDetector lets the manager-side CheckPropertyUpdate
// dispatch test pin every branch without standing up the real reindex
// provider. Records arguments so we can assert the dispatch contract
// (only the matching-namespace detector consulted, full FSM task list
// passed in, etc.).
type fakeSchemaMutationDetector struct {
	called        int
	lastClassName string
	lastPropName  string
	lastTaskCount int
	rejectWith    error
}

func (f *fakeSchemaMutationDetector) SetCompletionRecorder(_ TaskCompletionRecorder) {}
func (f *fakeSchemaMutationDetector) GetLocalTasks() []TaskDescriptor                { return nil }
func (f *fakeSchemaMutationDetector) CleanupTask(_ TaskDescriptor) error             { return nil }
func (f *fakeSchemaMutationDetector) StartTask(_ *Task) (TaskHandle, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeSchemaMutationDetector) CheckPropertyUpdate(className, propertyName string, existingTasks []*Task) error {
	f.called++
	f.lastClassName = className
	f.lastPropName = propertyName
	f.lastTaskCount = len(existingTasks)
	return f.rejectWith
}

func (f *fakeSchemaMutationDetector) CheckClassMutation(className string, existingTasks []*Task) error {
	f.called++
	f.lastClassName = className
	f.lastTaskCount = len(existingTasks)
	return f.rejectWith
}

func (f *fakeSchemaMutationDetector) CheckTenantMutation(className string, tenants []string, existingTasks []*Task) error {
	f.called++
	f.lastClassName = className
	f.lastTaskCount = len(existingTasks)
	return f.rejectWith
}

// TestManager_CheckPropertyUpdate_DispatchToDetectors pins the cross-
// FSM dispatch: the schema FSM's UpdateProperty apply consults this
// method, which fans out to every registered
// [SchemaMutationDetector] with the current FSM-stored task list for
// each detector's namespace.
//
// Motivating bug: https://github.com/weaviate/0-weaviate-issues/issues/218. Symmetric to the existing
// TestManager_AddTask_ConflictDetector pattern.
func TestManager_CheckPropertyUpdate_DispatchToDetectors(t *testing.T) {
	t.Run("no detectors registered → nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		require.NoError(t, h.manager.CheckPropertyUpdate("C", "name"))
	})

	t.Run("detector returns nil → CheckPropertyUpdate returns nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{rejectWith: nil}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})

		require.NoError(t, h.manager.CheckPropertyUpdate("C", "name"))
		require.Equal(t, 1, detector.called,
			"detector MUST be consulted exactly once per CheckPropertyUpdate")
		require.Equal(t, "C", detector.lastClassName)
		require.Equal(t, "name", detector.lastPropName)
	})

	t.Run("detector returns error → CheckPropertyUpdate propagates", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{
			rejectWith: fmt.Errorf("simulated conflict: reindex in flight on prop"),
		}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})

		err := h.manager.CheckPropertyUpdate("C", "name")
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated conflict")
	})

	t.Run("detector receives full namespace-scoped task list at apply time", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{rejectWith: nil}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"test": detector,
		})

		// Seed a task into the manager.
		c := toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "test",
			Id:                    "T1",
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			UnitIds:               []string{"su-1"},
		})
		require.NoError(t, h.manager.AddTask(c, 100))

		require.NoError(t, h.manager.CheckPropertyUpdate("C", "name"))
		require.Equal(t, 1, detector.lastTaskCount,
			"detector MUST be passed the FSM-stored task list at apply time")
	})

	t.Run("nil detector entry → skipped gracefully", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": nil,
		})
		require.NoError(t, h.manager.CheckPropertyUpdate("C", "name"))
	})

	t.Run("nil-safe: SetSchemaMutationDetectors(nil) is a no-op", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		h.manager.SetSchemaMutationDetectors(nil)
		require.NoError(t, h.manager.CheckPropertyUpdate("C", "name"))
	})
}

// TestManager_CheckClassMutation_DispatchToDetectors pins the dispatch
// for the class-wide guard. Symmetric to the CheckPropertyUpdate
// dispatch suite — same nil-safety, same propagate-rejection,
// same full-task-list semantics.
func TestManager_CheckClassMutation_DispatchToDetectors(t *testing.T) {
	t.Run("no detectors registered → nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		require.NoError(t, h.manager.CheckClassMutation("C"))
	})

	t.Run("detector returns nil → CheckClassMutation returns nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{rejectWith: nil}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})
		require.NoError(t, h.manager.CheckClassMutation("C"))
		require.Equal(t, 1, detector.called)
		require.Equal(t, "C", detector.lastClassName)
	})

	t.Run("detector returns error → CheckClassMutation propagates", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{
			rejectWith: fmt.Errorf("simulated: reindex in flight on class"),
		}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})
		err := h.manager.CheckClassMutation("C")
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated")
	})
}

// TestManager_CheckTenantMutation_DispatchToDetectors pins the dispatch
// for the tenant-level guard.
func TestManager_CheckTenantMutation_DispatchToDetectors(t *testing.T) {
	t.Run("no detectors registered → nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		require.NoError(t, h.manager.CheckTenantMutation("C", []string{"t1"}))
	})

	t.Run("detector returns nil → CheckTenantMutation returns nil", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{rejectWith: nil}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})
		require.NoError(t, h.manager.CheckTenantMutation("C", []string{"t1", "t2"}))
		require.Equal(t, 1, detector.called)
	})

	t.Run("detector returns error → CheckTenantMutation propagates", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		detector := &fakeSchemaMutationDetector{
			rejectWith: fmt.Errorf("simulated: reindex in flight, tenants blocked"),
		}
		h.manager.SetSchemaMutationDetectors(map[string]SchemaMutationDetector{
			"reindex": detector,
		})
		err := h.manager.CheckTenantMutation("C", []string{"t1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "simulated")
	})
}

func TestManager_DeleteTasksForCollection(t *testing.T) {
	// Conservative: ("", false) on parse error keeps the cascade from
	// matching when the payload is not the expected shape.
	collectionExtractor := func(payload []byte) (string, bool) {
		var p struct {
			Collection string `json:"collection"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return "", false
		}
		if p.Collection == "" {
			return "", false
		}
		return p.Collection, true
	}

	addRawTask := func(t *testing.T, h *testHarness, ns, id string, payload []byte, unitID string) {
		t.Helper()
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             ns,
			Id:                    id,
			Payload:               payload,
			SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			UnitIds:               []string{unitID},
		}), 1))
	}

	t.Run("removes only tasks whose payload matches the collection", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		h.manager.RegisterCollectionExtractor("scoped", collectionExtractor)

		mkTask := func(id string, payload any) {
			t.Helper()
			bytes, err := json.Marshal(payload)
			require.NoError(t, err)
			require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             "scoped",
				Id:                    id,
				Payload:               bytes,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitIds:               []string{"u-" + id},
			}), 1))
		}
		mkTask("foo-1", map[string]string{"collection": "Foo"})
		mkTask("foo-2", map[string]string{"collection": "Foo"})
		mkTask("bar-1", map[string]string{"collection": "Bar"})
		mkTask("nope", "opaque-string")

		removed := h.manager.DeleteTasksForCollection("Foo")
		require.Len(t, removed, 2, "should remove both Foo tasks")

		removedIDs := []string{removed[0].ID, removed[1].ID}
		sort.Strings(removedIDs)
		assert.Equal(t, []string{"foo-1", "foo-2"}, removedIDs)

		all, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		surviving := []string{}
		for _, ts := range all["scoped"] {
			surviving = append(surviving, ts.ID)
		}
		sort.Strings(surviving)
		assert.Equal(t, []string{"bar-1", "nope"}, surviving)
	})

	t.Run("namespace without registered extractor is untouched", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		// Payload incidentally contains the deleted-class name; tasks in
		// an unscoped namespace MUST survive regardless.
		bytes, err := json.Marshal(map[string]string{"collection": "Foo"})
		require.NoError(t, err)
		addRawTask(t, h, "unscoped", "u-1", bytes, "u-only")

		removed := h.manager.DeleteTasksForCollection("Foo")
		assert.Empty(t, removed, "tasks in namespaces without an extractor must not be removed")

		all, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		assert.Len(t, all["unscoped"], 1, "unscoped task must still be there")
	})

	t.Run("empty collection name is a no-op (refuses to nuke everything)", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		// Guard against a sloppy extractor emitting ("", true) accidentally
		// matching every task on DeleteTasksForCollection("").
		h.manager.RegisterCollectionExtractor("scoped", func([]byte) (string, bool) {
			return "", true
		})

		addRawTask(t, h, "scoped", "still-here", []byte("anything"), "u-1")

		removed := h.manager.DeleteTasksForCollection("")
		assert.Empty(t, removed, "empty collection name must be refused")

		all, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		assert.Len(t, all["scoped"], 1, "task must still exist after empty-name query")
	})

	t.Run("RegisterCollectionExtractor is idempotent (last write wins)", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		h.manager.RegisterCollectionExtractor("scoped", func([]byte) (string, bool) { return "Foo", true })
		h.manager.RegisterCollectionExtractor("scoped", func([]byte) (string, bool) { return "", false })

		addRawTask(t, h, "scoped", "task", []byte("payload"), "u-1")

		removed := h.manager.DeleteTasksForCollection("Foo")
		assert.Empty(t, removed, "second extractor takes effect; task must NOT be removed")
	})

	t.Run("RegisterCollectionExtractor rejects nil extractor and empty namespace", func(t *testing.T) {
		// Without these guards, DeleteTasksForCollection would either panic
		// on the nil function pointer or shadow legitimate "" tasks.
		h := newTestHarness(t).init(t)

		h.manager.RegisterCollectionExtractor("scoped", nil)
		addRawTask(t, h, "scoped", "task", []byte("anything"), "u-1")
		require.NotPanics(t, func() {
			removed := h.manager.DeleteTasksForCollection("Foo")
			assert.Empty(t, removed, "nil extractor must not match anything")
		})

		h.manager.RegisterCollectionExtractor("", func([]byte) (string, bool) { return "Foo", true })
		addRawTask(t, h, "", "task-in-empty-ns", []byte("anything"), "u-2")
		removed := h.manager.DeleteTasksForCollection("Foo")
		assert.Empty(t, removed, "empty-namespace registration must be ignored")
	})
}

// addBarrierTaskWithUnits is the barrier-mode counterpart to
// addTaskWithUnits — same shape, but the task opts into the PrepComplete
// barrier so AllUnitsTerminal routes STARTED → PREPARING (instead of
// straight to SWAPPING) and PreparationCompletionAcks become the gate on the
// PREPARING → SWAPPING transition.
func addBarrierTaskWithUnits(t *testing.T, h *testHarness, ns, id string, version uint64, units []string) {
	t.Helper()
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:               ns,
		Id:                      id,
		SubmittedAtUnixMillis:   h.clock.Now().UnixMilli(),
		UnitIds:                 units,
		NeedsPreparationBarrier: true,
	}), version)
	require.NoError(t, err)
}

// drivePreparing transitions a barrier task to PREPARING by claiming
// every unit's progress and recording successful unit completion for
// every node. Mirrors the body of TestManager_RecordPostCompletionAck_*
// up to the FINALIZING transition, but here AllUnitsTerminal routes
// STARTED → PREPARING because NeedsPreparationBarrier=true.
func drivePreparing(t *testing.T, h *testHarness, ns, id string, version uint64, nodes []string) {
	t.Helper()
	for _, n := range nodes {
		updateProgress(t, h, ns, id, version, n, "u-"+n, 0.1)
	}
	for _, n := range nodes {
		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            ns,
			Id:                   id,
			Version:              version,
			NodeId:               n,
			UnitId:               "u-" + n,
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}
	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusPreparing, tasks[ns][0].Status,
		"barrier task with all units terminal MUST land in PREPARING (not SWAPPING)")
}

// TestManager_RecordPreparationCompleteAck_Success pins the core PREP barrier
// invariant: PREPARING → SWAPPING happens only after EVERY expected
// node ack lands with Success=true. Until the last ack arrives, the
// task stays PREPARING (no node fires its atomic swap).
func TestManager_RecordPreparationCompleteAck_Success(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	nodes := []string{"node-1", "node-2", "node-3"}
	addBarrierTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1", "u-node-2", "u-node-3"})
	drivePreparing(t, h, "ns", "task1", version, nodes)

	// First two acks: task must stay PREPARING — the barrier hasn't
	// lifted yet (load-bearing property; the third node must NOT see
	// SWAPPING and prematurely fire its OnSwapRequested).
	for _, n := range nodes[:2] {
		require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace:         "ns",
			Id:                "task1",
			Version:           version,
			NodeId:            n,
			Success:           true,
			AckedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
		tasks, _ := h.manager.ListDistributedTasks(context.Background())
		require.Equal(t, TaskStatusPreparing, tasks["ns"][0].Status,
			"PREPARING → SWAPPING must NOT fire until EVERY expected node has acked")
	}

	// Third ack lifts the barrier.
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-3",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Equal(t, TaskStatusSwapping, task.Status,
		"PREPARING → SWAPPING fires the moment the last expected ack lands")
	require.Len(t, task.PreparationCompletionAcks, 3)
	for _, n := range nodes {
		ack, ok := task.PreparationCompletionAcks[n]
		require.True(t, ok, "PrepAck for %s must be recorded", n)
		require.True(t, ack.Success)
		require.False(t, ack.AckedAt.IsZero(), "AckedAt must be set on apply")
	}
}

// TestManager_RecordPreparationCompleteAck_FailureTransitionsToFailed pins
// the primary safety property of the PREP barrier: a single Success=false
// ack immediately flips PREPARING → FAILED, so no node proceeds to
// the atomic swap. Without this the barrier degenerates to a no-op:
// one node's PREP failure would still let every other node swap.
func TestManager_RecordPreparationCompleteAck_FailureTransitionsToFailed(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	nodes := []string{"node-1", "node-2"}
	addBarrierTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1", "u-node-2"})
	drivePreparing(t, h, "ns", "task1", version, nodes)

	// node-1 acks success.
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.Equal(t, TaskStatusPreparing, tasks["ns"][0].Status,
		"one success ack does not lift the barrier — both nodes still owe an ack")

	// node-2 acks failure — apply must flip the task to FAILED
	// immediately, even though node-1 succeeded.
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-2",
		Success:           false,
		Error:             "synthetic prep failure",
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	require.Equal(t, TaskStatusFailed, task.Status,
		"any Success=false PrepAck must flip PREPARING → FAILED — no node may swap")
	require.Contains(t, task.Error, "prep failed on node node-2")
	require.Contains(t, task.Error, "synthetic prep failure")
}

// TestManager_RecordPreparationCompleteAck_Idempotent pins the duplicate-ack
// contract: the first ack per (task, node) wins; subsequent acks for
// the same node are silently no-op'd. The scheduler's wake/tick retries
// must not be able to oscillate task status under apply-RPC flakes.
func TestManager_RecordPreparationCompleteAck_Idempotent(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addBarrierTaskWithUnits(t, h, "ns", "task1", version, []string{"u-node-1"})
	drivePreparing(t, h, "ns", "task1", version, []string{"node-1"})

	// First ack: success → barrier lifts (single-node).
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.True(t, tasks["ns"][0].PreparationCompletionAcks["node-1"].Success)
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status)

	// Duplicate ack with Success=false: must be silently dropped.
	// If the duplicate took effect it would flip Success→false in the
	// recorded ack AND flip SWAPPING → FAILED, both of which are wrong.
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         "ns",
		Id:                "task1",
		Version:           version,
		NodeId:            "node-1",
		Success:           false,
		Error:             "this must be ignored",
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	require.True(t, tasks["ns"][0].PreparationCompletionAcks["node-1"].Success,
		"duplicate ack must not flip recorded success → failure")
	require.Equal(t, TaskStatusSwapping, tasks["ns"][0].Status,
		"duplicate ack must not flip SWAPPING → FAILED")
}

// TestManager_RecordPreparationCompleteAck_AckOrderCommutativity pins that
// ack-arrival-order is irrelevant: the same set of acks produces the
// same end state regardless of which node's apply lands first. The
// barrier-lift transition is a pure function of (Units, PrepAcks),
// not of the apply order.
func TestManager_RecordPreparationCompleteAck_AckOrderCommutativity(t *testing.T) {
	for _, perm := range [][]string{
		{"node-1", "node-2", "node-3"},
		{"node-3", "node-1", "node-2"},
		{"node-2", "node-3", "node-1"},
	} {
		t.Run(strings.Join(perm, ","), func(t *testing.T) {
			h := newTestHarness(t).init(t)
			var version uint64 = 10
			nodes := []string{"node-1", "node-2", "node-3"}
			addBarrierTaskWithUnits(t, h, "ns", "task1", version,
				[]string{"u-node-1", "u-node-2", "u-node-3"})
			drivePreparing(t, h, "ns", "task1", version, nodes)

			for _, n := range perm {
				require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace:         "ns",
					Id:                "task1",
					Version:           version,
					NodeId:            n,
					Success:           true,
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			}

			tasks, _ := h.manager.ListDistributedTasks(context.Background())
			task := tasks["ns"][0]
			require.Equal(t, TaskStatusSwapping, task.Status,
				"end state must be SWAPPING regardless of ack-arrival order")
			require.Len(t, task.PreparationCompletionAcks, 3)
		})
	}
}

// TestManager_Restore_ReplacesExistingState pins the hashicorp/raft
// FSM contract that snapshot installation REPLACES the in-memory
// state. A follower that already applied log entries before the
// leader sends a snapshot must end up identical to the leader after
// Restore returns.
//
// Without this, any task that was applied locally but is NOT present
// in the snapshot (typical: cancelled+pruned by the leader between
// local apply and snapshot install) survives as a phantom on the
// follower — the next scheduler tick observes it and fires
// OnTaskCompleted / schema flips that no other node sees. Same bug
// family the SchemaMutationDetector exists to catch.
func TestManager_Restore_ReplacesExistingState(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)

	// Source manager: holds the snapshot that represents the leader's
	// view at install time. Only "task1" exists here.
	src := newTestHarness(t).init(t)
	require.NoError(t, src.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns1",
		Id:                    "task1",
		Payload:               []byte("from-snapshot"),
		SubmittedAtUnixMillis: now.UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 1))
	snap, err := src.manager.Snapshot()
	require.NoError(t, err)

	// Destination manager: simulates a follower that already applied
	// "phantom" before the leader sends the snapshot. The leader
	// doesn't know about "phantom" — it must not survive Restore.
	dst := newTestHarness(t).init(t)
	require.NoError(t, dst.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns2",
		Id:                    "phantom",
		Payload:               []byte("must-be-erased"),
		SubmittedAtUnixMillis: now.UnixMilli(),
		UnitIds:               []string{"su-x"},
	}), 99))

	require.NoError(t, dst.manager.Restore(snap))

	tasks, err := dst.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	require.Lenf(t, tasks, 1, "post-Restore namespaces must equal snapshot namespaces; phantom 'ns2' survived")
	require.Len(t, tasks["ns1"], 1, "snapshot's ns1/task1 must be present after Restore")
	require.Equal(t, "task1", tasks["ns1"][0].ID)
	require.NotContains(t, tasks, "ns2", "phantom namespace must not survive Restore")
}
