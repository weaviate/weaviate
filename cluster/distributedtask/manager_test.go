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
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
			Namespace:            namespace,
			Id:                   taskID,
			Version:              version,
			NodeId:               "local-node",
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

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
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
			Namespace:            namespace,
			Id:                   taskID,
			Version:              version,
			NodeId:               "local-node",
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = h.manager.AddTask(addTaskCmd, version-10)
		require.ErrorContains(t, err, "already finished with version")
	})
}

func TestManager_RecordNodeCompletion_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
				Namespace:            "test",
				Id:                   "1",
				Version:              1,
				NodeId:               "local-node",
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.RecordNodeCompletion(c, 1)
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
			})

			completeCmd = toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
				Namespace:            namespace,
				Id:                   taskID,
				Version:              1,
				NodeId:               "local-node",
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(completeCmd, 1)
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task is already completed", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			namespace        = "test"
			taskID           = "1"
			version   uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace:             namespace,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})

			completeCmd = toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
				Namespace:            namespace,
				Id:                   taskID,
				Version:              version,
				NodeId:               "local-node",
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(completeCmd, 1)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(completeCmd, 1)
		require.ErrorContains(t, err, "no longer running")
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
	}), 13))

	require.NoError(t, m.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
		Namespace:            "ns1",
		Id:                   "task2",
		Version:              13,
		NodeId:               "local-node",
		FinishedAtUnixMillis: now.Add(time.Minute).UnixMilli(),
	}), 1))

	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns2",
		Id:                    "task3",
		Payload:               []byte("test3"),
		SubmittedAtUnixMillis: now.UnixMilli(),
	}), 15))

	return map[string][]*Task{
		"ns1": {
			{
				Namespace: "ns1",
				TaskDescriptor: TaskDescriptor{
					ID:      "task1",
					Version: 10,
				},
				Payload:       []byte("test1"),
				Status:        TaskStatusCancelled,
				StartedAt:     now,
				FinishedAt:    now.Add(time.Minute),
				FinishedNodes: map[string]bool{},
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
				FinishedNodes: map[string]bool{
					"local-node": true,
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
				Payload:       []byte("test3"),
				Status:        TaskStatusStarted,
				StartedAt:     now,
				FinishedNodes: map[string]bool{},
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
	assert.Equal(t, expected.FinishedNodes, actual.FinishedNodes)
	assert.Equal(t, expected.HasSubUnits(), actual.HasSubUnits())
	if expected.HasSubUnits() {
		assert.Equal(t, len(expected.SubUnits), len(actual.SubUnits))
		for id, expectedSU := range expected.SubUnits {
			actualSU, ok := actual.SubUnits[id]
			assert.True(t, ok, "sub-unit %s should exist", id)
			if ok {
				assert.Equal(t, expectedSU.Status, actualSU.Status)
				assert.Equal(t, expectedSU.Progress, actualSU.Progress)
				assert.Equal(t, expectedSU.Error, actualSU.Error)
			}
		}
	}
}

// addTaskWithSubUnits is a test helper that creates a task with the given sub-units.
func addTaskWithSubUnits(t *testing.T, h *testHarness, ns, id string, version uint64, subUnits []string) {
	t.Helper()
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             ns,
		Id:                    id,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		SubUnitIds:            subUnits,
	}), version)
	require.NoError(t, err)
}

// completeSubUnit records a successful sub-unit completion.
func completeSubUnit(t *testing.T, h *testHarness, ns, id string, version uint64, node, su string) {
	t.Helper()
	err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		SubUnitId:            su,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

// failSubUnit records a failed sub-unit completion.
func failSubUnit(t *testing.T, h *testHarness, ns, id string, version uint64, node, su, errMsg string) {
	t.Helper()
	err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
		Namespace:            ns,
		Id:                   id,
		Version:              version,
		NodeId:               node,
		SubUnitId:            su,
		Error:                errMsg,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

// updateProgress updates a sub-unit's progress value.
func updateProgress(t *testing.T, h *testHarness, ns, id string, version uint64, node, su string, progress float32) {
	t.Helper()
	err := h.manager.UpdateSubUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
		Namespace:           ns,
		Id:                  id,
		Version:             version,
		NodeId:              node,
		SubUnitId:           su,
		Progress:            progress,
		UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)
}

func TestManager_AddTask_WithSubUnits(t *testing.T) {
	h := newTestHarness(t).init(t)

	var version uint64 = 10
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "task1",
		Payload:               []byte("test"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		SubUnitIds:            []string{"su-1", "su-2", "su-3"},
	}), version)
	require.NoError(t, err)

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.True(t, task.HasSubUnits())
	require.Len(t, task.SubUnits, 3)
	for _, id := range []string{"su-1", "su-2", "su-3"} {
		su, ok := task.SubUnits[id]
		require.True(t, ok, "sub-unit %s should exist", id)
		assert.Equal(t, SubUnitStatusPending, su.Status)
		assert.Equal(t, id, su.ID)
	}
}

func TestManager_RecordSubUnitCompletion_Success(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})

	// Complete first sub-unit
	completeSubUnit(t, h, "ns", "task1", version, "node-1", "su-1")

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	assert.Equal(t, TaskStatusStarted, task.Status)
	assert.Equal(t, SubUnitStatusCompleted, task.SubUnits["su-1"].Status)
	assert.Equal(t, float32(1.0), task.SubUnits["su-1"].Progress)
	assert.Equal(t, SubUnitStatusPending, task.SubUnits["su-2"].Status)

	// Complete second sub-unit → task should finish
	completeSubUnit(t, h, "ns", "task1", version, "node-2", "su-2")

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task = tasks["ns"][0]
	assert.Equal(t, TaskStatusFinished, task.Status)
}

func TestManager_RecordSubUnitCompletion_WithError(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})

	// Fail first sub-unit → task should immediately fail
	failSubUnit(t, h, "ns", "task1", version, "node-1", "su-1", "disk full")

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks["ns"][0]
	assert.Equal(t, TaskStatusFailed, task.Status)
	assert.Contains(t, task.Error, "disk full")
	assert.Equal(t, SubUnitStatusFailed, task.SubUnits["su-1"].Status)
}

func TestManager_RecordSubUnitCompletion_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
			Namespace: "ns", Id: "nonexistent", Version: 1,
			NodeId: "node-1", SubUnitId: "su-1", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task without sub-units", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: "ns", Id: "task1", SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), version))

		err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", SubUnitId: "su-1", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not have sub-units")
	})

	t.Run("sub-unit does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1"})

		err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", SubUnitId: "su-nonexistent", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("sub-unit already terminal", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
		completeSubUnit(t, h, "ns", "task1", version, "node-1", "su-1")

		err := h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletionRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", SubUnitId: "su-1", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "already terminal")
	})
}

func TestManager_UpdateSubUnitProgress(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1"})

	// Update progress → should transition from PENDING to IN_PROGRESS
	updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.5)

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	su := tasks["ns"][0].SubUnits["su-1"]
	assert.Equal(t, SubUnitStatusInProgress, su.Status)
	assert.Equal(t, float32(0.5), su.Progress)
	assert.Equal(t, "node-1", su.NodeID)
}

func TestManager_UpdateSubUnitProgress_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		err := h.manager.UpdateSubUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
			Namespace: "ns", Id: "nonexistent", Version: 1,
			NodeId: "node-1", SubUnitId: "su-1", Progress: 0.5, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not exist")
	})

	t.Run("task without sub-units", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace: "ns", Id: "task1", SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), version))

		err := h.manager.UpdateSubUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", SubUnitId: "su-1", Progress: 0.5, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.ErrorContains(t, err, "does not have sub-units")
	})

	t.Run("terminal sub-unit ignored", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
		completeSubUnit(t, h, "ns", "task1", version, "node-1", "su-1")

		// Progress update should be silently ignored
		err := h.manager.UpdateSubUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
			Namespace: "ns", Id: "task1", Version: version,
			NodeId: "node-1", SubUnitId: "su-1", Progress: 0.5, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
		}))
		require.NoError(t, err)
	})
}

func TestManager_UpdateSubUnitProgress_InvalidValues(t *testing.T) {
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
			addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1"})

			err := h.manager.UpdateSubUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskSubUnitProgressRequest{
				Namespace: "ns", Id: "task1", Version: version,
				NodeId: "node-1", SubUnitId: "su-1", Progress: tc.progress, UpdatedAtUnixMillis: h.clock.Now().UnixMilli(),
			}))
			require.ErrorContains(t, err, "between 0.0 and 1.0")
		})
	}

	t.Run("boundary values accepted", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		var version uint64 = 10
		addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1"})

		for _, progress := range []float32{0.0, 0.5, 1.0} {
			updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", progress)
		}
	})
}

func TestManager_SnapshotRestore_WithSubUnits(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	addTaskWithSubUnits(t, h, "ns", "task1", version, []string{"su-1", "su-2"})
	updateProgress(t, h, "ns", "task1", version, "node-1", "su-1", 0.7)

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	h2 := newTestHarness(t).init(t)
	require.NoError(t, h2.manager.Restore(snap))

	tasks, err := h2.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	task := tasks["ns"][0]
	require.True(t, task.HasSubUnits())
	require.Len(t, task.SubUnits, 2)
	assert.Equal(t, SubUnitStatusInProgress, task.SubUnits["su-1"].Status)
	assert.Equal(t, float32(0.7), task.SubUnits["su-1"].Progress)
	assert.Equal(t, SubUnitStatusPending, task.SubUnits["su-2"].Status)
}

func TestManager_LegacyPathUnchanged(t *testing.T) {
	h := newTestHarness(t).init(t)
	var version uint64 = 10
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: "ns", Id: "task1", SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version))

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	require.False(t, tasks["ns"][0].HasSubUnits())
	require.Nil(t, tasks["ns"][0].SubUnits)

	// Legacy node completion should still work
	require.NoError(t, h.manager.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
		Namespace: "ns", Id: "task1", Version: version, NodeId: "local-node", FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 1))

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	assert.Equal(t, TaskStatusFinished, tasks["ns"][0].Status)
}
