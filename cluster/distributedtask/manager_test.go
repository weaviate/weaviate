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

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

const (
	testShard1 = "shard-1"
	testShard2 = "shard-2"
	testShard3 = "shard-3"
	testNode1  = "node-1"
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
	if expected.SubUnits == nil {
		assert.Nil(t, actual.SubUnits)
	} else {
		require.NotNil(t, actual.SubUnits)
		assert.Equal(t, len(expected.SubUnits), len(actual.SubUnits))
		for id, expSU := range expected.SubUnits {
			actSU, ok := actual.SubUnits[id]
			require.True(t, ok, "sub-unit %s not found", id)
			assert.Equal(t, expSU.ID, actSU.ID)
			assert.Equal(t, expSU.Status, actSU.Status)
			assert.Equal(t, expSU.Error, actSU.Error)
		}
	}
}

// ---- Sub-unit tracking tests ----

func TestManagerSubUnitTrackingAllComplete(t *testing.T) {
	var (
		h         = newTestHarness(t).init(t)
		namespace = "ns"
		taskID    = "task-1"
		version   = uint64(10)
		now       = h.clock.Now()
	)

	// Create task with two sub-units.
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: now.UnixMilli(),
	}), version)
	require.NoError(t, err)

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Nil(t, tasks[namespace][0].SubUnits, "node-completion mode when no sub_unit_ids")

	// Re-create with sub-unit IDs via the extended request.
	h = newTestHarness(t).init(t)
	now = h.clock.Now().Truncate(time.Millisecond)
	err = h.manager.AddTask(toCmd(t, &addDistributedTaskRequestForTest{
		Namespace:             namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: now.UnixMilli(),
		SubUnitIds:            []string{testShard1, testShard2},
	}), version)
	require.NoError(t, err)

	tasks, err = h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	task := tasks[namespace][0]
	require.NotNil(t, task.SubUnits)
	require.Equal(t, TaskStatusStarted, task.Status)
	require.Equal(t, SubUnitStatusPending, task.SubUnits[testShard1].Status)
	require.Equal(t, SubUnitStatusPending, task.SubUnits[testShard2].Status)

	// Complete shard-1.
	completedAt := now.Add(time.Minute).Truncate(time.Millisecond)
	err = h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            testShard1,
		NodeId:               testNode1,
		FinishedAtUnixMillis: completedAt.UnixMilli(),
	}))
	require.NoError(t, err)

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task = tasks[namespace][0]
	require.Equal(t, TaskStatusStarted, task.Status, "task still running with one sub-unit pending")
	require.Equal(t, SubUnitStatusCompleted, task.SubUnits[testShard1].Status)
	require.Equal(t, SubUnitStatusPending, task.SubUnits[testShard2].Status)

	// Complete shard-2 → task should transition to FINISHED.
	err = h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            testShard2,
		NodeId:               "node-2",
		FinishedAtUnixMillis: completedAt.UnixMilli(),
	}))
	require.NoError(t, err)

	tasks, _ = h.manager.ListDistributedTasks(context.Background())
	task = tasks[namespace][0]
	require.Equal(t, TaskStatusFinished, task.Status)
	require.Equal(t, SubUnitStatusCompleted, task.SubUnits[testShard1].Status)
	require.Equal(t, SubUnitStatusCompleted, task.SubUnits[testShard2].Status)
	require.Equal(t, completedAt.UTC(), task.FinishedAt.UTC())
}

func TestManagerSubUnitTrackingFailureTransitionsTask(t *testing.T) {
	var (
		h         = newTestHarness(t).init(t)
		namespace = "ns"
		taskID    = "task-fail"
		version   = uint64(10)
		now       = h.clock.Now().Truncate(time.Millisecond)
	)

	require.NoError(t, h.manager.AddTask(toCmd(t, &addDistributedTaskRequestForTest{
		Namespace:             namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: now.UnixMilli(),
		SubUnitIds:            []string{testShard1, testShard2, testShard3},
	}), version))

	// Complete shard-1 successfully.
	require.NoError(t, h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            testShard1,
		NodeId:               testNode1,
		FinishedAtUnixMillis: now.Add(time.Minute).UnixMilli(),
	})))

	// Fail shard-2.
	errMsg := "disk full"
	failedAt := now.Add(2 * time.Minute).Truncate(time.Millisecond)
	require.NoError(t, h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            testShard2,
		NodeId:               "node-2",
		Error:                &errMsg,
		FinishedAtUnixMillis: failedAt.UnixMilli(),
	})))

	tasks, _ := h.manager.ListDistributedTasks(context.Background())
	task := tasks[namespace][0]
	require.Equal(t, TaskStatusFailed, task.Status)
	require.Contains(t, task.Error, testShard2)
	require.Contains(t, task.Error, "disk full")
	require.Equal(t, SubUnitStatusCompleted, task.SubUnits[testShard1].Status)
	require.Equal(t, SubUnitStatusFailed, task.SubUnits[testShard2].Status)
	require.Equal(t, SubUnitStatusPending, task.SubUnits[testShard3].Status)
	require.Equal(t, failedAt.UTC(), task.FinishedAt.UTC())
}

func TestManagerSubUnitTrackingProgressThrottling(t *testing.T) {
	const minInterval = 5 * time.Second

	clock := h2clock()
	m := NewManager(ManagerParameters{
		Clock:                      clock,
		CompletedTaskTTL:           24 * time.Hour,
		SubUnitProgressMinInterval: minInterval,
	})

	var (
		namespace = "ns"
		taskID    = "task-progress"
		version   = uint64(1)
	)

	require.NoError(t, m.AddTask(toCmd(t, &addDistributedTaskRequestForTest{
		Namespace:             namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: clock.Now().UnixMilli(),
		SubUnitIds:            []string{testShard1},
	}), version))

	// First progress update should be accepted (UpdatedAt.IsZero() initially).
	require.NoError(t, m.RecordSubUnitProgress(toCmd(t, &cmd.RecordDistributedTaskSubUnitProgressRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   version,
		SubUnitId: testShard1,
		NodeId:    testNode1,
		Progress:  0.25,
	})))

	tasks, _ := m.ListDistributedTasks(context.Background())
	require.InDelta(t, 0.25, tasks[namespace][0].SubUnits[testShard1].Progress, 0.001)
	require.Equal(t, SubUnitStatusInProgress, tasks[namespace][0].SubUnits[testShard1].Status)

	// Rapid second update: should be silently dropped (within minInterval).
	require.NoError(t, m.RecordSubUnitProgress(toCmd(t, &cmd.RecordDistributedTaskSubUnitProgressRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   version,
		SubUnitId: testShard1,
		NodeId:    testNode1,
		Progress:  0.50,
	})))
	tasks, _ = m.ListDistributedTasks(context.Background())
	require.InDelta(t, 0.25, tasks[namespace][0].SubUnits[testShard1].Progress, 0.001, "rapid update must be throttled")

	// Advance clock past minInterval → update accepted.
	clock.Advance(minInterval + time.Second)
	require.NoError(t, m.RecordSubUnitProgress(toCmd(t, &cmd.RecordDistributedTaskSubUnitProgressRequest{
		Namespace: namespace,
		Id:        taskID,
		Version:   version,
		SubUnitId: testShard1,
		NodeId:    testNode1,
		Progress:  0.75,
	})))
	tasks, _ = m.ListDistributedTasks(context.Background())
	require.InDelta(t, 0.75, tasks[namespace][0].SubUnits[testShard1].Progress, 0.001, "update after interval must be applied")
}

func TestManagerSubUnitTrackingSnapshotRestore(t *testing.T) {
	var (
		h         = newTestHarness(t).init(t)
		namespace = "ns"
		taskID    = "task-snap"
		version   = uint64(10)
		now       = h.clock.Now().Truncate(time.Millisecond)
	)

	require.NoError(t, h.manager.AddTask(toCmd(t, &addDistributedTaskRequestForTest{
		Namespace:             namespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: now.UnixMilli(),
		SubUnitIds:            []string{testShard1, testShard2},
	}), version))

	// Complete shard-1 to get some state.
	require.NoError(t, h.manager.RecordSubUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskSubUnitCompletedRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              version,
		SubUnitId:            testShard1,
		NodeId:               testNode1,
		FinishedAtUnixMillis: now.Add(time.Minute).UnixMilli(),
	})))

	snap, err := h.manager.Snapshot()
	require.NoError(t, err)

	// Restore into a fresh manager.
	h2 := newTestHarness(t).init(t)
	require.NoError(t, h2.manager.Restore(snap))

	tasks, err := h2.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks[namespace], 1)
	task := tasks[namespace][0]
	require.Equal(t, TaskStatusStarted, task.Status)
	require.Equal(t, SubUnitStatusCompleted, task.SubUnits[testShard1].Status)
	require.Equal(t, SubUnitStatusPending, task.SubUnits[testShard2].Status)
}

// addDistributedTaskRequestForTest mirrors AddDistributedTaskRequestWithSubUnits
// with JSON tags matching the protobuf encoding so it can be passed to toCmd.
type addDistributedTaskRequestForTest struct {
	Namespace             string   `json:"namespace,omitempty"`
	Id                    string   `json:"id,omitempty"`
	Payload               []byte   `json:"payload,omitempty"`
	SubmittedAtUnixMillis int64    `json:"submitted_at_unix_millis,omitempty"`
	SubUnitIds            []string `json:"sub_unit_ids,omitempty"`
}

// h2clock returns a fake clock helper for tests that need to be independent of
// the shared testHarness clock.
func h2clock() *clockwork.FakeClock {
	return clockwork.NewFakeClock()
}
