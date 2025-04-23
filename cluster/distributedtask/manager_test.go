//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
}
