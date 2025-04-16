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
	"fmt"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	mocks "github.com/weaviate/weaviate/mocks/cluster/distributedtask"
)

func TestHappyPathTaskLifecycleWithSingleNode(t *testing.T) {
	defer leaktest.Check(t)() // TODO: maybe this should be in TestMain

	var (
		h                  = newTestHarness(t).init(t)
		taskID             = "1234"
		version     uint64 = 10
		taskPayload        = []byte("payload")
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		Payload:               taskPayload,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, h.tasksNamespace, startedTask.Type)
	require.Equal(t, taskID, startedTask.ID)
	require.Equal(t, taskPayload, startedTask.Payload)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, taskID, version)
	startedTask.Complete()

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	h.advanceClock(schedulerTickDuration)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// advance the clock just before expected clean up time to check whether it respects it
	h.advanceClock(completedTaskTTL - h.clockAdvancedSoFar - time.Minute)

	h.expectCleanUpTask(t, h.tasksNamespace, taskID, version)
	h.advanceClock(schedulerTickDuration + time.Minute)

	require.Empty(t, h.manager.ListTasks())
}

func TestHappyPathTaskLifecycleWithMultipleNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t)
	h.nodesInTheCluster = 2
	h = h.init(t)

	var (
		taskID             = "1234"
		version     uint64 = 10
		taskPayload        = []byte("payload")
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		Payload:               taskPayload,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	// local task launched
	localTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, localTask.ID)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, taskID, version)
	localTask.Complete()
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	// local task completed
	h.advanceClock(schedulerTickDuration)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// however, task is not finished in the cluster yet
	tasks := h.manager.ListTasks()[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, TaskStatusStarted, tasks[0].Status)

	// finish the task across the cluster
	h.completeTaskFromNode(t, h.tasksNamespace, taskID, version, "remote-node")

	tasks = h.manager.ListTasks()[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)
	require.Equal(t, map[string]bool{
		h.localNodeID: true,
		"remote-node": true,
	}, tasks[0].FinishedNodes)
}

func TestTaskCancellation(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		h              = newTestHarness(t).init(t)
		taskID         = "1234"
		version uint64 = 10
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	cancellationTime := h.clock.Now().UnixMilli()
	err = h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		Version:               version,
		CancelledAtUnixMillis: cancellationTime,
	}))
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.cancelledCh).ID)

	tasks := h.manager.ListTasks()[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Type)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, version, tasks[0].Version)
	require.Equal(t, TaskStatusCancelled, tasks[0].Status)
	require.Equal(t, cancellationTime, tasks[0].FinishedAt.UnixMilli())
}

func TestTaskFailureInAnotherNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t)
	h.nodesInTheCluster = 2
	h = h.init(t)
	var (
		taskID         = "1234"
		version uint64 = 10
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	// send a failure command from another node
	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	h.recordTaskCompletion(t, h.tasksNamespace, taskID, version, "other-node", &failureMessage)

	// locally running task should be cancelled
	h.advanceClock(schedulerTickDuration)
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.cancelledCh).ID)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	tasks := h.manager.ListTasks()[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Type)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, version, tasks[0].Version)
	require.Equal(t, TaskStatusFailed, tasks[0].Status)
	require.Equal(t, failureTime, tasks[0].FinishedAt.UnixMilli())
}

func TestTaskFailureInLocalNode(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t)
	h.nodesInTheCluster = 2
	h = h.init(t)
	var (
		taskID         = "1234"
		version uint64 = 10
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(schedulerTickDuration)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	h.expectRecordNodeTaskFailure(t, h.tasksNamespace, taskID, version, failureMessage)
	startedTask.Fail(failureMessage)

	recvWithTimeout(t, h.provider.failedCh)

	h.advanceClock(schedulerTickDuration)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	tasks := h.manager.ListTasks()[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Type)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, version, tasks[0].Version)
	require.Equal(t, TaskStatusFailed, tasks[0].Status)
	require.Equal(t, failureTime, tasks[0].FinishedAt.UnixMilli())
}

func TestTaskRecovery(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		h          = newTestHarness(t).init(t)
		tasksCount = 5
	)

	// add some tasks before launching the scheduler
	tasksIDs := map[string]bool{}
	for i := range tasksCount {
		taskID := fmt.Sprintf("%d", i)
		err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Type:                  h.tasksNamespace,
			Id:                    taskID,
			SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)
		tasksIDs[taskID] = true
	}

	h.startScheduler(t)
	defer h.scheduler.Close()

	// tasksIDs should be launched right away
	launchedTasks := map[string]*testTask{}
	for range tasksCount {
		launchedTask := recvWithTimeout(t, h.provider.startedCh)
		require.Contains(t, tasksIDs, launchedTask.ID)
		launchedTasks[launchedTask.ID] = launchedTask
	}
	require.Len(t, launchedTasks, tasksCount)

	// clean up launched goroutines
	for _, task := range launchedTasks {
		task.Terminate()
	}
}

func TestRemoveCleanedUpTaskLocalState(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		localTaskList = []TaskDescriptor{
			{ID: "1", Version: 1},
			{ID: "2", Version: 10},
			{ID: "3", Version: 15},
		}
		provider = newTestTaskProvider(localTaskList)
	)

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{
		h.tasksNamespace: provider,
	}
	h = h.init(t)

	// add one of the local tasks to the manager state before launching the scheduler
	// to simulate that it was there before the restart
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    "3",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 15)
	require.NoError(t, err)

	// add one new task
	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  h.tasksNamespace,
		Id:                    "4",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 18)
	require.NoError(t, err)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// make sure only tasks are not running cleaned up
	require.Len(t, provider.cleanedUpTasks, 2)
	require.Contains(t, provider.cleanedUpTasks, localTaskList[0])
	require.Contains(t, provider.cleanedUpTasks, localTaskList[1])

	expectStartedTasks := map[string]struct{}{"3": {}, "4": {}}
	for range len(expectStartedTasks) {
		startedTask := <-provider.startedCh
		require.Contains(t, expectStartedTasks, startedTask.ID)
		startedTask.Terminate()
	}
}

func TestMultiNamespaceMultiTasks(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		tasksNamespace1     = "tasks-namespace-1"
		provider1StaleTasks = []TaskDescriptor{
			{ID: "1", Version: 1},
			{ID: "2", Version: 10},
		}
		provider1 = newTestTaskProvider(provider1StaleTasks)

		tasksNamespace2 = "tasks-namespace-2"
		provider2       = newTestTaskProvider(nil)
	)

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{
		tasksNamespace1: provider1,
		tasksNamespace2: provider2,
	}
	h = h.init(t)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// cleanup tasks for one of the providers
	require.Len(t, provider1.cleanedUpTasks, 2)
	require.Contains(t, provider1.cleanedUpTasks, provider1StaleTasks[0])
	require.Contains(t, provider1.cleanedUpTasks, provider1StaleTasks[1])

	require.Len(t, provider2.cleanedUpTasks, 0)

	// add some tasks for both providers
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  tasksNamespace1,
		Id:                    "complete",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 10)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  tasksNamespace2,
		Id:                    "fail",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 11)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Type:                  tasksNamespace1,
		Id:                    "cancel",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 12)
	require.NoError(t, err)

	h.advanceClock(schedulerTickDuration)
	require.Equal(t, 3, h.scheduler.totalRunningTaskCount())

	startedTasks := map[string]*testTask{}
	for range 2 {
		task := recvWithTimeout(t, provider1.startedCh)
		startedTasks[task.ID] = task
	}
	for range 1 {
		task := recvWithTimeout(t, provider2.startedCh)
		startedTasks[task.ID] = task
	}
	require.Len(t, startedTasks, 3)

	h.expectRecordNodeTaskCompletion(t, tasksNamespace1, "complete", 10)
	startedTasks["complete"].Complete()
	recvWithTimeout(t, provider1.completedCh)

	h.expectRecordNodeTaskFailure(t, tasksNamespace2, "fail", 11, "failed")
	startedTasks["fail"].Fail("failed")
	recvWithTimeout(t, provider2.failedCh)

	err = h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Type:                  tasksNamespace1,
		Id:                    "cancel",
		Version:               12,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)

	h.advanceClock(schedulerTickDuration)

	require.Zero(t, h.scheduler.totalRunningTaskCount())
}

func TestManager_AddTask_Failures(t *testing.T) {
	t.Run("add duplicate task", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  "test",
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

			taskType   = "test"
			taskID     = "1"
			addTaskCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
			Type:                 taskType,
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

			taskType   = "test"
			taskID     = "1"
			addTaskCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
			version uint64 = 100
		)

		err := h.manager.AddTask(addTaskCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
			Type:                 taskType,
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
				Type:                 "test",
				Id:                   "1",
				Version:              1,
				NodeId:               "local-node",
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.RecordNodeCompletion(c, 1)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task with the given version does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})

			completeCmd = toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
				Type:                 taskType,
				Id:                   taskID,
				Version:              1,
				NodeId:               "local-node",
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.RecordNodeCompletion(completeCmd, 1)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task is already completed", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})

			completeCmd = toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
				Type:                 taskType,
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
		require.ErrorIs(t, err, ErrTaskIsNoLongerRunning)
	})
}

func TestManager_CancelTask_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Type:                  "test",
				Id:                    "1",
				Version:               1,
				CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.CancelTask(c)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task with the given version does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				Version:               version - 1,
				CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CancelTask(cancelCmd)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task is already cancelled", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Type:                  "test",
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
		require.ErrorIs(t, err, ErrTaskIsNoLongerRunning)
	})
}

func TestManager_CleanUpTask_Failures(t *testing.T) {
	t.Run("task does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)
			c = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Type:    "test",
				Id:      "1",
				Version: 1,
			})
		)

		err := h.manager.CleanUpTask(c)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task with the given version does not exist", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * completedTaskTTL).UnixMilli(),
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Type:    taskType,
				Id:      taskID,
				Version: version - 1,
			})
		)

		err := h.manager.AddTask(addCmd, version)
		require.NoError(t, err)

		err = h.manager.CleanUpTask(cleanUpCmd)
		require.ErrorIs(t, err, ErrTaskDoesNotExist)
	})

	t.Run("task is still running", func(t *testing.T) {
		var (
			h = newTestHarness(t).init(t)

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * completedTaskTTL).UnixMilli(),
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Type:    taskType,
				Id:      taskID,
				Version: version,
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

			taskType        = "test"
			taskID          = "1"
			version  uint64 = 10

			addCmd = toCmd(t, &cmd.AddDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				SubmittedAtUnixMillis: h.clock.Now().Add(-3 * completedTaskTTL).UnixMilli(),
			})

			cancelCmd = toCmd(t, &cmd.CancelDistributedTaskRequest{
				Type:                  taskType,
				Id:                    taskID,
				Version:               version,
				CancelledAtUnixMillis: h.clock.Now().Add(-completedTaskTTL).Add(time.Minute).UnixMilli(),
			})

			cleanUpCmd = toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Type:    taskType,
				Id:      taskID,
				Version: version,
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

func recvWithTimeout[T any](t *testing.T, ch <-chan T) T {
	select {
	case el := <-ch:
		return el
	case <-time.After(time.Second):
		require.Fail(t, "timeout")
	}
	panic("unreachable")
}

func toCmd[T any](t *testing.T, subCommand T) *cmd.ApplyRequest {
	bytes, err := json.Marshal(subCommand)
	require.NoError(t, err)

	return &cmd.ApplyRequest{
		SubCommand: bytes,
	}
}

type testHarness struct {
	localNodeID         string
	clock               *clockwork.FakeClock
	logger              logrus.FieldLogger
	stateChanger        *mocks.TaskStatusChanger
	provider            *testTaskProvider
	tasksNamespace      string
	nodesInTheCluster   int
	registeredProviders map[string]Provider

	manager   *Manager
	scheduler *Scheduler

	clockAdvancedSoFar time.Duration
}

func newTestHarness(t *testing.T) *testHarness {
	var (
		defaultNamespace = "tasks-namespace"
		defaultProvider  = newTestTaskProvider(nil)
	)

	return &testHarness{
		localNodeID:       "local-node",
		tasksNamespace:    defaultNamespace,
		nodesInTheCluster: 1,
		clock:             clockwork.NewFakeClock(),
		logger:            logrus.StandardLogger(),
		stateChanger:      mocks.NewTaskStatusChanger(t),
		provider:          defaultProvider,
		registeredProviders: map[string]Provider{
			defaultNamespace: defaultProvider,
		},
	}
}

func (h *testHarness) init(t *testing.T) *testHarness {
	h.manager = NewManager(h.clock, h.logger)
	h.scheduler = NewScheduler(SchedulerParams{
		CompletionRecorder: h.stateChanger,
		TasksLister:        h.manager,
		Providers:          h.registeredProviders,
		Clock:              h.clock,
		LocalNode:          h.localNodeID,
	})
	return h
}

func (h *testHarness) advanceClock(duration time.Duration) {
	h.clock.Advance(duration)
	h.clockAdvancedSoFar += duration

	// after moving the clock, give some time for the unblocked goroutines to wake up and execute
	time.Sleep(50 * time.Millisecond)
}

func (h *testHarness) expectRecordNodeTaskCompletion(t *testing.T, expectTaskType, expectTaskID string, expectTaskVersion uint64) {
	h.stateChanger.EXPECT().RecordDistributedTaskNodeCompletion(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, taskType, taskID string, taskVersion uint64) error {
			require.Equal(t, expectTaskType, taskType)
			require.Equal(t, expectTaskID, taskID)
			require.Equal(t, expectTaskVersion, taskVersion)

			h.completeTaskFromNode(t, taskType, taskID, taskVersion, h.localNodeID)
			return nil
		})
}

func (h *testHarness) expectRecordNodeTaskFailure(t *testing.T, expectTaskType, expectTaskID string, expectTaskVersion uint64, expectErrMsg string) {
	h.stateChanger.EXPECT().RecordDistributedTaskNodeFailed(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, taskType, taskID string, taskVersion uint64, errMsg string) error {
			require.Equal(t, expectTaskType, taskType)
			require.Equal(t, expectTaskID, taskID)
			require.Equal(t, expectTaskVersion, taskVersion)
			require.Equal(t, expectErrMsg, errMsg)

			h.recordTaskCompletion(t, taskType, taskID, taskVersion, h.localNodeID, &expectErrMsg)
			return nil
		})
}

func (h *testHarness) completeTaskFromNode(t *testing.T, taskType, taskID string, taskVersion uint64, node string) {
	h.recordTaskCompletion(t, taskType, taskID, taskVersion, node, nil)
}

func (h *testHarness) recordTaskCompletion(t *testing.T, taskType, taskID string, taskVersion uint64, node string, errMsg *string) {
	c := toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
		Type:                 taskType,
		Id:                   taskID,
		Version:              taskVersion,
		NodeId:               node,
		Error:                errMsg,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	})

	require.NoError(t, h.manager.RecordNodeCompletion(c, h.nodesInTheCluster))
}

func (h *testHarness) expectCleanUpTask(t *testing.T, expectTaskType, expectTaskID string, expectTaskVersion uint64) {
	h.stateChanger.EXPECT().CleanUpDistributedTask(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, taskType, taskID string, taskVersion uint64) error {
			require.Equal(t, expectTaskType, taskType)
			require.Equal(t, expectTaskID, taskID)
			require.Equal(t, expectTaskVersion, taskVersion)

			err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Type:    taskType,
				Id:      taskID,
				Version: taskVersion,
			}))
			require.NoError(t, err)
			return nil
		})
}

func (h *testHarness) startScheduler(t *testing.T) {
	require.NoError(t, h.scheduler.Start())

	// give some time for the newly launched goroutines to start
	time.Sleep(50 * time.Millisecond)
}

type testTask struct {
	*Task

	completeCh chan struct{}
	failCh     chan string
	cancelCh   chan struct{}

	provider *testTaskProvider
}

func newTestTask(task *Task, p *testTaskProvider) *testTask {
	t := &testTask{
		Task:     task,
		provider: p,

		completeCh: make(chan struct{}),
		failCh:     make(chan string),
		cancelCh:   make(chan struct{}),
	}

	go t.run()

	return t
}

func (t *testTask) run() {
	t.provider.startedCh <- t

	select {
	case <-t.completeCh:
		err := t.provider.recorder.RecordDistributedTaskNodeCompletion(context.Background(), t.Type, t.ID, t.Version)
		if err != nil {
			panic(err) // TODO: hmm?
		}
		t.provider.completedCh <- t
		return
	case errMsh := <-t.failCh:
		err := t.provider.recorder.RecordDistributedTaskNodeFailed(context.Background(), t.Type, t.ID, t.Version, errMsh)
		if err != nil {
			panic(err) // TODO: hmm?
		}
		t.provider.failedCh <- t
	case <-t.cancelCh:
		t.provider.cancelledCh <- t
		return
	}
}

func (t *testTask) Complete() {
	close(t.completeCh)
}

func (t *testTask) Terminate() {
	close(t.cancelCh)
}

func (t *testTask) Fail(errMsg string) {
	t.failCh <- errMsg
}

type testTaskProvider struct {
	initialLocalTaskIds []TaskDescriptor
	cleanedUpTasks      map[TaskDescriptor]struct{}

	startedCh   chan *testTask
	completedCh chan *testTask
	failedCh    chan *testTask
	cancelledCh chan *testTask

	recorder TaskStatusChanger
}

func newTestTaskProvider(initialLocalTaskIds []TaskDescriptor) *testTaskProvider {
	return &testTaskProvider{
		initialLocalTaskIds: initialLocalTaskIds,
		cleanedUpTasks:      make(map[TaskDescriptor]struct{}),

		// give the channels plenty of space to avoid blocking test
		startedCh:   make(chan *testTask, 100),
		completedCh: make(chan *testTask, 100),
		failedCh:    make(chan *testTask, 100),
		cancelledCh: make(chan *testTask, 100),
	}
}

func (p *testTaskProvider) SetCompletionRecorder(recorder TaskStatusChanger) {
	p.recorder = recorder
}

func (p *testTaskProvider) GetLocalTaskIDs() []TaskDescriptor {
	return p.initialLocalTaskIds
}

func (p *testTaskProvider) CleanupTask(desc TaskDescriptor) error {
	p.cleanedUpTasks[desc] = struct{}{}
	return nil
}

func (p *testTaskProvider) StartTask(task *Task) (TaskHandle, error) {
	return newTestTask(task, p), nil
}
