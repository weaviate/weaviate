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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestHappyPathTaskLifecycleWithSingleNode(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		h                  = newTestHarness(t).init(t)
		taskID             = "1234"
		version     uint64 = 10
		taskPayload        = []byte("payload")
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		Payload:               taskPayload,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, h.tasksNamespace, startedTask.Namespace)
	require.Equal(t, taskID, startedTask.ID)
	require.Equal(t, taskPayload, startedTask.Payload)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, taskID, version)
	startedTask.Complete()

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// advance the clock just before expected clean up time to check whether it respects it
	h.advanceClock(h.completedTaskTTL - h.clockAdvancedSoFar - time.Minute)

	h.expectCleanUpTask(t, h.tasksNamespace, taskID, version)
	h.advanceClock(h.schedulerTickInterval + time.Minute)

	require.Empty(t, h.listManagerTasks(t))
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
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		Payload:               taskPayload,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	// local task launched
	localTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, localTask.ID)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, taskID, version)
	localTask.Complete()
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	// local task completed
	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// however, task is not finished in the cluster yet
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, TaskStatusStarted, tasks[0].Status)

	// finish the task across the cluster
	h.completeTaskFromNode(t, h.tasksNamespace, taskID, version, "remote-node")

	tasks = h.listManagerTasks(t)[h.tasksNamespace]
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
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	cancellationTime := h.clock.Now().UnixMilli()
	err = h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		Version:               version,
		CancelledAtUnixMillis: cancellationTime,
	}))
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.cancelledCh).ID)

	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Namespace)
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
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	// send a failure command from another node
	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	h.recordTaskCompletion(t, h.tasksNamespace, taskID, version, "other-node", &failureMessage)

	// locally running task should be cancelled
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.cancelledCh).ID)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Namespace)
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
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	h.expectRecordNodeTaskFailure(t, h.tasksNamespace, taskID, version, failureMessage)
	startedTask.Fail(failureMessage)

	recvWithTimeout(t, h.provider.failedCh)

	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, h.tasksNamespace, tasks[0].Namespace)
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
			Namespace:             h.tasksNamespace,
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

func TestRemoveCleanedUpTaskLocalStateOnStartup(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		localTaskList = []TaskDescriptor{
			{ID: "1", Version: 1},
			{ID: "2", Version: 10},
			{ID: "3", Version: 15},
		}
		provider = newTestTaskProvider(t, localTaskList)
	)

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{
		h.tasksNamespace: provider,
	}
	h = h.init(t)

	// add one of the local tasks to the manager state before launching the scheduler
	// to simulate that it was there before the restart
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "3",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 15)
	require.NoError(t, err)

	// add one new task
	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "4",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 18)
	require.NoError(t, err)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// make sure only tasks are not running cleaned up
	cleanedUpTasks := collectChToSet(t, 2, provider.cleanedUpCh)
	require.Len(t, cleanedUpTasks, 2)
	require.Contains(t, cleanedUpTasks, localTaskList[0])
	require.Contains(t, cleanedUpTasks, localTaskList[1])

	expectStartedTasks := map[string]struct{}{"3": {}, "4": {}}
	for range len(expectStartedTasks) {
		startedTask := <-provider.startedCh
		require.Contains(t, expectStartedTasks, startedTask.ID)
		startedTask.Terminate()
	}
}

func TestRemoveCleanedUpTaskLocalStateDuringRuntime(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 1)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, startedTask.ID, startedTask.Version)
	startedTask.Complete()

	recvWithTimeout(t, h.provider.completedCh)

	h.expectCleanUpTask(t, h.tasksNamespace, startedTask.ID, startedTask.Version)
	h.advanceClock(h.completedTaskTTL)

	h.advanceClock(h.schedulerTickInterval)
	cleanedDesc := recvWithTimeout(t, h.provider.cleanedUpCh)
	require.Equal(t, startedTask.TaskDescriptor, cleanedDesc)
}

func TestMultiNamespaceMultiTasks(t *testing.T) {
	defer leaktest.Check(t)()

	var (
		tasksNamespace1     = "tasks-namespace-1"
		provider1StaleTasks = []TaskDescriptor{
			{ID: "1", Version: 1},
			{ID: "2", Version: 10},
		}
		provider1 = newTestTaskProvider(t, provider1StaleTasks)

		tasksNamespace2 = "tasks-namespace-2"
		provider2       = newTestTaskProvider(t, nil)
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
	cleanedUpTasks := collectChToSet(t, 2, provider1.cleanedUpCh)
	require.Len(t, cleanedUpTasks, 2)
	require.Contains(t, cleanedUpTasks, provider1StaleTasks[0])
	require.Contains(t, cleanedUpTasks, provider1StaleTasks[1])

	require.Len(t, provider2.cleanedUpCh, 0)

	// add some tasks for both providers
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             tasksNamespace1,
		Id:                    "complete",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 10)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             tasksNamespace2,
		Id:                    "fail",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 11)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             tasksNamespace1,
		Id:                    "cancel",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 12)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)
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
		Namespace:             tasksNamespace1,
		Id:                    "cancel",
		Version:               12,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	}))
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	require.Zero(t, h.scheduler.totalRunningTaskCount())
}

func TestOverrideExistingFinishedTask(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)

	h.startScheduler(t)
	defer h.scheduler.Close()

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1",
		Payload:               []byte("old payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 1)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	startedTaskV1 := recvWithTimeout(t, h.provider.startedCh)

	h.expectRecordNodeTaskCompletion(t, h.tasksNamespace, startedTaskV1.ID, startedTaskV1.Version)
	startedTaskV1.Complete()
	recvWithTimeout(t, h.provider.completedCh)

	h.advanceClock(h.schedulerTickInterval)

	require.Zero(t, h.scheduler.totalRunningTaskCount())

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1",
		Payload:               []byte("new payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), 2)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	startedTaskV2 := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, []byte("new payload"), startedTaskV2.Payload)
	startedTaskV2.Terminate()

	require.Equal(t, startedTaskV1.TaskDescriptor, recvWithTimeout(t, h.provider.cleanedUpCh))
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
	localNodeID           string
	tasksNamespace        string
	nodesInTheCluster     int
	completedTaskTTL      time.Duration
	schedulerTickInterval time.Duration
	clock                 *clockwork.FakeClock
	logger                logrus.FieldLogger
	completionRecorder    *MockTaskCompletionRecorder
	cleaner               *MockTaskCleaner
	provider              *testTaskProvider
	registeredProviders   map[string]Provider

	manager   *Manager
	scheduler *Scheduler

	clockAdvancedSoFar time.Duration
}

func newTestHarness(t *testing.T) *testHarness {
	var (
		defaultNamespace = "tasks-namespace"
		defaultProvider  = newTestTaskProvider(t, nil)
		logger, _        = logrustest.NewNullLogger()
	)

	return &testHarness{
		localNodeID:           "local-node",
		tasksNamespace:        defaultNamespace,
		nodesInTheCluster:     1,
		completedTaskTTL:      24 * time.Hour,
		schedulerTickInterval: 30 * time.Second,
		clock:                 clockwork.NewFakeClock(),
		logger:                logger,
		completionRecorder:    NewMockTaskCompletionRecorder(t),
		cleaner:               NewMockTaskCleaner(t),
		provider:              defaultProvider,
		registeredProviders: map[string]Provider{
			defaultNamespace: defaultProvider,
		},
	}
}

func (h *testHarness) init(t *testing.T) *testHarness {
	h.manager = NewManager(ManagerParameters{
		Clock:            h.clock,
		CompletedTaskTTL: h.completedTaskTTL,
	})

	h.scheduler = NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRecorder,
		TasksLister:        h.manager,
		TaskCleaner:        h.cleaner,
		Providers:          h.registeredProviders,
		Clock:              h.clock,
		Logger:             h.logger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          h.localNodeID,
		CompletedTaskTTL:   h.completedTaskTTL,
		TickInterval:       h.schedulerTickInterval,
	})
	return h
}

func (h *testHarness) advanceClock(duration time.Duration) {
	h.clock.Advance(duration)
	h.clockAdvancedSoFar += duration

	// after moving the clock, give some time for the unblocked goroutines to wake up and execute
	time.Sleep(50 * time.Millisecond)
}

func (h *testHarness) expectRecordNodeTaskCompletion(t *testing.T, expectNamespace, expectTaskID string, expectTaskVersion uint64) {
	h.completionRecorder.EXPECT().RecordDistributedTaskNodeCompletion(mock.Anything, expectNamespace, expectTaskID, expectTaskVersion).
		RunAndReturn(func(_ context.Context, namespace, taskID string, taskVersion uint64) error {
			h.completeTaskFromNode(t, namespace, taskID, taskVersion, h.localNodeID)
			return nil
		})
}

func (h *testHarness) expectRecordNodeTaskFailure(t *testing.T, expectNamespace, expectTaskID string, expectTaskVersion uint64, expectErrMsg string) {
	h.completionRecorder.EXPECT().RecordDistributedTaskNodeFailure(mock.Anything, expectNamespace, expectTaskID, expectTaskVersion, expectErrMsg).
		RunAndReturn(func(_ context.Context, namespace, taskID string, taskVersion uint64, errMsg string) error {
			h.recordTaskCompletion(t, namespace, taskID, taskVersion, h.localNodeID, &expectErrMsg)
			return nil
		})
}

func (h *testHarness) completeTaskFromNode(t *testing.T, namespace, taskID string, taskVersion uint64, node string) {
	h.recordTaskCompletion(t, namespace, taskID, taskVersion, node, nil)
}

func (h *testHarness) recordTaskCompletion(t *testing.T, namespace, taskID string, taskVersion uint64, node string, errMsg *string) {
	c := toCmd(t, &cmd.RecordDistributedTaskNodeCompletionRequest{
		Namespace:            namespace,
		Id:                   taskID,
		Version:              taskVersion,
		NodeId:               node,
		Error:                errMsg,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	})

	require.NoError(t, h.manager.RecordNodeCompletion(c, h.nodesInTheCluster))
}

func (h *testHarness) expectCleanUpTask(t *testing.T, expectNamespace, expectTaskID string, expectTaskVersion uint64) {
	h.cleaner.EXPECT().CleanUpDistributedTask(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, namespace, taskID string, taskVersion uint64) error {
			require.Equal(t, expectNamespace, namespace)
			require.Equal(t, expectTaskID, taskID)
			require.Equal(t, expectTaskVersion, taskVersion)

			err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: namespace,
				Id:        taskID,
				Version:   taskVersion,
			}))
			require.NoError(t, err)
			return nil
		})
}

func (h *testHarness) startScheduler(t *testing.T) {
	require.NoError(t, h.scheduler.Start(context.Background()))

	// give some time for the newly launched goroutines to start
	time.Sleep(50 * time.Millisecond)
}

func (h *testHarness) listManagerTasks(t *testing.T) map[string][]*Task {
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	return tasks
}

type testTask struct {
	*Task

	completeCh chan struct{}
	failCh     chan string

	cancelled atomic.Bool
	cancelCh  chan struct{}

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
		err := t.provider.recorder.RecordDistributedTaskNodeCompletion(context.Background(), t.Namespace, t.ID, t.Version)
		require.NoError(t.provider.t, err)
		t.provider.completedCh <- t
		return
	case errMsg := <-t.failCh:
		err := t.provider.recorder.RecordDistributedTaskNodeFailure(context.Background(), t.Namespace, t.ID, t.Version, errMsg)
		require.NoError(t.provider.t, err)
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
	if t.cancelled.CompareAndSwap(false, true) {
		close(t.cancelCh)
	}
}

func (t *testTask) Fail(errMsg string) {
	t.failCh <- errMsg
}

type testTaskProvider struct {
	t *testing.T

	mu           sync.Mutex
	localTaskIds []TaskDescriptor

	startedCh   chan *testTask
	completedCh chan *testTask
	failedCh    chan *testTask
	cancelledCh chan *testTask
	cleanedUpCh chan TaskDescriptor

	recorder TaskCompletionRecorder
}

func newTestTaskProvider(t *testing.T, initialLocalTaskIds []TaskDescriptor) *testTaskProvider {
	return &testTaskProvider{
		t: t,

		localTaskIds: initialLocalTaskIds,

		// give the channels plenty of space to avoid blocking test
		startedCh:   make(chan *testTask, 100),
		completedCh: make(chan *testTask, 100),
		failedCh:    make(chan *testTask, 100),
		cancelledCh: make(chan *testTask, 100),
		cleanedUpCh: make(chan TaskDescriptor, 100),
	}
}

func (p *testTaskProvider) SetCompletionRecorder(recorder TaskCompletionRecorder) {
	p.recorder = recorder
}

func (p *testTaskProvider) GetLocalTasks() []TaskDescriptor {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.localTaskIds
}

func (p *testTaskProvider) CleanupTask(desc TaskDescriptor) error {
	p.cleanedUpCh <- desc
	return nil
}

func (p *testTaskProvider) StartTask(task *Task) (TaskHandle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.localTaskIds = append(p.localTaskIds, task.TaskDescriptor)
	return newTestTask(task, p), nil
}

func collectChToSet[T comparable](t *testing.T, expectCount int, ch chan T) map[T]struct{} {
	cleanedUpTasks := map[T]struct{}{}
	for range expectCount {
		cleanedUpTasks[recvWithTimeout(t, ch)] = struct{}{}
	}
	return cleanedUpTasks
}
