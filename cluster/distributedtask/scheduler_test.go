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

const (
	testUnitLocal  = "su-local"
	testUnitRemote = "su-remote"
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
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, h.tasksNamespace, startedTask.Namespace)
	require.Equal(t, taskID, startedTask.ID)
	require.Equal(t, taskPayload, startedTask.Payload)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, taskID, version)
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

	h := newTestHarness(t).init(t)

	var (
		taskID             = "1234"
		version     uint64 = 10
		taskPayload        = []byte("payload")
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// Two units: one for local node, one for remote node.
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		Payload:               taskPayload,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-local", "su-remote"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	// local task launched
	localTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, localTask.ID)

	// Remote node claims su-remote via a progress update, so the local scheduler
	// knows it only owns su-local.
	updateProgress(t, h, h.tasksNamespace, taskID, version, "remote-node", "su-remote", 0.1)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, taskID, version)
	localTask.Complete()
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	// local task completed
	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// however, task is not finished in the cluster yet (remote unit still pending)
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, TaskStatusStarted, tasks[0].Status)

	// remote node completes its unit
	completeUnit(t, h, h.tasksNamespace, taskID, version, "remote-node", "su-remote")

	tasks = h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)
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
		UnitIds:               []string{"su-1"},
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

	h := newTestHarness(t).init(t)
	var (
		taskID         = "1234"
		version uint64 = 10
	)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// Two units: one for local node, one for remote node.
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-local", "su-remote"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	// remote node fails its unit
	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	failUnit(t, h, h.tasksNamespace, taskID, version, "other-node", "su-remote", failureMessage)

	// locally running task should be cancelled (task is now FAILED)
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

	h := newTestHarness(t).init(t)
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
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	h.expectRecordUnitFailure(t, h.tasksNamespace, taskID, version, failureMessage)
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
			UnitIds:               []string{"su-1"},
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
		UnitIds:               []string{"su-1"},
	}), 15)
	require.NoError(t, err)

	// add one new task
	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "4",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
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
		UnitIds:               []string{"su-1"},
	}), 1)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, startedTask.ID, startedTask.Version)
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
		UnitIds:               []string{"su-1"},
	}), 10)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             tasksNamespace2,
		Id:                    "fail",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 11)
	require.NoError(t, err)

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             tasksNamespace1,
		Id:                    "cancel",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
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

	h.expectRecordUnitCompletion(t, tasksNamespace1, "complete", 10)
	startedTasks["complete"].Complete()
	recvWithTimeout(t, provider1.completedCh)

	h.expectRecordUnitFailure(t, tasksNamespace2, "fail", 11, "failed")
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
		UnitIds:               []string{"su-1"},
	}), 1)
	require.NoError(t, err)

	h.advanceClock(h.schedulerTickInterval)

	startedTaskV1 := recvWithTimeout(t, h.provider.startedCh)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, startedTaskV1.ID, startedTaskV1.Version)
	startedTaskV1.Complete()
	recvWithTimeout(t, h.provider.completedCh)

	h.advanceClock(h.schedulerTickInterval)

	require.Zero(t, h.scheduler.totalRunningTaskCount())

	err = h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1",
		Payload:               []byte("new payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
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

// expectRecordUnitCompletion sets up the mock to expect unit completion and
// actually records it with the manager.
func (h *testHarness) expectRecordUnitCompletion(t *testing.T, expectNamespace, expectTaskID string, expectTaskVersion uint64) {
	h.completionRecorder.EXPECT().RecordDistributedTaskUnitCompletion(
		mock.Anything, expectNamespace, expectTaskID, expectTaskVersion, mock.Anything, mock.Anything,
	).RunAndReturn(func(_ context.Context, namespace, taskID string, taskVersion uint64, nodeID, unitID string) error {
		completeUnit(t, h, namespace, taskID, taskVersion, nodeID, unitID)
		return nil
	})
}

// expectRecordUnitFailure sets up the mock to expect unit failure and
// actually records it with the manager.
func (h *testHarness) expectRecordUnitFailure(t *testing.T, expectNamespace, expectTaskID string, expectTaskVersion uint64, expectErrMsg string) {
	h.completionRecorder.EXPECT().RecordDistributedTaskUnitFailure(
		mock.Anything, expectNamespace, expectTaskID, expectTaskVersion, mock.Anything, mock.Anything, expectErrMsg,
	).RunAndReturn(func(_ context.Context, namespace, taskID string, taskVersion uint64, nodeID, unitID, errMsg string) error {
		failUnit(t, h, namespace, taskID, taskVersion, nodeID, unitID, errMsg)
		return nil
	})
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
	doneCh    chan struct{}

	provider *testTaskProvider
}

func newTestTask(task *Task, p *testTaskProvider) *testTask {
	t := &testTask{
		Task:     task,
		provider: p,

		completeCh: make(chan struct{}),
		failCh:     make(chan string),
		cancelCh:   make(chan struct{}),
		doneCh:     make(chan struct{}),
	}

	go t.run()

	return t
}

func (t *testTask) run() {
	defer close(t.doneCh)

	t.provider.startedCh <- t

	// Find the first non-terminal unit assigned to this node (or unassigned)
	var suID string
	for id, su := range t.Units {
		if su.Status != UnitStatusCompleted && su.Status != UnitStatusFailed {
			if su.NodeID == "" || su.NodeID == t.provider.nodeID {
				suID = id
				break
			}
		}
	}

	select {
	case <-t.completeCh:
		if suID != "" {
			err := t.provider.recorder.RecordDistributedTaskUnitCompletion(
				context.Background(), t.Namespace, t.ID, t.Version, t.provider.nodeID, suID,
			)
			require.NoError(t.provider.t, err)
		}
		t.provider.completedCh <- t
		return
	case errMsg := <-t.failCh:
		if suID != "" {
			err := t.provider.recorder.RecordDistributedTaskUnitFailure(
				context.Background(), t.Namespace, t.ID, t.Version, t.provider.nodeID, suID, errMsg,
			)
			require.NoError(t.provider.t, err)
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
	if t.cancelled.CompareAndSwap(false, true) {
		close(t.cancelCh)
	}
}

func (t *testTask) Done() <-chan struct{} { return t.doneCh }

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
	nodeID   string
}

func newTestTaskProvider(t *testing.T, initialLocalTaskIds []TaskDescriptor) *testTaskProvider {
	return &testTaskProvider{
		t: t,

		localTaskIds: initialLocalTaskIds,
		nodeID:       "local-node",

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

// groupCompletedEvent captures the arguments passed to OnGroupCompleted.
type groupCompletedEvent struct {
	Task              *Task
	GroupID           string
	LocalGroupUnitIDs []string
}

// testUnitAwareProvider extends testTaskProvider with unit awareness
type testUnitAwareProvider struct {
	*testTaskProvider
	onGroupCompletedCh chan groupCompletedEvent
	onTaskCompletedCh  chan *Task
}

func newTestUnitAwareProvider(t *testing.T) *testUnitAwareProvider {
	return &testUnitAwareProvider{
		testTaskProvider:   newTestTaskProvider(t, nil),
		onGroupCompletedCh: make(chan groupCompletedEvent, 100),
		onTaskCompletedCh:  make(chan *Task, 100),
	}
}

func (p *testUnitAwareProvider) OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) {
	p.onGroupCompletedCh <- groupCompletedEvent{Task: task, GroupID: groupID, LocalGroupUnitIDs: localGroupUnitIDs}
}

func (p *testUnitAwareProvider) OnTaskCompleted(task *Task) {
	p.onTaskCompletedCh <- task
}

// initUnitHarness sets up a test harness with a UnitAwareProvider for the given namespace.
func initUnitHarness(t *testing.T, namespace string) (*testHarness, *testUnitAwareProvider) {
	provider := newTestUnitAwareProvider(t)
	h := newTestHarness(t)
	h.tasksNamespace = namespace
	h.provider = provider.testTaskProvider
	h.registeredProviders = map[string]Provider{namespace: provider}
	h = h.init(t)
	return h, provider
}

func TestSubUnitTask_OnTaskCompletedFires(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchUnitTask(t, h, provider, namespace, "task1", version, []string{"su-1", "su-2"})

	// Complete both units
	for _, suID := range []string{"su-1", "su-2"} {
		completeUnit(t, h, namespace, "task1", version, h.localNodeID, suID)
	}

	// Tick to detect the terminal state and fire OnTaskCompleted
	h.advanceClock(h.schedulerTickInterval)

	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, "task1", completedTask.ID)
	require.Equal(t, TaskStatusFinished, completedTask.Status)

	// Verify it fires exactly once by ticking again
	h.advanceClock(h.schedulerTickInterval)
	select {
	case <-provider.onTaskCompletedCh:
		require.Fail(t, "OnTaskCompleted should fire exactly once")
	case <-time.After(100 * time.Millisecond):
	}

	startedTask.Terminate()
}

// launchAndFailUnit creates a task, assigns one unit to the local node,
// launches the task, fails the unit, and returns the started testTask.
func launchAndFailUnit(
	t *testing.T, h *testHarness, provider *testUnitAwareProvider,
	namespace string, version uint64,
) *testTask {
	t.Helper()
	addTaskWithUnits(t, h, namespace, "task1", version, []string{"su-1", "su-2"})
	updateProgress(t, h, namespace, "task1", version, h.localNodeID, "su-1", 0.1)

	h.advanceClock(h.schedulerTickInterval)
	startedTask := recvWithTimeout(t, provider.startedCh)

	failUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1", "oops")
	h.advanceClock(h.schedulerTickInterval)
	return startedTask
}

func TestSubUnitTask_OnTaskCompletedFires_OnFailure(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := launchAndFailUnit(t, h, provider, namespace, version)

	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, TaskStatusFailed, completedTask.Status)

	startedTask.Terminate()
}

func TestSubUnitTask_OnGroupCompletedFires(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchUnitTask(t, h, provider, namespace, "task1", version, []string{"su-1", "su-2"})

	// Complete both units
	for _, suID := range []string{"su-1", "su-2"} {
		completeUnit(t, h, namespace, "task1", version, h.localNodeID, suID)
	}

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted should fire with correct local unit IDs
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "task1", event.Task.ID)
	require.ElementsMatch(t, []string{"su-1", "su-2"}, event.LocalGroupUnitIDs)

	// OnTaskCompleted should also fire
	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, "task1", completedTask.ID)
	require.Equal(t, TaskStatusFinished, completedTask.Status)

	startedTask.Terminate()
}

func TestSubUnitTask_OnGroupCompletedBeforeOnTaskCompleted(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchUnitTask(t, h, provider, namespace, "task1", version, []string{"su-1"})

	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	h.advanceClock(h.schedulerTickInterval)

	// Both callbacks fire in the same tick. Verify OnGroupCompleted fires first
	// by draining both channels and checking order.
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "task1", event.Task.ID)

	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, "task1", completedTask.ID)

	startedTask.Terminate()
}

func TestSubUnitTask_OnGroupCompletedOnFailure(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := launchAndFailUnit(t, h, provider, namespace, version)

	// OnGroupCompleted should fire on failure too
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "task1", event.Task.ID)
	require.Equal(t, TaskStatusFailed, event.Task.Status)
	require.ElementsMatch(t, []string{"su-1"}, event.LocalGroupUnitIDs)

	// OnTaskCompleted should also fire
	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, TaskStatusFailed, completedTask.Status)

	startedTask.Terminate()
}

func TestSubUnitTask_OnGroupCompletedSkipsNodesWithNoLocalSubUnits(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	addTaskWithUnits(t, h, namespace, "task1", version, []string{"su-1"})

	// Assign the unit to a DIFFERENT node
	updateProgress(t, h, namespace, "task1", version, "remote-node", "su-1", 0.1)
	completeUnit(t, h, namespace, "task1", version, "remote-node", "su-1")

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted should NOT fire because this node has no local units
	select {
	case <-provider.onGroupCompletedCh:
		require.Fail(t, "OnGroupCompleted should not fire on node with no local units")
	case <-time.After(100 * time.Millisecond):
	}

	// OnTaskCompleted should still fire
	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, "task1", completedTask.ID)
	require.Equal(t, TaskStatusFinished, completedTask.Status)
}

func TestSubUnitTask_CallbacksFireExactlyOnce(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchUnitTask(t, h, provider, namespace, "task1", version, []string{"su-1"})

	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	h.advanceClock(h.schedulerTickInterval)

	// Drain the callbacks
	recvWithTimeout(t, provider.onGroupCompletedCh)
	recvWithTimeout(t, provider.onTaskCompletedCh)

	// Tick again — no extra events
	h.advanceClock(h.schedulerTickInterval)
	select {
	case <-provider.onGroupCompletedCh:
		require.Fail(t, "OnGroupCompleted should fire exactly once")
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-provider.onTaskCompletedCh:
		require.Fail(t, "OnTaskCompleted should fire exactly once")
	case <-time.After(100 * time.Millisecond):
	}

	startedTask.Terminate()
}

func TestSubUnitTask_DeadHandleDetection(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	addTaskWithUnits(t, h, namespace, "task1", version, []string{"su-1", "su-2"})

	h.advanceClock(h.schedulerTickInterval)

	// Task is started — the provider goroutine picks it up
	startedTask1 := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "task1", startedTask1.ID)

	// Simulate the provider goroutine exiting after completing some work (but not all units).
	// In real life this happens when processUnits returns after processing only local shards.
	// We terminate the task to cause the goroutine to exit.
	startedTask1.Terminate()
	// Wait for Done() to close
	<-startedTask1.Done()

	// At this point the handle is dead but there are still pending units.
	// On next tick, the scheduler should detect the dead handle and re-launch.
	h.advanceClock(h.schedulerTickInterval)

	startedTask2 := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "task1", startedTask2.ID)
	startedTask2.Terminate()
}

func TestSubUnitTask_NoSpuriousRestart(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	addTaskWithUnits(t, h, namespace, "task1", version, []string{"su-1"})

	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "task1", startedTask.ID)

	// Tick again — the handle is still alive (goroutine is blocked on completeCh/failCh/cancelCh).
	// The scheduler should NOT restart the task.
	h.advanceClock(h.schedulerTickInterval)

	select {
	case task := <-provider.startedCh:
		require.Fail(t, "task should not be restarted while handle is alive", "got task %s", task.ID)
	case <-time.After(200 * time.Millisecond):
		// expected — no spurious restart
	}

	startedTask.Terminate()
}

func TestSubUnitTask_ProviderErrorRecovery(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	addTaskWithUnits(t, h, namespace, "task1", version, []string{"su-1"})

	h.advanceClock(h.schedulerTickInterval)

	// First launch — simulate provider goroutine exiting early (e.g. GetLocalShardNames failed)
	startedTask1 := recvWithTimeout(t, provider.startedCh)
	startedTask1.Terminate()
	<-startedTask1.Done()

	// Scheduler should re-launch on next tick since units are still pending
	h.advanceClock(h.schedulerTickInterval)

	startedTask2 := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "task1", startedTask2.ID)

	// Now complete the unit so the task finishes
	updateProgress(t, h, namespace, "task1", version, h.localNodeID, "su-1", 0.1)
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	startedTask2.Terminate()

	h.advanceClock(h.schedulerTickInterval)

	// Task should be finished now
	tasks := h.listManagerTasks(t)[namespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)
}

func TestSubUnitTask_MultiNodeSimulation(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startUnitTest(t)
	defer h.scheduler.Close()

	addTaskWithUnits(t, h, namespace, "task1", version, []string{testUnitLocal, testUnitRemote})

	// Assign su-local to local node, su-remote to a remote node
	updateProgress(t, h, namespace, "task1", version, h.localNodeID, testUnitLocal, 0.1)
	updateProgress(t, h, namespace, "task1", version, "remote-node", testUnitRemote, 0.1)

	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, "task1", startedTask.ID)

	// Complete local unit
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, testUnitLocal)

	// Remote unit is still pending — task is still STARTED
	tasks := h.listManagerTasks(t)[namespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusStarted, tasks[0].Status)

	// Complete remote unit
	completeUnit(t, h, namespace, "task1", version, "remote-node", testUnitRemote)

	// Task should now be FINISHED
	tasks = h.listManagerTasks(t)[namespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted should fire with only the local unit
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.ElementsMatch(t, []string{testUnitLocal}, event.LocalGroupUnitIDs)

	startedTask.Terminate()
}

func TestLegacyTask_NoSubUnits_UnchangedBehavior(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)

	h.startScheduler(t)
	defer h.scheduler.Close()

	var (
		taskID         = "legacy-task"
		version uint64 = 10
	)

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, taskID, version)
	startedTask.Complete()
	recvWithTimeout(t, h.provider.completedCh)

	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())
}

// failingLister wraps a TasksLister and makes the first N calls fail.
type failingLister struct {
	delegate   TasksLister
	failCount  int
	callsMu    sync.Mutex
	callsSoFar int
}

func (f *failingLister) ListDistributedTasks(ctx context.Context) (map[string][]*Task, error) {
	f.callsMu.Lock()
	defer f.callsMu.Unlock()
	f.callsSoFar++
	if f.callsSoFar <= f.failCount {
		return nil, fmt.Errorf("simulated Raft not ready")
	}
	return f.delegate.ListDistributedTasks(ctx)
}

func TestScheduler_StartsEvenWhenInitialListFails(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t)

	// Initialize manager first, then wrap it with a failing lister
	h.manager = NewManager(ManagerParameters{
		Clock:            h.clock,
		CompletedTaskTTL: h.completedTaskTTL,
	})

	lister := &failingLister{delegate: h.manager, failCount: 1}

	h.scheduler = NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRecorder,
		TasksLister:        lister,
		TaskCleaner:        h.cleaner,
		Providers:          h.registeredProviders,
		Clock:              h.clock,
		Logger:             h.logger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          h.localNodeID,
		CompletedTaskTTL:   h.completedTaskTTL,
		TickInterval:       h.schedulerTickInterval,
	})

	// Start should succeed even though listing fails
	require.NoError(t, h.scheduler.Start(context.Background()))
	defer h.scheduler.Close()

	// Give goroutines time to start
	time.Sleep(50 * time.Millisecond)

	// Add a task — the scheduler loop should pick it up on the next tick
	var (
		taskID         = "task-after-fail"
		version uint64 = 10
	)

	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)

	// Advance clock — tick() will list tasks successfully (failCount=1 already exhausted by Start())
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	h.expectRecordUnitCompletion(t, h.tasksNamespace, taskID, version)
	startedTask.Complete()
	recvWithTimeout(t, h.provider.completedCh)

	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())
}

// addTaskWithUnitSpecs creates a task with grouped units via UnitSpecs.
func addTaskWithUnitSpecs(t *testing.T, h *testHarness, ns, id string, version uint64, specs []*cmd.UnitSpec) {
	t.Helper()
	err := h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             ns,
		Id:                    id,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs:             specs,
	}), version)
	require.NoError(t, err)
}

// startUnitTest initialises a unit harness with namespace "su-namespace",
// starts the scheduler, and registers a cleanup. It returns the harness,
// provider, namespace and a default version for convenience.
func startUnitTest(t *testing.T) (h *testHarness, provider *testUnitAwareProvider, namespace string, version uint64) {
	t.Helper()
	namespace = "su-namespace"
	h, provider = initUnitHarness(t, namespace)
	h.startScheduler(t)
	return h, provider, namespace, 10
}

// startGroupTest is the same as startUnitTest but uses "group-namespace".
func startGroupTest(t *testing.T) (h *testHarness, provider *testUnitAwareProvider, namespace string, version uint64) {
	t.Helper()
	namespace = "group-namespace"
	h, provider = initUnitHarness(t, namespace)
	h.startScheduler(t)
	return h, provider, namespace, 10
}

// addAndLaunchUnitTask adds a task with the given units, assigns them
// all to the local node via progress updates, advances the clock, and returns
// the started testTask.
func addAndLaunchUnitTask(
	t *testing.T, h *testHarness, provider *testUnitAwareProvider,
	namespace, taskID string, version uint64, units []string,
) *testTask {
	t.Helper()
	addTaskWithUnits(t, h, namespace, taskID, version, units)
	for _, suID := range units {
		updateProgress(t, h, namespace, taskID, version, h.localNodeID, suID, 0.1)
	}
	h.advanceClock(h.schedulerTickInterval)
	startedTask := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)
	return startedTask
}

// addAndLaunchGroupTask adds a task with UnitSpecs, assigns all units
// to the local node, advances the clock, and returns the started testTask.
func addAndLaunchGroupTask(
	t *testing.T, h *testHarness, provider *testUnitAwareProvider,
	namespace, taskID string, version uint64, specs []*cmd.UnitSpec,
) *testTask {
	t.Helper()
	addTaskWithUnitSpecs(t, h, namespace, taskID, version, specs)
	for _, spec := range specs {
		updateProgress(t, h, namespace, taskID, version, h.localNodeID, spec.Id, 0.1)
	}
	h.advanceClock(h.schedulerTickInterval)
	startedTask := recvWithTimeout(t, provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)
	return startedTask
}

func TestGroupTask_OnGroupCompletedFiresMidFlight(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startGroupTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchGroupTask(t, h, provider, namespace, "task1", version, []*cmd.UnitSpec{
		{Id: "su-1", GroupId: "groupA"},
		{Id: "su-2", GroupId: "groupA"},
		{Id: "su-3", GroupId: "groupB"},
		{Id: "su-4", GroupId: "groupB"},
	})

	// Complete groupA (su-1, su-2) but not groupB
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-2")

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted should fire for groupA mid-flight (task is still STARTED)
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "groupA", event.GroupID)
	require.ElementsMatch(t, []string{"su-1", "su-2"}, event.LocalGroupUnitIDs)
	require.Equal(t, TaskStatusStarted, event.Task.Status)

	// OnTaskCompleted should NOT fire yet (task is still STARTED)
	select {
	case <-provider.onTaskCompletedCh:
		require.Fail(t, "OnTaskCompleted should not fire while task is STARTED")
	case <-time.After(100 * time.Millisecond):
	}

	// Complete groupB
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-3")
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-4")

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted for groupB
	event = recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "groupB", event.GroupID)
	require.ElementsMatch(t, []string{"su-3", "su-4"}, event.LocalGroupUnitIDs)

	// OnTaskCompleted should fire now
	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, "task1", completedTask.ID)
	require.Equal(t, TaskStatusFinished, completedTask.Status)

	startedTask.Terminate()
}

func TestGroupTask_OneGroupFails(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startGroupTest(t)
	defer h.scheduler.Close()

	startedTask := addAndLaunchGroupTask(t, h, provider, namespace, "task1", version, []*cmd.UnitSpec{
		{Id: "su-1", GroupId: "groupA"},
		{Id: "su-2", GroupId: "groupB"},
	})

	// Complete groupA
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	h.advanceClock(h.schedulerTickInterval)

	eventA := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "groupA", eventA.GroupID)

	// Fail groupB → task goes FAILED
	failUnit(t, h, namespace, "task1", version, h.localNodeID, "su-2", "oops")
	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted for groupB should fire (task terminal)
	eventB := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "groupB", eventB.GroupID)
	require.Equal(t, TaskStatusFailed, eventB.Task.Status)

	// OnTaskCompleted should fire with FAILED
	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, TaskStatusFailed, completedTask.Status)

	startedTask.Terminate()
}

func TestGroupTask_DefaultGroupPreservesOldBehavior(t *testing.T) {
	defer leaktest.Check(t)()

	h, provider, namespace, version := startGroupTest(t)
	defer h.scheduler.Close()

	// No explicit groups — all units in default group ""
	startedTask := addAndLaunchUnitTask(t, h, provider, namespace, "task1", version, []string{"su-1", "su-2"})

	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-1")
	completeUnit(t, h, namespace, "task1", version, h.localNodeID, "su-2")

	h.advanceClock(h.schedulerTickInterval)

	// OnGroupCompleted fires once with groupID="" and all local units
	event := recvWithTimeout(t, provider.onGroupCompletedCh)
	require.Equal(t, "", event.GroupID)
	require.ElementsMatch(t, []string{"su-1", "su-2"}, event.LocalGroupUnitIDs)

	completedTask := recvWithTimeout(t, provider.onTaskCompletedCh)
	require.Equal(t, TaskStatusFinished, completedTask.Status)

	startedTask.Terminate()
}
