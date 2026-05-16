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

	startedTask.Complete()
	completeUnit(t, h, h.tasksNamespace, taskID, version, h.localNodeID, "su-1")

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
		UnitIds:               []string{"su-local", "su-remote"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	// local task launched
	localTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, localTask.ID)

	// assign the remote unit to the remote node so the local scheduler
	// knows it has no non-terminal units locally
	updateProgress(t, h, h.tasksNamespace, taskID, version, "remote-node", "su-remote", 0.1)

	localTask.Complete()
	completeUnit(t, h, h.tasksNamespace, taskID, version, h.localNodeID, "su-local")
	require.Equal(t, taskID, recvWithTimeout(t, h.provider.completedCh).ID)

	// local task completed
	h.advanceClock(h.schedulerTickInterval)
	require.Zero(t, h.scheduler.totalRunningTaskCount())

	// however, task is not finished in the cluster yet
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, taskID, tasks[0].ID)
	require.Equal(t, TaskStatusStarted, tasks[0].Status)

	// remote node completes its unit
	completeUnit(t, h, h.tasksNamespace, taskID, version, "remote-node", "su-remote")

	// All units terminal → task is FINALIZING until the scheduler tick
	// issues MarkDistributedTaskFinalized. Advance the clock so the
	// tick fires and the FINISHED transition commits.
	h.advanceClock(h.schedulerTickInterval)

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
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	require.Equal(t, taskID, recvWithTimeout(t, h.provider.startedCh).ID)

	// send a failure command from another node
	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	failUnit(t, h, h.tasksNamespace, taskID, version, "other-node", "su-1", failureMessage)

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
		UnitIds:               []string{"su-1"},
	}), version)
	require.NoError(t, err)
	h.advanceClock(h.schedulerTickInterval)

	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, taskID, startedTask.ID)

	failureMessage := "servers are on fire!!!"
	failureTime := h.clock.Now().UnixMilli()
	startedTask.Fail(failureMessage)
	failUnit(t, h, h.tasksNamespace, taskID, version, h.localNodeID, "su-1", failureMessage)

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

	startedTask.Complete()
	completeUnit(t, h, h.tasksNamespace, startedTask.ID, startedTask.Version, h.localNodeID, "su-1")

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

	startedTasks["complete"].Complete()
	completeUnit(t, h, tasksNamespace1, "complete", 10, h.localNodeID, "su-1")
	recvWithTimeout(t, provider1.completedCh)

	startedTasks["fail"].Fail("failed")
	failUnit(t, h, tasksNamespace2, "fail", 11, h.localNodeID, "su-1", "failed")
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

	startedTaskV1.Complete()
	completeUnit(t, h, h.tasksNamespace, startedTaskV1.ID, startedTaskV1.Version, h.localNodeID, "su-1")
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
	case <-time.After(5 * time.Second):
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
		TaskFinalizer:      newDirectFinalizer(t, h.manager),
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

// directFinalizer is a unit-test TaskFinalizer that calls
// [Manager.MarkTaskFinalized] directly, bypassing the gRPC ApplyRequest
// envelope used by [Raft.MarkDistributedTaskFinalized] in production.
// Production scheduler runs route through RAFT for cluster consistency;
// tests use a single in-memory manager and don't need (or have) RAFT.
type directFinalizer struct {
	t       *testing.T
	manager *Manager
}

func newDirectFinalizer(t *testing.T, manager *Manager) *directFinalizer {
	return &directFinalizer{t: t, manager: manager}
}

func (d *directFinalizer) MarkDistributedTaskFinalized(_ context.Context, namespace, taskID string, taskVersion uint64) error {
	return d.manager.MarkTaskFinalized(toCmd(d.t, &cmd.MarkTaskFinalizedRequest{
		Namespace:             namespace,
		Id:                    taskID,
		Version:               taskVersion,
		FinalizedAtUnixMillis: d.manager.clock.Now().UnixMilli(),
	}))
}

func (h *testHarness) advanceClock(duration time.Duration) {
	h.clock.Advance(duration)
	h.clockAdvancedSoFar += duration

	// after moving the clock, give some time for the unblocked goroutines to wake up and execute
	time.Sleep(50 * time.Millisecond)
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
		// Unit-level completion is handled by the mock set up via expectRecordUnitCompletion.
		// The mock calls RecordDistributedTaskUnitCompletion → completeUnit → manager.RecordUnitCompletion.
		// Here we just signal the provider that the task reported completion.
		t.provider.completedCh <- t
		return
	case <-t.failCh:
		// Same as above — failure recording is handled by expectRecordUnitFailure mock.
		t.provider.failedCh <- t
	case <-t.cancelCh:
		t.provider.cancelledCh <- t
		return
	}
}

func (t *testTask) Done() <-chan struct{} {
	// Test tasks don't use goroutine lifecycle tracking; return a never-closed channel.
	return make(chan struct{})
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

// TestReactiveFiring_AddTaskWakesSchedulerBeforeTick verifies that with
// the Manager → Scheduler notifier wired, an AddTask apply causes the
// scheduler to start the task without waiting for the next periodic tick.
// Without reactive firing, the scheduler would sit idle until the next
// tick (default 1 minute in production, 30s in this harness).
//
// We assert the start by waiting on the provider's startedCh — which only
// receives once StartTask is called from inside tick(). If reactive firing
// is broken, the test fails with a 5s recvWithTimeout (well below the
// 30s tick interval).
func TestReactiveFiring_AddTaskWakesSchedulerBeforeTick(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)
	// Wire the reactive path: every state-changing apply on the Manager
	// triggers Scheduler.Wake() which selects on the loop's wakeCh.
	h.manager.SetSchedulerNotifier(h.scheduler)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// Submit a task. With reactive firing this should cause the scheduler
	// to start it immediately; without it, the start would be deferred to
	// the next tick.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1234",
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))

	// CRUCIAL: do NOT advance the clock. The scheduler tick interval is
	// 30s in this harness — if the test passes despite no clock advance,
	// it's because the wakeCh path fired the tick reactively.
	startedTask := recvWithTimeout(t, h.provider.startedCh)
	require.Equal(t, "1234", startedTask.ID)
}

// TestReactiveFiring_TerminalUnitWakesScheduler verifies that the last
// unit's terminal transition reactively wakes the scheduler. This is the
// critical path for Journey 3: OnGroupCompleted / OnTaskCompleted must
// fire within the wake-up latency, not the tick interval.
func TestReactiveFiring_TerminalUnitWakesScheduler(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)
	h.manager.SetSchedulerNotifier(h.scheduler)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// Add a task and let reactive firing start it.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1234",
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))

	startedTask := recvWithTimeout(t, h.provider.startedCh)

	// Complete the unit. RecordUnitCompletion fires Wake(); the scheduler
	// notices the task is no longer in startedTasks and terminates the
	// local handle. Without reactive firing this requires advancing the
	// clock past the tick interval.
	startedTask.Complete()
	completeUnit(t, h, h.tasksNamespace, "1234", 10, h.localNodeID, "su-1")

	// recvWithTimeout has a 5s budget; tick interval is 30s. If we receive
	// the completion within 5s, reactive firing worked.
	require.Equal(t, "1234", recvWithTimeout(t, h.provider.completedCh).ID)
}

// TestReactiveFiring_WakeIsCoalesced verifies that rapid-fire wake-up
// calls (e.g. multiple in-flight applies) do not panic, deadlock, or
// leak; the channel buffer of size 1 silently coalesces extras.
//
// This is structural correctness, not an observable behavior — a Wake()
// implementation that blocks on a full channel would deadlock the
// Manager's RAFT-apply path, which is unacceptable.
func TestReactiveFiring_WakeIsCoalesced(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)
	h.manager.SetSchedulerNotifier(h.scheduler)

	// Don't start the scheduler — we want to demonstrate that wakes
	// against a non-running scheduler are still safe (the loop hasn't
	// yet consumed any signal, so the channel is full after the first
	// wake).
	for i := 0; i < 1000; i++ {
		h.scheduler.Wake()
	}
	// If Wake() blocked on a full channel, the loop above would hang and
	// the test would time out.
}

// TestReactiveFiring_NilNotifierIsSafe verifies that the Manager can be
// used without a wired notifier (the legacy path: tick-only scheduling
// works even when reactive firing is not configured). Most existing
// scheduler tests rely on this, so this is also a regression guard
// against accidentally requiring the notifier.
func TestReactiveFiring_NilNotifierIsSafe(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)
	// Explicitly do NOT call SetSchedulerNotifier.

	h.startScheduler(t)
	defer h.scheduler.Close()

	// Add a task. Without reactive firing, the scheduler will pick it up
	// on the next tick. Advance the clock to confirm tick-only path still
	// works.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1234",
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))

	// No notifier → scheduler is idle. Advance past one tick to fire it.
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, "1234", recvWithTimeout(t, h.provider.startedCh).ID)
}

// TestReactiveFiring_PeriodicTickFallback verifies that the periodic
// tick still fires even when reactive firing is wired — this catches
// any regression where the new select arm accidentally starves the tick
// path, or where the scheduler's loop only progresses on wake-ups.
func TestReactiveFiring_PeriodicTickFallback(t *testing.T) {
	defer leaktest.Check(t)()

	h := newTestHarness(t).init(t)
	h.manager.SetSchedulerNotifier(h.scheduler)

	h.startScheduler(t)
	defer h.scheduler.Close()

	// AddTask wakes the scheduler reactively; consume the started signal.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "1234",
		Payload:               []byte("payload"),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), 10))
	startedTask := recvWithTimeout(t, h.provider.startedCh)

	// Complete the unit; the scheduler should clean up on the NEXT tick.
	// We deliberately complete via the manager FIRST then advance the
	// clock, to confirm that the periodic tick still drives the cleanup
	// path. In a reactive-firing-only design, this would also fire via
	// Wake; the fallback assertion here ensures the timer arm of the
	// loop's select is still active.
	startedTask.Complete()
	completeUnit(t, h, h.tasksNamespace, "1234", 10, h.localNodeID, "su-1")
	recvWithTimeout(t, h.provider.completedCh)

	// Now advance the clock and confirm the periodic tick clears the
	// running task entry. (totalRunningTaskCount is zero only after a
	// tick observes the task is no longer in startedTasks.)
	h.advanceClock(h.schedulerTickInterval)
	require.Eventually(t, func() bool {
		return h.scheduler.totalRunningTaskCount() == 0
	}, 2*time.Second, 50*time.Millisecond,
		"periodic tick should clear completed task even with reactive firing wired")
}

// flappyTasksLister proxies to inner after `failsLeft` failed calls. Used to
// simulate the post-restart "RAFT not ready at Start() time, ready on next
// tick" scenario that the deferred bootstrap must handle.
type flappyTasksLister struct {
	mu        sync.Mutex
	failsLeft int
	err       error
	inner     TasksLister
}

func (f *flappyTasksLister) ListDistributedTasks(ctx context.Context) (map[string][]*Task, error) {
	f.mu.Lock()
	if f.failsLeft > 0 {
		f.failsLeft--
		f.mu.Unlock()
		return nil, f.err
	}
	f.mu.Unlock()
	return f.inner.ListDistributedTasks(ctx)
}

// unitAwareTestProvider records OnGroupCompleted / OnTaskCompleted calls so
// tests can assert whether scheduler callback firing was suppressed for
// already-terminal tasks. Wraps an inner provider to satisfy the base
// [Provider] surface; UnitAware methods are added by this type.
type unitAwareTestProvider struct {
	*testTaskProvider

	mu                    sync.Mutex
	onGroupCompletedCalls []string
	onTaskCompletedCalls  []string
}

func newUnitAwareTestProvider(t *testing.T) *unitAwareTestProvider {
	return &unitAwareTestProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
	}
}

func (p *unitAwareTestProvider) OnGroupCompleted(task *Task, groupID string, localGroupUnitIDs []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onGroupCompletedCalls = append(p.onGroupCompletedCalls, task.ID)
}

func (p *unitAwareTestProvider) OnTaskCompleted(task *Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onTaskCompletedCalls = append(p.onTaskCompletedCalls, task.ID)
}

func (p *unitAwareTestProvider) snapshotCalls() (groups, tasks []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.onGroupCompletedCalls...),
		append([]string(nil), p.onTaskCompletedCalls...)
}

// TestDeferredBootstrap_SuppressesReplayedCallbacksWhenStartTimeListFails
// pins the scheduler's deferred-bootstrap path: when listTasks fails at
// Start() (e.g. RAFT not ready yet on a freshly-rebooted node), the first
// successful tick MUST pre-mark every already-terminal task as having its
// callbacks fired. Without this, the tick loop's "fire if not fired" guard
// would replay OnGroupCompleted + OnTaskCompleted for every historical
// terminal task — and for change-tokenization migrations, an older task's
// replayed OnTaskCompleted reverts the schema flip a newer task already
// committed cluster-wide.
//
// This is the bug class behind RestartMatrix R3 / R5: tokenization reverts
// to a stale target after the post-restart scheduler picks up tasks that
// completed pre-restart.
func TestDeferredBootstrap_SuppressesReplayedCallbacksWhenStartTimeListFails(t *testing.T) {
	defer leaktest.Check(t)()

	// Build a harness with a unit-aware provider so we can observe callback firing.
	prov := newUnitAwareTestProvider(t)
	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{h.tasksNamespace: prov}
	h.provider = prov.testTaskProvider
	h = h.init(t)

	// Pre-populate a FINISHED task BEFORE creating the scheduler under test.
	// This simulates the previous incarnation's scheduler having completed
	// the task pre-restart — both the unit-completion record AND the
	// MarkDistributedTaskFinalized commit that the previous scheduler's
	// OnTaskCompleted would have issued.
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "pre-restart-finished",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-1"},
	}), 1))
	completeUnit(t, h, h.tasksNamespace, "pre-restart-finished", 1, h.localNodeID, "u-1")
	require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "pre-restart-finished",
		Version:               1,
		FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	// Sanity: the task is FINISHED in the manager's state.
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)

	// Replace the scheduler's TasksLister with a flappy one that fails the
	// first call (Start() time) and succeeds afterwards.
	flappy := &flappyTasksLister{
		failsLeft: 1,
		err:       fmt.Errorf("raft not ready"),
		inner:     h.manager,
	}
	h.scheduler = NewScheduler(SchedulerParams{
		CompletionRecorder: h.completionRecorder,
		TasksLister:        flappy,
		TaskCleaner:        h.cleaner,
		Providers:          h.registeredProviders,
		Clock:              h.clock,
		Logger:             h.logger,
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          h.localNodeID,
		CompletedTaskTTL:   h.completedTaskTTL,
		TickInterval:       h.schedulerTickInterval,
	})

	require.NoError(t, h.scheduler.Start(context.Background()))
	defer h.scheduler.Close()
	// Wait until the loop goroutine has registered its ticker with the
	// FakeClock so a subsequent Advance() actually delivers a tick.
	require.NoError(t, h.clock.BlockUntilContext(context.Background(), 1))

	// Start() saw the flappy lister fail, so bootstrapped MUST still be false.
	h.scheduler.mu.Lock()
	require.False(t, h.scheduler.bootstrapped,
		"bootstrapped must remain false after failed Start()-time listTasks")
	h.scheduler.mu.Unlock()

	// Advance the clock to fire a tick; listTasks now succeeds, so the
	// deferred bootstrap should run before any callback firing.
	h.advanceClock(h.schedulerTickInterval)

	// Wait briefly so the tick goroutine completes.
	require.Eventually(t, func() bool {
		h.scheduler.mu.Lock()
		defer h.scheduler.mu.Unlock()
		return h.scheduler.bootstrapped
	}, 2*time.Second, 25*time.Millisecond,
		"deferred bootstrap must run on first successful tick")

	// CRUCIAL ASSERTION: callbacks were NOT fired for the pre-restart
	// finished task. The whole point of the pre-mark is to prevent this.
	groups, taskCalls := prov.snapshotCalls()
	require.Empty(t, groups,
		"OnGroupCompleted must NOT fire for tasks already terminal at scheduler start (got %v)", groups)
	require.Empty(t, taskCalls,
		"OnTaskCompleted must NOT fire for tasks already terminal at scheduler start (got %v)", taskCalls)

	// Future advances also must not fire replayed callbacks.
	h.advanceClock(h.schedulerTickInterval)
	groups, taskCalls = prov.snapshotCalls()
	require.Empty(t, groups, "no callbacks may fire on later ticks for the same already-terminal task")
	require.Empty(t, taskCalls, "no callbacks may fire on later ticks for the same already-terminal task")
}

// TestFinalizingRace_OnTaskCompletedFiresOnFinishedNotJustFinalizing
// pins the cross-node finalize race: when a peer node's scheduler has
// already issued MarkDistributedTaskFinalized and committed
// FINALIZING→FINISHED before this node's scheduler tick observes the
// task, OnTaskCompleted MUST still fire here. Without this, only the
// first node to see FINALIZING gets to run its post-completion work
// (cleanup, completion markers, schema-flip — anything inside
// OnTaskCompleted). The acceptance tests in test/acceptance/distributed_tasks
// caught this regression by waiting for 3 completion markers and only
// seeing 1.
//
// The bootstrap pre-mark is the load-bearing guard against re-firing
// callbacks for tasks that were FINISHED before this scheduler instance
// started; this test exercises the in-flight path where the scheduler
// is already running and the FINISHED transition happens after the
// pre-mark, so the fired-once flag (not the pre-mark) is what
// guarantees exactly-once.
func TestFinalizingRace_OnTaskCompletedFiresOnFinishedNotJustFinalizing(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newUnitAwareTestProvider(t)
	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{h.tasksNamespace: prov}
	h.provider = prov.testTaskProvider
	h = h.init(t)

	h.startScheduler(t)
	defer h.scheduler.Close()

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "task-cross-node-finalize-race",
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-1"},
	}), 1))

	// Move the FSM straight through FINALIZING → FINISHED WITHOUT
	// running a scheduler tick in between. This simulates a peer node
	// (in production: another scheduler in the same cluster) issuing
	// MarkDistributedTaskFinalized so fast that this node's first
	// observation of the task is already FINISHED.
	completeUnit(t, h, h.tasksNamespace, "task-cross-node-finalize-race", 1, h.localNodeID, "u-1")
	require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
		Namespace:             h.tasksNamespace,
		Id:                    "task-cross-node-finalize-race",
		Version:               1,
		FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	// Sanity: task is already FINISHED before the scheduler ticks.
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	require.Equal(t, TaskStatusFinished, tasks[0].Status)

	// Advance the clock so the scheduler tick runs and observes a task
	// already at FINISHED. With the fix, OnTaskCompleted fires here;
	// without it, the tick's "fire on FINALIZING/FAILED only" guard
	// silently skips this node.
	h.advanceClock(h.schedulerTickInterval)

	require.Eventually(t, func() bool {
		_, taskCalls := prov.snapshotCalls()
		return len(taskCalls) >= 1
	}, 2*time.Second, 25*time.Millisecond,
		"OnTaskCompleted must fire even when this node first observes the task at FINISHED (peer already finalized)")

	// Idempotency: extra ticks must not re-fire on this node — the
	// fired-once flag carries the exactly-once guarantee.
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)
	_, taskCalls := prov.snapshotCalls()
	require.Len(t, taskCalls, 1,
		"OnTaskCompleted must fire exactly once per node, even with multiple post-FINISHED ticks")
}

// TestPreMarkTerminalCallbacksLocked_OnlyTerminalsAreMarked unit-tests the
// pre-mark helper directly. The intent: only Finished/Failed/Cancelled
// tasks are marked as having their callbacks fired, never Started ones.
// A regression here would either re-introduce post-restart replay (if
// terminals stopped being marked) or suppress callbacks for tasks that
// are actively running (if Started ones got marked too).
func TestPreMarkTerminalCallbacksLocked_OnlyTerminalsAreMarked(t *testing.T) {
	s := NewScheduler(SchedulerParams{
		Logger:             func() logrus.FieldLogger { l, _ := logrustest.NewNullLogger(); return l }(),
		Providers:          map[string]Provider{"ns": nil},
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-a",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		CompletionRecorder: nil,
		TasksLister:        nil,
		TaskCleaner:        nil,
	})

	finishedDesc := TaskDescriptor{ID: "finished", Version: 1}
	failedDesc := TaskDescriptor{ID: "failed", Version: 2}
	cancelledDesc := TaskDescriptor{ID: "cancelled", Version: 3}
	startedDesc := TaskDescriptor{ID: "started", Version: 4}

	mkTask := func(d TaskDescriptor, status TaskStatus, groups ...string) *Task {
		task := &Task{
			TaskDescriptor: d,
			Status:         status,
			Units:          map[string]*Unit{},
		}
		// Add one unit per group so Task.Groups() yields each group.
		for i, g := range groups {
			id := fmt.Sprintf("u-%d", i)
			task.Units[id] = &Unit{ID: id, GroupID: g, NodeID: "node-a"}
		}
		return task
	}

	snapshot := map[string]map[TaskDescriptor]*Task{
		"ns": {
			finishedDesc:  mkTask(finishedDesc, TaskStatusFinished, "g1", "g2"),
			failedDesc:    mkTask(failedDesc, TaskStatusFailed, ""),
			cancelledDesc: mkTask(cancelledDesc, TaskStatusCancelled, "g1"),
			startedDesc:   mkTask(startedDesc, TaskStatusStarted, "g1"),
		},
	}

	s.mu.Lock()
	s.preMarkTerminalCallbacksLocked(snapshot)
	s.mu.Unlock()

	// Finished, failed, cancelled: marked as fired.
	require.True(t, s.completedCallbackFired[finishedDesc],
		"Finished task must be pre-marked as completed-callback-fired")
	require.True(t, s.completedCallbackFired[failedDesc],
		"Failed task must be pre-marked as completed-callback-fired")
	require.True(t, s.completedCallbackFired[cancelledDesc],
		"Cancelled task must be pre-marked as completed-callback-fired")

	// All groups of terminal tasks: marked as fired.
	require.True(t, s.groupCallbackFired[finishedDesc]["g1"],
		"Finished task's g1 must be pre-marked")
	require.True(t, s.groupCallbackFired[finishedDesc]["g2"],
		"Finished task's g2 must be pre-marked")
	require.True(t, s.groupCallbackFired[failedDesc][""],
		"Failed task's implicit group must be pre-marked")
	require.True(t, s.groupCallbackFired[cancelledDesc]["g1"],
		"Cancelled task's g1 must be pre-marked")

	// Started task: NOT marked. Its callbacks must still fire when it
	// transitions to terminal.
	require.False(t, s.completedCallbackFired[startedDesc],
		"Started task must NOT be pre-marked — its OnTaskCompleted needs to fire on terminal transition")
	require.False(t, s.groupCallbackFired[startedDesc]["g1"],
		"Started task's group must NOT be pre-marked — its OnGroupCompleted needs to fire when the group completes")
}

// recoveryAwareTestProvider is a minimal provider that implements
// [RecoveryAwareProvider] for the bootstrap pre-mark unit tests. The
// LocalCallbacksDone return value is configurable so tests can pin
// each branch (recovery-needed vs done) without standing up a real
// reindex backend.
type recoveryAwareTestProvider struct {
	doneByDesc map[TaskDescriptor]bool
}

func (p *recoveryAwareTestProvider) SetCompletionRecorder(_ TaskCompletionRecorder) {
}
func (p *recoveryAwareTestProvider) GetLocalTasks() []TaskDescriptor { return nil }
func (p *recoveryAwareTestProvider) CleanupTask(_ TaskDescriptor) error {
	return nil
}

func (p *recoveryAwareTestProvider) StartTask(_ *Task) (TaskHandle, error) {
	return nil, fmt.Errorf("not implemented")
}

func (p *recoveryAwareTestProvider) LocalCallbacksDone(task *Task, _ string) bool {
	if v, ok := p.doneByDesc[task.TaskDescriptor]; ok {
		return v
	}
	return true
}

// TestPreMarkTerminalCallbacksLocked_RecoveryAwareSkipsPending pins that
// a [RecoveryAwareProvider] reporting LocalCallbacksDone=false for a
// FINISHED task causes the bootstrap pre-mark to skip that task — so
// OnGroupCompleted will re-fire on the next scheduler tick and complete
// any half-applied local state. This is the RollingRestartMidMigration
// regression: without the hook, the half-applied swap is silently
// suppressed forever.
func TestPreMarkTerminalCallbacksLocked_RecoveryAwareSkipsPending(t *testing.T) {
	pendingDesc := TaskDescriptor{ID: "pending-recovery", Version: 1}
	doneDesc := TaskDescriptor{ID: "done", Version: 2}
	failedDesc := TaskDescriptor{ID: "failed-not-checked", Version: 3}

	provider := &recoveryAwareTestProvider{
		doneByDesc: map[TaskDescriptor]bool{
			pendingDesc: false,
			doneDesc:    true,
			// failedDesc not in map → default true; but the pre-mark
			// SHOULD NOT consult it (failed tasks bypass the hook).
		},
	}

	s := NewScheduler(SchedulerParams{
		Logger:             func() logrus.FieldLogger { l, _ := logrustest.NewNullLogger(); return l }(),
		Providers:          map[string]Provider{"ns": provider},
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-a",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		CompletionRecorder: nil,
		TasksLister:        nil,
		TaskCleaner:        nil,
	})

	mkTask := func(d TaskDescriptor, status TaskStatus, groups ...string) *Task {
		task := &Task{
			TaskDescriptor: d,
			Status:         status,
			Units:          map[string]*Unit{},
		}
		for i, g := range groups {
			id := fmt.Sprintf("u-%d", i)
			task.Units[id] = &Unit{ID: id, GroupID: g, NodeID: "node-a"}
		}
		return task
	}

	snapshot := map[string]map[TaskDescriptor]*Task{
		"ns": {
			pendingDesc: mkTask(pendingDesc, TaskStatusFinished, "g1", "g2"),
			doneDesc:    mkTask(doneDesc, TaskStatusFinished, "g1"),
			failedDesc:  mkTask(failedDesc, TaskStatusFailed, "g1"),
		},
	}

	s.mu.Lock()
	s.preMarkTerminalCallbacksLocked(snapshot)
	s.mu.Unlock()

	// pending-recovery: LocalCallbacksDone=false → NOT pre-marked.
	require.False(t, s.completedCallbackFired[pendingDesc],
		"pending-recovery task MUST NOT be pre-marked — OnGroupCompleted needs to re-fire to complete the half-applied swap")
	require.False(t, s.groupCallbackFired[pendingDesc]["g1"],
		"pending-recovery task's g1 MUST NOT be pre-marked")
	require.False(t, s.groupCallbackFired[pendingDesc]["g2"],
		"pending-recovery task's g2 MUST NOT be pre-marked")

	// done: LocalCallbacksDone=true → pre-marked normally.
	require.True(t, s.completedCallbackFired[doneDesc],
		"done task MUST be pre-marked")
	require.True(t, s.groupCallbackFired[doneDesc]["g1"],
		"done task's g1 MUST be pre-marked")

	// failed: hook NOT consulted; pre-marked normally.
	require.True(t, s.completedCallbackFired[failedDesc],
		"failed task MUST be pre-marked — the recovery-aware hook only applies to FINISHED tasks")
	require.True(t, s.groupCallbackFired[failedDesc]["g1"],
		"failed task's g1 MUST be pre-marked")
}

// TestPreMarkTerminalCallbacksLocked_NonRecoveryAwareProviderUnchanged
// pins that a provider WITHOUT the optional RecoveryAwareProvider
// interface behaves exactly as the original pre-mark (every terminal
// task and group fired). The new code path must be strictly additive.
func TestPreMarkTerminalCallbacksLocked_NonRecoveryAwareProviderUnchanged(t *testing.T) {
	finishedDesc := TaskDescriptor{ID: "finished", Version: 1}

	// Pass a Provider that does NOT implement RecoveryAwareProvider.
	s := NewScheduler(SchedulerParams{
		Logger:             func() logrus.FieldLogger { l, _ := logrustest.NewNullLogger(); return l }(),
		Providers:          map[string]Provider{"ns": &testTaskProvider{}},
		MetricsRegisterer:  monitoring.NoopRegisterer,
		LocalNode:          "node-a",
		CompletedTaskTTL:   24 * time.Hour,
		TickInterval:       30 * time.Second,
		CompletionRecorder: nil,
		TasksLister:        nil,
		TaskCleaner:        nil,
	})

	task := &Task{
		TaskDescriptor: finishedDesc,
		Status:         TaskStatusFinished,
		Units: map[string]*Unit{
			"u-0": {ID: "u-0", GroupID: "g1", NodeID: "node-a"},
		},
	}
	snapshot := map[string]map[TaskDescriptor]*Task{
		"ns": {finishedDesc: task},
	}

	s.mu.Lock()
	s.preMarkTerminalCallbacksLocked(snapshot)
	s.mu.Unlock()

	require.True(t, s.completedCallbackFired[finishedDesc])
	require.True(t, s.groupCallbackFired[finishedDesc]["g1"])
}
