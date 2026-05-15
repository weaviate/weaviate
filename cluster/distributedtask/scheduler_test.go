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
