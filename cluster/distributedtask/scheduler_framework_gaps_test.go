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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// TestSchedulerStartTaskRetry covers StartTask retry and unclaimed-unit
// launch semantics.
func TestSchedulerStartTaskRetry(t *testing.T) {
	t.Run("StartTask error is retried on the next tick", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		tickInterval := 100 * time.Millisecond

		var mu sync.Mutex
		startCallCount := 0
		failFirst := true

		provider := &stubProvider{
			startTask: func(task *Task) (TaskHandle, error) {
				mu.Lock()
				defer mu.Unlock()
				startCallCount++
				if failFirst {
					failFirst = false
					return nil, fmt.Errorf("transient error")
				}
				return &stubHandle{doneCh: make(chan struct{})}, nil
			},
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       tickInterval,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		// First tick: StartTask fails.
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		assert.GreaterOrEqual(t, startCallCount, 1)
		mu.Unlock()

		// Second tick: retry succeeds.
		clock.Advance(tickInterval + time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		assert.GreaterOrEqual(t, startCallCount, 2)
		mu.Unlock()
	})

	t.Run("unclaimed units count as everyone's for launch", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var mu sync.Mutex
		var startedTask *Task

		provider := &stubProvider{
			startTask: func(task *Task) (TaskHandle, error) {
				mu.Lock()
				startedTask = task
				mu.Unlock()
				return &stubHandle{doneCh: make(chan struct{})}, nil
			},
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       time.Minute,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1", "u2"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(50 * time.Millisecond)

		// NodeHasNonTerminalUnits treats empty NodeID as belonging to any node.
		mu.Lock()
		require.NotNil(t, startedTask)
		assert.Equal(t, "task-1", startedTask.ID)
		mu.Unlock()
	})
}

func TestStaleTaskDetection(t *testing.T) {
	const (
		ns          = "ns"
		taskID      = "task-1"
		nodeID      = "node-1"
		unitID      = "u1"
		staleTimeMs = 5000 // 5 seconds
	)

	t.Run("opt-out: no stale_timeout_ms means never fires", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			ForceTerminator:    ft,
			Providers:          map[string]Provider{ns: &stubProvider{startTask: stubStartOK}},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          nodeID,
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       100 * time.Millisecond,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             ns,
			Id:                    taskID,
			UnitIds:               []string{unitID},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			StaleTimeoutMs:        0,
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		// Advance well past any timeout.
		clock.Advance(time.Hour)
		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, 0, ft.callCount())
	})

	t.Run("unclaimed unit past the bound triggers force-terminate", func(t *testing.T) {
		// Drives runStaleDetectionPhase directly to avoid FakeClock
		// timing issues with the scheduler loop.
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		scheduler := &Scheduler{
			forceTerminator: ft,
			clock:           clock,
			logger:          logger,
			perTaskState:    map[TaskDescriptor]*taskSchedulerState{},
			localNode:       nodeID,
		}

		desc := TaskDescriptor{ID: taskID, Version: 1}
		task := &Task{
			Namespace:      ns,
			TaskDescriptor: desc,
			Status:         TaskStatusStarted,
			StaleTimeoutMs: staleTimeMs,
			Units: map[string]*Unit{
				unitID: {ID: unitID, Status: UnitStatusPending},
			},
		}
		tasks := map[TaskDescriptor]*Task{desc: task}

		// First call establishes watermarks.
		scheduler.runStaleDetectionPhase(tasks)
		assert.Equal(t, 0, ft.callCount())

		// Advance past the 5s stale timeout.
		clock.Advance(6 * time.Second)
		scheduler.runStaleDetectionPhase(tasks)
		assert.GreaterOrEqual(t, ft.callCount(), 1)
	})

	t.Run("stale in-progress unit triggers force-terminate", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		scheduler := &Scheduler{
			forceTerminator: ft,
			clock:           clock,
			logger:          logger,
			perTaskState:    map[TaskDescriptor]*taskSchedulerState{},
			localNode:       nodeID,
		}

		desc := TaskDescriptor{ID: taskID, Version: 1}
		task := &Task{
			Namespace:      ns,
			TaskDescriptor: desc,
			Status:         TaskStatusStarted,
			StaleTimeoutMs: staleTimeMs,
			Units: map[string]*Unit{
				unitID: {
					ID:       unitID,
					NodeID:   nodeID,
					Status:   UnitStatusInProgress,
					Progress: 0.3,
				},
			},
		}
		tasks := map[TaskDescriptor]*Task{desc: task}

		scheduler.runStaleDetectionPhase(tasks)
		assert.Equal(t, 0, ft.callCount())

		clock.Advance(6 * time.Second)
		scheduler.runStaleDetectionPhase(tasks)
		assert.GreaterOrEqual(t, ft.callCount(), 1)
	})

	t.Run("missing post-completion ack triggers force-terminate", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		scheduler := &Scheduler{
			forceTerminator: ft,
			clock:           clock,
			logger:          logger,
			perTaskState:    map[TaskDescriptor]*taskSchedulerState{},
			localNode:       nodeID,
		}

		desc := TaskDescriptor{ID: taskID, Version: 1}
		task := &Task{
			Namespace:      ns,
			TaskDescriptor: desc,
			Status:         TaskStatusSwapping,
			StaleTimeoutMs: staleTimeMs,
			Units: map[string]*Unit{
				"u1": {ID: "u1", NodeID: "node-1", Status: UnitStatusCompleted},
				"u2": {ID: "u2", NodeID: "node-2", Status: UnitStatusCompleted},
			},
			// node-1 acked, node-2 has not.
			PostCompletionAcks: map[string]PostCompletionAck{
				"node-1": {Success: true},
			},
		}
		tasks := map[TaskDescriptor]*Task{desc: task}

		scheduler.runStaleDetectionPhase(tasks)
		assert.Equal(t, 0, ft.callCount())

		clock.Advance(6 * time.Second)
		scheduler.runStaleDetectionPhase(tasks)
		assert.GreaterOrEqual(t, ft.callCount(), 1)
	})

	t.Run("watermark reset on restart delays detection by at most one timeout", func(t *testing.T) {
		// Delay is safe: a reset watermark postpones detection, never
		// causes a false-positive termination.
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		desc := TaskDescriptor{ID: taskID, Version: 1}
		task := &Task{
			Namespace:      ns,
			TaskDescriptor: desc,
			Status:         TaskStatusStarted,
			StaleTimeoutMs: staleTimeMs,
			Units: map[string]*Unit{
				unitID: {ID: unitID, Status: UnitStatusPending},
			},
		}
		tasks := map[TaskDescriptor]*Task{desc: task}

		sched1 := &Scheduler{
			forceTerminator: ft,
			clock:           clock,
			logger:          logger,
			perTaskState:    map[TaskDescriptor]*taskSchedulerState{},
			localNode:       nodeID,
		}
		sched1.runStaleDetectionPhase(tasks)
		clock.Advance(6 * time.Second)
		sched1.runStaleDetectionPhase(tasks)
		assert.Equal(t, 1, ft.callCount(), "first instance detects staleness")

		// Simulate restart.
		ft2 := &fakeForceTerminator{}
		sched2 := &Scheduler{
			forceTerminator: ft2,
			clock:           clock,
			logger:          logger,
			perTaskState:    map[TaskDescriptor]*taskSchedulerState{},
			localNode:       nodeID,
		}

		sched2.runStaleDetectionPhase(tasks)
		assert.Equal(t, 0, ft2.callCount(), "new instance does not fire immediately")

		clock.Advance(6 * time.Second)
		sched2.runStaleDetectionPhase(tasks)
		assert.GreaterOrEqual(t, ft2.callCount(), 1,
			"new instance detects staleness after one timeout period")
	})

	t.Run("skewed reporter timestamps do not trigger false positives", func(t *testing.T) {
		// Staleness uses local-observation watermarks, not cross-clock
		// comparison. A skewed UpdatedAt is harmless when state changes.
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()
		ft := &fakeForceTerminator{}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			ForceTerminator:    ft,
			Providers:          map[string]Provider{ns: &stubProvider{startTask: stubStartOK}},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          nodeID,
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       100 * time.Millisecond,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             ns,
			Id:                    taskID,
			UnitIds:               []string{unitID},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
			StaleTimeoutMs:        staleTimeMs,
		}), 1)
		require.NoError(t, err)

		// Claim the unit with a skewed UpdatedAt (far in the past).
		err = manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: ns, Id: taskID, Version: 1,
			NodeId: nodeID, UnitId: unitID,
			Progress:            0.0,
			UpdatedAtUnixMillis: time.Now().Add(-time.Hour).UnixMilli(), // skewed far in the past
		}))
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(50 * time.Millisecond)

		// Advance 3s (below 5s timeout), then update progress.
		clock.Advance(3 * time.Second)
		time.Sleep(50 * time.Millisecond)

		err = manager.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: ns, Id: taskID, Version: 1,
			NodeId: nodeID, UnitId: unitID,
			Progress:            0.5,
			UpdatedAtUnixMillis: time.Now().Add(-time.Hour).UnixMilli(), // still skewed
		}))
		require.NoError(t, err)

		// Another 3s. Total 6s, but watermark reset at the progress change
		// so only 3s since the last state change.
		clock.Advance(3 * time.Second)
		time.Sleep(50 * time.Millisecond)

		// Progress changed, resetting the watermark, despite the skewed
		// UpdatedAt.
		assert.Equal(t, 0, ft.callCount())
	})
}

func TestTerminalCleanupRefire(t *testing.T) {
	t.Run("non-implementing provider keeps one-shot behavior", func(t *testing.T) {
		// Task transitions to terminal after scheduler starts to avoid
		// the bootstrap pre-mark.
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var mu sync.Mutex
		callCount := 0
		provider := &unitAwareStubProvider{
			stubProvider: stubProvider{startTask: stubStartOK},
			onTaskCompleted: func(task *Task) error {
				mu.Lock()
				callCount++
				mu.Unlock()
				return fmt.Errorf("transient error")
			},
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       1 * time.Second,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		// Let the scheduler pick up the task.
		time.Sleep(50 * time.Millisecond)

		err = manager.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			Reason:                  "test",
			RequestedTerminalStatus: string(TaskStatusFailed),
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// OnTaskCompleted fires for the newly-FAILED task.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 1, callCount)
		mu.Unlock()

		// Fired mark is set; no TerminalCleanupProvider, so no re-fire.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 1, callCount)
		mu.Unlock()
	})

	t.Run("TerminalCleanupProvider error re-fires next tick", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var mu sync.Mutex
		callCount := 0
		provider := &terminalCleanupStubProvider{
			unitAwareStubProvider: unitAwareStubProvider{
				stubProvider: stubProvider{startTask: stubStartOK},
				onTaskCompleted: func(task *Task) error {
					mu.Lock()
					callCount++
					cc := callCount
					mu.Unlock()
					if cc <= 2 {
						return fmt.Errorf("backend down")
					}
					return nil
				},
			},
			cleanupDone: false,
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       1 * time.Second,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(50 * time.Millisecond)

		// Transition to FAILED after scheduler started.
		err = manager.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			Reason:                  "test",
			RequestedTerminalStatus: string(TaskStatusFailed),
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// Fires OnTaskCompleted; error clears the fired mark.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 1, callCount)
		mu.Unlock()

		// Re-fires, error again.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 2, callCount)
		mu.Unlock()

		// Succeeds on the third attempt.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 3, callCount)
		mu.Unlock()

		// No further re-fires after success.
		clock.Advance(1 * time.Second)
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		assert.Equal(t, 3, callCount)
		mu.Unlock()
	})

	t.Run("exhausts no attempts budget and proposes no MarkFailed on CANCELLED", func(t *testing.T) {
		// The terminal re-fire path bypasses the SWAPPING escalation budget
		// (completedCallbackAttempts). The FSM refuses CANCELLED->FAILED, so
		// this path must never propose MarkTaskFailed.
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var mu sync.Mutex
		callCount := 0
		provider := &terminalCleanupStubProvider{
			unitAwareStubProvider: unitAwareStubProvider{
				stubProvider: stubProvider{startTask: stubStartOK},
				onTaskCompleted: func(task *Task) error {
					mu.Lock()
					callCount++
					mu.Unlock()
					return fmt.Errorf("backend down")
				},
			},
			cleanupDone: false,
		}

		markFailedCalled := false
		finalizer := &spyFinalizer{
			onMarkFailed: func() { markFailedCalled = true },
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			TaskFinalizer:      finalizer,
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       1 * time.Second,
		})

		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(50 * time.Millisecond)

		// CancelTask accepts STARTED.
		err = manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			Version:               1,
			CancelledAtUnixMillis: time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		// 7 ticks exceeds maxCompletedCallbackAttempts (5).
		for i := 0; i < 7; i++ {
			clock.Advance(1 * time.Second)
			time.Sleep(100 * time.Millisecond)
		}

		mu.Lock()
		assert.GreaterOrEqual(t, callCount, 7,
			"OnTaskCompleted should re-fire on every tick")
		mu.Unlock()

		assert.False(t, markFailedCalled,
			"MarkDistributedTaskFailed must never be proposed on the terminal re-fire path")

		task := manager.GetDistributedTask(context.Background(), "ns", "task-1")
		assert.Equal(t, TaskStatusCancelled, task.Status)
	})

	t.Run("bootstrap skips pre-mark when cleanup is pending", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		clock := clockwork.NewFakeClock()

		var callCount atomic.Int32
		provider := &terminalCleanupStubProvider{
			unitAwareStubProvider: unitAwareStubProvider{
				stubProvider: stubProvider{startTask: stubStartOK},
				onTaskCompleted: func(task *Task) error {
					callCount.Add(1)
					return nil
				},
			},
			cleanupDone: false, // reports not-done
		}

		manager := NewManager(ManagerParameters{
			Clock:            clock,
			CompletedTaskTTL: 24 * time.Hour,
			Logger:           logger,
		})

		// Task is already FAILED before scheduler starts (restart scenario).
		err := manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
			Namespace:             "ns",
			Id:                    "task-1",
			UnitIds:               []string{"u1"},
			SubmittedAtUnixMillis: time.Now().UnixMilli(),
		}), 1)
		require.NoError(t, err)

		err = manager.ForceTerminateTask(toCmd(t, &cmd.ForceTerminateDistributedTaskRequest{
			Namespace: "ns", Id: "task-1", Version: 1,
			Reason:                  "test",
			RequestedTerminalStatus: string(TaskStatusFailed),
			TerminatedAtUnixMillis:  time.Now().UnixMilli(),
		}))
		require.NoError(t, err)

		scheduler := NewScheduler(SchedulerParams{
			CompletionRecorder: &stubRecorder{},
			TaskLister:         manager,
			TaskCleaner:        &stubCleaner{},
			Providers:          map[string]Provider{"ns": provider},
			Clock:              clock,
			Logger:             logger,
			MetricsRegisterer:  monitoring.NoopRegisterer,
			LocalNode:          "node-1",
			CompletedTaskTTL:   24 * time.Hour,
			TickInterval:       100 * time.Millisecond,
		})

		// Bootstrap skips the pre-mark because TerminalCleanupDone returns false.
		require.NoError(t, scheduler.Start(context.Background()))
		defer scheduler.Close()

		time.Sleep(50 * time.Millisecond)
		clock.Advance(200 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)

		// OnTaskCompleted fires because the task was not pre-marked.
		assert.GreaterOrEqual(t, int(callCount.Load()), 1)
	})
}

// Stubs

type stubProvider struct {
	startTask  func(*Task) (TaskHandle, error)
	localTasks []TaskDescriptor
	recorder   TaskCompletionRecorder
}

func (p *stubProvider) SetCompletionRecorder(r TaskCompletionRecorder) { p.recorder = r }
func (p *stubProvider) GetLocalTasks() []TaskDescriptor                { return p.localTasks }
func (p *stubProvider) CleanupTask(_ TaskDescriptor) error             { return nil }

func (p *stubProvider) StartTask(task *Task) (TaskHandle, error) {
	if p.startTask != nil {
		return p.startTask(task)
	}
	return &stubHandle{doneCh: make(chan struct{})}, nil
}

func stubStartOK(_ *Task) (TaskHandle, error) {
	return &stubHandle{doneCh: make(chan struct{})}, nil
}

type stubHandle struct {
	doneCh chan struct{}
}

func (h *stubHandle) Terminate()            {}
func (h *stubHandle) Done() <-chan struct{} { return h.doneCh }

type stubRecorder struct{}

func (r *stubRecorder) RecordDistributedTaskUnitCompletion(context.Context, string, string, uint64, string, string) error {
	return nil
}

func (r *stubRecorder) RecordDistributedTaskUnitFailure(context.Context, string, string, uint64, string, string, string) error {
	return nil
}

func (r *stubRecorder) RecordDistributedTaskRetryableUnitFailure(context.Context, string, string, uint64, string, string, string) error {
	return nil
}

func (r *stubRecorder) UpdateDistributedTaskUnitProgress(context.Context, string, string, uint64, string, string, float32) error {
	return nil
}

type stubCleaner struct{}

func (c *stubCleaner) CleanUpDistributedTask(context.Context, string, string, uint64) error {
	return nil
}

type fakeForceTerminator struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeForceTerminator) ForceTerminateDistributedTask(_ context.Context, _, _ string, _ uint64, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return nil
}

func (f *fakeForceTerminator) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type unitAwareStubProvider struct {
	stubProvider
	onGroupCompleted func(*Task, string, []string) error
	onSwapRequested  func(*Task, string, []string) error
	onTaskCompleted  func(*Task) error
}

func (p *unitAwareStubProvider) OnGroupCompleted(task *Task, groupID string, localIDs []string) error {
	if p.onGroupCompleted != nil {
		return p.onGroupCompleted(task, groupID, localIDs)
	}
	return nil
}

func (p *unitAwareStubProvider) OnSwapRequested(task *Task, groupID string, localIDs []string) error {
	if p.onSwapRequested != nil {
		return p.onSwapRequested(task, groupID, localIDs)
	}
	return nil
}

func (p *unitAwareStubProvider) OnTaskCompleted(task *Task) error {
	if p.onTaskCompleted != nil {
		return p.onTaskCompleted(task)
	}
	return nil
}

type terminalCleanupStubProvider struct {
	unitAwareStubProvider
	cleanupDone bool
}

func (p *terminalCleanupStubProvider) TerminalCleanupDone(_ *Task, _ string) bool {
	return p.cleanupDone
}

type spyFinalizer struct {
	onMarkFailed func()
}

func (f *spyFinalizer) MarkDistributedTaskFinalized(_ context.Context, _, _ string, _ uint64) error {
	return nil
}

func (f *spyFinalizer) MarkDistributedTaskFailed(_ context.Context, _, _ string, _ uint64, _ string) error {
	if f.onMarkFailed != nil {
		f.onMarkFailed()
	}
	return nil
}

// Compile-time interface checks.
var (
	_ UnitAwareProvider       = (*unitAwareStubProvider)(nil)
	_ UnitAwareProvider       = (*terminalCleanupStubProvider)(nil)
	_ TerminalCleanupProvider = (*terminalCleanupStubProvider)(nil)
	_ ForceTerminator         = (*fakeForceTerminator)(nil)
	_ TaskFinalizer           = (*spyFinalizer)(nil)
)
