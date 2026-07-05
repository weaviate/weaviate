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
	"fmt"
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// retryFlipProvider models the reindex OnTaskCompleted schema flip with
// configurable failure behavior so the scheduler's #297 retry/fail policy can
// be driven deterministically:
//
//   - failFirstNSwaps: fail this many SWAPPING-path calls with a transient
//     error, then succeed (models a flip that recovers after a RAFT blip).
//   - permanent: every SWAPPING-path call returns an [ErrTaskCompletionPermanent]
//     error (models the target property being deleted mid-flight).
//   - alwaysFail: every SWAPPING-path call returns a transient error (models a
//     flip that never recovers — exercises the bounded-retry exhaustion path).
//
// Terminal-status invocations (the FAILED-status re-fire the scheduler makes
// for cleanup) return nil and do not touch schemaFlipped, mirroring the real
// provider's best-effort terminal cleanup.
type retryFlipProvider struct {
	*testTaskProvider

	failFirstNSwaps int
	permanent       bool
	alwaysFail      bool

	mu               sync.Mutex
	swappingAttempts int
	callsByStatus    map[TaskStatus]int
	schemaFlipped    bool
}

func newRetryFlipProvider(t *testing.T) *retryFlipProvider {
	return &retryFlipProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		callsByStatus:    map[TaskStatus]int{},
	}
}

func (p *retryFlipProvider) OnGroupCompleted(_ *Task, _ string, _ []string) error { return nil }
func (p *retryFlipProvider) OnSwapRequested(_ *Task, _ string, _ []string) error  { return nil }

func (p *retryFlipProvider) OnTaskCompleted(task *Task) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callsByStatus[task.Status]++
	if task.Status != TaskStatusSwapping {
		// Terminal-status cleanup re-fire: best-effort, never reopens the task.
		return nil
	}
	p.swappingAttempts++
	switch {
	case p.permanent:
		return fmt.Errorf("simulated permanent flip failure: %w", ErrTaskCompletionPermanent)
	case p.alwaysFail:
		return fmt.Errorf("simulated persistent transient flip failure")
	case p.swappingAttempts <= p.failFirstNSwaps:
		return fmt.Errorf("simulated transient flip failure (attempt %d)", p.swappingAttempts)
	default:
		p.schemaFlipped = true
		return nil
	}
}

func (p *retryFlipProvider) snapshot() (swappingAttempts int, flipped bool, byStatus map[TaskStatus]int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	byStatus = make(map[TaskStatus]int, len(p.callsByStatus))
	for k, v := range p.callsByStatus {
		byStatus[k] = v
	}
	return p.swappingAttempts, p.schemaFlipped, byStatus
}

// startRetryScenario builds a single-node harness, drives one non-barrier task
// to SWAPPING, and starts the scheduler. Callers then advance the clock to
// drive ticks (one OnTaskCompleted attempt per tick) and inspect the outcome.
func startRetryScenario(t *testing.T, prov *retryFlipProvider) (*testHarness, string) {
	t.Helper()

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{h.tasksNamespace: prov}
	h.provider = prov.testTaskProvider
	h = h.init(t)
	// init() only knows the two concrete provider types; register the base
	// explicitly so drain/leaktest see this provider's run goroutines.
	h.testProviders = append(h.testProviders, prov.testTaskProvider)

	const taskID = "retry-flip-task"

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-1"},
	}), 1))

	// Unit completion jumps STARTED → SWAPPING (non-barrier task).
	completeUnit(t, h, h.tasksNamespace, taskID, 1, h.localNodeID, "u-1")

	pre := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, pre, 1)
	require.Equal(t, TaskStatusSwapping, pre[0].Status,
		"precondition: task must be SWAPPING before the scheduler ticks")

	h.startScheduler(t)
	return h, taskID
}

func statusOf(t *testing.T, h *testHarness) TaskStatus {
	t.Helper()
	tasks := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, tasks, 1)
	return tasks[0].Status
}

// TestOnTaskCompleted_TransientFlipFailure_RetriesThenFinishes covers the
// transient path (#297): the flip fails once, the scheduler withholds FINISHED
// and re-fires on the next tick, the retry succeeds, and only then does the
// task reach FINISHED with the schema flipped.
func TestOnTaskCompleted_TransientFlipFailure_RetriesThenFinishes(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.failFirstNSwaps = 1 // fail once, then succeed
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	// Tick 1: OnTaskCompleted fails transiently → finalize withheld, task
	// must stay SWAPPING (NOT FINISHED).
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusSwapping, statusOf(t, h),
		"transient flip failure must withhold FINISHED and leave the task SWAPPING")

	// Tick 2: retry succeeds → task finalizes to FINISHED.
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFinished, statusOf(t, h),
		"a successful retry must let the task reach FINISHED")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, 2, attempts, "flip must be attempted exactly twice (fail, then succeed)")
	require.True(t, flipped, "schema must be flipped once the task is FINISHED")

	// Idempotency: extra ticks must not re-fire on the now-FINISHED task.
	h.advanceClock(h.schedulerTickInterval)
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, 2, attemptsAfter, "no further flip attempts after FINISHED")
}

// TestOnTaskCompleted_PermanentFlipFailure_FailsImmediately covers the
// permanent path (#297): a deterministically-unrecoverable flip failure (the
// target property was deleted) transitions the task straight to FAILED without
// burning the retry budget, and never reaches FINISHED.
func TestOnTaskCompleted_PermanentFlipFailure_FailsImmediately(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.permanent = true
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	// One tick: the permanent error fails the task immediately.
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h),
		"a permanent flip failure must transition the task to FAILED, not FINISHED")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, 1, attempts,
		"permanent failure must NOT retry: exactly one SWAPPING-path attempt")
	require.False(t, flipped, "schema must never be flipped on the permanent-failure path")

	// Extra ticks: task stays FAILED and the flip is never re-attempted at
	// SWAPPING (the FAILED-status re-fire is cleanup only).
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h))
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, 1, attemptsAfter, "no SWAPPING-path flip attempts after FAILED")
}

// TestOnTaskCompleted_TransientFlipFailure_ExhaustsRetriesThenFails covers the
// bounded-retry exhaustion path (#297): a transient-looking flip that never
// recovers is retried up to maxCompletedCallbackAttempts and then FAILED — it
// must NOT loop SWAPPING forever.
func TestOnTaskCompleted_TransientFlipFailure_ExhaustsRetriesThenFails(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.alwaysFail = true
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	// Drive one tick per attempt up to (and one past) the bound. The task
	// stays SWAPPING while the budget lasts, then flips to FAILED.
	for i := 1; i < maxCompletedCallbackAttempts; i++ {
		h.advanceClock(h.schedulerTickInterval)
		require.Equal(t, TaskStatusSwapping, statusOf(t, h),
			"task must stay SWAPPING while the retry budget is not yet exhausted (after %d attempts)", i)
	}
	// The maxCompletedCallbackAttempts-th failing attempt exhausts the budget.
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h),
		"task must FAIL once the retry budget is exhausted, never loop forever")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, maxCompletedCallbackAttempts, attempts,
		"SWAPPING-path flip must be attempted exactly maxCompletedCallbackAttempts times")
	require.False(t, flipped)

	// Cap check: many extra ticks must not grow the SWAPPING attempt count —
	// the FAILED task is terminal and the flip is not re-attempted.
	for i := 0; i < 5; i++ {
		h.advanceClock(h.schedulerTickInterval)
	}
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, maxCompletedCallbackAttempts, attemptsAfter,
		"no additional SWAPPING-path flip attempts after the task is FAILED (no infinite loop)")
	require.Equal(t, TaskStatusFailed, statusOf(t, h))
}
