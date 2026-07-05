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

// retryFlipProvider models the reindex OnTaskCompleted schema flip for the
// #297 retry/fail policy: failFirstNSwaps recovers after N failures,
// permanent is unrecoverable, alwaysFail exhausts the retry budget.
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

// startRetryScenario drives one task to SWAPPING and starts the scheduler;
// callers advance the clock to drive ticks.
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

// TestOnTaskCompleted_TransientFlipFailure_RetriesThenFinishes: a transient
// flip failure retries before FINISHED (weaviate/0-weaviate-issues#297).
func TestOnTaskCompleted_TransientFlipFailure_RetriesThenFinishes(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.failFirstNSwaps = 1 // fail once, then succeed
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusSwapping, statusOf(t, h),
		"transient flip failure must withhold FINISHED and leave the task SWAPPING")

	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFinished, statusOf(t, h),
		"a successful retry must let the task reach FINISHED")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, 2, attempts, "flip must be attempted exactly twice (fail, then succeed)")
	require.True(t, flipped, "schema must be flipped once the task is FINISHED")

	h.advanceClock(h.schedulerTickInterval)
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, 2, attemptsAfter, "no further flip attempts after FINISHED")
}

// TestOnTaskCompleted_PermanentFlipFailure_FailsImmediately: a permanent
// flip failure fails the task immediately, no retry (weaviate/0-weaviate-issues#297).
func TestOnTaskCompleted_PermanentFlipFailure_FailsImmediately(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.permanent = true
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h),
		"a permanent flip failure must transition the task to FAILED, not FINISHED")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, 1, attempts,
		"permanent failure must NOT retry: exactly one SWAPPING-path attempt")
	require.False(t, flipped, "schema must never be flipped on the permanent-failure path")

	// FAILED-status re-fire is cleanup only; no further SWAPPING attempts.
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h))
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, 1, attemptsAfter, "no SWAPPING-path flip attempts after FAILED")
}

// TestOnTaskCompleted_TransientFlipFailure_ExhaustsRetriesThenFails: retries
// exhaust at maxCompletedCallbackAttempts, then FAILS instead of looping
// forever (weaviate/0-weaviate-issues#297).
func TestOnTaskCompleted_TransientFlipFailure_ExhaustsRetriesThenFails(t *testing.T) {
	defer leaktest.Check(t)()

	prov := newRetryFlipProvider(t)
	prov.alwaysFail = true
	h, _ := startRetryScenario(t, prov)
	defer h.Close()

	// Ticks up to the bound; the task must stay SWAPPING until it's exhausted.
	for i := 1; i < maxCompletedCallbackAttempts; i++ {
		h.advanceClock(h.schedulerTickInterval)
		require.Equal(t, TaskStatusSwapping, statusOf(t, h),
			"task must stay SWAPPING while the retry budget is not yet exhausted (after %d attempts)", i)
	}
	h.advanceClock(h.schedulerTickInterval)
	require.Equal(t, TaskStatusFailed, statusOf(t, h),
		"task must FAIL once the retry budget is exhausted, never loop forever")

	attempts, flipped, _ := prov.snapshot()
	require.Equal(t, maxCompletedCallbackAttempts, attempts,
		"SWAPPING-path flip must be attempted exactly maxCompletedCallbackAttempts times")
	require.False(t, flipped)

	// Cap check: extra ticks must not grow the attempt count once FAILED.
	for i := 0; i < 5; i++ {
		h.advanceClock(h.schedulerTickInterval)
	}
	attemptsAfter, _, _ := prov.snapshot()
	require.Equal(t, maxCompletedCallbackAttempts, attemptsAfter,
		"no additional SWAPPING-path flip attempts after the task is FAILED (no infinite loop)")
	require.Equal(t, TaskStatusFailed, statusOf(t, h))
}
