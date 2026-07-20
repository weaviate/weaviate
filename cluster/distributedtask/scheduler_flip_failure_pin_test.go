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
	"errors"
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// flipFailingProvider models the reindex provider's schema-flip cutover
// (OnTaskCompleted -> flipSemanticMigrationSchema); flipShouldFail simulates
// a transient flip failure (weaviate/0-weaviate-issues#297).
type flipFailingProvider struct {
	*testTaskProvider

	flipShouldFail bool

	mu             sync.Mutex
	completedCalls int
	schemaFlipped  bool
}

func newFlipFailingProvider(t *testing.T, flipShouldFail bool) *flipFailingProvider {
	return &flipFailingProvider{
		testTaskProvider: newTestTaskProvider(t, nil),
		flipShouldFail:   flipShouldFail,
	}
}

func (p *flipFailingProvider) OnGroupCompleted(_ *Task, _ string, _ []string) error {
	return nil
}

func (p *flipFailingProvider) OnSwapRequested(_ *Task, _ string, _ []string) error {
	return nil
}

// OnTaskCompleted simulates the schema flip failing transiently; the
// scheduler must withhold FINISHED and retry (weaviate/0-weaviate-issues#297).
func (p *flipFailingProvider) OnTaskCompleted(task *Task) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completedCalls++
	if p.flipShouldFail && task.Status == TaskStatusSwapping {
		return errors.New("simulated transient schema-flip failure")
	}
	p.schemaFlipped = true
	return nil
}

func (p *flipFailingProvider) snapshot() (calls int, flipped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completedCalls, p.schemaFlipped
}

// submitTaskToSwapping wires prov into a fresh harness, adds one
// non-barrier task, and drives it to SWAPPING (where OnTaskCompleted
// fires) before starting the scheduler. base is prov's embedded
// testTaskProvider, registered so drain/leaktest see its goroutines.
// Caller owns h.Close().
func submitTaskToSwapping(t *testing.T, prov Provider, base *testTaskProvider, taskID string) *testHarness {
	t.Helper()

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{h.tasksNamespace: prov}
	h.provider = base
	h = h.init(t)
	h.testProviders = append(h.testProviders, base)

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-1"},
		// Non-barrier: unit completion jumps STARTED -> SWAPPING directly.
	}), 1))

	completeUnit(t, h, h.tasksNamespace, taskID, 1, h.localNodeID, "u-1")

	pre := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, pre, 1)
	require.Equal(t, TaskStatusSwapping, pre[0].Status,
		"precondition: task must be SWAPPING before the scheduler ticks")

	h.startScheduler(t)
	return h
}

// runFlipScenario drives one task STARTED -> SWAPPING -> (attempted)
// FINISHED and returns the final status plus the observed flip result.
func runFlipScenario(t *testing.T, prov *flipFailingProvider) (TaskStatus, int, bool) {
	t.Helper()
	h := submitTaskToSwapping(t, prov, prov.testTaskProvider, "flip-task")
	defer h.Close()

	// Extra ticks give a hypothetical retry a chance; confirms exactly-once firing.
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)

	final := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, final, 1)
	calls, flipped := prov.snapshot()
	return final[0].Status, calls, flipped
}

// TestOnTaskCompletedFlipFailure_TaskDoesNotReachFinished: a schema-flip
// failure in OnTaskCompleted must not let the task reach FINISHED
// (weaviate/0-weaviate-issues#297).
func TestOnTaskCompletedFlipFailure_TaskDoesNotReachFinished(t *testing.T) {
	// Positive control: proves the harness reaches FINISHED normally, ruling
	// out a trivially-red pin below.
	t.Run("positive_control_flip_succeeds", func(t *testing.T) {
		defer leaktest.Check(t)()
		prov := newFlipFailingProvider(t, false)
		status, calls, flipped := runFlipScenario(t, prov)
		require.GreaterOrEqual(t, calls, 1, "OnTaskCompleted must fire")
		require.Equal(t, TaskStatusFinished, status,
			"successful flip: task must reach FINISHED")
		require.True(t, flipped,
			"successful flip: schema must be flipped once FINISHED")
	})

	// Bug case: the scheduler must withhold FINISHED while the flip keeps
	// failing (weaviate/0-weaviate-issues#297).
	t.Run("flip_failure_must_block_finished", func(t *testing.T) {
		defer leaktest.Check(t)()
		prov := newFlipFailingProvider(t, true)
		status, calls, flipped := runFlipScenario(t, prov)
		require.GreaterOrEqual(t, calls, 1,
			"OnTaskCompleted must have been attempted")
		require.False(t, flipped,
			"sanity: the injected failure means the schema was never flipped")

		// #297: FINISHED must mean the schema flip succeeded too.
		require.NotEqual(t, TaskStatusFinished, status,
			"#297: task reached FINISHED while the schema flip failed inside "+
				"OnTaskCompleted (schemaFlipped=%v, OnTaskCompleted calls=%d) "+
				"- silent status/schema divergence", flipped, calls)
	})
}
