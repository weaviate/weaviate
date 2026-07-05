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
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// flipFailingProvider is a unit-aware provider that models the reindex
// provider's cluster-wide schema flip (adapters/repos/db/reindex_provider.go:
// OnTaskCompleted -> flipSemanticMigrationSchema).
//
// The real OnTaskCompleted has a VOID signature and swallows a failed flip
// (reindex_provider.go:1606-1611): it logs the error and returns, so the
// scheduler has no way to observe that the schema was NOT flipped. This fake
// reproduces exactly that seam: when flipShouldFail is set it records the
// attempt, leaves schemaFlipped=false, and returns normally. The void return
// means the scheduler cannot tell a successful flip from a failed one.
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

// OnTaskCompleted models the cluster-wide schema flip. On the failure path it
// CANNOT signal the failure to the scheduler because the interface method is
// void, which is the defect #297 describes.
func (p *flipFailingProvider) OnTaskCompleted(_ *Task) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completedCalls++
	if p.flipShouldFail {
		// Mirrors reindex_provider.go:1606-1611: flipSemanticMigrationSchema
		// returns an error, it is logged and swallowed, and the schema stays
		// pre-migration. The void return leaves the scheduler none the wiser.
		return
	}
	p.schemaFlipped = true
}

func (p *flipFailingProvider) snapshot() (calls int, flipped bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.completedCalls, p.schemaFlipped
}

// runFlipScenario builds a single-node harness with prov, drives one
// non-barrier task through STARTED -> SWAPPING (via unit completion), then
// lets the scheduler tick: Phase 2 fires OnTaskCompleted (the flip), and
// runFinalizePhase issues SWAPPING -> FINISHED. Returns the task's final
// status plus the provider's observed flip result.
func runFlipScenario(t *testing.T, prov *flipFailingProvider) (TaskStatus, int, bool) {
	t.Helper()

	h := newTestHarness(t)
	h.registeredProviders = map[string]Provider{h.tasksNamespace: prov}
	h.provider = prov.testTaskProvider
	h = h.init(t)
	// init() rebuilds testProviders from the two known concrete provider
	// types; flipFailingProvider isn't one of them, so register its base
	// provider explicitly for drain/leaktest.
	h.testProviders = append(h.testProviders, prov.testTaskProvider)

	const taskID = "flip-task"

	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             h.tasksNamespace,
		Id:                    taskID,
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"u-1"},
		// NeedsPreparationBarrier omitted (false): on unit completion the task
		// jumps STARTED -> SWAPPING (manager.go:438-444), the state where the
		// scheduler fires OnTaskCompleted then finalizes.
	}), 1))

	// Drive the unit to completion -> task becomes SWAPPING.
	completeUnit(t, h, h.tasksNamespace, taskID, 1, h.localNodeID, "u-1")

	pre := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, pre, 1)
	require.Equal(t, TaskStatusSwapping, pre[0].Status,
		"precondition: task must be SWAPPING before the scheduler ticks")

	h.startScheduler(t)
	defer h.Close()

	// One tick suffices (Phase 2 fires OnTaskCompleted, then runFinalizePhase
	// commits SWAPPING -> FINISHED). Advance a few extra ticks to give any
	// hypothetical retry path a chance and to confirm exactly-once firing.
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)
	h.advanceClock(h.schedulerTickInterval)

	final := h.listManagerTasks(t)[h.tasksNamespace]
	require.Len(t, final, 1)
	calls, flipped := prov.snapshot()
	return final[0].Status, calls, flipped
}

// TestOnTaskCompletedFlipFailure_TaskStillReachesFinished pins issue #297:
// a schema-flip failure inside the void OnTaskCompleted callback is invisible
// to the DTM scheduler, so the task still finalizes to FINISHED while the
// schema stays un-flipped, violating the TaskStatusFinished contract
// (types.go:376-379: "the task succeeded on every node AND every per-node
// post-completion callback has run") and the scheduler comment at
// scheduler.go:508-512 ("the FINISHED transition is committed ... only AFTER
// OnTaskCompleted returns successfully").
//
// The finalize-rollback retry (runFinalizePhase, scheduler.go:1147-1165) does
// NOT cover this: it clears completedCallbackFired only when
// MarkDistributedTaskFinalized itself returns an error, a DIFFERENT failure
// than the flip failing inside OnTaskCompleted. Because OnTaskCompleted is
// void, a failed flip never clears completedCallbackFired, so runFinalizePhase
// still sees the fired mark and commits FINISHED.
func TestOnTaskCompletedFlipFailure_TaskStillReachesFinished(t *testing.T) {
	// Positive control: the flip SUCCEEDS. Proves the harness drives a task
	// SWAPPING -> FINISHED and that FINISHED lines up with schemaFlipped=true.
	// This rules out a trivially-red pin (broken harness that never finalizes).
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

	// Bug case: the flip FAILS inside OnTaskCompleted. The desired (post-fix)
	// behaviour is that the scheduler withholds finalize, so the task does NOT
	// reach FINISHED while the schema is un-flipped. On current code it DOES,
	// which makes this assertion RED, proving #297 is live.
	t.Run("flip_failure_must_block_finished", func(t *testing.T) {
		defer leaktest.Check(t)()
		prov := newFlipFailingProvider(t, true)
		status, calls, flipped := runFlipScenario(t, prov)
		require.GreaterOrEqual(t, calls, 1,
			"OnTaskCompleted must have been attempted")
		require.False(t, flipped,
			"sanity: the injected failure means the schema was never flipped")

		// #297 load-bearing assertion. FINISHED must mean "every
		// post-completion callback ran successfully" (types.go:376-379). A
		// task that reaches FINISHED with an un-flipped schema is exactly the
		// status/schema divergence #297 describes.
		require.NotEqual(t, TaskStatusFinished, status,
			"#297: task reached FINISHED while the schema flip failed inside "+
				"OnTaskCompleted (schemaFlipped=%v, OnTaskCompleted calls=%d) "+
				"- silent status/schema divergence", flipped, calls)
	})
}
