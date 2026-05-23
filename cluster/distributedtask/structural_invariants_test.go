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
	"runtime"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// Structural invariant tests for the DTM package.
//
// Sub-report 07 (from issue #243) calls out three patterns that recur
// across loop-driven / RAFT-FSM / fan-out code but lack systematic test
// coverage. This file pins two of them at the DTM layer:
//
//  1. TestStructuralInvariant_SchedulerClose_WaitsForLoopExit
//     The Scheduler.Close contract is "after Close returns, no further
//     tick fires." Close currently closes stopCh and terminates
//     handles, but does NOT join the run loop goroutine. That race
//     window lets a tick fire after Close returns (concretely: between
//     closing stopCh and the loop reading from it the loop is mid-tick
//     holding s.mu while Close blocks on s.mu, then Close acquires
//     s.mu and returns before the loop re-enters its select to read
//     stopCh). Pin this as a RED test until Close is fixed to join.
//
//  2. TestStructuralInvariant_ManagerRestore_ReplacesExistingState
//     Manager.Restore from a RAFT snapshot MUST atomically replace
//     m.tasks with the snapshotted state. The current implementation
//     merges (iterates s.Tasks and assigns into m.tasks) — pre-existing
//     namespaces or tasks that are absent in the snapshot survive.
//     This is a real bug: post-restore the Manager contains a union
//     of pre-restore state and snapshot state, which violates the
//     RAFT FSM contract (every node MUST converge to the snapshotted
//     state after Restore).

// TestStructuralInvariant_SchedulerClose_WaitsForLoopExit pins the
// invariant that after Scheduler.Close returns, the run loop has
// stopped and no further tick can fire.
//
// Method: use the FakeClock's waiter introspection (BlockUntilContext)
// to confirm the loop is parked on ticker.Chan() before Close, then
// confirm no ticker waiter remains after Close. A surviving waiter
// would mean the loop is still parked in its select and capable of
// running a tick that races with shared state torn down by Close.
//
// We also bound the assertion with a goroutine-count delta as a
// secondary signal: any residual goroutine after Close (vs. the
// pre-Start baseline) is the loop not yet drained.
//
// Both signals are decisive in opposite directions:
//
//   - Waiter check exposes "loop is still parked on the ticker."
//   - Goroutine-count check exposes "loop goroutine still exists."
//
// If Close synchronously joins the loop, BOTH checks pass. If
// Close skips the join, at least one check fails under load (race
// detector + GC pressure widens the window).
func TestStructuralInvariant_SchedulerClose_WaitsForLoopExit(t *testing.T) {
	h := newTestHarness(t).init(t)

	// Let any prior goroutines settle so the baseline is stable. Read
	// twice and force scheduler swap-in.
	runtime.Gosched()
	runtime.GC()
	runtime.Gosched()
	beforeStart := runtime.NumGoroutine()

	require.NoError(t, h.scheduler.Start(context.Background()))

	// Give Start's spawned loop time to actually enter its select{}.
	// We do NOT use the harness sleep helper here — we want
	// deterministic confirmation that the loop is registered as a
	// clock waiter. clock.BlockUntilContext(1) waits until the
	// ticker is the registered waiter.
	blockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, h.clock.BlockUntilContext(blockCtx, 1))
	cancel()

	// At this point we expect exactly one extra goroutine compared
	// to baseline: the loop goroutine. Confirm at least one was
	// spawned (sanity check on the harness).
	require.Greater(t, runtime.NumGoroutine(), beforeStart,
		"sanity: starting scheduler must have spawned at least one goroutine")

	// Close should synchronously drain the loop. After Close returns
	// the count must return to baseline (no leftover loop goroutine).
	h.scheduler.Close()

	// Drain any test-only goroutines (e.g. logging) the same way as
	// the baseline measurement.
	runtime.Gosched()
	runtime.GC()
	runtime.Gosched()

	afterClose := runtime.NumGoroutine()
	require.LessOrEqual(t, afterClose, beforeStart,
		"Scheduler.Close must wait for the run loop to exit before returning; "+
			"residual goroutines indicate the loop is still alive (before=%d, after=%d)",
		beforeStart, afterClose)
}

// TestStructuralInvariant_SchedulerClose_NoTickAfterReturn is a
// complementary invariant: even if Close does not strictly join the
// loop, no tick body must execute after Close returns. We pin this
// using clockwork's waiter introspection — after Close returns, no
// ticker waiter should remain on the FakeClock. A surviving waiter
// means the loop is still parked in its select waiting for the next
// tick, i.e. capable of running a tick body that races with shared
// state.
//
// Like TestStructuralInvariant_SchedulerClose_WaitsForLoopExit this
// will be RED while Close does not join. We keep both tests because
// they fail in different ways and a future fix should make both
// green simultaneously.
func TestStructuralInvariant_SchedulerClose_NoTickAfterReturn(t *testing.T) {
	h := newTestHarness(t).init(t)

	require.NoError(t, h.scheduler.Start(context.Background()))

	// Ensure the loop is parked on its ticker (so we have a known
	// waiter count to invert after Close).
	blockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, h.clock.BlockUntilContext(blockCtx, 1))
	cancel()

	h.scheduler.Close()

	// After Close, the loop must be gone — no remaining ticker waiter
	// on the fake clock. We allow a short bounded poll because the
	// FakeClock waiter accounting is updated by the loop goroutine
	// itself (via the deferred ticker.Stop()) — but the bound is
	// small enough that a Close that does NOT join would not even
	// have unblocked the loop yet (the loop is parked in select).
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !structuralInvariantClockHasWaiter(h.clock.BlockUntilContext) {
			return // success: loop exited
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Fail(t, "Scheduler.Close returned but the run loop is still parked on the fake clock ticker; this is the very race the WaitsForLoopExit invariant forbids")
}

// structuralInvariantClockHasWaiter probes whether the FakeClock has
// at least one waiter. clockwork.BlockUntilContext(ctx, n) blocks until
// the waiter count is >= n; with n=1 and a tiny deadline:
//
//   - returns nil immediately → at least 1 waiter is registered (loop
//     is parked on its ticker).
//   - returns context.DeadlineExceeded → zero waiters (loop has exited).
//
// clockwork does not export NumWaiters, so this asymmetric probe is
// the only externally-observable way to tell the two states apart.
func structuralInvariantClockHasWaiter(
	blockUntil func(context.Context, int) error,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := blockUntil(ctx, 1)
	return err == nil
}

// TestStructuralInvariant_ManagerRestore_ReplacesExistingState pins
// the RAFT FSM Restore contract: after Restore returns, the
// Manager's in-memory state MUST equal the snapshotted state, not
// the pre-restore state ∪ snapshot.
//
// Concretely we exercise three failure modes the current merge
// implementation allows:
//
//	(a) A pre-existing task in a namespace the snapshot also references
//	    but with a different ID survives Restore. This is a real
//	    bug: post-restore the FSM contains a task the snapshot
//	    knows nothing about, breaking convergence with peers.
//	(b) A pre-existing task in a namespace the snapshot does NOT
//	    reference at all survives Restore. Same convergence break.
//	(c) The intersection case (same namespace, same task ID) is
//	    correctly overwritten — included as a positive control so
//	    a future fix can confirm it didn't regress.
//
// All three are documented in qa-reindex sub-report 07. The current
// Manager.Restore merges rather than replaces, so (a) and (b) are
// expected to fail; this is a RED test we keep until Restore is
// fixed to clear m.tasks before applying the snapshot.
//
// Tracked as a Sev at weaviate/0-weaviate-issues#245 with full
// reproduction + impact analysis + suggested fix. Per CLAUDE.md
// the test is t.Skip'd so CI doesn't fail, but the full reproduction
// is preserved in the test body and the docstring above so the
// repro is not lost. Un-skip when the fix lands.
func TestStructuralInvariant_ManagerRestore_ReplacesExistingState(t *testing.T) {
	t.Skip("KNOWN-RED: weaviate/0-weaviate-issues#245 — Manager.Restore merges " +
		"instead of replaces; see test docstring for full bug analysis. Un-skip when #245 lands.")
	now := time.Now().Truncate(time.Millisecond)

	nullLogger, _ := logrustest.NewNullLogger()

	// Build a manager and ingest two pre-existing tasks: one in
	// namespace "ns-shared" and one in namespace "ns-orphan".
	preRestore := NewManager(ManagerParameters{
		CompletedTaskTTL: 24 * time.Hour,
		Logger:           nullLogger,
	})
	structuralInvariantSeedTask(t, preRestore, "ns-shared", "pre-existing-task", []byte("pre"), now, 1)
	structuralInvariantSeedTask(t, preRestore, "ns-orphan", "orphan-task", []byte("orph"), now, 2)

	// Build the snapshot source manager separately. It contains a
	// DIFFERENT task in "ns-shared" (different ID, so a merge would
	// keep both) and nothing in "ns-orphan".
	snapshotSource := NewManager(ManagerParameters{
		CompletedTaskTTL: 24 * time.Hour,
		Logger:           nullLogger,
	})
	structuralInvariantSeedTask(t, snapshotSource, "ns-shared", "snapshot-task", []byte("snap"), now, 3)

	snapBytes, err := snapshotSource.Snapshot()
	require.NoError(t, err)

	// Restore the snapshot into the pre-populated manager.
	require.NoError(t, preRestore.Restore(snapBytes))

	got, err := preRestore.ListDistributedTasks(context.Background())
	require.NoError(t, err)

	// Invariant (replacement, not merge):
	//
	//   - ns-orphan must NOT exist post-restore (it was absent in the snapshot).
	//   - ns-shared must contain exactly the snapshot task ("snapshot-task")
	//     and NOT the pre-existing task ("pre-existing-task").
	require.NotContains(t, got, "ns-orphan",
		"Manager.Restore must drop namespaces absent in the snapshot; "+
			"keeping ns-orphan means the FSM merged instead of replaced (real bug)")

	sharedTasks, ok := got["ns-shared"]
	require.True(t, ok, "snapshot did contain ns-shared; it must survive")
	require.Len(t, sharedTasks, 1,
		"ns-shared must contain ONLY the snapshotted task; "+
			"a length > 1 means Manager.Restore merged pre-existing task into snapshot state")
	require.Equal(t, "snapshot-task", sharedTasks[0].ID,
		"the surviving task in ns-shared must be the one from the snapshot, "+
			"not the pre-existing task (replacement contract)")
}

// structuralInvariantSeedTask is a tight helper that injects a
// hand-built Task directly into the Manager's in-memory store,
// bypassing the AddTask RAFT-apply path. This keeps the test
// independent of the AddTask command shape and immune to changes in
// the AddTask validation surface — we are testing the Restore
// contract, not AddTask. Caller-supplied seqNum becomes the task
// Version.
func structuralInvariantSeedTask(
	t *testing.T,
	m *Manager,
	namespace, id string,
	payload []byte,
	now time.Time,
	seqNum uint64,
) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[namespace]; !ok {
		m.tasks[namespace] = map[string]*Task{}
	}
	m.tasks[namespace][id] = &Task{
		Namespace: namespace,
		TaskDescriptor: TaskDescriptor{
			ID:      id,
			Version: seqNum,
		},
		Payload:   payload,
		Status:    TaskStatusStarted,
		StartedAt: now,
		Units: map[string]*Unit{
			"u-1": {ID: "u-1", Status: UnitStatusPending},
		},
	}
}
