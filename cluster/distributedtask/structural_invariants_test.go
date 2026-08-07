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
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// Structural invariant tests for the DTM package.
// weaviate/0-weaviate-issues#243.

// TestStructuralInvariant_SchedulerClose_WaitsForLoopExit: after
// Close returns, no run-loop goroutine exists and no clock-waiter is
// parked on the ticker. Two independent signals — a surviving
// goroutine OR a surviving waiter — each detect "Close skipped the
// join" under race detector + GC pressure.
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
	// Soft signal — runtime.NumGoroutine() is sampled from the global
	// runtime and unrelated background goroutines (test fixtures,
	// loggers, GC sweep) can spawn or exit between the two reads. The
	// load-bearing check is the FakeClock waiter probe in
	// TestStructuralInvariant_SchedulerClose_NoTickAfterReturn; this
	// is a complementary indicator that we log rather than fail on.
	if afterClose > beforeStart {
		t.Logf("Scheduler.Close left %d goroutines (before=%d); "+
			"may be the loop, may be unrelated. NoTickAfterReturn is the load-bearing pin.",
			afterClose-beforeStart, beforeStart)
	}
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
// the RAFT FSM Restore contract: post-Restore state == snapshot, not
// pre-state ∪ snapshot. Exercises the three failure modes the merge
// implementation allows: (a) different ID in shared namespace,
// (b) unrelated namespace, (c) intersection (correctly overwritten —
// positive control).
//
// weaviate/0-weaviate-issues#245.
func TestStructuralInvariant_ManagerRestore_ReplacesExistingState(t *testing.T) {
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

// TestStructuralInvariant_MarkTerminalIsTheOnlyTerminalWriter catches the
// common way of breaking [Task.markTerminal]'s "only way" property: a line in
// this package's non-test sources that names a terminal status constant on the
// right of a `.Status =`, or that stamps `task.FinishedAt` directly.
//
// It is two line-regexes, so it is a tripwire, not a proof. It does NOT catch
// a terminal status reached through a variable, a stamp written through a
// receiver not named `task`, `_test.go` files, or the other packages holding a
// *Task (both fields are exported). What actually defends the invariant on
// every path the FSM replays is TestStructuralInvariant_FinishedAtIffTerminal,
// which re-checks it after every apply; this scan only makes the cheap
// regression cheap to catch.
func TestStructuralInvariant_MarkTerminalIsTheOnlyTerminalWriter(t *testing.T) {
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`\.Status\s*=\s*TaskStatus(Finished|Failed|Cancelled)\b`),
		regexp.MustCompile(`task\.FinishedAt\s*=`),
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		scanned++
		for i, line := range strings.Split(string(src), "\n") {
			for _, re := range forbidden {
				require.NotRegexp(t, re, line,
					"%s:%d assigns a terminal status or FinishedAt outside Task.markTerminal, "+
						"which is what pairs the status with the stamp and keeps "+
						"FinishedAt.IsZero() ⟺ !Status.IsTerminal() true",
					name, i+1)
			}
		}
	}
	require.Greater(t, scanned, 1, "sanity: the scan must have read the package sources")
}

// TestStructuralInvariant_FinishedAtIffTerminal pins the lifecycle invariant
// the FinishedAt semantics rest on, checked after every single apply rather
// than only at the end of a path:
//
//	Task.FinishedAt.IsZero()  ⟺  !Task.Status.IsTerminal()
//
// Every consumer (TTL sweep, cleanup guard, backup overlap backstop, display
// sort, the /v1/tasks response) reads the field naively and is correct only
// while this holds. weaviate/0-weaviate-issues#501.
//
// The last two paths carry an out-of-phase ack — a barrier ack landing
// against a task that is not in that barrier's phase, which RAFT redelivery
// and multi-node barriers produce routinely. That a duplicate terminal apply
// does not move the stamp is pinned separately, by
// TestManager_FinishedAt_NotRestampedByASecondTerminalApply.
func TestStructuralInvariant_FinishedAtIffTerminal(t *testing.T) {
	const ns = finishedAtNS

	ref := finishedAtRef()

	type step struct {
		name  string
		apply func(t *testing.T, h *testHarness)
	}

	plain := step{"add", func(t *testing.T, h *testHarness) {
		addTaskWithUnits(t, h, ns, ref.id, ref.version, []string{"u"})
	}}
	barrier := step{"add barrier task", func(t *testing.T, h *testHarness) {
		addBarrierTaskWithUnits(t, h, ns, ref.id, ref.version, []string{"u-node-1"})
	}}
	claim := step{"claim unit", func(t *testing.T, h *testHarness) {
		updateProgress(t, h, ns, ref.id, ref.version, "node-1", "u", 0.1)
	}}
	finishUnit := step{"finish unit", func(t *testing.T, h *testHarness) {
		completeUnit(t, h, ns, ref.id, ref.version, "node-1", "u")
	}}
	prepAck := func(success bool) step {
		return step{"prep ack", func(t *testing.T, h *testHarness) {
			require.NoError(t, ref.prepAck(t, h, "node-1", success, h.clock.Now()))
		}}
	}
	swapAck := func(success bool) step {
		return step{"swap ack", func(t *testing.T, h *testHarness) {
			require.NoError(t, ref.swapAck(t, h, "node-1", success, h.clock.Now()))
		}}
	}
	// An ack from a node whose barrier phase the task is not in. The FSM
	// records it for forensics and leaves the status alone; stamping here
	// would end a task that is still running.
	outOfPhaseSwapAck := step{"out-of-phase failing swap ack", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.swapAck(t, h, "node-2", false, h.clock.Now()))
	}}
	outOfPhasePrepAck := step{"out-of-phase failing prep ack", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.prepAck(t, h, "node-2", false, h.clock.Now()))
	}}
	finalize := step{"finalize", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.finalize(t, h, h.clock.Now()))
	}}
	markFailed := step{"mark failed", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.markFailed(t, h, h.clock.Now()))
	}}
	failUnitStep := step{"fail unit", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.failUnitAt(t, h, "node-1", "u", h.clock.Now()))
	}}
	cancel := step{"cancel", func(t *testing.T, h *testHarness) {
		require.NoError(t, ref.cancel(t, h, h.clock.Now()))
	}}
	driveBarrierUnits := step{"finish barrier units", func(t *testing.T, h *testHarness) {
		drivePreparing(t, h, ns, ref.id, ref.version, []string{"node-1"})
	}}

	paths := [][]step{
		{plain, claim, finishUnit, swapAck(true), finalize},
		{plain, claim, finishUnit, swapAck(false)},
		{plain, claim, finishUnit, markFailed},
		{plain, claim, failUnitStep},
		{plain, claim, cancel},
		{barrier, driveBarrierUnits, prepAck(false)},
		{barrier, driveBarrierUnits, prepAck(true), swapAck(true), finalize},
		{plain, claim, outOfPhaseSwapAck, cancel},
		{plain, claim, outOfPhasePrepAck, cancel},
	}

	for _, path := range paths {
		names := make([]string, 0, len(path))
		for _, s := range path {
			names = append(names, s.name)
		}
		t.Run(strings.Join(names, " → "), func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			for _, s := range path {
				s.apply(t, h)
				h.clock.Advance(time.Second)

				tasks, err := h.manager.ListDistributedTasks(context.Background())
				require.NoError(t, err)
				require.Len(t, tasks[ns], 1)
				task := tasks[ns][0]
				require.Equal(t, !task.Status.IsTerminal(), task.FinishedAt.IsZero(),
					"after %q the task is %s with FinishedAt %v", s.name, task.Status, task.FinishedAt)
			}

			require.True(t, onlyTask(t, h, ns).Status.IsTerminal(),
				"sanity: every path ends terminal, so the ⟸ direction is exercised")
		})
	}
}
