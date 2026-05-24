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

package reindex

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Structural invariant tests for the ReindexProvider's drain /
// cancel-and-wait surface.
//
// The DTM-layer pieces live in
// cluster/distributedtask/structural_invariants_test.go; this file pins
// the reindex-provider pieces (see weaviate/0-weaviate-issues#243 for
// the broader pyramid analysis):
//
//   TestStructuralInvariant_WaitForLocalTaskDrain_BlocksUntilHandleDone
//   TestStructuralInvariant_WaitForLocalTaskDrain_NoHandleReturnsNil
//   TestStructuralInvariant_WaitForLocalTaskDrain_RespectsContextCancel
//
// Together these pin: WaitForLocalTaskDrain must wait for the per-task
// goroutine's doneCh, must not block when no handle is present (the
// "already drained / never started" case), and must respect the
// caller's context (so a cancel→cleanup orchestrator can give up
// rather than wait forever on a runaway worker).

// structuralInvariantNewBareProvider returns a ReindexProvider with
// just the maps initialized, mimicking the literal-construction
// pattern used by reindex_conflict_test.go. The constructor
// (NewReindexProvider) needs a full *DB which we don't have in unit
// tests; the maps cover everything we exercise here
// (runningHandles + the WaitForLocalTaskDrain path).
func structuralInvariantNewBareProvider() *ReindexProvider {
	return &ReindexProvider{
		RunningHandles: make(map[distributedtask.TaskDescriptor]*reindexTaskHandle),
		Payloads:       make(map[distributedtask.TaskDescriptor]*ReindexTaskPayload),
		ReindexTasks:   make(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric),
		ActiveWorkers:  make(map[distributedtask.TaskDescriptor]map[string]bool),
	}
}

// structuralInvariantInjectHandle installs a hand-built reindexTaskHandle
// for the given descriptor without going through StartTask (which
// requires a fully wired DB / Index / schema manager). Returns the
// injected handle so the test can close its doneCh on demand.
func structuralInvariantInjectHandle(
	p *ReindexProvider,
	desc distributedtask.TaskDescriptor,
) *reindexTaskHandle {
	_, cancel := context.WithCancel(context.Background())
	handle := &reindexTaskHandle{
		Cancel: cancel,
		DoneCh: make(chan struct{}),
	}
	p.mu.Lock()
	p.RunningHandles[desc] = handle
	p.mu.Unlock()
	return handle
}

// TestStructuralInvariant_WaitForLocalTaskDrain_BlocksUntilHandleDone:
// the safety gate for cancel→drain→cleanup sidecar teardown must not
// return until the worker's doneCh closes. Otherwise
// CleanStalePartialReindexState races with the worker still writing
// to __reindex / __ingest buckets.
func TestStructuralInvariant_WaitForLocalTaskDrain_BlocksUntilHandleDone(t *testing.T) {
	p := structuralInvariantNewBareProvider()
	desc := distributedtask.TaskDescriptor{ID: "task-blocking", Version: 7}
	handle := structuralInvariantInjectHandle(p, desc)

	var (
		wg        sync.WaitGroup
		returnedC = make(chan error, 1)
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Use a generous context — we expect the drain to be
		// gated on doneCh, not the context. Five seconds is far
		// above the deterministic synchronization we use below.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		returnedC <- p.WaitForLocalTaskDrain(ctx, desc)
	}()

	// Phase 1 — drain must NOT return while doneCh is open. We
	// poll with a short bounded delay (50ms is well above the
	// scheduler quantum even on a loaded CI box).
	select {
	case err := <-returnedC:
		t.Fatalf("WaitForLocalTaskDrain returned before handle was done: err=%v", err)
	case <-time.After(50 * time.Millisecond):
		// expected: drain is still blocked
	}

	// Phase 2 — close doneCh (simulate the per-task goroutine
	// exiting via its defer). Drain must return promptly with
	// nil.
	close(handle.DoneCh)

	select {
	case err := <-returnedC:
		require.NoError(t, err,
			"WaitForLocalTaskDrain must return nil once the handle's doneCh is closed")
	case <-time.After(1 * time.Second):
		t.Fatal("WaitForLocalTaskDrain did not return after doneCh was closed")
	}

	wg.Wait()
}

// TestStructuralInvariant_WaitForLocalTaskDrain_NoHandleReturnsNil
// pins the no-handle case: when no per-task goroutine is registered
// for the descriptor (already drained, or never started on this
// node), WaitForLocalTaskDrain MUST return nil immediately rather
// than block forever or fall through to ctx.Done().
//
// The orchestrator's cancel→drain sequence relies on this: a task
// that never ran on this node still triggers WaitForLocalTaskDrain
// during cleanup, and we must not stall the cleanup goroutine on a
// non-existent worker.
func TestStructuralInvariant_WaitForLocalTaskDrain_NoHandleReturnsNil(t *testing.T) {
	p := structuralInvariantNewBareProvider()
	desc := distributedtask.TaskDescriptor{ID: "task-never-ran", Version: 1}

	// Use a context that's already cancelled. If the no-handle
	// path is correctly short-circuited, we'll see nil (not
	// ctx.Err()) — proving WaitForLocalTaskDrain didn't fall
	// through to the select.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, p.WaitForLocalTaskDrain(ctx, desc),
		"WaitForLocalTaskDrain must short-circuit with nil when no handle is registered; "+
			"falling through to ctx.Done() would surface a misleading ctx.Err() to the orchestrator")
}

// TestStructuralInvariant_WaitForLocalTaskDrain_RespectsContextCancel
// pins the timeout escape hatch: when the per-task goroutine is
// stuck (doneCh still open) and the caller's context is cancelled,
// WaitForLocalTaskDrain MUST return ctx.Err() so the orchestrator
// can give up rather than block indefinitely.
//
// The cancel→drain→cleanup sequence wraps the drain in a bounded
// context for exactly this reason: a runaway worker (e.g. stuck in
// an inverted-index iteration that ignored ctx) must not pin the
// shutdown path forever.
func TestStructuralInvariant_WaitForLocalTaskDrain_RespectsContextCancel(t *testing.T) {
	p := structuralInvariantNewBareProvider()
	desc := distributedtask.TaskDescriptor{ID: "task-runaway", Version: 42}
	// Inject a handle whose doneCh will NEVER close (simulates a
	// stuck worker).
	_ = structuralInvariantInjectHandle(p, desc)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := p.WaitForLocalTaskDrain(ctx, desc)
	require.Error(t, err,
		"WaitForLocalTaskDrain must surface ctx.Err() when the deadline expires while the handle's doneCh remains open")
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"the returned error must be the original ctx.Err() so orchestrator can distinguish "+
			"timeout from other failures")
}

// TestStructuralInvariant_StartTask_HandleTerminateDrainsSpawnedWorker
// is the fan-out drain counterpart: ReindexProvider.StartTask spawns
// a goroutine (the worker) and returns a TaskHandle whose Terminate
// MUST cancel that worker's context. The worker's defer chain then
// closes handle.DoneCh. So calling Terminate→<-Done() MUST observe
// the goroutine exiting.
//
// We can't drive StartTask directly here (it needs a full DB).
// Instead we exercise the invariant against a hand-built
// reindexTaskHandle whose cancel hook matches the StartTask wiring:
// Terminate triggers ctx cancellation, and a worker that respects ctx
// closes doneCh in its defer. This pins the shape of the
// handle.cancel + handle.DoneCh contract that StartTask exposes to
// the Scheduler.
func TestStructuralInvariant_StartTask_HandleTerminateDrainsSpawnedWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handle := &reindexTaskHandle{
		Cancel: cancel,
		DoneCh: make(chan struct{}),
	}

	// Simulate the StartTask-spawned worker: a goroutine that
	// exits when ctx is cancelled and closes doneCh in its defer.
	// This mirrors the per-task goroutine in StartTask (see
	// reindex_provider.go).
	workerExited := make(chan struct{})
	go func() {
		defer close(handle.DoneCh)
		defer close(workerExited)
		<-ctx.Done()
	}()

	handle.Terminate()

	// The worker must exit promptly after Terminate.
	select {
	case <-workerExited:
		// expected
	case <-time.After(1 * time.Second):
		t.Fatal("worker did not exit after handle.Terminate(); cancel→drain contract is broken")
	}

	// And handle.Done() must be observable (closed). This is
	// what WaitForLocalTaskDrain and Scheduler.tick's
	// dead-handle reaper depend on.
	select {
	case <-handle.Done():
		// expected
	case <-time.After(1 * time.Second):
		t.Fatal("handle.Done() did not close after worker exited; " +
			"Scheduler tick's dead-handle reaper would never observe this task as terminated")
	}
}
