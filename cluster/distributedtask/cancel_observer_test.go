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
	"time"

	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

const (
	observerNamespace = "test"
	observerTaskID    = "1"
	observerVersion   = uint64(10)
)

func observerAddCmd(t *testing.T, h *testHarness) *cmd.ApplyRequest {
	return toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    observerTaskID,
		Payload:               []byte(`{"collection":"Movies"}`),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	})
}

// observerCancelCmd stamps the cancel with a time relative to the harness clock,
// which is what decides whether the observer still cares about it.
func observerCancelCmd(t *testing.T, h *testHarness, cancelledAgo time.Duration) *cmd.ApplyRequest {
	return toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    observerTaskID,
		Version:               observerVersion,
		CancelledAtUnixMillis: h.clock.Now().Add(-cancelledAgo).UnixMilli(),
	})
}

// observerRecorder collects what the observer saw; the observer runs on the
// drainer goroutine, so every read needs the lock.
type observerRecorder struct {
	mu   sync.Mutex
	seen []*Task
}

func (r *observerRecorder) record(task *Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, task)
}

func (r *observerRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func (r *observerRecorder) first() *Task {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[0]
}

func TestManagerCancelObserver(t *testing.T) {
	t.Run("a registered observer sees the cancelled task", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var rec observerRecorder
		h.manager.RegisterCancelObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.Zero(t, rec.count(), "adding a task must not fire the cancel observer")

		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))

		require.Eventually(t, func() bool { return rec.count() == 1 },
			5*time.Second, 5*time.Millisecond, "the observer must fire for the cancel")

		observed := rec.first()
		require.Equal(t, observerTaskID, observed.ID)
		require.Equal(t, observerNamespace, observed.Namespace)
		require.Equal(t, observerVersion, observed.Version)
		require.Equal(t, TaskStatusCancelled, observed.Status,
			"the observer must see the task already in its cancelled state")
		require.JSONEq(t, `{"collection":"Movies"}`, string(observed.Payload),
			"the payload is what identifies the shards to gate")
	})

	// Observers take their own locks, which the admission path and the HTTP
	// handlers also take. That is only safe while the apply never waits for one,
	// so an observer that is busy must not hold the apply up.
	t.Run("the apply does not wait for observer code", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		release := make(chan struct{})
		defer close(release)

		var rec observerRecorder
		h.manager.RegisterCancelObserver(observerNamespace, func(task *Task) {
			<-release
			rec.record(task)
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

		applied := make(chan error, 1)
		go func() { applied <- h.manager.CancelTask(observerCancelCmd(t, h, 0)) }()

		select {
		case err := <-applied:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.Fail(t, "the cancel apply is waiting on observer code")
		}
		require.Zero(t, rec.count(), "the observer cannot have finished yet")
	})

	// A node catching up on the RAFT log applies cancels nobody is waiting on
	// any more. Every signal an observer raises has expired by then, so paying
	// for them means a restart holds gates it can never usefully release.
	t.Run("a cancel older than the observer window is skipped", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var rec observerRecorder
		h.manager.RegisterCancelObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(
			observerCancelCmd(t, h, cancelObserverStaleAfter+time.Minute)))

		require.Never(t, func() bool { return rec.count() > 0 },
			200*time.Millisecond, 5*time.Millisecond,
			"a cancel this old must not reach the observer")
	})

	// A full queue must not drop: the event feeds the answer another node reads
	// as "this owner is done", so a missing one is read as confirmation.
	t.Run("a full queue delivers rather than drops", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		release := make(chan struct{})
		var rec observerRecorder
		h.manager.RegisterCancelObserver(observerNamespace, func(task *Task) {
			<-release
			rec.record(task)
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

		// One event parks in the observer, the queue then fills, and the rest
		// have to find another way through.
		const overflow = 3
		total := cancelDispatchQueueDepth + 1 + overflow
		task := &Task{
			TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
			Namespace:      observerNamespace,
			Status:         TaskStatusCancelled,
			FinishedAt:     h.clock.Now(),
		}
		h.manager.mu.Lock()
		for range total {
			h.manager.dispatchCancelWithLock(task)
		}
		h.manager.mu.Unlock()

		close(release)
		require.Eventually(t, func() bool { return rec.count() == total },
			5*time.Second, 5*time.Millisecond,
			"every queued cancel must reach the observer")
	})

	t.Run("a namespace without an observer applies normally", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		h.manager.RegisterCancelObserver("some-other-namespace", func(*Task) {
			require.Fail(t, "another namespace's observer must not fire")
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))
	})

	t.Run("nil and empty registrations are dropped", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		h.manager.RegisterCancelObserver(observerNamespace, nil)
		h.manager.RegisterCancelObserver("", func(*Task) {
			require.Fail(t, "an observer registered under an empty namespace must never fire")
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))
	})
}

// Close stops the drainer, so a cancel applying after shutdown must not be
// dispatched. Without this the drainer goroutine outlives the Manager and keeps
// a provider reachable after the node has torn its dependencies down.
func TestManagerCloseStopsTheCancelDrainer(t *testing.T) {
	h := newTestHarness(t).init(t)

	var rec observerRecorder
	h.manager.RegisterCancelObserver(observerNamespace, rec.record)
	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

	h.manager.Close()
	h.manager.Close() // idempotent: shutdown runs on paths that may both fire

	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))

	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"the drainer must not dispatch after Close; its goroutine has been told to exit")
}
