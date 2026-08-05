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

	"github.com/jonboulle/clockwork"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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

	// A full queue keeps delivering up to the overflow bound; past it production
	// drops. Losing an event does not fail the cancel open, but it costs the node
	// waiting on it its whole confirmation budget before it answers unconfirmed,
	// so the events below the bound are worth the goroutines they take.
	t.Run("a full queue delivers up to the overflow bound", func(t *testing.T) {
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

// Close has to stop two separate things, and each half is pinned on its own:
// the drainer goroutine, which otherwise outlives the Manager and keeps a
// provider reachable after the node tore its dependencies down, and the apply
// path's handover, which otherwise keeps feeding that goroutine.
func TestManagerCloseStopsTheCancelDrainer(t *testing.T) {
	h := newTestHarness(t).init(t)

	var rec observerRecorder
	h.manager.RegisterCancelObserver(observerNamespace, rec.record)
	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

	h.manager.Close()
	h.manager.Close() // idempotent: shutdown runs on paths that may both fire

	// Put an event on the queue by hand, bypassing the apply path's own guard:
	// only a live drainer can take it off again, so this asks about the
	// goroutine and nothing else.
	h.manager.cancelDispatch <- &Task{
		TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
		Namespace:      observerNamespace,
		Status:         TaskStatusCancelled,
		FinishedAt:     h.clock.Now(),
	}
	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"a queued event must not reach an observer after Close; the drainer has been told to exit")

	// The drainer is gone by now, so the queue below reads the apply path alone
	// rather than racing a consumer.
	for len(h.manager.cancelDispatch) > 0 {
		<-h.manager.cancelDispatch
	}
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))
	require.Empty(t, h.manager.cancelDispatch,
		"the apply path must not hand a cancel over after Close")
}

// newCancelDispatchManager builds a Manager without the scheduler around it,
// because the tests below need many of them and touch nothing but the dispatch
// path.
func newCancelDispatchManager() *Manager {
	logger, _ := logrustest.NewNullLogger()
	return NewManager(ManagerParameters{
		Clock:  clockwork.NewFakeClock(),
		Logger: logger,
	})
}

func cancelDispatchTask(m *Manager) *Task {
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
		Namespace:      observerNamespace,
		Status:         TaskStatusCancelled,
		FinishedAt:     m.clock.Now(),
	}
}

// The apply path spawns a goroutine per event the queue could not take, so a
// wedged observer would otherwise turn a cancel storm into unbounded fan-out:
// every one of those goroutines parks on the same wedge for the lifetime of the
// process. Past the bound the events have to be dropped instead.
func TestCancelDispatchOverflowIsBounded(t *testing.T) {
	m := newCancelDispatchManager()
	defer m.Close()

	release := make(chan struct{})
	var rec observerRecorder
	m.RegisterCancelObserver(observerNamespace, func(task *Task) {
		<-release
		rec.record(task)
	})

	// The drainer can take one event off the queue and then block looking its
	// observer up, so the queue swallows one more than its depth before the
	// overflow path is reached at all.
	maxDelivered := cancelDispatchQueueDepth + 1 + cancelDispatchOverflowLimit
	const pastTheBound = 16

	m.mu.Lock()
	for range maxDelivered + pastTheBound {
		m.dispatchCancelWithLock(cancelDispatchTask(m))
	}
	m.mu.Unlock()

	// The observer is wedged on release, so nothing has decremented yet: the
	// count is exactly what the apply path was allowed to spawn.
	require.EqualValues(t, cancelDispatchOverflowLimit, m.cancelOverflowInFlight.Load(),
		"a wedged observer must not hold more goroutines than the overflow bound")

	close(release)
	require.Eventually(t, func() bool { return rec.count() >= maxDelivered-1 },
		5*time.Second, 5*time.Millisecond,
		"every event the queue and the bounded overflow accepted must still reach the observer")
	require.Never(t, func() bool { return rec.count() > maxDelivered },
		300*time.Millisecond, 10*time.Millisecond,
		"events past the overflow bound must be dropped, not fanned out")
	require.EqualValues(t, 0, m.cancelOverflowInFlight.Load(),
		"every overflow goroutine must have released its slot")
}

// Close tears the Manager's dependencies down, and an overflow goroutine is not
// joined by it, so its own stop-signal check is the only thing between a
// shutdown and an observer call into a torn-down node.
func TestCancelDispatchOverflowSkipsTheObserverAfterClose(t *testing.T) {
	m := newCancelDispatchManager()

	var rec observerRecorder
	m.mu.Lock()
	// Registered by hand: RegisterCancelObserver would start the drainer, which
	// would empty the queue this test needs full.
	m.cancelObservers[observerNamespace] = rec.record
	m.mu.Unlock()

	for range cancelDispatchQueueDepth {
		m.cancelDispatch <- cancelDispatchTask(m)
	}

	// The state under test is a shutdown landing between the dispatch and the
	// goroutine's first instruction. Close cannot stage it: it also latches the
	// apply path shut, and the dispatch below has to get past that.
	close(m.cancelDispatchDone)

	m.mu.Lock()
	m.dispatchCancelWithLock(cancelDispatchTask(m))
	m.mu.Unlock()

	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"an overflow dispatch must not call an observer once the stop signal is closed")
}

// A drainer that has a closed stop signal AND a queued event is choosing between
// two ready select cases, which lands on the exit about half the time on its
// own. So one drainer cannot tell a working recheck from a missing one. Many
// independent drainers can: a missing recheck has to win every coin flip.
func TestCloseDropsQueuedCancelsAcrossManyDrainers(t *testing.T) {
	const drainers = 32

	var rec observerRecorder
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	for range drainers {
		m := newCancelDispatchManager()
		m.RegisterCancelObserver(observerNamespace, func(task *Task) {
			// Only the first call per drainer has to be observable; a
			// blocking send here would wedge the extra calls a broken
			// recheck makes, and those are the point of the test.
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			rec.record(task)
		})

		for range 8 {
			m.cancelDispatch <- cancelDispatchTask(m)
		}
		// Parking one event inside the observer proves the drainer is past its
		// select, and leaves the rest queued so Close finds it with both cases
		// ready.
		<-entered
		m.Close()
	}

	close(release)
	require.Eventually(t, func() bool { return rec.count() >= drainers },
		5*time.Second, 5*time.Millisecond,
		"the observer call each drainer had in flight before Close must finish")
	require.Never(t, func() bool { return rec.count() > drainers },
		500*time.Millisecond, 10*time.Millisecond,
		"no drainer may deliver a queued cancel after Close")
}

// A panicking observer must not end cancel observation for everyone.
//
// GoWrapper's recover is outside the drainer loop, so a panic there exits the
// goroutine while cancelDrainerRunning stays true — nothing restarts it, and
// re-registering does not help. One namespace's bug then silences cancels for
// every namespace until the process restarts.
func TestCancelDrainerSurvivesAPanickingObserver(t *testing.T) {
	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	var rec observerRecorder
	panicked := make(chan struct{}, 1)
	h.manager.RegisterCancelObserver(observerNamespace, func(task *Task) {
		if task.Version == observerVersion {
			select {
			case panicked <- struct{}{}:
			default:
			}
			panic("observer blew up")
		}
		rec.record(task)
	})

	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0)))

	select {
	case <-panicked:
	case <-time.After(5 * time.Second):
		t.Fatal("the panicking observer was never reached")
	}

	// A second, later cancel: the drainer has to still be there to deliver it.
	const nextVersion = observerVersion + 1
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    "2",
		Payload:               []byte(`{"collection":"Movies"}`),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	}), nextVersion))
	require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    "2",
		Version:               nextVersion,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	})))

	require.Eventually(t, func() bool { return rec.count() == 1 },
		10*time.Second, 10*time.Millisecond,
		"the drainer died with the panicking observer, so no later cancel is ever observed again")
}
