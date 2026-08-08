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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
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

// observerCancelCmd stamps the cancel with a time relative to the harness clock.
// The stamp is the proposing node's, so nothing about dispatch may turn on it.
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

func TestManagerTerminalObserver(t *testing.T) {
	t.Run("a registered observer sees the cancelled task", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.Zero(t, rec.count(), "adding a task must not fire the cancel observer")

		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

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
		h.manager.RegisterTerminalObserver(observerNamespace, func(task *Task) {
			<-release
			rec.record(task)
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

		applied := make(chan error, 1)
		go func() { applied <- h.manager.CancelTask(observerCancelCmd(t, h, 0), false) }()

		select {
		case err := <-applied:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.Fail(t, "the cancel apply is waiting on observer code")
		}
		require.Zero(t, rec.count(), "the observer cannot have finished yet")
	})

	// A node replaying its RAFT log at startup applies cancels nobody is waiting
	// on any more. Every signal an observer raises has expired by then, so
	// paying for them means a restart holds gates it can never usefully release.
	//
	// The cancel is stamped as happening right now, so only the replay flag can
	// suppress it: an implementation that goes back to judging by the age of the
	// transition delivers this one.
	t.Run("a cancel replayed from the RAFT log is skipped", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), true))

		require.Never(t, func() bool { return rec.count() > 0 },
			200*time.Millisecond, 5*time.Millisecond,
			"a cancel the FSM flagged as replayed must not reach the observer")
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
		h.manager.RegisterTerminalObserver(observerNamespace, func(task *Task) {
			<-release
			rec.record(task)
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

		// One event parks in the observer, the queue then fills, and the rest
		// have to find another way through.
		const overflow = 3
		total := terminalDispatchQueueDepth + 1 + overflow
		fillDispatchQueue(h.manager, terminalDispatchTask(h.manager), total)

		close(release)
		require.Eventually(t, func() bool { return rec.count() == total },
			5*time.Second, 5*time.Millisecond,
			"every queued cancel must reach the observer")
	})

	// Dispatch is queued to a goroutine, so a foreign observer that does fire
	// fires after the apply returns. Recording and awaiting it is what makes
	// this fail; a require.Fail inside the observer would land on a finished t.
	t.Run("a namespace without an observer applies normally", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var foreign observerRecorder
		h.manager.RegisterTerminalObserver("some-other-namespace", foreign.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

		require.Never(t, func() bool { return foreign.count() > 0 },
			200*time.Millisecond, 10*time.Millisecond,
			"another namespace's observer must not fire")
	})

	// Registration is last-write-wins, so a nil stored over a live observer
	// would end cancel observation for the namespace with nothing logged and
	// nothing failing — the cancels simply stop being signalled.
	t.Run("a nil registration must not silence the live observer", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
		h.manager.RegisterTerminalObserver(observerNamespace, nil)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

		require.Eventually(t, func() bool { return rec.count() == 1 },
			5*time.Second, 5*time.Millisecond,
			"a nil re-registration must be dropped, not stored over the live observer")
	})

	t.Run("an observer registered under an empty namespace never runs", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		var live, empty observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, live.record)
		h.manager.RegisterTerminalObserver("", empty.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))
		require.Eventually(t, func() bool { return live.count() == 1 },
			5*time.Second, 5*time.Millisecond)

		// Dispatched by hand: no task reaches apply with an empty namespace, so
		// this is the only way to ask whether such a registration is reachable
		// at all.
		h.manager.mu.Lock()
		h.manager.dispatchTerminalWithLock(&Task{
			TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
			Status:         TaskStatusCancelled,
			FinishedAt:     h.clock.Now(),
		}, false)
		h.manager.mu.Unlock()

		require.Never(t, func() bool { return empty.count() > 0 },
			200*time.Millisecond, 10*time.Millisecond,
			"an observer registered under an empty namespace must never fire")
	})
}

// Close has to stop two separate things, and each half is pinned on its own:
// the drainer goroutine, which otherwise outlives the Manager and keeps a
// provider reachable after the node tore its dependencies down, and the apply
// path's handover, which otherwise keeps feeding that goroutine.
func TestManagerCloseStopsTheTerminalDrainer(t *testing.T) {
	h := newTestHarness(t).init(t)

	var rec observerRecorder
	h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

	h.manager.Close()
	h.manager.Close() // idempotent: shutdown runs on paths that may both fire

	// Put an event on the queue by hand, bypassing the apply path's own guard:
	// only a live drainer can take it off again, so this asks about the
	// goroutine and nothing else.
	h.manager.terminalDispatch <- terminalDispatchTask(h.manager)
	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"a queued event must not reach an observer after Close; the drainer has been told to exit")

	// The drainer is gone by now, so the queue below reads the apply path alone
	// rather than racing a consumer.
	for len(h.manager.terminalDispatch) > 0 {
		<-h.manager.terminalDispatch
	}
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))
	require.Empty(t, h.manager.terminalDispatch,
		"the apply path must not hand a cancel over after Close")
}

// newTerminalDispatchManager builds a Manager without the scheduler around it,
// because the tests below need many of them and touch nothing but the dispatch
// path.
func newTerminalDispatchManager() *Manager {
	logger, _ := logrustest.NewNullLogger()
	return NewManager(ManagerParameters{
		Clock:  clockwork.NewFakeClock(),
		Logger: logger,
	})
}

func terminalDispatchTask(m *Manager) *Task {
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
		Namespace:      observerNamespace,
		Status:         TaskStatusCancelled,
		FinishedAt:     m.clock.Now(),
	}
}

// fillDispatchQueue hands the same terminal event to the dispatch path n times,
// holding the Manager's lock the way the apply path does.
func fillDispatchQueue(m *Manager, task *Task, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for range n {
		m.dispatchTerminalWithLock(task, false)
	}
}

// The apply path spawns a goroutine per event the queue could not take, so a
// wedged observer would otherwise turn a cancel storm into unbounded fan-out:
// every one of those goroutines parks on the same wedge for the lifetime of the
// process. Past the bound the events have to be dropped instead.
func TestTerminalDispatchOverflowIsBounded(t *testing.T) {
	m := newTerminalDispatchManager()
	defer m.Close()

	release := make(chan struct{})
	var rec observerRecorder
	m.RegisterTerminalObserver(observerNamespace, func(task *Task) {
		<-release
		rec.record(task)
	})

	// Literals, not the constants themselves. Derived from
	// terminalDispatchQueueDepth and terminalDispatchOverflowLimit these numbers
	// move with them, and the assertions below then hold at any bound,
	// including one large enough to be no bound at all. 256 queue slots, one
	// more the drainer can take and then block looking its observer up, and
	// 32 goroutines of overflow.
	const (
		maxDelivered = 256 + 1 + 32
		pastTheBound = 16
	)

	m.mu.Lock()
	for range maxDelivered + pastTheBound {
		m.dispatchTerminalWithLock(terminalDispatchTask(m), false)
	}
	m.mu.Unlock()

	// The observer is wedged on release, so nothing has decremented yet: the
	// count is exactly what the apply path was allowed to spawn.
	require.EqualValues(t, 32, m.terminalOverflowInFlight.Load(),
		"a wedged observer must not hold more than 32 overflow goroutines")

	close(release)
	require.Eventually(t, func() bool { return rec.count() >= maxDelivered-1 },
		5*time.Second, 5*time.Millisecond,
		"every event the queue and the bounded overflow accepted must still reach the observer")
	require.Never(t, func() bool { return rec.count() > maxDelivered },
		300*time.Millisecond, 10*time.Millisecond,
		"events past the overflow bound must be dropped, not fanned out")
	require.EqualValues(t, 0, m.terminalOverflowInFlight.Load(),
		"every overflow goroutine must have released its slot")
}

// Close tears the Manager's dependencies down, and an overflow goroutine is not
// joined by it, so its own stop-signal check is the only thing between a
// shutdown and an observer call into a torn-down node.
func TestTerminalDispatchOverflowSkipsTheObserverAfterClose(t *testing.T) {
	m := newTerminalDispatchManager()

	var rec observerRecorder
	m.mu.Lock()
	// Registered by hand: RegisterTerminalObserver would start the drainer, which
	// would empty the queue this test needs full.
	m.terminalObservers[observerNamespace] = rec.record
	m.mu.Unlock()

	for range terminalDispatchQueueDepth {
		m.terminalDispatch <- terminalDispatchTask(m)
	}

	// The state under test is a shutdown landing between the dispatch and the
	// goroutine's first instruction. Close cannot stage it: it also latches the
	// apply path shut, and the dispatch below has to get past that.
	close(m.terminalDispatchDone)

	m.mu.Lock()
	m.dispatchTerminalWithLock(terminalDispatchTask(m), false)
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
		m := newTerminalDispatchManager()
		m.RegisterTerminalObserver(observerNamespace, func(task *Task) {
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
			m.terminalDispatch <- terminalDispatchTask(m)
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
// The overflow arm dispatches on its own goroutine rather than through the
// drainer, so it needs the drainer's containment too. GoWrapper's recover is
// conditional on DISABLE_RECOVERY_ON_PANIC, which the acceptance image sets to
// "true" — so relying on it means a panicking observer takes the node down
// under queue overflow and nowhere else, which is the worst possible place for
// it to be the only uncontained path.
func TestTerminalOverflowDispatchSurvivesAPanickingObserver(t *testing.T) {
	h := newTestHarness(t)
	logger, hook := logrustest.NewNullLogger()
	h.logger = logger
	h.init(t)
	defer h.manager.Close()

	release := make(chan struct{})
	var panics atomic.Int64
	h.manager.RegisterTerminalObserver(observerNamespace, func(task *Task) {
		<-release
		panics.Add(1)
		panic("overflow observer blew up")
	})

	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

	// One event parks in the observer, the queue fills, and the remainder go
	// down the overflow arm — the path under test.
	const overflow = 3
	total := terminalDispatchQueueDepth + 1 + overflow
	fillDispatchQueue(h.manager, terminalDispatchTask(h.manager), total)

	close(release)

	// Counting, not detecting. The drainer already contains its own panics and
	// logs this exact line, so "at least one appeared" is satisfied by the
	// queued events alone and says nothing about the overflow arm. Only the
	// total distinguishes them: every dispatch panics once, so all of them
	// contained means the overflow arm is contained too.
	contained := func() int {
		n := 0
		for _, e := range hook.AllEntries() {
			if strings.Contains(e.Message, "terminal observer panicked") &&
				strings.Contains(e.Message, "overflow observer blew up") {
				n++
			}
		}
		return n
	}

	require.Eventuallyf(t, func() bool { return int(panics.Load()) == total },
		10*time.Second, 10*time.Millisecond,
		"every dispatched event must reach the observer; reached %d of %d", panics.Load(), total)

	require.Eventuallyf(t, func() bool { return contained() == total },
		10*time.Second, 10*time.Millisecond,
		"every observer panic must be contained and logged the same way the drainer contains one; "+
			"contained %d of %d panics — the shortfall is the overflow arm relying on GoWrapper, "+
			"whose recover the acceptance image disables", contained(), total)
}

// GoWrapper's recover is outside the drainer loop, so a panic there exits the
// goroutine while terminalDrainerRunning stays true — nothing restarts it, and
// re-registering does not help. One namespace's bug then silences cancels for
// every namespace until the process restarts.
func TestTerminalDrainerSurvivesAPanickingObserver(t *testing.T) {
	h := newTestHarness(t)
	// The recovered panic is the only evidence an operator gets that an
	// observer is broken: the drainer swallows it and carries on, so without
	// the log line the namespace looks healthy while its cancels vanish.
	logger, hook := logrustest.NewNullLogger()
	h.logger = logger
	h.init(t)
	defer h.manager.Close()

	var rec observerRecorder
	panicked := make(chan struct{}, 1)
	h.manager.RegisterTerminalObserver(observerNamespace, func(task *Task) {
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
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

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
	}), false))

	require.Eventually(t, func() bool { return rec.count() == 1 },
		10*time.Second, 10*time.Millisecond,
		"the drainer died with the panicking observer, so no later cancel is ever observed again")

	var panicEntry *logrus.Entry
	require.Eventuallyf(t, func() bool {
		for _, e := range hook.AllEntries() {
			if strings.Contains(e.Message, "terminal observer panicked") {
				panicEntry = e
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond,
		"a swallowed observer panic must be logged; entries seen: %v", hook.AllEntries())

	require.Equal(t, logrus.ErrorLevel, panicEntry.Level,
		"below Error the only signal that an observer is broken does not reach an operator")
	require.Equal(t, observerNamespace, panicEntry.Data["namespace"],
		"the log must name the namespace whose observer panicked; every other one still works")
	require.Equal(t, observerTaskID, panicEntry.Data["task_id"],
		"the log must name the task whose cancel was dropped")
	require.Contains(t, panicEntry.Message, "observer blew up",
		"the panic value is what identifies the bug; a message without it is not actionable")
}
