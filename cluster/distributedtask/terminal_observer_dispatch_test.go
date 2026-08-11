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
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
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
	defer leaktest.Check(t)()

	t.Run("a registered observer sees the cancelled task", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer leaktest.Check(t)()
		defer h.Close()

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
			"the payload is what tells an observer which collection the task was bound to")
	})

	// A replayed ending finished long ago, so signalling it would announce an
	// event that is not happening now. The apply path decides this on the FSM's
	// own replay flag; nothing about the task itself distinguishes the two.
	t.Run("an ending replayed from the RAFT log is skipped", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer leaktest.Check(t)()
		defer h.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), true))

		require.Never(t, func() bool { return rec.count() > 0 },
			300*time.Millisecond, 10*time.Millisecond,
			"a cancel applied while catching up must not reach the observer")
	})

	// FAILED is a terminal status like CANCELLED, and a task that failed is
	// just as done as one that was cancelled.
	t.Run("a unit failure fires the observer too", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer leaktest.Check(t)()
		defer h.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            observerNamespace,
			Id:                   observerTaskID,
			Version:              observerVersion,
			NodeId:               "node-1",
			UnitId:               "su-1",
			Error:                "synthetic unit failure",
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), false))

		require.Eventually(t, func() bool { return rec.count() == 1 },
			5*time.Second, 5*time.Millisecond, "the observer must fire for the failure")
		require.Equal(t, TaskStatusFailed, rec.first().Status)
	})

	// Registration is last-write-wins, so a nil stored over a live observer
	// would end cancel observation for the namespace with nothing logged and
	// nothing failing — the cancels simply stop being signalled.
	t.Run("a nil registration must not silence the live observer", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer leaktest.Check(t)()
		defer h.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
		h.manager.RegisterTerminalObserver(observerNamespace, nil)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

		require.Eventually(t, func() bool { return rec.count() == 1 },
			5*time.Second, 5*time.Millisecond,
			"a nil re-registration must be dropped, not stored over the live observer")
	})

	// A task restored from a peer's snapshot with an already-FAILED unit fails
	// closed when its last unit lands. That ending is permanent and no other
	// apply on this node will ever announce it, so it has to be signalled here.
	t.Run("a task that fails closed on restore fires the observer", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer leaktest.Check(t)()
		defer h.Close()

		var rec observerRecorder
		h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

		// AddTask cannot reach a STARTED task with a FAILED unit — the failure
		// path fails the task itself. Restore is the only way in.
		snap, err := json.Marshal(&snapshot{Tasks: map[string][]*Task{
			observerNamespace: {{
				Namespace:      observerNamespace,
				TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
				Status:         TaskStatusStarted,
				Units: map[string]*Unit{
					"su-failed":  {ID: "su-failed", NodeID: "node-1", Status: UnitStatusFailed, Error: "boom"},
					"su-pending": {ID: "su-pending", NodeID: "node-2", Status: UnitStatusPending},
				},
			}},
		}})
		require.NoError(t, err)
		require.NoError(t, h.manager.Restore(snap))

		require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace:            observerNamespace,
			Id:                   observerTaskID,
			Version:              observerVersion,
			NodeId:               "node-2",
			UnitId:               "su-pending",
			FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), false))

		require.Eventually(t, func() bool { return rec.count() == 1 },
			5*time.Second, 5*time.Millisecond,
			"the fail-closed restore path must signal its ending like every other terminal transition")

		observed := rec.first()
		require.Equal(t, TaskStatusFailed, observed.Status)
		require.Equal(t, h.clock.Now().UnixMilli(), observed.FinishedAt.UnixMilli(),
			"the observer's copy must carry the stamp written by the same apply")
	})
}

// The observer is handed a clone because the FSM keeps mutating its own copy
// after the dispatch. Handing over the live task instead breaks nothing that
// any other test looks at, so pin it here: whatever an observer does to what it
// was given must not reach FSM state.
func TestTerminalObserverCannotMutateFSMState(t *testing.T) {
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()

	scribbled := make(chan struct{})
	h.manager.RegisterTerminalObserver(observerNamespace, func(task *Task) {
		task.Status = TaskStatusFinished
		task.Error = "scribbled by the observer"
		for _, u := range task.Units {
			u.Status = UnitStatusFailed
		}
		close(scribbled)
	})

	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, 0), false))

	select {
	case <-scribbled:
	case <-time.After(5 * time.Second):
		t.Fatal("the observer never ran")
	}

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks[observerNamespace], 1)

	stored := tasks[observerNamespace][0]
	require.Equal(t, TaskStatusCancelled, stored.Status,
		"the observer must have written to a copy, not to the task the FSM keeps")
	require.Empty(t, stored.Error)
	require.Equal(t, UnitStatusPending, stored.Units["su-1"].Status,
		"Units is deep-copied too, so a scribble on a unit must not land either")
}

// Without the single-drainer guard every registration starts another drainer,
// and two namespaces registering is the normal case. One drainer is what makes
// the bounded overflow arm the only concurrency in this path: a wedged observer
// has to stall the queue rather than have a second drainer walk past it.
func TestTerminalDispatchRunsOnOneDrainer(t *testing.T) {
	defer leaktest.Check(t)()
	m := newTerminalDispatchManager()
	defer m.Close()

	entered := make(chan struct{})
	release := make(chan struct{})
	m.RegisterTerminalObserver("wedged-namespace", func(*Task) {
		close(entered)
		<-release
	})

	var rec observerRecorder
	m.RegisterTerminalObserver("second-namespace", rec.record)

	dispatchOne := func(namespace string) {
		task := terminalDispatchTask(m)
		task.Namespace = namespace
		m.mu.Lock()
		defer m.mu.Unlock()
		m.dispatchTerminalWithLock(task, false)
	}

	dispatchOne("wedged-namespace")
	<-entered
	dispatchOne("second-namespace")

	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"a second registration must not bring up a second drainer; one wedged observer stalls the queue")

	close(release)
	require.Eventually(t, func() bool { return rec.count() == 1 },
		5*time.Second, 5*time.Millisecond,
		"the queued event must be delivered once the wedged observer returns")
}

// Close has to stop two separate things, and each half is pinned on its own:
// the drainer goroutine, which otherwise outlives the Manager and keeps a
// provider reachable after the node tore its dependencies down, and the apply
// path's handover, which otherwise keeps feeding that goroutine.
func TestManagerCloseStopsTheTerminalDrainer(t *testing.T) {
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()

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
	defer leaktest.Check(t)()
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
	defer leaktest.Check(t)()
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
	defer leaktest.Check(t)()
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
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()
	hook := h.loggerHook

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
