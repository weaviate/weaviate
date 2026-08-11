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

// observerCancelCmd stamps the cancel with a proposer-relative time; dispatch
// must not key on it.
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

	// The skip is keyed on the FSM's replay flag, not on the task itself.
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

	// A nil re-registration must not silently overwrite the live observer.
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

	// The fail-closed restore path must still signal its terminal ending.
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

// Pins that an observer mutating its Task copy must never reach FSM state.
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

// Pins that repeated registrations reuse a single drainer goroutine, so a
// wedged observer stalls the queue instead of a second drainer walking past it.
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

// Pins that Close stops both the drainer goroutine and the apply path's
// handover to it.
func TestManagerCloseStopsTheTerminalDrainer(t *testing.T) {
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()

	var rec observerRecorder
	h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))

	h.manager.Close()
	h.manager.Close() // idempotent: shutdown runs on paths that may both fire

	// Bypass the apply path's guard to isolate the drainer's own behavior.
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

// newTerminalDispatchManager builds a Manager without a scheduler, for tests
// that only touch the dispatch path.
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

// Pins that overflow goroutines are bounded, so a wedged observer cannot turn
// a cancel storm into unbounded fan-out.
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

	// maxDelivered = queue depth + 1 the drainer can hold while blocked on the
	// observer + overflow limit. Kept as literals so the assertions below stay
	// meaningful even if the constants change.
	const (
		maxDelivered = 256 + 1 + 32
		pastTheBound = 16
	)

	m.mu.Lock()
	for range maxDelivered + pastTheBound {
		m.dispatchTerminalWithLock(terminalDispatchTask(m), false)
	}
	m.mu.Unlock()

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

// Pins that an overflow goroutine's own stop-signal check, not Close joining
// it, is what keeps it from calling into a torn-down node.
func TestTerminalDispatchOverflowSkipsTheObserverAfterClose(t *testing.T) {
	defer leaktest.Check(t)()
	m := newTerminalDispatchManager()

	var rec observerRecorder
	m.mu.Lock()
	// Registered by hand: RegisterTerminalObserver would start the drainer and
	// drain the queue this test needs full.
	m.terminalObservers[observerNamespace] = rec.record
	m.mu.Unlock()

	for range terminalDispatchQueueDepth {
		m.terminalDispatch <- terminalDispatchTask(m)
	}

	// Close m.terminalDispatchDone directly, bypassing Close's own latch on the
	// apply path, to reach the goroutine's own stop-signal check.
	close(m.terminalDispatchDone)

	m.mu.Lock()
	m.dispatchTerminalWithLock(terminalDispatchTask(m), false)
	m.mu.Unlock()

	require.Never(t, func() bool { return rec.count() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"an overflow dispatch must not call an observer once the stop signal is closed")
}

// A single drainer can't distinguish a working done-recheck from a missing
// one (select picks randomly ~half the time either way); many independent
// drainers can, since a missing recheck has to win every coin flip.
func TestCloseDropsQueuedCancelsAcrossManyDrainers(t *testing.T) {
	defer leaktest.Check(t)()
	const drainers = 32

	var rec observerRecorder
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	for range drainers {
		m := newTerminalDispatchManager()
		m.RegisterTerminalObserver(observerNamespace, func(task *Task) {
			// Non-blocking: a broken recheck makes extra calls, and those
			// extra calls are what this test is checking for.
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
		// Confirms the drainer is past its select with the rest still queued,
		// so Close finds both cases ready.
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

// Pins that the overflow goroutine contains a panicking observer the same way
// the drainer does, rather than relying on GoWrapper's recover (which the
// acceptance image disables via DISABLE_RECOVERY_ON_PANIC).
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

	// One event parks in the observer, the queue fills, and the rest go down
	// the overflow arm under test.
	const overflow = 3
	total := terminalDispatchQueueDepth + 1 + overflow
	fillDispatchQueue(h.manager, terminalDispatchTask(h.manager), total)

	close(release)

	// Must count every occurrence, not just detect one: the drainer already
	// contains its own panics, so only matching the total proves the overflow
	// arm is contained too.
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

// restoreTaskInStatus puts a single task into the FSM at the given status,
// which is the only way to reach the mid-flight PREPARING/SWAPPING states
// without driving a full multi-node scheduler run.
func restoreTaskInStatus(t *testing.T, m *Manager, status TaskStatus) {
	t.Helper()
	snap, err := json.Marshal(&snapshot{Tasks: map[string][]*Task{
		observerNamespace: {{
			Namespace:      observerNamespace,
			TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
			Status:         status,
			Units: map[string]*Unit{
				"su-1": {ID: "su-1", NodeID: "node-1", Status: UnitStatusCompleted},
			},
		}},
	}})
	require.NoError(t, err)
	require.NoError(t, m.Restore(snap))
}

// Pins the three FAILED routes that end a task after its units are already
// terminal. A namespace that never hears one of these waits forever, because
// no later transition follows to correct it.
func TestTerminalObserverFiresOnEveryFailedRoute(t *testing.T) {
	defer leaktest.Check(t)()

	tests := []struct {
		name   string
		status TaskStatus
		apply  func(t *testing.T, h *testHarness) error
	}{
		{
			name:   "swap failure reported by a node",
			status: TaskStatusSwapping,
			apply: func(t *testing.T, h *testHarness) error {
				return h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          observerNamespace,
					Id:                 observerTaskID,
					Version:            observerVersion,
					Error:              "cutover failed on node-1",
					FailedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false)
			},
		},
		{
			name:   "post-completion ack reports failure",
			status: TaskStatusSwapping,
			apply: func(t *testing.T, h *testHarness) error {
				return h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
					Namespace:         observerNamespace,
					Id:                observerTaskID,
					Version:           observerVersion,
					NodeId:            "node-1",
					Success:           false,
					Error:             "swap failed",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false)
			},
		},
		{
			name:   "preparation ack reports failure",
			status: TaskStatusPreparing,
			apply: func(t *testing.T, h *testHarness) error {
				return h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace:         observerNamespace,
					Id:                observerTaskID,
					Version:           observerVersion,
					NodeId:            "node-1",
					Success:           false,
					Error:             "prep failed",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer leaktest.Check(t)()
			defer h.Close()

			var rec observerRecorder
			h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
			restoreTaskInStatus(t, h.manager, test.status)

			require.NoError(t, test.apply(t, h))

			require.Eventually(t, func() bool { return rec.count() == 1 },
				5*time.Second, 5*time.Millisecond,
				"this route ends the task for good, so it must reach the observer")
			require.Equal(t, TaskStatusFailed, rec.first().Status)
		})
	}
}

// Pins the claim that a cluster with no registered observer pays nothing:
// without the lookup guard every terminal transition would clone the task and
// fill a queue no drainer is running to empty.
func TestTerminalDispatchSkipsUnregisteredNamespaces(t *testing.T) {
	defer leaktest.Check(t)()
	m := newTerminalDispatchManager()
	defer m.Close()

	m.mu.Lock()
	m.dispatchTerminalWithLock(terminalDispatchTask(m), false)
	m.mu.Unlock()

	require.Empty(t, m.terminalDispatch,
		"a namespace nobody registered for must not queue anything")
}
