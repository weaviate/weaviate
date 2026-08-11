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

func observerAddCmd(t *testing.T, h *testHarness, id string) *cmd.ApplyRequest {
	return toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    id,
		Payload:               []byte(`{"collection":"Movies"}`),
		SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitIds:               []string{"su-1"},
	})
}

func observerCancelCmd(t *testing.T, h *testHarness, id string) *cmd.ApplyRequest {
	return toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             observerNamespace,
		Id:                    id,
		Version:               observerVersion,
		CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
	})
}

// observerUnitCompletionCmd reports unit as done on node; a non-empty unitErr
// makes it a failure report.
func observerUnitCompletionCmd(t *testing.T, h *testHarness, node, unit, unitErr string) *cmd.ApplyRequest {
	return toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            observerNamespace,
		Id:                   observerTaskID,
		Version:              observerVersion,
		NodeId:               node,
		UnitId:               unit,
		Error:                unitErr,
		FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
	})
}

// newObserverHarness returns a harness with a recorder already registered for
// observerNamespace, plus the leak check and shutdown every observer test needs.
func newObserverHarness(t *testing.T) (*testHarness, *observerRecorder) {
	t.Helper()
	h := newTestHarness(t).init(t)
	t.Cleanup(leaktest.Check(t))
	t.Cleanup(h.Close)

	rec := &observerRecorder{}
	h.manager.RegisterTerminalObserver(observerNamespace, rec.record)
	return h, rec
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

func (r *observerRecorder) waitForCount(t *testing.T, want int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return r.count() == want },
		5*time.Second, 5*time.Millisecond, msg)
}

func (r *observerRecorder) waitForAtLeast(t *testing.T, want int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return r.count() >= want },
		5*time.Second, 5*time.Millisecond, msg)
}

func (r *observerRecorder) requireNeverExceeds(t *testing.T, most int, within time.Duration, msg string) {
	t.Helper()
	require.Never(t, func() bool { return r.count() > most },
		within, 10*time.Millisecond, msg)
}

func (r *observerRecorder) requireStaysSilent(t *testing.T, msg string) {
	t.Helper()
	r.requireNeverExceeds(t, 0, 300*time.Millisecond, msg)
}

func TestManagerTerminalObserver(t *testing.T) {
	defer leaktest.Check(t)()

	t.Run("a registered observer sees the cancelled task", func(t *testing.T) {
		h, rec := newObserverHarness(t)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, observerTaskID), false))

		rec.waitForCount(t, 1, "the observer must fire for the cancel")
		// Barrier: the cancel has landed, so a second event now could only
		// have come from AddTask, which must announce nothing.
		rec.requireNeverExceeds(t, 1, 300*time.Millisecond,
			"only the cancel may be announced; adding a task must stay silent")

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
		h, rec := newObserverHarness(t)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, observerTaskID), true))

		rec.requireStaysSilent(t, "a cancel applied while catching up must not reach the observer")
	})

	t.Run("a unit failure fires the observer too", func(t *testing.T) {
		h, rec := newObserverHarness(t)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
		require.NoError(t, h.manager.RecordUnitCompletion(
			observerUnitCompletionCmd(t, h, "node-1", "su-1", "synthetic unit failure"), false))

		rec.waitForCount(t, 1, "the observer must fire for the failure")
		require.Equal(t, TaskStatusFailed, rec.first().Status)
	})

	t.Run("a nil registration must not silence the live observer", func(t *testing.T) {
		h, rec := newObserverHarness(t)
		h.manager.RegisterTerminalObserver(observerNamespace, nil)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, observerTaskID), false))

		rec.waitForCount(t, 1,
			"a nil re-registration must be dropped, not stored over the live observer")
	})

	// The third delivery case from the TerminalObserver contract: endings
	// that arrive inside an installed snapshot are never announced.
	t.Run("a task already terminal in an installed snapshot must not fire", func(t *testing.T) {
		for _, status := range []TaskStatus{TaskStatusCancelled, TaskStatusFailed} {
			t.Run(string(status), func(t *testing.T) {
				h, rec := newObserverHarness(t)

				restoreTask(t, h.manager, status, map[string]*Unit{
					"su-1": {ID: "su-1", NodeID: "node-1", Status: UnitStatusCompleted},
				})

				// Barrier: once this live cancel lands, a missing event for the
				// restored task means the contract held, not a slow drainer.
				const cancelledTaskID = "2"
				require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, cancelledTaskID), observerVersion))
				require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, cancelledTaskID), false))

				rec.waitForAtLeast(t, 1, "the live cancel must fire the observer")
				require.Equal(t, cancelledTaskID, rec.first().ID,
					"only the live cancel may be announced; a snapshot-installed ending must stay silent")
				require.Equal(t, 1, rec.count())
			})
		}
	})

	t.Run("a task that fails closed on restore fires the observer", func(t *testing.T) {
		h, rec := newObserverHarness(t)

		// AddTask cannot reach a STARTED task with a FAILED unit — the failure
		// path fails the task itself. Restore is the only way in.
		restoreTask(t, h.manager, TaskStatusStarted, map[string]*Unit{
			"su-failed":  {ID: "su-failed", NodeID: "node-1", Status: UnitStatusFailed, Error: "boom"},
			"su-pending": {ID: "su-pending", NodeID: "node-2", Status: UnitStatusPending},
		})

		require.NoError(t, h.manager.RecordUnitCompletion(
			observerUnitCompletionCmd(t, h, "node-2", "su-pending", ""), false))

		rec.waitForCount(t, 1,
			"the fail-closed restore path must signal its ending like every other terminal transition")

		observed := rec.first()
		require.Equal(t, TaskStatusFailed, observed.Status)
		require.Equal(t, h.clock.Now().UnixMilli(), observed.FinishedAt.UnixMilli(),
			"the observer's copy must carry the stamp written by the same apply")
	})

	// The fourth delivery case from the TerminalObserver contract: a
	// non-terminal task removed by the DELETE_CLASS cascade never goes
	// CANCELLED or FAILED, so it never fires.
	t.Run("a task removed by the DELETE_CLASS cascade must not fire", func(t *testing.T) {
		h, rec := newObserverHarness(t)
		h.manager.RegisterCollectionExtractor(observerNamespace, func(payload []byte) (string, bool) {
			var p struct {
				Collection string `json:"collection"`
			}
			if err := json.Unmarshal(payload, &p); err != nil {
				return "", false
			}
			return p.Collection, true
		})

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))

		removed := h.manager.DeleteTasksForCollection("Movies")
		require.Len(t, removed, 1)
		require.Equal(t, observerTaskID, removed[0].ID)

		tasks, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		require.Empty(t, tasks[observerNamespace],
			"the cascade must remove the task from the listing")

		// Barrier: once this live cancel lands, a missing event for the removed
		// task means the contract held, not a slow drainer.
		const cancelledTaskID = "2"
		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, cancelledTaskID), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, cancelledTaskID), false))

		rec.waitForAtLeast(t, 1, "the live cancel must fire the observer")
		require.Equal(t, cancelledTaskID, rec.first().ID,
			"only the live cancel may be announced; the cascade removal must stay silent")
		require.Equal(t, 1, rec.count())
	})

	// Only CANCELLED and FAILED fire the observer; MarkTaskFinalized (FINISHED)
	// must stay silent.
	t.Run("reaching FINISHED must not fire the observer", func(t *testing.T) {
		h, rec := newObserverHarness(t)

		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
		require.NoError(t, h.manager.RecordUnitCompletion(
			observerUnitCompletionCmd(t, h, "node-1", "su-1", ""), false))
		require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
			Namespace:         observerNamespace,
			Id:                observerTaskID,
			Version:           observerVersion,
			NodeId:            "node-1",
			Success:           true,
			AckedAtUnixMillis: h.clock.Now().UnixMilli(),
		}), false))
		require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
			Namespace:             observerNamespace,
			Id:                    observerTaskID,
			Version:               observerVersion,
			FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))

		tasks, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		require.Equal(t, TaskStatusFinished, tasks[observerNamespace][0].Status)

		// Barrier: once this second task's cancel event lands, a missing
		// FINISHED event means the contract held, not a slow drainer.
		const cancelledTaskID = "2"
		require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, cancelledTaskID), observerVersion))
		require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, cancelledTaskID), false))

		rec.waitForAtLeast(t, 1, "the cancel must fire the observer")
		require.Equal(t, cancelledTaskID, rec.first().ID,
			"only the cancelled task may be announced; FINISHED must stay silent")
		require.Equal(t, 1, rec.count())
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

	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, observerTaskID), false))

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

	rec.requireStaysSilent(t,
		"a second registration must not bring up a second drainer; one wedged observer stalls the queue")

	close(release)
	rec.waitForCount(t, 1,
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
	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))

	h.manager.Close()
	h.manager.Close() // idempotent: shutdown runs on paths that may both fire

	// Bypass the apply path's guard to isolate the drainer's own behavior.
	h.manager.terminalDispatch <- terminalDispatchTask(h.manager)
	rec.requireStaysSilent(t,
		"a queued event must not reach an observer after Close; the drainer has been told to exit")

	// The drainer is gone by now, so the queue below reads the apply path alone
	// rather than racing a consumer. Non-blocking anyway: a live drainer could
	// take the last element between the length check and the receive, and a
	// bare receive would then hang until the test timeout.
drain:
	for {
		select {
		case <-h.manager.terminalDispatch:
		default:
			break drain
		}
	}
	require.NoError(t, h.manager.CancelTask(observerCancelCmd(t, h, observerTaskID), false))
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

	fillDispatchQueue(m, terminalDispatchTask(m), maxDelivered+pastTheBound)

	require.EqualValues(t, 32, m.terminalOverflowInFlight.Load(),
		"a wedged observer must not hold more than 32 overflow goroutines")

	close(release)
	rec.waitForAtLeast(t, maxDelivered-1,
		"every event the queue and the bounded overflow accepted must still reach the observer")
	rec.requireNeverExceeds(t, maxDelivered, 300*time.Millisecond,
		"events past the overflow bound must be dropped, not fanned out")
	require.Eventually(t, func() bool {
		return m.terminalOverflowInFlight.Load() == 0
	}, 5*time.Second, 10*time.Millisecond,
		"every overflow goroutine must have released its slot")
}

// errorEntriesMatching returns the error-level log entries whose message
// contains substr.
func errorEntriesMatching(hook *logrustest.Hook, substr string) []*logrus.Entry {
	var matched []*logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.ErrorLevel && strings.Contains(e.Message, substr) {
			matched = append(matched, e)
		}
	}
	return matched
}

// Pins the only trace a degraded or lost terminal ending leaves. Without it an
// observer that has silently stopped receiving looks exactly like a namespace
// with nothing to report, and the delivery contract's answer to that is "watch
// the logs".
func TestTerminalDispatchReportsDegradedAndLostEndings(t *testing.T) {
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()

	release := make(chan struct{})
	defer close(release)
	h.manager.RegisterTerminalObserver(observerNamespace, func(*Task) { <-release })

	// One event parks in the wedged observer, terminalDispatchQueueDepth fill
	// the queue, the next terminalDispatchOverflowLimit take the overflow arm,
	// and everything past that is dropped.
	const pastTheBound = 16
	maxDelivered := terminalDispatchQueueDepth + 1 + terminalDispatchOverflowLimit
	fillDispatchQueue(h.manager, terminalDispatchTask(h.manager), maxDelivered+pastTheBound)

	tests := []struct {
		name    string
		message string
	}{
		{
			name:    "the overflow arm reports the observer falling behind",
			message: "the observer is not keeping up",
		},
		{
			name:    "the drop arm reports the ending it threw away",
			message: "dropping this terminal event",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entries := errorEntriesMatching(h.loggerHook, test.message)
			require.NotEmpty(t, entries,
				"this arm must report at error level; an operator has no other signal that an ending was degraded or lost")

			// Fields, not just the message: they are what lets an operator
			// name the ending that went missing.
			reported := entries[0]
			require.Equal(t, observerNamespace, reported.Data["namespace"])
			require.Equal(t, observerTaskID, reported.Data["task_id"])
			require.Equal(t, observerVersion, reported.Data["version"])
		})
	}
}

// Pins that the observer's own recover honors the same switch GoWrapper does.
// docker-compose-test.yml and test/integration/run.sh set it so a panic kills
// the process; recovering regardless would make a panicking observer the one
// bug an acceptance run can never fail on.
func TestTerminalObserverRecoveryHonorsDisableRecoveryOnPanic(t *testing.T) {
	tests := []struct {
		name      string
		envValue  string
		wantPanic bool
	}{
		{name: "unset keeps the drainer alive", envValue: "", wantPanic: false},
		{name: "false keeps the drainer alive", envValue: "false", wantPanic: false},
		{name: "true lets the panic escape", envValue: "true", wantPanic: true},
		{name: "on lets the panic escape", envValue: "on", wantPanic: true},
		{name: "1 lets the panic escape", envValue: "1", wantPanic: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer leaktest.Check(t)()
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", test.envValue)

			m := newTerminalDispatchManager()
			defer m.Close()
			m.RegisterTerminalObserver(observerNamespace, panickingTerminalObserver)

			// Called directly rather than through the drainer: an escaping
			// panic there would take this test process down, which is the
			// whole point of the switch.
			deliver := func() { m.runTerminalObserverSafely(terminalDispatchTask(m)) }

			if test.wantPanic {
				require.PanicsWithValue(t, "named observer blew up", deliver,
					"with recovery disabled the panic must reach GoWrapper, which also declines to recover, and fail the run")
				return
			}
			require.NotPanics(t, deliver,
				"with recovery enabled one namespace's panic must not end the drainer for all of them")
		})
	}
}

// Pins that a Manager built without a logger still has one. The drop and panic
// reports are the only trace a lost ending leaves, and a nil logger would turn
// the first of them into a nil dereference on the RAFT-apply path.
func TestNewManagerSubstitutesAMissingLogger(t *testing.T) {
	m := NewManager(ManagerParameters{})
	defer m.Close()

	require.NotNil(t, m.logger)
	require.NotPanics(t, func() {
		m.logger.WithField("namespace", observerNamespace).Error("dropping this terminal event")
	})
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
	// Mark closed only after the dispatch (the latch would short-circuit the
	// overflow path under test), so a later m.Close() cannot double-close the
	// channel closed by hand above.
	m.terminalDispatchClosed = true
	m.mu.Unlock()

	rec.requireStaysSilent(t,
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
	rec.waitForAtLeast(t, drainers,
		"the observer call each drainer had in flight before Close must finish")
	rec.requireNeverExceeds(t, drainers, 500*time.Millisecond,
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

	require.NoError(t, h.manager.AddTask(observerAddCmd(t, h, observerTaskID), observerVersion))

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

// panickingTerminalObserver is a named function so the stack-frame assertion
// in TestTerminalObserverPanicLogsTheObserverStack can match its frame.
func panickingTerminalObserver(*Task) {
	panic("named observer blew up")
}

// Pins that a contained observer panic still reports the way GoWrapper would:
// the logged stack includes the observer's own frame (not just the panic
// value), and the drainer keeps delivering to other namespaces' observers.
func TestTerminalObserverPanicLogsTheObserverStack(t *testing.T) {
	defer leaktest.Check(t)()
	h := newTestHarness(t).init(t)
	defer h.Close()
	hook := h.loggerHook

	const healthyNamespace = "healthy"
	var rec observerRecorder
	h.manager.RegisterTerminalObserver(observerNamespace, panickingTerminalObserver)
	h.manager.RegisterTerminalObserver(healthyNamespace, rec.record)

	healthyTask := terminalDispatchTask(h.manager)
	healthyTask.Namespace = healthyNamespace

	h.manager.mu.Lock()
	h.manager.dispatchTerminalWithLock(terminalDispatchTask(h.manager), false)
	h.manager.dispatchTerminalWithLock(healthyTask, false)
	h.manager.mu.Unlock()

	rec.waitForCount(t, 1,
		"a panic in one namespace's observer must not stop delivery to another's")

	stackLogged := func() bool {
		for _, e := range hook.AllEntries() {
			if strings.Contains(e.Message, "panickingTerminalObserver") {
				return true
			}
		}
		return false
	}
	require.Eventuallyf(t, stackLogged, 5*time.Second, 10*time.Millisecond,
		"the panic log must include a stack with the observer's frame, not just the panic value")
}

// restoreTask installs a single task in the given status and unit layout,
// reaching states no apply sequence can build (PREPARING/SWAPPING, or a STARTED
// task that already holds a failed unit).
func restoreTask(t *testing.T, m *Manager, status TaskStatus, units map[string]*Unit) {
	t.Helper()
	snap, err := json.Marshal(&snapshot{Tasks: map[string][]*Task{
		observerNamespace: {{
			Namespace:      observerNamespace,
			TaskDescriptor: TaskDescriptor{ID: observerTaskID, Version: observerVersion},
			Status:         status,
			Units:          units,
		}},
	}})
	require.NoError(t, err)
	require.NoError(t, m.Restore(snap))
}

// Pins the three FAILED routes that must still reach the terminal observer.
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
			h, rec := newObserverHarness(t)
			restoreTask(t, h.manager, test.status, map[string]*Unit{
				"su-1": {ID: "su-1", NodeID: "node-1", Status: UnitStatusCompleted},
			})

			require.NoError(t, test.apply(t, h))

			rec.waitForCount(t, 1,
				"this route ends the task for good, so it must reach the observer")
			require.Equal(t, TaskStatusFailed, rec.first().Status)
		})
	}
}

// Pins that unregistered namespaces never get dispatched to.
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

// Pins the documented "empty arguments are dropped" half of the registration
// contract: an empty namespace must store nothing and start no drainer.
func TestRegisterTerminalObserverEmptyNamespaceIsDropped(t *testing.T) {
	defer leaktest.Check(t)()
	m := newTerminalDispatchManager()
	defer m.Close()

	m.RegisterTerminalObserver("", func(*Task) {})

	m.mu.RLock()
	defer m.mu.RUnlock()
	require.Empty(t, m.terminalObservers,
		"an empty-namespace registration must not be stored")
	require.False(t, m.terminalDrainerRunning,
		"an empty-namespace registration must not start a drainer")
}

// Pins that a registration after Close is dropped whole: no observer is
// stored and no drainer goroutine is started.
func TestRegisterTerminalObserverAfterCloseIsDropped(t *testing.T) {
	defer leaktest.Check(t)()
	m := newTerminalDispatchManager()
	m.Close()

	m.RegisterTerminalObserver(observerNamespace, func(*Task) {})

	m.mu.RLock()
	defer m.mu.RUnlock()
	require.False(t, m.terminalDrainerRunning,
		"a post-Close registration must not start a drainer")
	require.Empty(t, m.terminalObservers,
		"a post-Close registration must not store the observer")
}
