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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// A FAILED task ends exactly like a cancelled one on every node: it stops
// reading live the instant the apply lands, while the partial state its units
// wrote is still on disk until the teardown that the next scheduler tick starts
// — a fallback tick away, one minute by default. The observer is what holds the
// gate over that window, so every apply that can flip a task to FAILED has to
// raise it. Only CANCELLED did, which left the whole FAILED half of the
// terminal set uncovered.
func TestFailedApplyRaisesTheTerminalObserver(t *testing.T) {
	const (
		ns      = "ns"
		taskID  = "task1"
		version = uint64(10)
	)

	completeUnits := func(t *testing.T, h *testHarness, nodes []string) {
		t.Helper()
		for _, n := range nodes {
			updateProgress(t, h, ns, taskID, version, n, "u-"+n, 0.1)
		}
		for _, n := range nodes {
			completeUnit(t, h, ns, taskID, version, n, "u-"+n)
		}
	}

	tests := []struct {
		name string
		// drive takes a fresh task all the way to FAILED through one of the
		// apply paths that can produce that status.
		drive func(t *testing.T, h *testHarness)
	}{
		{
			name: "a unit reports an error",
			drive: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u-node-1", 0.1)
				failUnit(t, h, ns, taskID, version, "node-1", "u-node-1", "synthetic unit failure")
			},
		},
		{
			name: "a node's post-completion swap ack reports failure",
			drive: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				completeUnits(t, h, []string{"node-1"})
				require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic swap failure",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
		{
			name: "a node's prep-barrier ack reports failure",
			drive: func(t *testing.T, h *testHarness) {
				addBarrierTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				drivePreparing(t, h, ns, taskID, version, []string{"node-1"})
				require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic prep failure",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
		{
			name: "the scheduler marks a swapping task failed",
			drive: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				completeUnits(t, h, []string{"node-1"})
				require.NoError(t, h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          ns,
					Id:                 taskID,
					Version:            version,
					Error:              "synthetic cutover failure",
					FailedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			var rec observerRecorder
			h.manager.RegisterTerminalObserver(ns, rec.record)

			tc.drive(t, h)

			tasks, err := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, err)
			require.Equal(t, TaskStatusFailed, tasks[ns][0].Status,
				"sanity: this path has to leave the task FAILED")

			require.Eventually(t, func() bool { return rec.count() == 1 },
				5*time.Second, 5*time.Millisecond,
				"the apply-time window between FAILED and the teardown is ungated: "+
					"the observer that holds the gate was never raised")

			observed := rec.first()
			require.Equal(t, TaskStatusFailed, observed.Status,
				"the observer must see the task already in its failed state")
			require.Equal(t, taskID, observed.ID)
			require.Equal(t, version, observed.Version)
		})
	}
}

// The companion to TestTerminalDispatchIgnoresProposerClockSkew, one case per
// apply that can fail a task. Each stages a task whose units stopped long ago,
// so task.FinishedAt is far in the past by the time the failure lands: a
// dispatch that judges by any timestamp drops these, silently reopening the
// window it exists to close.
func TestFailedApplyDispatchIgnoresHowOldTheTaskLooks(t *testing.T) {
	const (
		ns      = "ns"
		taskID  = "task1"
		version = uint64(10)
	)

	// Comfortably past any bound an age-based implementation would plausibly
	// pick, and a literal so it cannot follow one.
	const unitsStoppedAgo = 2 * time.Minute

	tests := []struct {
		name string
		// stage takes a task to the state the failing apply needs, leaving
		// task.FinishedAt at the moment the units stopped.
		stage func(t *testing.T, h *testHarness)
		// fail applies the failure, stamped with the current clock.
		fail func(t *testing.T, h *testHarness)
	}{
		{
			name: "a swap that outlasts the bound",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u-node-1", 0.1)
				completeUnit(t, h, ns, taskID, version, "node-1", "u-node-1")
			},
			fail: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic swap failure",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
		{
			name: "a prep barrier that outlasts the bound",
			stage: func(t *testing.T, h *testHarness) {
				addBarrierTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				drivePreparing(t, h, ns, taskID, version, []string{"node-1"})
			},
			fail: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic prep failure",
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
		{
			name: "a cutover the scheduler gives up on after the bound",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u-node-1", 0.1)
				completeUnit(t, h, ns, taskID, version, "node-1", "u-node-1")
			},
			fail: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          ns,
					Id:                 taskID,
					Version:            version,
					Error:              "synthetic cutover failure",
					FailedAtUnixMillis: h.clock.Now().UnixMilli(),
				}), false))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			var rec observerRecorder
			h.manager.RegisterTerminalObserver(ns, rec.record)

			tc.stage(t, h)

			tasksBefore, err := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, err)
			finishedAt := tasksBefore[ns][0].FinishedAt
			require.False(t, finishedAt.IsZero(),
				"sanity: this case only says anything while FinishedAt is set and lags the failure")

			h.clock.Advance(unitsStoppedAgo)

			require.Zero(t, rec.count(), "sanity: staging must not have fired the observer")

			tc.fail(t, h)

			require.Equal(t, finishedAt, func() time.Time {
				tasks, err := h.manager.ListDistributedTasks(context.Background())
				require.NoError(t, err)
				return tasks[ns][0].FinishedAt
			}(), "sanity: the failing apply must leave FinishedAt at the units-stopped moment")

			require.Eventually(t, func() bool { return rec.count() == 1 },
				5*time.Second, 5*time.Millisecond,
				"the dispatch was decided by how old the task looks, so a phase "+
					"slower than that bound was read as a replayed RAFT entry and "+
					"its terminal dispatch dropped")
		})
	}
}

// wakeCounter stands in for the Scheduler as the Manager's notifier. Wake runs
// under the Manager's write lock, so counting has to be safe from there.
type wakeCounter struct{ n atomic.Int64 }

func (w *wakeCounter) Wake() { w.n.Add(1) }

// Pins: dispatch must not read timestamps to decide replay-vs-live. They are
// stamped by the proposing node, so clock skew would make a live ending look
// replayed and skip the observer. Only the FSM's own local replay flag may
// suppress a dispatch.
func TestTerminalDispatchIgnoresProposerClockSkew(t *testing.T) {
	tests := []struct {
		name string
		// proposerSkew is how far the proposing node's clock sits from this
		// node's: negative means the stamps arrive looking old.
		proposerSkew time.Duration
		catchingUp   bool
		wantDispatch bool
	}{
		{name: "clocks in step", wantDispatch: true},
		{name: "proposer 90s behind", proposerSkew: -90 * time.Second, wantDispatch: true},
		{name: "proposer 90s ahead", proposerSkew: 90 * time.Second, wantDispatch: true},
		{name: "replayed from the RAFT log", catchingUp: true},
		{name: "replayed, proposer 90s behind", proposerSkew: -90 * time.Second, catchingUp: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			var woke wakeCounter
			h.manager.SetSchedulerNotifier(&woke)

			var rec observerRecorder
			h.manager.RegisterTerminalObserver(observerNamespace, rec.record)

			require.NoError(t, h.manager.AddTask(observerAddCmd(t, h), observerVersion))
			require.NoError(t, h.manager.CancelTask(
				observerCancelCmd(t, h, -tc.proposerSkew), tc.catchingUp))

			if tc.wantDispatch {
				require.Eventually(t, func() bool { return rec.count() == 1 },
					5*time.Second, 5*time.Millisecond,
					"a live ending must reach the observer whatever the proposer's clock says; "+
						"skipping it leaves the window between the apply and the teardown ungated")
			} else {
				require.Never(t, func() bool { return rec.count() > 0 },
					200*time.Millisecond, 5*time.Millisecond,
					"an ending the FSM flagged as replayed must not reach the observer")
			}

			// Skipping the dispatch may only cost the side effect. The state the
			// scheduler converges from, and the wake-up that makes it look, are
			// what bound the degradation to one tick.
			tasks, err := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, err)
			require.Len(t, tasks[observerNamespace], 1)
			require.Equal(t, TaskStatusCancelled, tasks[observerNamespace][0].Status,
				"the cancel must land in FSM state whether or not the observer ran")
			require.Positive(t, woke.n.Load(),
				"the scheduler must still be woken, or a skipped dispatch would wait for the next tick")
		})
	}
}
