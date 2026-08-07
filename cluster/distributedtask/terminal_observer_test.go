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
			require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
				Namespace:            ns,
				Id:                   taskID,
				Version:              version,
				NodeId:               n,
				UnitId:               "u-" + n,
				FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
			})))
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
				require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
					Namespace:            ns,
					Id:                   taskID,
					Version:              version,
					NodeId:               "node-1",
					UnitId:               "u-node-1",
					Error:                "synthetic unit failure",
					FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
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
				})))
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
				})))
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
				})))
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

// The staleness bound is measured against when the failure happened, and on the
// paths below that is not task.FinishedAt — that field deliberately stays at the
// moment the units stopped. A phase outlasting the bound would otherwise look
// like a replayed RAFT entry and be dropped, silently reopening the window this
// dispatch exists to close.
//
// One case per apply that dispatches with a timestamp of its own. Each stages a
// task whose units stopped long enough ago to be stale, then fails it now: an
// apply reading task.FinishedAt instead drops the dispatch here.
func TestFailedApplyMeasuresStalenessFromTheFailureNotTheUnitsStopping(t *testing.T) {
	const (
		ns      = "ns"
		taskID  = "task1"
		version = uint64(10)
	)

	// Well past terminalObserverStaleAfter, and a literal so that widening the
	// constant does not quietly turn these cases green.
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
				require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
					Namespace:            ns,
					Id:                   taskID,
					Version:              version,
					NodeId:               "node-1",
					UnitId:               "u-node-1",
					FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
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
				})))
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
				})))
			},
		},
		{
			name: "a cutover the scheduler gives up on after the bound",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u-node-1", 0.1)
				require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
					Namespace:            ns,
					Id:                   taskID,
					Version:              version,
					NodeId:               "node-1",
					UnitId:               "u-node-1",
					FinishedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			},
			fail: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          ns,
					Id:                 taskID,
					Version:            version,
					Error:              "synthetic cutover failure",
					FailedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
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
				"the failure was aged against task.FinishedAt rather than its own "+
					"timestamp, so a phase slower than the staleness bound was read "+
					"as a replayed RAFT entry and its terminal dispatch dropped")
		})
	}
}
