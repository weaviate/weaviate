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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

const (
	finishedAtNS      = "ns"
	finishedAtID      = "task1"
	finishedAtVersion = uint64(10)
)

// ackPrep records one node's PREP-phase ack through the FSM.
func ackPrep(t *testing.T, h *testHarness, node string, success bool) {
	t.Helper()
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         finishedAtNS,
		Id:                finishedAtID,
		Version:           finishedAtVersion,
		NodeId:            node,
		Success:           success,
		Error:             errIfFailed(success),
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
}

// ackSwap records one node's SWAP-phase ack through the FSM.
func ackSwap(t *testing.T, h *testHarness, node string, success bool) {
	t.Helper()
	require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace:         finishedAtNS,
		Id:                finishedAtID,
		Version:           finishedAtVersion,
		NodeId:            node,
		Success:           success,
		Error:             errIfFailed(success),
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	})))
}

func errIfFailed(success bool) string {
	if success {
		return ""
	}
	return "boom"
}

// TestManager_FinishedAtIsStampedAtTheTerminalTransition walks every FSM path
// that reaches a terminal status. The fake clock advances between the last
// in-flight step and the terminal one, so a stamp taken when the units stopped
// is distinguishable from a stamp taken when the task actually finished.
//
// The coordination phases this covers (PREPARING, SWAPPING) run per-shard prep
// and two cluster-wide ack barriers, the second of which has no timeout — so a
// stamp set on entry to them can be served for an unbounded time while the
// task is still running.
func TestManager_FinishedAtIsStampedAtTheTerminalTransition(t *testing.T) {
	// driveToSwapping walks a barrier task STARTED → PREPARING → SWAPPING.
	driveToSwapping := func(t *testing.T, h *testHarness) {
		addBarrierTaskWithUnits(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"u-n1"})
		drivePreparing(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"n1"})
		require.True(t, h.manager.tasks[finishedAtNS][finishedAtID].FinishedAt.IsZero(),
			"a PREPARING task must not carry a finish time")
		h.clock.Advance(time.Minute)
		ackPrep(t, h, "n1", true)
	}

	tests := []struct {
		name string
		// inFlight drives the task to the last non-terminal state on this path.
		inFlight func(t *testing.T, h *testHarness)
		// terminal performs the single transition into a terminal status.
		terminal   func(t *testing.T, h *testHarness)
		wantStatus TaskStatus
	}{
		{
			name: "a failing unit fails the task",
			inFlight: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"u-n1", "u-n2"})
				completeUnit(t, h, finishedAtNS, finishedAtID, finishedAtVersion, "n1", "u-n1")
			},
			terminal: func(t *testing.T, h *testHarness) {
				failUnit(t, h, finishedAtNS, finishedAtID, finishedAtVersion, "n2", "u-n2", "boom")
			},
			wantStatus: TaskStatusFailed,
		},
		{
			name: "cancel from STARTED",
			inFlight: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"u-n1"})
			},
			terminal: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
					Namespace:             finishedAtNS,
					Id:                    finishedAtID,
					Version:               finishedAtVersion,
					CancelledAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			},
			wantStatus: TaskStatusCancelled,
		},
		{
			name: "a failing PREP ack fails the task from PREPARING",
			inFlight: func(t *testing.T, h *testHarness) {
				addBarrierTaskWithUnits(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"u-n1"})
				drivePreparing(t, h, finishedAtNS, finishedAtID, finishedAtVersion, []string{"n1"})
			},
			terminal: func(t *testing.T, h *testHarness) {
				ackPrep(t, h, "n1", false)
			},
			wantStatus: TaskStatusFailed,
		},
		{
			name:     "a failing SWAP ack fails the task from SWAPPING",
			inFlight: driveToSwapping,
			terminal: func(t *testing.T, h *testHarness) {
				ackSwap(t, h, "n1", false)
			},
			wantStatus: TaskStatusFailed,
		},
		{
			name: "finalize from SWAPPING",
			inFlight: func(t *testing.T, h *testHarness) {
				driveToSwapping(t, h)
				ackSwap(t, h, "n1", true)
			},
			terminal: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
					Namespace:             finishedAtNS,
					Id:                    finishedAtID,
					Version:               finishedAtVersion,
					FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			},
			wantStatus: TaskStatusFinished,
		},
		{
			name:     "a swallowed cutover failure fails the task from SWAPPING",
			inFlight: driveToSwapping,
			terminal: func(t *testing.T, h *testHarness) {
				require.NoError(t, h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          finishedAtNS,
					Id:                 finishedAtID,
					Version:            finishedAtVersion,
					Error:              "flip failed",
					FailedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			},
			wantStatus: TaskStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			tt.inFlight(t, h)

			inFlight := h.manager.tasks[finishedAtNS][finishedAtID]
			require.False(t, inFlight.Status.IsTerminal(),
				"fixture must stop short of a terminal status, got %q", inFlight.Status)
			require.True(t, inFlight.FinishedAt.IsZero(),
				"a task in %q must not carry a finish time", inFlight.Status)

			h.clock.Advance(time.Minute)
			wantFinishedAt := h.clock.Now()
			tt.terminal(t, h)

			got := h.manager.tasks[finishedAtNS][finishedAtID]
			require.Equal(t, tt.wantStatus, got.Status)
			require.Equal(t, wantFinishedAt.UnixMilli(), got.FinishedAt.UnixMilli(),
				"FinishedAt must be the terminal moment, not an earlier phase change")
		})
	}
}

// TestManager_Restore_ClearsFinishedAtOnNonTerminalTasks covers a snapshot
// written by an earlier build of this version line, which stamped FinishedAt
// when the units stopped. Such a snapshot can hold a PREPARING or SWAPPING
// task with a stamp already set; restoring it verbatim would serve a finish
// time for a task that is still running.
func TestManager_Restore_ClearsFinishedAtOnNonTerminalTasks(t *testing.T) {
	var (
		startedAt = time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
		stamped   = startedAt.Add(time.Hour)
	)

	for _, status := range []TaskStatus{
		TaskStatusStarted, TaskStatusPreparing, TaskStatusSwapping,
		TaskStatusFinished, TaskStatusFailed, TaskStatusCancelled,
		unknownFutureStatus,
	} {
		t.Run(string(status), func(t *testing.T) {
			h := newTestHarness(t).init(t)

			bytes, err := json.Marshal(&snapshot{Tasks: map[string][]*Task{
				finishedAtNS: {{
					Namespace:      finishedAtNS,
					TaskDescriptor: TaskDescriptor{ID: finishedAtID, Version: finishedAtVersion},
					Status:         status,
					StartedAt:      startedAt,
					FinishedAt:     stamped,
				}},
			}})
			require.NoError(t, err)
			require.NoError(t, h.manager.Restore(bytes))

			got := h.manager.tasks[finishedAtNS][finishedAtID]
			require.Equal(t, status, got.Status, "Restore must not change the status")
			require.Equal(t, status.IsTerminal(), !got.FinishedAt.IsZero(),
				"a restored task in %q carries a finish time iff it is terminal", status)
		})
	}
}
