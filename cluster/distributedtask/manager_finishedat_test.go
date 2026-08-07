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

// Task.FinishedAt is stamped exactly once, at the transition into a terminal
// status, from the timestamp on the RAFT request that caused it — never from
// the applying node's clock, which would let two nodes compute different state
// from the same log entry. weaviate/0-weaviate-issues#501.
//
// One case per way a task can end. Each drives the task to the edge of the
// transition, moves the clock so a stamp taken any earlier is distinguishable,
// then applies the terminal request.
func TestManager_FinishedAt_StampedAtTheTerminalTransition(t *testing.T) {
	const (
		ns      = "ns"
		taskID  = "task1"
		version = uint64(10)
	)

	tests := []struct {
		name string
		// stage leaves the task one apply short of terminal.
		stage func(t *testing.T, h *testHarness)
		// terminate applies that last one, stamped at terminateAt.
		terminate      func(t *testing.T, h *testHarness, terminateAt time.Time)
		expectedStatus TaskStatus
	}{
		{
			name: "unit failure",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u", 0.1)
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
					Namespace:            ns,
					Id:                   taskID,
					Version:              version,
					NodeId:               "node-1",
					UnitId:               "u",
					Error:                "synthetic unit failure",
					FinishedAtUnixMillis: terminateAt.UnixMilli(),
				}), false))
			},
			expectedStatus: TaskStatusFailed,
		},
		{
			name: "prep-ack failure",
			stage: func(t *testing.T, h *testHarness) {
				addBarrierTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
				drivePreparing(t, h, ns, taskID, version, []string{"node-1"})
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic prep failure",
					AckedAtUnixMillis: terminateAt.UnixMilli(),
				}), false))
			},
			expectedStatus: TaskStatusFailed,
		},
		{
			name: "swap-ack failure",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u"})
				completeUnit(t, h, ns, taskID, version, "node-1", "u")
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
					Namespace:         ns,
					Id:                taskID,
					Version:           version,
					NodeId:            "node-1",
					Success:           false,
					Error:             "synthetic swap failure",
					AckedAtUnixMillis: terminateAt.UnixMilli(),
				}), false))
			},
			expectedStatus: TaskStatusFailed,
		},
		{
			name: "cutover failure",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u"})
				completeUnit(t, h, ns, taskID, version, "node-1", "u")
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
					Namespace:          ns,
					Id:                 taskID,
					Version:            version,
					Error:              "synthetic cutover failure",
					FailedAtUnixMillis: terminateAt.UnixMilli(),
				}), false))
			},
			expectedStatus: TaskStatusFailed,
		},
		{
			name: "finalize",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u"})
				completeUnit(t, h, ns, taskID, version, "node-1", "u")
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
					Namespace:             ns,
					Id:                    taskID,
					Version:               version,
					FinalizedAtUnixMillis: terminateAt.UnixMilli(),
				})))
			},
			expectedStatus: TaskStatusFinished,
		},
		{
			name: "cancel",
			stage: func(t *testing.T, h *testHarness) {
				addTaskWithUnits(t, h, ns, taskID, version, []string{"u"})
				updateProgress(t, h, ns, taskID, version, "node-1", "u", 0.1)
			},
			terminate: func(t *testing.T, h *testHarness, terminateAt time.Time) {
				require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
					Namespace:             ns,
					Id:                    taskID,
					Version:               version,
					CancelledAtUnixMillis: terminateAt.UnixMilli(),
				}), false))
			},
			expectedStatus: TaskStatusCancelled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			tc.stage(t, h)

			staged := onlyTask(t, h, ns)
			require.False(t, staged.Status.IsTerminal(), "sanity: stage must stop short of terminal")
			require.True(t, staged.FinishedAt.IsZero(),
				"a task that has not ended carries no end time")

			// Far enough apart that a stamp from any earlier moment in the
			// lifecycle, or from the applying node's own clock, is visible.
			h.clock.Advance(2 * time.Minute)
			terminateAt := h.clock.Now().Add(time.Hour)

			tc.terminate(t, h, terminateAt)

			ended := onlyTask(t, h, ns)
			require.Equal(t, tc.expectedStatus, ended.Status)
			require.Equal(t, terminateAt.UnixMilli(), ended.FinishedAt.UnixMilli(),
				"FinishedAt must come off the request, not the applying node's clock")
		})
	}
}

// The units stopping is not the task ending: FinishedAt stays zero across
// STARTED → PREPARING → SWAPPING, however long the coordination phases run.
func TestManager_FinishedAt_ZeroUntilTerminal(t *testing.T) {
	const (
		ns      = "ns"
		taskID  = "task1"
		version = uint64(10)
	)

	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	addBarrierTaskWithUnits(t, h, ns, taskID, version, []string{"u-node-1"})
	require.Equal(t, TaskStatusStarted, onlyTask(t, h, ns).Status)
	require.True(t, onlyTask(t, h, ns).FinishedAt.IsZero())

	drivePreparing(t, h, ns, taskID, version, []string{"node-1"})
	require.True(t, onlyTask(t, h, ns).FinishedAt.IsZero(),
		"the units stopped, the task did not end")

	h.clock.Advance(10 * time.Minute)
	require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace:         ns,
		Id:                taskID,
		Version:           version,
		NodeId:            "node-1",
		Success:           true,
		AckedAtUnixMillis: h.clock.Now().UnixMilli(),
	}), false))
	require.Equal(t, TaskStatusSwapping, onlyTask(t, h, ns).Status)
	require.True(t, onlyTask(t, h, ns).FinishedAt.IsZero(),
		"a swap in flight is not a finished task")
}

func onlyTask(t *testing.T, h *testHarness, ns string) *Task {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks[ns], 1)
	return tasks[ns][0]
}
