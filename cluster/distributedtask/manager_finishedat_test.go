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

const (
	finishedAtNS      = "ns"
	finishedAtTaskID  = "task1"
	finishedAtVersion = uint64(10)
)

// taskRef names the task under test so the apply helpers below don't repeat
// namespace/id/version at every call site. Each helper applies exactly one
// RAFT command at a caller-chosen timestamp and returns the FSM's answer, so
// a caller can pin the refusal as readily as the success.
type taskRef struct {
	ns      string
	id      string
	version uint64
}

func finishedAtRef() taskRef {
	return taskRef{ns: finishedAtNS, id: finishedAtTaskID, version: finishedAtVersion}
}

func (r taskRef) finalize(t *testing.T, h *testHarness, at time.Time) error {
	t.Helper()
	return h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
		Namespace: r.ns, Id: r.id, Version: r.version,
		FinalizedAtUnixMillis: at.UnixMilli(),
	}))
}

func (r taskRef) markFailed(t *testing.T, h *testHarness, at time.Time) error {
	t.Helper()
	return h.manager.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
		Namespace: r.ns, Id: r.id, Version: r.version, Error: "synthetic cutover failure",
		FailedAtUnixMillis: at.UnixMilli(),
	}), false)
}

func (r taskRef) cancel(t *testing.T, h *testHarness, at time.Time) error {
	t.Helper()
	return h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace: r.ns, Id: r.id, Version: r.version,
		CancelledAtUnixMillis: at.UnixMilli(),
	}), false)
}

func (r taskRef) prepAck(t *testing.T, h *testHarness, node string, success bool, at time.Time) error {
	t.Helper()
	return h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace: r.ns, Id: r.id, Version: r.version, NodeId: node,
		Success: success, Error: prepAckError(success),
		AckedAtUnixMillis: at.UnixMilli(),
	}), false)
}

func (r taskRef) swapAck(t *testing.T, h *testHarness, node string, success bool, at time.Time) error {
	t.Helper()
	return h.manager.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
		Namespace: r.ns, Id: r.id, Version: r.version, NodeId: node,
		Success: success, Error: swapAckError(success),
		AckedAtUnixMillis: at.UnixMilli(),
	}), false)
}

func (r taskRef) failUnitAt(t *testing.T, h *testHarness, node, unitID string, at time.Time) error {
	t.Helper()
	return h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace: r.ns, Id: r.id, Version: r.version, NodeId: node, UnitId: unitID,
		Error:                "synthetic unit failure",
		FinishedAtUnixMillis: at.UnixMilli(),
	}), false)
}

func prepAckError(success bool) string {
	if success {
		return ""
	}
	return "synthetic prep failure"
}

func swapAckError(success bool) string {
	if success {
		return ""
	}
	return "synthetic swap failure"
}

// terminalApply is one RAFT command that can end the task, applied at a
// caller-chosen timestamp. Named values rather than inline closures so the
// tables below read as a matrix of (how it ends) x (what happens next).
type terminalApply func(t *testing.T, h *testHarness, at time.Time) error

func applyFinalize(t *testing.T, h *testHarness, at time.Time) error {
	return finishedAtRef().finalize(t, h, at)
}

func applyMarkFailed(t *testing.T, h *testHarness, at time.Time) error {
	return finishedAtRef().markFailed(t, h, at)
}

func applyCancel(t *testing.T, h *testHarness, at time.Time) error {
	return finishedAtRef().cancel(t, h, at)
}

func applyFailUnit(t *testing.T, h *testHarness, at time.Time) error {
	return finishedAtRef().failUnitAt(t, h, "node-1", "u", at)
}

func applyFailingSwapAckFrom(node string) terminalApply {
	return func(t *testing.T, h *testHarness, at time.Time) error {
		return finishedAtRef().swapAck(t, h, node, false, at)
	}
}

func applyFailingPrepAckFrom(node string) terminalApply {
	return func(t *testing.T, h *testHarness, at time.Time) error {
		return finishedAtRef().prepAck(t, h, node, false, at)
	}
}

// Task.FinishedAt is stamped exactly once, at the transition into a terminal
// status, from the timestamp on the RAFT request that caused it — never from
// the applying node's clock, which would let two nodes compute different state
// from the same log entry. weaviate/0-weaviate-issues#501.
//
// One case per way a task can end. Each drives the task to the edge of the
// transition, moves the clock so a stamp taken any earlier is distinguishable,
// then applies the terminal request.
func TestManager_FinishedAt_StampedAtTheTerminalTransition(t *testing.T) {
	tests := []struct {
		name string
		// stage leaves the task one apply short of terminal.
		stage func(t *testing.T, h *testHarness)
		// terminate applies that last one, stamped at terminateAt.
		terminate      terminalApply
		expectedStatus TaskStatus
	}{
		{"unit failure", stageStartedWithClaimedUnit, applyFailUnit, TaskStatusFailed},
		{"prep-ack failure", stagePreparing, applyFailingPrepAckFrom("node-1"), TaskStatusFailed},
		{"swap-ack failure", stageSwapping, applyFailingSwapAckFrom("node-1"), TaskStatusFailed},
		{"cutover failure", stageSwapping, applyMarkFailed, TaskStatusFailed},
		{"finalize", stageSwapping, applyFinalize, TaskStatusFinished},
		{"cancel", stageStartedWithClaimedUnit, applyCancel, TaskStatusCancelled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			tc.stage(t, h)

			staged := onlyTask(t, h, finishedAtNS)
			require.False(t, staged.Status.IsTerminal(), "sanity: stage must stop short of terminal")
			require.True(t, staged.FinishedAt.IsZero(),
				"a task that has not ended carries no end time")

			// Far enough apart that a stamp from any earlier moment in the
			// lifecycle, or from the applying node's own clock, is visible.
			h.clock.Advance(2 * time.Minute)
			terminateAt := h.clock.Now().Add(time.Hour)

			require.NoError(t, tc.terminate(t, h, terminateAt))

			ended := onlyTask(t, h, finishedAtNS)
			require.Equal(t, tc.expectedStatus, ended.Status)
			require.Equal(t, terminateAt.UnixMilli(), ended.FinishedAt.UnixMilli(),
				"FinishedAt must come off the request, not the applying node's clock")
		})
	}
}

// The other half of "stamped exactly once": a second terminal command against
// an already-ended task must leave the stamp where it is, whether the FSM
// treats it as an idempotent no-op or refuses it outright. RAFT redelivers
// entries and multiple nodes propose terminal transitions concurrently, so
// duplicates are routine. A node that restamped on the second one would
// disagree with a node restored from a snapshot taken between the two.
func TestManager_FinishedAt_NotRestampedByASecondTerminalApply(t *testing.T) {
	tests := []struct {
		name     string
		stage    func(t *testing.T, h *testHarness)
		first    terminalApply
		second   terminalApply
		wantErr  bool
		endState TaskStatus
	}{
		{"finalize twice", stageSwapping, applyFinalize, applyFinalize, false, TaskStatusFinished},
		{"mark failed twice", stageSwapping, applyMarkFailed, applyMarkFailed, false, TaskStatusFailed},
		{"cancel twice", stageStartedWithClaimedUnit, applyCancel, applyCancel, true, TaskStatusCancelled},
		{
			"finalize after a peer already failed the task",
			stageSwapping, applyMarkFailed, applyFinalize, true, TaskStatusFailed,
		},
		{
			"mark failed after a peer already finalized the task",
			stageSwapping, applyFinalize, applyMarkFailed, true, TaskStatusFinished,
		},
		{
			"a second node's failing swap-ack after the first already failed the task",
			stageSwapping,
			applyFailingSwapAckFrom("node-1"), applyFailingSwapAckFrom("node-2"),
			false, TaskStatusFailed,
		},
		{
			"a second node's failing prep-ack after the first already failed the task",
			stagePreparing,
			applyFailingPrepAckFrom("node-1"), applyFailingPrepAckFrom("node-2"),
			false, TaskStatusFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			tc.stage(t, h)

			endedAt := h.clock.Now().Add(time.Hour)
			require.NoError(t, tc.first(t, h, endedAt))
			require.Equal(t, endedAt.UnixMilli(), onlyTask(t, h, finishedAtNS).FinishedAt.UnixMilli())

			// A full day later, so a restamp cannot be mistaken for rounding.
			err := tc.second(t, h, endedAt.Add(24*time.Hour))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			ended := onlyTask(t, h, finishedAtNS)
			require.Equal(t, tc.endState, ended.Status)
			require.Equal(t, endedAt.UnixMilli(), ended.FinishedAt.UnixMilli(),
				"the second terminal apply moved the stamp")
		})
	}
}

// The units stopping is not the task ending: FinishedAt stays zero across
// STARTED → PREPARING → SWAPPING, however long the coordination phases run.
func TestManager_FinishedAt_ZeroUntilTerminal(t *testing.T) {
	ref := finishedAtRef()

	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	addBarrierTaskWithUnits(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"u-node-1"})
	require.Equal(t, TaskStatusStarted, onlyTask(t, h, finishedAtNS).Status)
	require.True(t, onlyTask(t, h, finishedAtNS).FinishedAt.IsZero())

	drivePreparing(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"node-1"})
	require.True(t, onlyTask(t, h, finishedAtNS).FinishedAt.IsZero(),
		"the units stopped, the task did not end")

	h.clock.Advance(10 * time.Minute)
	require.NoError(t, ref.prepAck(t, h, "node-1", true, h.clock.Now()))
	require.Equal(t, TaskStatusSwapping, onlyTask(t, h, finishedAtNS).Status)
	require.True(t, onlyTask(t, h, finishedAtNS).FinishedAt.IsZero(),
		"a swap in flight is not a finished task")
}

// stageStartedWithClaimedUnit leaves the task STARTED with one unit claimed.
func stageStartedWithClaimedUnit(t *testing.T, h *testHarness) {
	t.Helper()
	addTaskWithUnits(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"u"})
	updateProgress(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, "node-1", "u", 0.1)
}

// stageSwapping leaves the task in SWAPPING, its units done and the bucket
// swap outstanding.
func stageSwapping(t *testing.T, h *testHarness) {
	t.Helper()
	addTaskWithUnits(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"u"})
	completeUnit(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, "node-1", "u")
}

// stagePreparing leaves a barrier task in PREPARING, waiting on prep acks.
func stagePreparing(t *testing.T, h *testHarness) {
	t.Helper()
	addBarrierTaskWithUnits(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"u-node-1"})
	drivePreparing(t, h, finishedAtNS, finishedAtTaskID, finishedAtVersion, []string{"node-1"})
}

func onlyTask(t *testing.T, h *testHarness, ns string) *Task {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks[ns], 1)
	return tasks[ns][0]
}
