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

// TestManager_FinishedAtIsStampedAtTheTerminalTransition covers three of the
// four FSM paths that reach a terminal status out of a coordination phase: a
// failing PREP ack out of PREPARING, a failing SWAP ack out of SWAPPING, and
// finalize out of SWAPPING. The fake clock advances between the last in-flight
// step and the terminal one, so a stamp taken when the units stopped is
// distinguishable from a stamp taken when the task actually finished.
//
// Those phases run per-shard prep and two cluster-wide ack barriers, the
// second of which has no timeout — so a stamp set on entry to them can be
// served for an unbounded time while the task is still running. The fourth
// exit, SWAPPING → FAILED via MarkTaskFailed, is pinned by
// TestManager_MarkTaskFailed. Two more terminal paths leave STARTED directly:
// a failing unit (TestTaskFailureInAnotherNode, TestTaskFailureInLocalNode)
// and cancel (TestTaskCancellation,
// TestManager_CancelTask_AcceptsOnlyTheCancellableStatuses).
func TestManager_FinishedAtIsStampedAtTheTerminalTransition(t *testing.T) {
	tests := []struct {
		name string
		// from is the last non-terminal state on this path.
		from TaskStatus
		// terminal performs the single transition into a terminal status.
		terminal   func(t *testing.T, h *testHarness, ns, id string, version uint64)
		wantStatus TaskStatus
	}{
		{
			name: "a failing PREP ack fails the task from PREPARING",
			from: TaskStatusPreparing,
			terminal: func(t *testing.T, h *testHarness, ns, id string, version uint64) {
				ackPrep(t, h, ns, id, version, "n1", false)
			},
			wantStatus: TaskStatusFailed,
		},
		{
			name: "a failing SWAP ack fails the task from SWAPPING",
			from: TaskStatusSwapping,
			terminal: func(t *testing.T, h *testHarness, ns, id string, version uint64) {
				ackSwap(t, h, ns, id, version, "n1", false)
			},
			wantStatus: TaskStatusFailed,
		},
		{
			name: "finalize from SWAPPING",
			from: TaskStatusSwapping,
			terminal: func(t *testing.T, h *testHarness, ns, id string, version uint64) {
				require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
					Namespace:             ns,
					Id:                    id,
					Version:               version,
					FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			},
			wantStatus: TaskStatusFinished,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			ns, id, version := fixtureInStatus(t, h, tt.from)

			inFlight := h.manager.tasks[ns][id]
			require.False(t, inFlight.Status.IsTerminal(),
				"fixture must stop short of a terminal status, got %q", inFlight.Status)
			require.True(t, inFlight.FinishedAt.IsZero(),
				"a task in %q must not carry a finish time", inFlight.Status)

			// An hour, so the retention probe below has half an hour of
			// margin on each side of the TTL boundary.
			h.clock.Advance(time.Hour)
			wantFinishedAt := h.clock.Now()
			tt.terminal(t, h, ns, id, version)

			got := h.manager.tasks[ns][id]
			require.Equal(t, tt.wantStatus, got.Status)
			require.Equal(t, wantFinishedAt.UnixMilli(), got.FinishedAt.UnixMilli(),
				"FinishedAt must be the terminal moment, not an earlier phase change")

			// Retention counts from the stamp, so half an hour short of a
			// full TTL past the terminal transition the record is still
			// readable — even though it is half an hour past a TTL measured
			// from the moment the units stopped.
			h.clock.Advance(h.completedTaskTTL - 30*time.Minute)
			require.ErrorContains(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: ns,
				Id:        id,
				Version:   version,
			})), "too fresh")
		})
	}
}

// TestManager_Restore_ClearsFinishedAtOnNonTerminalTasks covers a snapshot
// written by an earlier build of this version line, which stamped FinishedAt
// when the units stopped. Such a snapshot can hold a PREPARING or SWAPPING
// task with a stamp already set; restoring it verbatim would serve a finish
// time for a task that is still running.
func TestManager_Restore_ClearsFinishedAtOnNonTerminalTasks(t *testing.T) {
	const (
		ns      = "ns"
		id      = "task1"
		version = uint64(10)
	)
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
				ns: {{
					Namespace:      ns,
					TaskDescriptor: TaskDescriptor{ID: id, Version: version},
					Status:         status,
					StartedAt:      startedAt,
					FinishedAt:     stamped,
				}},
			}})
			require.NoError(t, err)
			require.NoError(t, h.manager.Restore(bytes))

			got := h.manager.tasks[ns][id]
			require.Equal(t, status, got.Status, "Restore must not change the status")
			require.Equal(t, status.IsTerminal(), !got.FinishedAt.IsZero(),
				"a restored task in %q carries a finish time iff it is terminal", status)
		})
	}
}
