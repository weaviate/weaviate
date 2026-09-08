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

// TestManager_FinishedAtIsStampedAtTheTerminalTransition pins that FinishedAt
// is set on the terminal transition, not when units complete. Covers three
// of four coordination-phase exits; the fourth is TestManager_MarkTaskFailed.
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

			// Retention counts from the stamp above; a stamp taken when the
			// units stopped instead would already read as expired here.
			h.clock.Advance(h.completedTaskTTL - 30*time.Minute)
			require.ErrorContains(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: ns,
				Id:        id,
				Version:   version,
			})), "too fresh")
		})
	}
}

// TestManager_Restore_ClearsFinishedAtOnNonTerminalTasks pins that Restore
// clears FinishedAt from any non-terminal task in an older snapshot.
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

// TestManager_UnitUpdatedAtIncludesTheTerminalTransition pins that a unit's
// updatedAt covers the transition that ended it. The API omits a zero
// timestamp, so a unit that fails before its first progress report would
// otherwise come back with a finishedAt and no updatedAt at all.
func TestManager_UnitUpdatedAtIncludesTheTerminalTransition(t *testing.T) {
	const (
		ns      = "ns"
		id      = "task1"
		version = uint64(10)
		unitID  = "u-n1"
		node    = "n1"
	)

	tests := []struct {
		name string
		// reportsProgress claims the unit with a progress report before it
		// goes terminal, which is the only writer of updatedAt today.
		reportsProgress bool
		terminal        func(t *testing.T, h *testHarness)
		wantStatus      UnitStatus
	}{
		{
			name:       "failed, never reported progress",
			terminal:   func(t *testing.T, h *testHarness) { failUnit(t, h, ns, id, version, node, unitID, "boom") },
			wantStatus: UnitStatusFailed,
		},
		{
			name:       "completed, never reported progress",
			terminal:   func(t *testing.T, h *testHarness) { completeUnit(t, h, ns, id, version, node, unitID) },
			wantStatus: UnitStatusCompleted,
		},
		{
			name:            "failed after reporting progress",
			reportsProgress: true,
			terminal:        func(t *testing.T, h *testHarness) { failUnit(t, h, ns, id, version, node, unitID, "boom") },
			wantStatus:      UnitStatusFailed,
		},
		{
			name:            "completed after reporting progress",
			reportsProgress: true,
			terminal:        func(t *testing.T, h *testHarness) { completeUnit(t, h, ns, id, version, node, unitID) },
			wantStatus:      UnitStatusCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			addTaskWithUnits(t, h, ns, id, version, []string{unitID})

			if tt.reportsProgress {
				updateProgress(t, h, ns, id, version, node, unitID, 0.5)
			}

			before := h.manager.tasks[ns][id].Units[unitID]
			require.Equal(t, tt.reportsProgress, !before.UpdatedAt.IsZero(),
				"before going terminal a unit carries an update time iff it reported progress")

			h.clock.Advance(time.Hour)
			wantUpdatedAt := h.clock.Now()
			tt.terminal(t, h)

			got := h.manager.tasks[ns][id].Units[unitID]
			require.Equal(t, tt.wantStatus, got.Status)
			require.False(t, got.UpdatedAt.IsZero(),
				"a terminal unit must report when it was last touched")
			require.Equal(t, wantUpdatedAt.UnixMilli(), got.UpdatedAt.UnixMilli(),
				"the terminal transition is the last touch, so it must move updatedAt")
			require.Equal(t, wantUpdatedAt.UnixMilli(), got.FinishedAt.UnixMilli())
		})
	}
}
