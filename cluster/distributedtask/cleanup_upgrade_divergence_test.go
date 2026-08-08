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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// Two nodes running this binary can hold different finish times for the same
// task, and one replicated cleanup entry still has to leave them in the same
// state.
//
// The two routes to one terminal task:
//
//	restored — the node came up on a snapshot written before finish times were
//	           stamped at the finalize. [Manager.Restore] repairs the stamp to
//	           the newest moment the task records, and no field records the
//	           finalize, so the repaired value is the moment the units stopped.
//	replayed — the node applied the log with this binary, so
//	           [Manager.MarkTaskFinalized] stamped the finalize.
//
// The difference is the SWAPPING window. A proposal whose age lands inside it
// is past the TTL measured from one stamp and inside it measured from the
// other, so an apply that subtracted its own copy would delete the task on one
// node and refuse on the other. Nothing repairs that afterwards: the sweep
// proposes from the leader's list, so once the leader has dropped the task no
// further entry is ever proposed and the peer keeps it forever.

const (
	divergenceNamespace = "tasks-namespace"
	divergenceTaskID    = "pre-upgrade"
	divergenceVersion   = uint64(7)
)

// newRestoredNode builds a node whose only knowledge of the task came from an
// unversioned snapshot, the way a node that restarted onto pre-upgrade state
// sees it. The snapshot carries the stamp an old binary wrote — the moment the
// units stopped — and no field recording the finalize, so the restore repair
// cannot move it forward.
func newRestoredNode(t *testing.T, startedAt, unitsStoppedAt time.Time) *testHarness {
	t.Helper()

	h := newTestHarness(t).init(t)
	snapshot, err := json.Marshal(map[string]any{
		"tasks": map[string][]*Task{
			divergenceNamespace: {{
				Namespace:      divergenceNamespace,
				TaskDescriptor: TaskDescriptor{ID: divergenceTaskID, Version: divergenceVersion},
				Status:         TaskStatusFinished,
				StartedAt:      startedAt,
				FinishedAt:     unitsStoppedAt,
				Units: map[string]*Unit{
					"u": {ID: "u", NodeID: "local-node", Status: UnitStatusCompleted, Progress: 1, FinishedAt: unitsStoppedAt},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, h.manager.Restore(snapshot))
	return h
}

// newReplayedNode builds a node that reached the same terminal task by applying
// the log with this binary, so the finalize moment is what got stamped.
func newReplayedNode(t *testing.T, startedAt, unitsStoppedAt, finalizedAt time.Time) *testHarness {
	t.Helper()

	h := newTestHarness(t).init(t)
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace:             divergenceNamespace,
		Id:                    divergenceTaskID,
		SubmittedAtUnixMillis: startedAt.UnixMilli(),
		UnitIds:               []string{"u"},
	}), divergenceVersion))
	require.NoError(t, h.manager.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace:            divergenceNamespace,
		Id:                   divergenceTaskID,
		Version:              divergenceVersion,
		NodeId:               "local-node",
		UnitId:               "u",
		FinishedAtUnixMillis: unitsStoppedAt.UnixMilli(),
	}), false))
	require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
		Namespace:             divergenceNamespace,
		Id:                    divergenceTaskID,
		Version:               divergenceVersion,
		FinalizedAtUnixMillis: finalizedAt.UnixMilli(),
	})))
	return h
}

// requireStamp asserts the node's stored finish time, comparing instants: the
// FSM rebuilds stamps with time.UnixMilli, which yields the local zone.
func requireStamp(t *testing.T, h *testHarness, want time.Time, node string) {
	t.Helper()
	h.manager.mu.RLock()
	defer h.manager.mu.RUnlock()
	got := h.manager.tasks[divergenceNamespace][divergenceTaskID].FinishedAt
	require.Truef(t, want.Equal(got), "%s: want finish time %s, got %s", node, want, got)
}

func taskCount(t *testing.T, h *testHarness) int {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	return len(tasks[divergenceNamespace])
}

func TestManager_CleanUpTask_RestoredAndReplayedNodesReachTheSameState(t *testing.T) {
	base := time.Date(2026, 8, 3, 22, 23, 17, 0, time.UTC)
	var (
		startedAt      = base.Add(-time.Hour)
		unitsStoppedAt = base
		// The SWAPPING window: the finalize lands after the units stopped, so
		// the two nodes' stamps sit this far apart.
		finalizedAt = base.Add(5 * time.Second)
		ttl         = 24 * time.Hour
		// Chosen to land inside that window: past the TTL measured from the
		// units-stopped stamp, still inside it measured from the finalize.
		proposedAt = unitsStoppedAt.Add(ttl).Add(2 * time.Second)
	)
	require.True(t, proposedAt.Sub(unitsStoppedAt) >= ttl, "fixture must be past the TTL on the restored node's stamp")
	require.True(t, proposedAt.Sub(finalizedAt) < ttl, "fixture must be inside the TTL on the replayed node's stamp")

	for _, tc := range []struct {
		name string
		// proposerStamp is the finish time the proposing node read off its own
		// copy of the task. Either node can be the leader that sweeps.
		proposerStamp time.Time
		wantRemaining int
	}{
		{
			name:          "the restored node proposes, and both nodes delete",
			proposerStamp: unitsStoppedAt,
			wantRemaining: 0,
		},
		{
			name:          "the replayed node proposes, and both nodes keep",
			proposerStamp: finalizedAt,
			wantRemaining: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restored := newRestoredNode(t, startedAt, unitsStoppedAt)
			defer restored.manager.Close()
			replayed := newReplayedNode(t, startedAt, unitsStoppedAt, finalizedAt)
			defer replayed.manager.Close()

			// The premise: the same task, two stamps.
			requireStamp(t, restored, unitsStoppedAt, "restored node")
			requireStamp(t, replayed, finalizedAt, "replayed node")

			entry := toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace:            divergenceNamespace,
				Id:                   divergenceTaskID,
				Version:              divergenceVersion,
				FinishedAtUnixMillis: tc.proposerStamp.UnixMilli(),
				ProposedAtUnixMillis: proposedAt.UnixMilli(),
				TtlMillis:            ttl.Milliseconds(),
			})
			restoredErr := restored.manager.CleanUpTask(entry)
			replayedErr := replayed.manager.CleanUpTask(entry)

			require.Equal(t, tc.wantRemaining, taskCount(t, restored), "restored node")
			require.Equal(t, tc.wantRemaining, taskCount(t, replayed), "replayed node")
			require.Equal(t, restoredErr == nil, replayedErr == nil,
				"one log entry, one outcome: restored=%v replayed=%v", restoredErr, replayedErr)
		})
	}
}
