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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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

// stampMismatchWarns returns the warnings the node logged about holding a
// different finish time than the proposer. It is the only operator-facing signal
// that a rolling upgrade is producing divergent stamps, so the fixture pins both
// that it fires on the node that differs and that it stays silent on the node
// that agrees.
func stampMismatchWarns(h *testHarness) []string {
	var out []string
	for _, entry := range h.logHook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "but the cleanup proposer read") {
			out = append(out, entry.Message)
		}
	}
	return out
}

func taskCount(t *testing.T, h *testHarness) int {
	t.Helper()
	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	return len(tasks[divergenceNamespace])
}

func TestManager_CleanUpTask_RestoredAndReplayedNodesReachTheSameState(t *testing.T) {
	// Anchor the fixture to the same clock the nodes get, so the two stamps
	// straddle the TTL boundary measured from the applying node's own clock as
	// well as from the proposer's numbers. That is what makes the arm below
	// carrying no measurements decisive.
	reference := newTestHarness(t)
	ttl := reference.completedTaskTTL
	base := reference.clock.Now().Add(-ttl - 2*time.Second).Truncate(time.Millisecond)
	var (
		startedAt      = base.Add(-time.Hour)
		unitsStoppedAt = base
		// The SWAPPING window: the finalize lands after the units stopped, so
		// the two nodes' stamps sit this far apart.
		finalizedAt = base.Add(5 * time.Second)
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
		// noMeasurements sends the entry an old proposer writes: no finish
		// time, no moment, no TTL.
		noMeasurements bool
		wantRemaining  int
		// warningNode is the node whose own stamp differs from the proposer's,
		// and so the only one that must log the mismatch warning.
		warningNode string
	}{
		{
			name:          "the restored node proposes, and both nodes delete",
			proposerStamp: unitsStoppedAt,
			wantRemaining: 0,
			warningNode:   "replayed",
		},
		{
			name:          "the replayed node proposes, and both nodes keep",
			proposerStamp: finalizedAt,
			wantRemaining: 1,
			warningNode:   "restored",
		},
		{
			// A proposer too old to send measurements. There is nothing on the
			// entry to decide from, and the two nodes' own stamps sit either
			// side of the TTL boundary, so an apply that fell back to its own
			// age check would delete on the restored node and refuse on the
			// replayed one. Both defer instead.
			name:           "a proposer too old to send measurements leaves both nodes holding the task",
			noMeasurements: true,
			wantRemaining:  1,
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

			req := &cmd.CleanUpDistributedTaskRequest{
				Namespace: divergenceNamespace,
				Id:        divergenceTaskID,
				Version:   divergenceVersion,
			}
			if !tc.noMeasurements {
				req.FinishedAtUnixMillis = tc.proposerStamp.UnixMilli()
				req.ProposedAtUnixMillis = proposedAt.UnixMilli()
				req.TtlMillis = ttl.Milliseconds()
			}
			entry := toCmd(t, req)
			restoredErr := restored.manager.CleanUpTask(entry)
			replayedErr := replayed.manager.CleanUpTask(entry)

			require.Equal(t, taskCount(t, restored), taskCount(t, replayed),
				"one entry, one outcome: the two nodes must hold the same number of tasks")
			require.Equal(t, tc.wantRemaining, taskCount(t, restored), "restored node")
			require.Equal(t, tc.wantRemaining, taskCount(t, replayed), "replayed node")
			require.Equal(t, restoredErr == nil, replayedErr == nil,
				"one log entry, one outcome: restored=%v replayed=%v", restoredErr, replayedErr)

			for node, h := range map[string]*testHarness{"restored": restored, "replayed": replayed} {
				warns := stampMismatchWarns(h)
				if node == tc.warningNode {
					require.Len(t, warns, 1,
						"the %s node holds a different stamp than the proposer and must say so", node)
					require.Contains(t, warns[0], divergenceTaskID)
					continue
				}
				require.Empty(t, warns,
					"the %s node agrees with the proposer's stamp and must stay silent", node)
			}
		})
	}
}

// The mismatch warn fires once per applied entry, and the state that produces
// it is a whole backlog: a rolling upgrade hands this node every task whose
// stamp differs from the proposer's. The sampler is the only thing between that
// and one log line per task, so pin the budget it holds to.
func TestManager_CleanUpTask_StampMismatchWarnKeepsToItsBudget(t *testing.T) {
	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	// A fixed count, not one derived from the budget: deriving it would make
	// the assertion hold for any budget, including one large enough to let the
	// whole backlog through.
	const entries = 20
	require.Less(t, stampMismatchWarnBudget, entries, "the burst must be larger than the budget")

	var (
		ttl        = h.completedTaskTTL
		finishedAt = h.clock.Now().Add(-2 * ttl).Truncate(time.Millisecond)
	)

	for i := 0; i < entries; i++ {
		id := fmt.Sprintf("mismatched-%d", i)
		seedTerminalTaskStampedAt(t, h.manager, divergenceNamespace, id, divergenceVersion, finishedAt)
		require.NoError(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
			Namespace: divergenceNamespace,
			Id:        id,
			Version:   divergenceVersion,
			// A second off this node's copy, which is what a node that
			// restored a pre-stamp snapshot holds.
			FinishedAtUnixMillis: finishedAt.Add(time.Second).UnixMilli(),
			ProposedAtUnixMillis: h.clock.Now().UnixMilli(),
			TtlMillis:            ttl.Milliseconds(),
		})))
	}

	require.Len(t, stampMismatchWarns(h), stampMismatchWarnBudget,
		"%d mismatched entries must produce exactly the budget's worth of warnings", entries)

	// The budget spent above must be this sampler's own. The scheduler's sites
	// share a different one and must still have all of theirs.
	h.logHook.Reset()
	for i := 0; i < 5; i++ {
		h.scheduler.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
			l.Error("failed to start distributed task")
		})
	}
	require.Len(t, h.logHook.AllEntries(), 5,
		"the mismatch warn must not draw on the budget the scheduler's error sites share")
}
