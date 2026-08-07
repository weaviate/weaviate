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

// CleanUpTask used to decide a replicated command's outcome from
// m.clock.Since(task.FinishedAt) — the applying node's wall clock against the
// applying node's stored stamp. Two nodes that disagree on either fork on
// whether the entry deletes, and since GET /v1/schema/{class}/indexes reads
// locally the fork is visible to the caller. During a rolling upgrade the
// stamps differ by the whole prep-plus-swap window on every task, so the fork
// needs no clock failure at all.
//
// The decision now travels on the request. The tests below pin that the apply
// obeys it, that it still refuses on the task's own state, and that a request
// from a binary that sends no decision keeps the old local check.

// seedTerminalTaskStampedAt installs a single FINISHED task carrying the given
// finish time straight into the task map.
func seedTerminalTaskStampedAt(t *testing.T, m *Manager, namespace, taskID string, version uint64, finishedAt time.Time) {
	t.Helper()
	seedTerminalTaskWithoutAStamp(t, m, namespace, taskID, version)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[namespace][taskID].FinishedAt = finishedAt
}

func TestManager_CleanUpTask_ObeysTheProposersTTLVerdict(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	for _, tc := range []struct {
		name string
		// stampAge is how far in the past the task's finish time sits on this
		// node, measured at the moment the command applies.
		stampAge   time.Duration
		ttlElapsed bool
		wantKept   bool
	}{
		{
			name:       "a fresh stamp still deletes when the proposer says the TTL elapsed",
			stampAge:   time.Minute,
			ttlElapsed: true,
			wantKept:   false,
		},
		{
			name:       "an expired stamp deletes on the proposer's verdict too",
			stampAge:   100 * 365 * 24 * time.Hour,
			ttlElapsed: true,
			wantKept:   false,
		},
		{
			name:       "no verdict on the request falls back to the local age check, which refuses a fresh task",
			stampAge:   time.Minute,
			ttlElapsed: false,
			wantKept:   true,
		},
		{
			name:       "no verdict on the request falls back to the local age check, which deletes an expired task",
			stampAge:   100 * 365 * 24 * time.Hour,
			ttlElapsed: false,
			wantKept:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			seedTerminalTaskStampedAt(t, h.manager, ns, taskID, version, h.clock.Now().Add(-tc.stampAge))

			err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: ns, Id: taskID, Version: version, TtlElapsed: tc.ttlElapsed,
			}))

			tasks, listErr := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, listErr)
			if tc.wantKept {
				require.ErrorContains(t, err, "too fresh to clean up")
				require.Len(t, tasks[ns], 1)
				return
			}
			require.NoError(t, err)
			require.Empty(t, tasks[ns])
		})
	}
}

// The determinism property itself: the same log entry applied to two nodes
// that disagree about the task's finish time by more than a TTL still leaves
// both maps in the same state.
func TestManager_CleanUpTask_DivergentStampsReachTheSameState(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	running := newTestHarness(t).init(t)
	defer running.manager.Close()
	restored := newTestHarness(t).init(t)
	defer restored.manager.Close()

	// The rolling-upgrade shape: the node that restored from a snapshot
	// repaired the stamp forward by the prep-plus-swap window, so its copy of
	// the task looks far younger than the other node's.
	seedTerminalTaskStampedAt(t, running.manager, ns, taskID, version,
		running.clock.Now().Add(-2*running.completedTaskTTL))
	seedTerminalTaskStampedAt(t, restored.manager, ns, taskID, version,
		restored.clock.Now())

	entry := &cmd.CleanUpDistributedTaskRequest{
		Namespace: ns, Id: taskID, Version: version, TtlElapsed: true,
	}
	require.NoError(t, running.manager.CleanUpTask(toCmd(t, entry)))
	require.NoError(t, restored.manager.CleanUpTask(toCmd(t, entry)))

	for name, h := range map[string]*testHarness{"running": running, "restored": restored} {
		tasks, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		require.Empty(t, tasks[ns],
			"%s node must end in the same state as its peer; one log entry, one outcome", name)
	}
}

// The proposer's verdict does not override the task's own state. Both refusals
// below are properties of the task alone, so they are the same answer on every
// node whatever its clock says.
func TestManager_CleanUpTask_StateInvariantsOutrankTheProposersVerdict(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		version = uint64(7)
	)

	t.Run("a running task is refused", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		seedTerminalTaskWithoutAStamp(t, h.manager, ns, "running", version)
		h.manager.mu.Lock()
		h.manager.tasks[ns]["running"].Status = TaskStatusStarted
		h.manager.mu.Unlock()

		require.ErrorContains(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
			Namespace: ns, Id: "running", Version: version, TtlElapsed: true,
		})), "is still running")
	})

	t.Run("a terminal task without a finish time is refused", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		seedTerminalTaskWithoutAStamp(t, h.manager, ns, "unstamped", version)

		require.ErrorContains(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
			Namespace: ns, Id: "unstamped", Version: version, TtlElapsed: true,
		})), "carries no finish time")
	})
}
