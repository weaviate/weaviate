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

// A terminal task with no finish time has no age, and both TTL predicates
// compute age as clock.Since(FinishedAt) — on a zero stamp that is roughly two
// thousand years, which clears every TTL. Deleting the task on that arithmetic
// would also hide it from the backup overlap backstop
// (adapters/repos/db/reindex_activity_lookup.go), which refuses a capture on
// exactly this state. Both sites therefore keep the task.
//
// The state itself is only reachable across server versions: a node old enough
// to end a task without stamping it, applying against state a newer node
// produced. It reaches this build by RAFT snapshot restore, where
// [Task.repairTerminalStamp] now stamps it — so these two guards are what
// still holds if the state ever reaches the task map unrepaired. The tests
// below therefore seed the map directly rather than through Restore.

// seedTerminalTaskWithoutAStamp installs a single FINISHED task carrying no
// finish time straight into the task map.
func seedTerminalTaskWithoutAStamp(t *testing.T, m *Manager, namespace, taskID string, version uint64) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[namespace]; !ok {
		m.tasks[namespace] = map[string]*Task{}
	}
	m.tasks[namespace][taskID] = &Task{
		Namespace:      namespace,
		TaskDescriptor: TaskDescriptor{ID: taskID, Version: version},
		Status:         TaskStatusFinished,
		StartedAt:      m.clock.Now().Add(-time.Hour),
		Units:          map[string]*Unit{"u": {ID: "u", Status: UnitStatusCompleted, Progress: 1}},
	}
}

func TestManager_CleanUpTask_RefusesATerminalTaskWithoutAFinishTime(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "unstamped"
		version = uint64(7)
	)

	// The same request at wildly different local clocks. The apply runs on
	// every node from one log entry, so a decision that moved with the local
	// clock would delete the task on one node and keep it on another.
	for _, tc := range []struct {
		name    string
		advance time.Duration
	}{
		{"at the moment of restore", 0},
		{"a TTL later", 25 * time.Hour},
		{"a century later", 100 * 365 * 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			seedTerminalTaskWithoutAStamp(t, h.manager, ns, taskID, version)
			h.clock.Advance(tc.advance)

			err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
				Namespace: ns, Id: taskID, Version: version,
			}))
			require.ErrorContains(t, err, "carries no finish time")

			tasks, listErr := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, listErr)
			require.Len(t, tasks[ns], 1,
				"the task must survive so the backup overlap backstop can still refuse on it")
		})
	}
}

// The positive control for the guard above: a terminal task that does carry a
// stamp older than the TTL is still deleted, so the guard is not simply
// disabling cleanup.
func TestManager_CleanUpTask_StillDeletesAStampedExpiredTask(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	seedTerminalTaskWithoutAStamp(t, h.manager, ns, taskID, version)
	h.manager.mu.Lock()
	h.manager.tasks[ns][taskID].FinishedAt = h.clock.Now()
	h.manager.mu.Unlock()

	h.clock.Advance(h.completedTaskTTL + time.Minute)

	require.NoError(t, h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
		Namespace: ns, Id: taskID, Version: version,
	})))

	tasks, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Empty(t, tasks[ns])
}

// The sweep half. MockTaskCleaner fails the test on any call it was not told
// to expect, so "the scheduler never proposes cleanup" is asserted by setting
// no expectation at all.
func TestScheduler_Sweep_KeepsATerminalTaskWithoutAFinishTime(t *testing.T) {
	h := newTestHarness(t)
	h.init(t)
	defer h.Close()

	seedTerminalTaskWithoutAStamp(t, h.manager, h.tasksNamespace, "unstamped", 7)

	h.startScheduler(t)
	// Well past the TTL, and several ticks, so a sweep that read the zero
	// stamp as an age would have proposed cleanup many times over.
	for i := 0; i < 3; i++ {
		h.advanceClock(h.completedTaskTTL)
	}

	tasks := h.listManagerTasks(t)
	require.Len(t, tasks[h.tasksNamespace], 1)
}
