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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// A terminal task with no finish time has no age, and both TTL predicates
// measure age from FinishedAt — on a zero stamp that is roughly two thousand
// years, which clears every TTL. Deleting the task on that arithmetic
// would also hide it from the backup overlap backstop
// (adapters/repos/db/reindex_activity_lookup.go), which refuses a capture on
// exactly this state. Both sites therefore keep the task.
//
// The state itself is only reachable across server versions: a node old enough
// to end a task without stamping it, applying against state a newer node
// produced. [Manager.Restore] repairs the stamp, but it runs on restore only,
// so a node that has not restarted keeps the zero — including a leader, whose
// list is the one both guards read. They stay live for a whole rolling upgrade
// for that reason. The tests below seed the map directly rather than through
// Restore, which would repair the state under test.

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

	h := newTestHarness(t).init(t)
	defer h.manager.Close()

	seedTerminalTaskWithoutAStamp(t, h.manager, ns, taskID, version)

	// The apply runs on every node from one log entry, so a decision that moved
	// with the local clock would delete the task on one node and keep it on
	// another. The guard returns before the clock is read, so the same request
	// a century apart refuses identically.
	refuse := func(when string) {
		err := h.manager.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{
			Namespace: ns, Id: taskID, Version: version,
		}))
		require.ErrorContains(t, err, "carries no finish time", when)

		tasks, listErr := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, listErr)
		require.Len(t, tasks[ns], 1,
			"%s: the task must survive so the backup overlap backstop can still refuse on it", when)
	}

	refuse("at the moment of restore")
	h.clock.Advance(100 * 365 * 24 * time.Hour)
	refuse("a century later")
}

// The positive control — a stamped task past its TTL is still deleted, so this
// guard is not simply disabling cleanup — lives in the table in
// cleanup_determinism_test.go.

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

// The warn the sweep emits for those tasks sits on the path they take on every
// tick, forever — nothing cleans them up. On the shared scheduler budget that
// silences the four other sites, one of which reports a task that failed to
// start.
func TestScheduler_Sweep_ZeroStampWarnKeepsToItsOwnBudget(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	h := newTestHarness(t)
	h.logger = logger
	h.init(t)
	defer h.Close()

	// More unstamped tasks than the shared budget holds, so a warn drawing on
	// it would exhaust the budget within one tick.
	for i := 0; i < 6; i++ {
		seedTerminalTaskWithoutAStamp(t, h.manager, h.tasksNamespace, fmt.Sprintf("unstamped-%d", i), 7)
	}

	h.startScheduler(t)
	for i := 0; i < 3; i++ {
		h.advanceClock(h.completedTaskTTL)
	}

	var warns int
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "carry no finish time") {
			warns++
		}
	}
	require.Equal(t, 1, warns,
		"the state does not change between ticks, so it must be reported once, not once per "+
			"task per tick")

	// The four sites that share sampledLogger must still have their full
	// budget: this is what a warn on the shared budget takes away.
	hook.Reset()
	for i := 0; i < 5; i++ {
		h.scheduler.sampledLogger.WithSampling(func(l logrus.FieldLogger) {
			l.Error("failed to start distributed task")
		})
	}
	require.Len(t, hook.AllEntries(), 5,
		"the zero-stamp warn spent the budget the scheduler's error sites share")
}
