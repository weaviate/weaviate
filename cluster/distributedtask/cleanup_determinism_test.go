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

// CleanUpTask decides a replicated command's outcome from what the proposing
// sweep measured — the moment it looked and the TTL it looked against — plus the
// task's own finish time. Both operands travel with the log entry or live in the
// FSM, so every node reaches the same answer.
//
// Reading the applying node's wall clock instead would fork two nodes on whether
// the entry deletes, and since GET /v1/schema/{class}/indexes reads locally the
// fork is visible to the caller.
//
// The tests below pin that the apply uses the proposer's numbers in both
// directions, that it still refuses on the task's own state, and that a request
// carrying no measurements keeps the local age check.

// seedTerminalTaskStampedAt installs a single FINISHED task carrying the given
// finish time straight into the task map.
func seedTerminalTaskStampedAt(t *testing.T, m *Manager, namespace, taskID string, version uint64, finishedAt time.Time) {
	t.Helper()
	seedTerminalTaskWithoutAStamp(t, m, namespace, taskID, version)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[namespace][taskID].FinishedAt = finishedAt
}

func TestManager_CleanUpTask_DecidesFromTheProposersMeasurements(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	for _, tc := range []struct {
		name string
		// stampAge is how far in the past the task's finish time sits,
		// measured from this node's clock at the moment the command applies.
		stampAge time.Duration
		// proposerElapsed is the age the proposer measured (its measuring
		// moment minus the task's finish time) and proposerTTL is the TTL it
		// measured against. A zero proposerTTL means the request carries no
		// measurements at all.
		proposerElapsed time.Duration
		proposerTTL     time.Duration
		wantKept        bool
	}{
		{
			name:            "a task this node reads as fresh deletes when the proposer's TTL is shorter",
			stampAge:        time.Minute,
			proposerElapsed: time.Minute,
			proposerTTL:     time.Second,
			wantKept:        false,
		},
		{
			name:            "a task this node reads as long expired is kept when the proposer's TTL is longer",
			stampAge:        100 * 365 * 24 * time.Hour,
			proposerElapsed: 100 * 365 * 24 * time.Hour,
			proposerTTL:     200 * 365 * 24 * time.Hour,
			wantKept:        true,
		},
		{
			name:            "the proposer's moment decides, not this node's: a proposer that looked before the TTL ran out keeps the task",
			stampAge:        48 * time.Hour,
			proposerElapsed: time.Hour,
			proposerTTL:     24 * time.Hour,
			wantKept:        true,
		},
		{
			name:            "elapsed exactly equal to the TTL deletes",
			stampAge:        24 * time.Hour,
			proposerElapsed: 24 * time.Hour,
			proposerTTL:     24 * time.Hour,
			wantKept:        false,
		},
		{
			name:     "no measurements on the request fall back to the local age check, which refuses a fresh task",
			stampAge: time.Minute,
			wantKept: true,
		},
		{
			name:     "no measurements on the request fall back to the local age check, which deletes an expired task",
			stampAge: 100 * 365 * 24 * time.Hour,
			wantKept: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			// FinishedAt is millisecond-aligned in production (the FSM stamps
			// it from a *_unix_millis field), so align the fixture the same
			// way — the wire carries the proposer's moment in milliseconds too.
			finishedAt := h.clock.Now().Add(-tc.stampAge).Truncate(time.Millisecond)
			seedTerminalTaskStampedAt(t, h.manager, ns, taskID, version, finishedAt)

			req := &cmd.CleanUpDistributedTaskRequest{Namespace: ns, Id: taskID, Version: version}
			if tc.proposerTTL != 0 {
				req.ProposedAtUnixMillis = finishedAt.Add(tc.proposerElapsed).UnixMilli()
				req.TtlMillis = tc.proposerTTL.Milliseconds()
			}
			err := h.manager.CleanUpTask(toCmd(t, req))

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

// The determinism property itself: the same log entry applied to two nodes whose
// wall clocks are more than a TTL apart still leaves both maps in the same state.
func TestManager_CleanUpTask_DivergentClocksReachTheSameState(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	behind := newTestHarness(t).init(t)
	defer behind.manager.Close()
	ahead := newTestHarness(t).init(t)
	defer ahead.manager.Close()
	ahead.clock.Advance(3 * ahead.completedTaskTTL)

	// One replicated finish time, two nodes that disagree about what time it is
	// by more than the TTL. Reading the local clock, one would call the task
	// fresh and the other long expired.
	finishedAt := behind.clock.Now().Truncate(time.Millisecond)
	seedTerminalTaskStampedAt(t, behind.manager, ns, taskID, version, finishedAt)
	seedTerminalTaskStampedAt(t, ahead.manager, ns, taskID, version, finishedAt)

	entry := &cmd.CleanUpDistributedTaskRequest{
		Namespace:            ns,
		Id:                   taskID,
		Version:              version,
		ProposedAtUnixMillis: finishedAt.Add(2 * time.Hour).UnixMilli(),
		TtlMillis:            time.Hour.Milliseconds(),
	}
	require.NoError(t, behind.manager.CleanUpTask(toCmd(t, entry)))
	require.NoError(t, ahead.manager.CleanUpTask(toCmd(t, entry)))

	for name, h := range map[string]*testHarness{"behind": behind, "ahead": ahead} {
		tasks, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		require.Empty(t, tasks[ns],
			"%s node must end in the same state as its peer; one log entry, one outcome", name)
	}
}

// The proposer's measurements do not override the task's own state. Both refusals
// below are properties of the task alone, so they are the same answer on every
// node whatever its clock says.
func TestManager_CleanUpTask_StateInvariantsOutrankTheProposersMeasurements(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		version = uint64(7)
	)

	elapsed := func(h *testHarness, id string) *cmd.CleanUpDistributedTaskRequest {
		return &cmd.CleanUpDistributedTaskRequest{
			Namespace:            ns,
			Id:                   id,
			Version:              version,
			ProposedAtUnixMillis: h.clock.Now().UnixMilli(),
			TtlMillis:            time.Second.Milliseconds(),
		}
	}

	t.Run("a running task is refused", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		seedTerminalTaskWithoutAStamp(t, h.manager, ns, "running", version)
		h.manager.mu.Lock()
		h.manager.tasks[ns]["running"].Status = TaskStatusStarted
		h.manager.mu.Unlock()

		require.ErrorContains(t,
			h.manager.CleanUpTask(toCmd(t, elapsed(h, "running"))), "is still running")
	})

	t.Run("a terminal task without a finish time is refused", func(t *testing.T) {
		h := newTestHarness(t).init(t)
		defer h.manager.Close()

		seedTerminalTaskWithoutAStamp(t, h.manager, ns, "unstamped", version)

		require.ErrorContains(t,
			h.manager.CleanUpTask(toCmd(t, elapsed(h, "unstamped"))), "carries no finish time")
	})
}
