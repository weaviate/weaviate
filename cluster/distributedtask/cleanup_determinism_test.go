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
// sweep measured: the finish time it read, the moment it looked, and the TTL it
// looked against. All three travel with the log entry, so every node reaches the
// same answer without reading state of its own.
//
// Reading anything local instead would fork two nodes on whether the entry
// deletes, and since GET /v1/schema/{class}/indexes reads locally the fork is
// visible to the caller. Two things can differ: the wall clock, and the stored
// finish time itself, on a node that restored a snapshot written before finish
// times were stamped at the finalize.
//
// The tests below pin that the apply uses the proposer's numbers in both
// directions, that two nodes holding different finish times still reach the same
// state, that state invariants still refuse locally, and that a request carrying
// no measurements defers instead of deciding from anything local.

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
			stampAge:        30 * 24 * time.Hour,
			proposerElapsed: 30 * 24 * time.Hour,
			proposerTTL:     60 * 24 * time.Hour,
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
		// The deferral arms. Both stamp ages are on the request-less path, one
		// well inside the harness TTL and one well past it, because the point
		// is that the local age the node would have computed does not enter
		// into it either way.
		{
			name:     "no measurements on the request defer, keeping a task this node reads as fresh",
			stampAge: time.Minute,
			wantKept: true,
		},
		{
			name:     "no measurements on the request defer, keeping a task this node reads as long expired",
			stampAge: 6 * 24 * time.Hour,
			wantKept: true,
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
				req.FinishedAtUnixMillis = finishedAt.UnixMilli()
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
		FinishedAtUnixMillis: finishedAt.UnixMilli(),
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
			FinishedAtUnixMillis: h.clock.Now().Add(-time.Hour).UnixMilli(),
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

// The entry is the only input, so a nonsense operand on it has no local
// substitute. Each of these deletes on arrival if the subtraction is trusted
// blindly, which is the one direction this change is not allowed to fail in:
// the backup overlap backstop reads the list the deletion would empty.
func TestManager_CleanUpTask_DefersAnEntryWhoseMeasurementsAreNonsense(t *testing.T) {
	const (
		ns      = "tasks-namespace"
		taskID  = "stamped"
		version = uint64(7)
	)

	for _, tc := range []struct {
		name string
		// mangle rewrites an otherwise-expired request into the nonsense one
		// under test.
		mangle func(r *cmd.CleanUpDistributedTaskRequest)
	}{
		{
			name: "a zero TTL",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.TtlMillis = 0
			},
		},
		{
			name: "a negative TTL",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.TtlMillis = -1
			},
		},
		{
			// The TTL stays positive here, which is what separates this row
			// from the two above: the zero stamp has to be caught on its own.
			name: "a zero finish time",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.FinishedAtUnixMillis = 0
			},
		},
		{
			name: "a negative finish time",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.FinishedAtUnixMillis = -1
			},
		},
		{
			// What a writer stamping from a zero time.Time sends: far from
			// zero, and far in the past.
			name: "a finish time taken from a zero time.Time",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.FinishedAtUnixMillis = time.Time{}.UnixMilli()
			},
		},
		{
			name: "a zero proposal moment",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.ProposedAtUnixMillis = 0
			},
		},
		{
			name: "a negative proposal moment",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.ProposedAtUnixMillis = -1
			},
		},
		{
			name: "a finish time later than the moment the sweep looked",
			mangle: func(r *cmd.CleanUpDistributedTaskRequest) {
				r.FinishedAtUnixMillis = r.ProposedAtUnixMillis + time.Hour.Milliseconds()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t).init(t)
			defer h.manager.Close()

			finishedAt := h.clock.Now().Add(-30 * 24 * time.Hour).Truncate(time.Millisecond)
			seedTerminalTaskStampedAt(t, h.manager, ns, taskID, version, finishedAt)

			req := &cmd.CleanUpDistributedTaskRequest{
				Namespace:            ns,
				Id:                   taskID,
				Version:              version,
				FinishedAtUnixMillis: finishedAt.UnixMilli(),
				ProposedAtUnixMillis: h.clock.Now().UnixMilli(),
				TtlMillis:            h.completedTaskTTL.Milliseconds(),
			}
			tc.mangle(req)

			require.ErrorContains(t, h.manager.CleanUpTask(toCmd(t, req)), "too fresh to clean up")
			tasks, err := h.manager.ListDistributedTasks(context.Background())
			require.NoError(t, err)
			require.Len(t, tasks[ns], 1, "the task must survive an entry it cannot be decided from")
		})
	}
}
