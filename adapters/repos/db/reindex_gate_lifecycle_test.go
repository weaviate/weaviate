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

package db

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Every site that closes the cleanup gate must also reopen it. Stuck-closed is
// the safe direction, so nothing fails loudly when a pairing is missed: the
// cluster just stops accepting backups. These tests are the only thing that
// makes such a leak visible.

func lifecycleProvider(t *testing.T, serverCtx context.Context) *ReindexProvider {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	return &ReindexProvider{
		cleanupInProgress:     make(map[reindexCleanupKey]int),
		submitInProgress:      make(map[reindexCleanupKey]int),
		cancelSeen:            make(map[string]int),
		cancelApplyGates:      make(map[distributedtask.TaskDescriptor]func()),
		cancelTeardownSettled: make(map[distributedtask.TaskDescriptor]time.Time),
		runningHandles:        map[distributedtask.TaskDescriptor]*reindexTaskHandle{},
		serverCtx:             serverCtx,
		logger:                logger,
		// No local index, so the teardown sweeps nothing and the test is about
		// the gate lifecycle around it rather than the sweep itself.
		db: &DB{},
	}
}

func lifecycleTask(t *testing.T, id string) (*distributedtask.Task, *ReindexTaskPayload) {
	t.Helper()
	payload := &ReindexTaskPayload{
		Collection:    "Movies",
		Properties:    []string{"body"},
		MigrationType: ReindexTypeChangeTokenization,
		UnitToShard:   map[string]string{"u1": "shard1"},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        raw,
	}, payload
}

// gatesIdle reports the invariant every lifecycle must return to: no registry
// entry left behind on any of the four raise sites.
func gatesIdle(p *ReindexProvider) bool {
	p.cleanupInProgressMu.RLock()
	holds := len(p.cleanupInProgress) + len(p.submitInProgress)
	p.cleanupInProgressMu.RUnlock()

	p.cancelAppliedMu.RLock()
	parked := len(p.cancelApplyGates)
	p.cancelAppliedMu.RUnlock()

	return holds == 0 && parked == 0
}

// The cancel-apply gate must be adopted and released by the teardown, promptly.
// A fixed hold instead would refuse backups long after the teardown finished.
func TestCancelApplyGateIsReleasedByTheTeardown(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	task, payload := lifecycleTask(t, "task-lifecycle-1")
	logger, _ := logrustest.NewNullLogger()

	p.OnCancelApplied(task)
	require.True(t, p.IsCleanupInProgress("Movies", "shard1"),
		"the apply must close the gate over the gap before the teardown starts")
	require.Equal(t, ReindexHoldCleanup, p.HoldForShard("Movies", "shard1"))

	p.autoCleanupAfterTerminal(task, payload, logger)

	require.False(t, p.IsCleanupInProgress("Movies", "shard1"),
		"the teardown must reopen the gate as soon as it finishes, not on a timer")
	require.True(t, gatesIdle(p), "no gate may be left parked or held")
}

// A task with nothing to tear down returns before doing any work; the apply
// gate must not survive it.
func TestCancelApplyGateIsReleasedWhenThereIsNothingToTearDown(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	task, payload := lifecycleTask(t, "task-lifecycle-2")
	payload.Properties = nil
	logger, _ := logrustest.NewNullLogger()

	p.OnCancelApplied(task)
	require.True(t, p.IsCleanupInProgress("Movies", "shard1"))

	p.autoCleanupAfterTerminal(task, payload, logger)

	require.True(t, gatesIdle(p),
		"an early-returning teardown must still hand the apply gate back")
}

// Confirmation and blocking are separate signals: the probe stays positive for
// its window while the backup gate reopens with the teardown.
func TestCancelConfirmationOutlivesTheBlockingGate(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	task, payload := lifecycleTask(t, "task-lifecycle-3")
	logger, _ := logrustest.NewNullLogger()

	p.OnCancelApplied(task)
	p.autoCleanupAfterTerminal(task, payload, logger)

	require.False(t, p.IsCleanupInProgress("Movies", "shard1"),
		"backups must be admitted again once the teardown is done")
	require.True(t, p.AnyCleanupInProgressForCollection("Movies"),
		"the cancel must still be confirmable to the node handling it")
}

// The submission sweep is the fourth raise site. It must be distinguishable
// from a teardown, or an ordinary submission is reported as a cancelled
// migration, and it must reopen.
func TestSubmitHoldIsDistinctAndReleases(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)

	release := p.MarkSubmitInProgress("Movies")
	require.Equal(t, ReindexHoldSubmit, p.HoldForShard("Movies", "shard1"),
		"a submission sweep must not be reported as a cancelled migration")
	require.True(t, p.AnyCleanupInProgress(), "the restore gate must see it too")

	release()
	require.Equal(t, ReindexHoldNone, p.HoldForShard("Movies", "shard1"))
	require.True(t, gatesIdle(p))
}

// N raises must produce N releases on every site. An unbalanced pair leaks
// silently in production, so it has to fail here.
func TestGateRefcountsBalanceOverManyCycles(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	logger, _ := logrustest.NewNullLogger()

	const cycles = 50
	for i := range cycles {
		task, payload := lifecycleTask(t, "task-soak")
		task.Version = uint64(i + 1)

		p.OnCancelApplied(task)
		p.autoCleanupAfterTerminal(task, payload, logger)

		releaseSubmit := p.MarkSubmitInProgress("Movies")
		releaseDrain, err := p.DrainWithCleanupGate(serverCtx, payload, task.TaskDescriptor)
		require.NoError(t, err)
		releaseDrain()
		releaseSubmit()

		require.Truef(t, gatesIdle(p), "cycle %d left a gate held", i)
	}

	require.Eventually(t, func() bool {
		p.cancelAppliedMu.RLock()
		defer p.cancelAppliedMu.RUnlock()
		return len(p.cancelSeen) == 0
	}, reindexCancelConfirmWindow+5*time.Second, 50*time.Millisecond,
		"the confirmation window must expire rather than accumulate")
}

// The apply and the teardown race, and the teardown can win: the scheduler reads
// task state from the leader while the apply is local, so on a follower the
// teardown runs first and finds nothing parked. The gate must still reopen at
// once — waiting for the cap means a minute of refused backups cluster-wide.
func TestCancelApplyGateReleasesWhenTheTeardownRanFirst(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	task, payload := lifecycleTask(t, "task-teardown-first")
	logger, _ := logrustest.NewNullLogger()

	// Teardown first, apply second — the follower ordering.
	p.autoCleanupAfterTerminal(task, payload, logger)
	p.OnCancelApplied(task)

	require.False(t, p.IsCleanupInProgress("Movies", "shard1"),
		"an apply that lost the race to its own teardown has no gap left to cover, "+
			"so it must not hold the gate until the cap")
	require.True(t, gatesIdle(p), "no gate may be left parked or held")
}

// The restore gate must track the teardown, not the confirmation window laid on
// top of it. Reading the latch instead refuses restores for its full fixed
// duration — long after the files are gone — while telling the caller they are
// still being removed.
func TestBlockingHoldClearsWithTheTeardownNotTheConfirmationWindow(t *testing.T) {
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := lifecycleProvider(t, serverCtx)
	task, payload := lifecycleTask(t, "task-restore-gate")
	logger, _ := logrustest.NewNullLogger()

	p.OnCancelApplied(task)
	require.True(t, p.BlockingHoldForCollection("Movies"),
		"the teardown is pending, so a restore must be refused")

	p.autoCleanupAfterTerminal(task, payload, logger)

	require.False(t, p.BlockingHoldForCollection("Movies"),
		"the teardown is done, so the restore gate must open with it and not wait out the confirmation window")
	require.True(t, p.AnyCleanupInProgressForCollection("Movies"),
		"the confirmation window is unaffected; it is what the cancel handler polls")
}
