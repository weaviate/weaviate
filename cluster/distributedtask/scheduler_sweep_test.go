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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// The sweep's half of the cleanup contract. It selects tasks by comparing the
// moment it looked against each task's finish time, and it has to send exactly
// the three numbers it compared: the apply redoes the comparison and reads
// nothing of its own, so a request built from anything else reaches a verdict
// the sweep never made.

type sweepProposal struct {
	taskID     string
	finishedAt time.Time
	proposedAt time.Time
	ttl        time.Duration
}

// recordSweepProposals accepts any number of cleanup proposals, records them,
// and reports them back in call order. Nothing is forwarded to the Manager, so
// the tasks stay in the list and the sweep is free to look again.
func recordSweepProposals(h *testHarness, onCall func()) func() []sweepProposal {
	var (
		mu   sync.Mutex
		seen []sweepProposal
	)
	h.cleaner.EXPECT().CleanUpDistributedTask(mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _, taskID string, _ uint64, finishedAt, proposedAt time.Time, ttl time.Duration) error {
			mu.Lock()
			seen = append(seen, sweepProposal{taskID: taskID, finishedAt: finishedAt, proposedAt: proposedAt, ttl: ttl})
			mu.Unlock()
			if onCall != nil {
				onCall()
			}
			return nil
		}).Maybe()

	return func() []sweepProposal {
		mu.Lock()
		defer mu.Unlock()
		return append([]sweepProposal(nil), seen...)
	}
}

// A task whose age is exactly the TTL is swept. The apply's identical boundary
// is pinned in cleanup_determinism_test.go; the two have to agree, or the sweep
// proposes an entry the apply then refuses.
func TestScheduler_Sweep_ProposesAtExactlyTheTTL(t *testing.T) {
	h := newTestHarness(t)
	h.init(t)
	defer h.Close()

	// Two ages in one sweep: one exactly at the boundary, one well past it. The
	// older task is what stops a request from carrying a finish time derived
	// from the swept moment and the TTL rather than read off the task.
	stamps := map[string]time.Time{
		"exactly-at-the-ttl": h.clock.Now(),
		"long-expired":       h.clock.Now().Add(-3 * time.Hour),
	}
	for id, finishedAt := range stamps {
		seedTerminalTaskStampedAt(t, h.manager, h.tasksNamespace, id, 7, finishedAt)
	}
	proposals := recordSweepProposals(h, nil)

	h.startScheduler(t)
	h.advanceClock(h.completedTaskTTL)

	// Nothing forwards the proposals to the Manager, so the tasks stay in the
	// list and later ticks propose them again.
	seen := proposals()
	proposed := map[string]sweepProposal{}
	for _, p := range seen {
		proposed[p.taskID] = p
	}
	require.Contains(t, proposed, "exactly-at-the-ttl",
		"a task whose age equals the TTL exactly must be swept")
	require.Len(t, proposed, len(stamps))

	for id, want := range stamps {
		got := proposed[id]
		require.Equal(t, h.completedTaskTTL, got.ttl, id)
		require.Truef(t, want.Equal(got.finishedAt),
			"%s: the request must carry the task's own finish time, not one derived from the "+
				"swept moment: want %s, got %s", id, want, got.finishedAt)
		require.LessOrEqual(t, got.ttl, got.proposedAt.Sub(got.finishedAt),
			"%s: the numbers sent must reproduce the comparison that selected the task", id)
	}
}

// One sweep, one measuring moment. The sweep filters on a single clock read and
// sends that same value with every task it selected, so a slow batch cannot have
// its later entries decided against a later moment than the one that chose them.
func TestScheduler_Sweep_SendsOneMomentForTheWholeBatch(t *testing.T) {
	h := newTestHarness(t)
	h.init(t)
	defer h.Close()

	finishedAt := h.clock.Now()
	for _, id := range []string{"first", "second"} {
		seedTerminalTaskStampedAt(t, h.manager, h.tasksNamespace, id, 7, finishedAt)
	}

	// Time passes between the two proposals of one batch, which is what makes
	// a per-task clock read distinguishable from the sweep's single one.
	var once sync.Once
	proposals := recordSweepProposals(h, func() {
		once.Do(func() { h.clock.Advance(time.Hour) })
	})

	h.startScheduler(t)
	h.advanceClock(2 * h.completedTaskTTL)

	seen := proposals()
	require.GreaterOrEqual(t, len(seen), 2)
	require.True(t, seen[0].proposedAt.Equal(seen[1].proposedAt),
		"both entries of one sweep must carry the moment that selected them: got %s and %s",
		seen[0].proposedAt, seen[1].proposedAt)
}
