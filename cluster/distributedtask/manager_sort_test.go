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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

// TestSortTasksForDisplay covers the helper used by
// [Manager.ListDistributedTasks] to keep slice order stable across
// adjacent polls. Sort key, in order:
//
//  1. STARTED tasks first (most user-relevant).
//  2. Within priority, activity-time DESC (newest first). For terminal
//     tasks that's FinishedAt; otherwise StartedAt.
//  3. Tiebreak by ID ASC.
func TestSortTasksForDisplay(t *testing.T) {
	base := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	mk := func(id string, status TaskStatus, started, finished time.Time) *Task {
		return &Task{
			Namespace:      "ns",
			TaskDescriptor: TaskDescriptor{ID: id},
			Status:         status,
			StartedAt:      started,
			FinishedAt:     finished,
		}
	}

	t.Run("STARTED beats terminal regardless of activity time", func(t *testing.T) {
		// Terminal task finished 1h after the STARTED one started, so by
		// recency alone the terminal would win. STARTED must still rank
		// first because operators care about what's currently running.
		started := mk("started-old", TaskStatusStarted, base, time.Time{})
		finished := mk("finished-recent", TaskStatusFinished, base.Add(30*time.Minute), base.Add(time.Hour))

		got := []*Task{finished, started}
		sortTasksForDisplay(got)

		require.Equal(t, []string{"started-old", "finished-recent"}, ids(got))
	})

	t.Run("within-priority recency DESC", func(t *testing.T) {
		// Three terminal tasks at different FinishedAt: most recent first.
		old := mk("a-old", TaskStatusFinished, base, base.Add(time.Minute))
		mid := mk("b-mid", TaskStatusFailed, base, base.Add(2*time.Minute))
		newest := mk("c-newest", TaskStatusCancelled, base, base.Add(3*time.Minute))

		got := []*Task{old, newest, mid}
		sortTasksForDisplay(got)

		require.Equal(t, []string{"c-newest", "b-mid", "a-old"}, ids(got))
	})

	t.Run("STARTED tasks use StartedAt as activity time", func(t *testing.T) {
		// STARTED tasks have a zero FinishedAt; sort must fall back to
		// StartedAt without producing "Jan 1, year 1" garbage.
		earlier := mk("early", TaskStatusStarted, base, time.Time{})
		later := mk("late", TaskStatusStarted, base.Add(time.Minute), time.Time{})

		got := []*Task{earlier, later}
		sortTasksForDisplay(got)

		require.Equal(t, []string{"late", "early"}, ids(got))
	})

	t.Run("ID tiebreak when priority and time are equal", func(t *testing.T) {
		// Two terminal tasks at the exact same FinishedAt — without the
		// tiebreak the order would be input-order-dependent (and Go map
		// iteration is randomized, so input-order is unstable upstream).
		t1 := mk("z-last", TaskStatusFinished, base, base.Add(time.Minute))
		t2 := mk("a-first", TaskStatusFinished, base, base.Add(time.Minute))

		got := []*Task{t1, t2}
		sortTasksForDisplay(got)

		require.Equal(t, []string{"a-first", "z-last"}, ids(got))
	})

	t.Run("randomized input -> identical output", func(t *testing.T) {
		// Strongest assertion: take a representative set, shuffle it 32
		// different ways, and confirm every sort produces the same
		// sequence. This is exactly the property the bug report cared
		// about — the frontend saw adjacent /v1/tasks polls return
		// different orders for the same set of tasks because the
		// underlying map iteration randomizes.
		canonical := []*Task{
			mk("active-1", TaskStatusStarted, base.Add(5*time.Minute), time.Time{}),
			mk("active-2", TaskStatusStarted, base.Add(1*time.Minute), time.Time{}),
			mk("done-c", TaskStatusFinished, base, base.Add(10*time.Minute)),
			mk("done-b", TaskStatusFailed, base, base.Add(8*time.Minute)),
			mk("done-a", TaskStatusCancelled, base, base.Add(8*time.Minute)),
		}
		sortTasksForDisplay(canonical)
		expected := ids(canonical)

		rng := rand.New(rand.NewSource(42))
		for trial := 0; trial < 32; trial++ {
			shuffled := make([]*Task, len(canonical))
			copy(shuffled, canonical)
			rng.Shuffle(len(shuffled), func(i, j int) {
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			})
			sortTasksForDisplay(shuffled)
			require.Equal(t, expected, ids(shuffled), "trial %d", trial)
		}
	})

	t.Run("empty and single-task slices are no-ops", func(t *testing.T) {
		var empty []*Task
		sortTasksForDisplay(empty)
		require.Empty(t, empty)

		single := []*Task{mk("only", TaskStatusStarted, base, time.Time{})}
		sortTasksForDisplay(single)
		require.Equal(t, []string{"only"}, ids(single))
	})
}

// TestManager_ListDistributedTasks_OrderIsStable is the end-to-end
// assertion against the actual Manager: after ingesting a representative
// mix of tasks, repeated ListDistributedTasks calls must return slices
// in byte-identical order. Without the sort, Go's randomized map
// iteration would produce different orders across calls — exactly the
// "adjacent polls return chaos" symptom reported on issue #10675.
func TestManager_ListDistributedTasks_OrderIsStable(t *testing.T) {
	h := newTestHarness(t).init(t)
	now := h.clock.Now().Truncate(time.Millisecond)

	// Three STARTED tasks plus two terminal ones, mixed insertion order.
	// All in the same namespace to force the sort path to run.
	for i, payload := range []*cmd.AddDistributedTaskRequest{
		{Namespace: "ns", Id: "started-c", SubmittedAtUnixMillis: now.UnixMilli(), UnitIds: []string{"u-1"}},
		{Namespace: "ns", Id: "started-a", SubmittedAtUnixMillis: now.Add(2 * time.Minute).UnixMilli(), UnitIds: []string{"u-1"}},
		{Namespace: "ns", Id: "started-b", SubmittedAtUnixMillis: now.Add(time.Minute).UnixMilli(), UnitIds: []string{"u-1"}},
	} {
		require.NoError(t, h.manager.AddTask(toCmd(t, payload), uint64(10+i)))
	}

	// Cancel two of them so we have terminal tasks too.
	require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "started-b",
		Version:               12,
		CancelledAtUnixMillis: now.Add(3 * time.Minute).UnixMilli(),
	})))
	require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
		Namespace:             "ns",
		Id:                    "started-c",
		Version:               10,
		CancelledAtUnixMillis: now.Add(4 * time.Minute).UnixMilli(),
	})))

	// Repeat the listing 50 times — Go map iteration randomizes per call,
	// so without the sort one of these would almost certainly diverge.
	first, err := h.manager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	firstIDs := ids(first["ns"])
	require.Len(t, firstIDs, 3)
	for i := 0; i < 50; i++ {
		next, err := h.manager.ListDistributedTasks(context.Background())
		require.NoError(t, err)
		assert.Equal(t, firstIDs, ids(next["ns"]), "list call %d returned different order", i)
	}

	// And confirm the order is sensible: the lone remaining STARTED
	// (started-a) is first, then the two terminals by FinishedAt DESC.
	require.Equal(t, []string{"started-a", "started-c", "started-b"}, firstIDs)
}

func ids(tasks []*Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.ID
	}
	return out
}
