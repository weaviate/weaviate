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
	"container/heap"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica"
)

// newSchedulerForUnitTest returns a scheduler suitable for direct internal-method
// testing. Start() is intentionally not called; goroutines are not launched.
func newSchedulerForUnitTest(t *testing.T) *AsyncReplicationScheduler {
	t.Helper()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(1),
		AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(time.Hour),
		AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)
	return sched
}

// TestAsyncSchedulerHeap verifies the min-heap invariants:
//   - entries are popped in ascending nextRunAt order
//   - heapIdx is correct after Push, Pop, Fix, and Remove
func TestAsyncSchedulerHeap(t *testing.T) {
	t.Run("MinHeapOrdering", func(t *testing.T) {
		now := time.Now()
		e1 := &asyncSchedulerEntry{nextRunAt: now.Add(3 * time.Second)}
		e2 := &asyncSchedulerEntry{nextRunAt: now.Add(1 * time.Second)}
		e3 := &asyncSchedulerEntry{nextRunAt: now.Add(2 * time.Second)}

		h := make(asyncSchedulerHeap, 0)
		heap.Push(&h, e1)
		heap.Push(&h, e2)
		heap.Push(&h, e3)

		require.Equal(t, 3, h.Len())
		assert.Equal(t, e2, heap.Pop(&h), "smallest nextRunAt first")
		assert.Equal(t, e3, heap.Pop(&h))
		assert.Equal(t, e1, heap.Pop(&h), "largest nextRunAt last")
	})

	t.Run("HeapIdxAfterPush", func(t *testing.T) {
		h := make(asyncSchedulerHeap, 0)
		for i := range 5 {
			heap.Push(&h, &asyncSchedulerEntry{nextRunAt: time.Now().Add(time.Duration(5-i) * time.Second)})
		}
		for i, e := range h {
			assert.Equal(t, i, e.heapIdx, "heapIdx must equal slice position after push (pos %d)", i)
		}
	})

	t.Run("HeapIdxAfterPop", func(t *testing.T) {
		h := make(asyncSchedulerHeap, 0)
		for i := range 5 {
			heap.Push(&h, &asyncSchedulerEntry{nextRunAt: time.Now().Add(time.Duration(i) * time.Second)})
		}
		heap.Pop(&h)
		for i, e := range h {
			assert.Equal(t, i, e.heapIdx, "heapIdx must equal slice position after pop (pos %d)", i)
		}
	})

	t.Run("FixMovesEntryToTop", func(t *testing.T) {
		now := time.Now()
		e1 := &asyncSchedulerEntry{nextRunAt: now.Add(10 * time.Second)}
		e2 := &asyncSchedulerEntry{nextRunAt: now.Add(20 * time.Second)}

		h := make(asyncSchedulerHeap, 0)
		heap.Push(&h, e1)
		heap.Push(&h, e2)

		// Move e2 into the past → it should become the new minimum.
		e2.nextRunAt = now.Add(-1 * time.Second)
		heap.Fix(&h, e2.heapIdx)

		assert.Equal(t, e2, heap.Pop(&h), "after Fix, e2 (past) must be at top")
	})

	t.Run("HeapIdxAfterRemoveFromMiddle", func(t *testing.T) {
		now := time.Now()
		entries := make([]*asyncSchedulerEntry, 5)
		h := make(asyncSchedulerHeap, 0)
		for i := range 5 {
			entries[i] = &asyncSchedulerEntry{nextRunAt: now.Add(time.Duration(i+1) * time.Second)}
			heap.Push(&h, entries[i])
		}
		heap.Remove(&h, entries[2].heapIdx) // remove the 3 s entry
		require.Equal(t, 4, h.Len())
		for i, e := range h {
			assert.Equal(t, i, e.heapIdx, "heapIdx must be consistent after Remove (pos %d)", i)
		}
	})
}

// TestAsyncSchedulerTimeUntilNextLocked verifies the floor at minDispatchInterval.
func TestAsyncSchedulerTimeUntilNextLocked(t *testing.T) {
	t.Run("EmptyHeap", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		assert.GreaterOrEqual(t, sched.timeUntilNextLocked(), time.Hour)
	})

	t.Run("PastEntryReturnsMinInterval", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		heap.Push(&sched.h, &asyncSchedulerEntry{nextRunAt: time.Now().Add(-5 * time.Second)})
		assert.Equal(t, minDispatchInterval, sched.timeUntilNextLocked())
	})

	t.Run("FutureEntry", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		heap.Push(&sched.h, &asyncSchedulerEntry{nextRunAt: time.Now().Add(5 * time.Second)})
		d := sched.timeUntilNextLocked()
		assert.GreaterOrEqual(t, d, 4*time.Second)
		assert.LessOrEqual(t, d, 5*time.Second+100*time.Millisecond)
	})
}

// TestAsyncSchedulerOnAddRemoveLocked tests the internal registration helpers
// directly, without starting scheduler goroutines.
func TestAsyncSchedulerOnAddRemoveLocked(t *testing.T) {
	t.Run("AddCreatesEntryAndHeapEntry", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		assert.Len(t, sched.entries, 1)
		assert.Equal(t, 1, sched.h.Len())
		require.NotNil(t, sched.entries[s])
		assert.Equal(t, s, sched.entries[s].shard)
	})

	t.Run("AddIsIdempotent", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		sched.onAddLocked(s)
		assert.Len(t, sched.entries, 1, "double-register must not create a second entry")
		assert.Equal(t, 1, sched.h.Len())
	})

	t.Run("AddMultipleShards", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s1, s2 := &Shard{}, &Shard{}
		sched.onAddLocked(s1)
		sched.onAddLocked(s2)
		assert.Len(t, sched.entries, 2)
		assert.Equal(t, 2, sched.h.Len())
	})

	t.Run("RemoveCleansUpEntryAndHeap", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		sched.onRemoveLocked(s)
		assert.Empty(t, sched.entries)
		assert.Equal(t, 0, sched.h.Len())
	})

	t.Run("RemoveIsIdempotent", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		sched.onRemoveLocked(s)
		assert.NotPanics(t, func() { sched.onRemoveLocked(s) })
		assert.Empty(t, sched.entries)
	})

	t.Run("RemoveNotRegisteredShard", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		assert.NotPanics(t, func() { sched.onRemoveLocked(&Shard{}) })
	})

	t.Run("RemoveOneOfTwoKeepsHeapConsistent", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s1, s2 := &Shard{}, &Shard{}
		sched.onAddLocked(s1)
		sched.onAddLocked(s2)
		sched.onRemoveLocked(s1)
		assert.Len(t, sched.entries, 1)
		assert.Equal(t, 1, sched.h.Len())
		for i, e := range sched.h {
			assert.Equal(t, i, e.heapIdx, "heapIdx must be consistent after partial removal (pos %d)", i)
		}
	})
}

// TestAsyncSchedulerReprioritizeNowLocked verifies that:
//   - a non-inFlight entry is moved to the top of the heap
//   - an inFlight entry gets asyncRepImmediateReschedule set instead
func TestAsyncSchedulerReprioritizeNowLocked(t *testing.T) {
	t.Run("MovesEntryToHeapTop", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s1, s2 := &Shard{}, &Shard{}
		sched.onAddLocked(s1)
		sched.onAddLocked(s2)

		// Push both far into the future so neither is "due".
		future := time.Now().Add(time.Hour)
		sched.entries[s1].nextRunAt = future
		sched.entries[s2].nextRunAt = future.Add(time.Second)
		heap.Init(&sched.h)

		sched.reprioritizeNowLocked(s2)

		assert.Equal(t, s2, sched.h[0].shard, "reprioritized shard must be at heap top")
	})

	t.Run("InFlightSetsRescheduleFlag", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		entry := sched.entries[s]
		entry.inFlight = true
		heap.Remove(&sched.h, entry.heapIdx) // simulates dispatch

		sched.reprioritizeNowLocked(s)

		assert.True(t, s.asyncRepImmediateReschedule.Load(),
			"inFlight shard must have asyncRepImmediateReschedule set")
	})

	t.Run("NoopForUnregisteredShard", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		assert.NotPanics(t, func() { sched.reprioritizeNowLocked(&Shard{}) })
	})

	t.Run("DoesNotDemoteAlreadyOverdueEntry", func(t *testing.T) {
		// An entry whose nextRunAt is already in the past must NOT be moved
		// forward to time.Now(): that would place it behind shards that became
		// due more recently, starving it.
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)

		past := time.Now().Add(-5 * time.Second)
		sched.entries[s].nextRunAt = past
		heap.Init(&sched.h) // restore heap invariant after direct mutation

		sched.reprioritizeNowLocked(s)

		assert.Equal(t, past, sched.entries[s].nextRunAt,
			"reprioritizeNowLocked must not modify nextRunAt of an already-overdue entry")
	})
}

// TestHeapFIFOTieBreakingSeq verifies that when multiple entries share the same
// nextRunAt the heap pops them in ascending seq order (i.e. FIFO on enqueue
// time). This exercises the Less() tie-break that was added to prevent
// arbitrary heap-internal ordering from starving recently-enqueued shards.
func TestHeapFIFOTieBreakingSeq(t *testing.T) {
	now := time.Now()
	seqValues := []uint64{10, 3, 7, 1, 5}

	h := make(asyncSchedulerHeap, 0)
	for _, seq := range seqValues {
		heap.Push(&h, &asyncSchedulerEntry{nextRunAt: now, seq: seq})
	}

	var got []uint64
	for h.Len() > 0 {
		e := heap.Pop(&h).(*asyncSchedulerEntry)
		got = append(got, e.seq)
	}

	require.Equal(t, len(seqValues), len(got))
	for i := 1; i < len(got); i++ {
		assert.Less(t, got[i-1], got[i],
			"entries with equal nextRunAt must pop in ascending seq (FIFO) order; "+
				"got seq[%d]=%d before seq[%d]=%d", i-1, got[i-1], i, got[i])
	}
}

// TestOnAddLockedMonotoneSeq verifies that successive onAddLocked calls stamp
// strictly increasing seq values so the FIFO tie-break is well-defined.
func TestOnAddLockedMonotoneSeq(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	const n = 5
	shards := make([]*Shard, n)
	for i := range shards {
		shards[i] = &Shard{}
		sched.onAddLocked(shards[i])
	}

	for i := 1; i < n; i++ {
		assert.Less(t, sched.entries[shards[i-1]].seq, sched.entries[shards[i]].seq,
			"seq must increase monotonically with each onAddLocked call (pos %d)", i)
	}
}

// TestOnResultLockedEpochRelativeScheduling verifies the epoch-relative
// scheduling introduced in onResultLocked:
//
//   - A shard that ran late (its due-time is in the past) gets an earlier
//     nextRunAt than a shard that ran exactly on time, so it climbs back up
//     the heap and is not permanently starved by on-time shards.
//   - A shard delayed by more than one interval has its look-back capped to
//     exactly one interval (the "floor"), preventing a burst of catch-up cycles.
//   - An on-time shard is scheduled at dueTime + interval.
//
// Also checks that each onResultLocked call stamps a fresh, increasing seq.
func TestOnResultLockedEpochRelativeScheduling(t *testing.T) {
	const freq = 10 * time.Second
	cfg := AsyncReplicationConfig{
		frequency:                 freq,
		frequencyWhilePropagating: freq,
	}

	// simulateDispatch mimics what dispatchDueLocked does: remove the entry
	// from the heap and mark it inFlight, returning the entry for later use.
	simulateDispatch := func(sched *AsyncReplicationScheduler, s *Shard) *asyncSchedulerEntry {
		entry := sched.entries[s]
		heap.Remove(&sched.h, entry.heapIdx)
		entry.inFlight = true
		return entry
	}

	t.Run("LateShardScheduledEarlierThanOnTimeShard", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		sLate, sOnTime := &Shard{}, &Shard{}
		sched.onAddLocked(sLate)
		sched.onAddLocked(sOnTime)

		now := time.Now()

		// sLate: due 5 s ago (worker was slow).
		sched.entries[sLate].nextRunAt = now.Add(-5 * time.Second)
		entryLate := simulateDispatch(sched, sLate)

		// sOnTime: due right now.
		sched.entries[sOnTime].nextRunAt = now
		entryOnTime := simulateDispatch(sched, sOnTime)

		sched.onResultLocked(asyncSchedulerResult{entry: entryLate, cfg: cfg})
		sched.onResultLocked(asyncSchedulerResult{entry: entryOnTime, cfg: cfg})

		// Epoch-relative: late shard next = (now-5s)+10s = now+5s
		//                 on-time shard next = now+10s
		assert.True(t,
			sched.entries[sLate].nextRunAt.Before(sched.entries[sOnTime].nextRunAt),
			"late shard must get an earlier nextRunAt than the on-time shard (epoch-relative scheduling)")
	})

	t.Run("VeryLateShardFloorCapsLookBack", func(t *testing.T) {
		// A shard delayed by more than one interval: the floor clamps the
		// look-back base to now-interval, so nextRunAt ≈ now.
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)

		sched.entries[s].nextRunAt = time.Now().Add(-3 * freq)
		entry := simulateDispatch(sched, s)

		sched.onResultLocked(asyncSchedulerResult{entry: entry, cfg: cfg})

		nextRun := sched.entries[s].nextRunAt
		// base = now - freq (floor), nextRunAt = now - freq + freq = now
		assert.True(t, nextRun.Before(time.Now().Add(100*time.Millisecond)),
			"very late shard nextRunAt must be capped at ≈now by the one-interval floor")
	})

	t.Run("OnTimeShardScheduledAtDuePlusInterval", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)

		due := time.Now()
		sched.entries[s].nextRunAt = due
		entry := simulateDispatch(sched, s)

		sched.onResultLocked(asyncSchedulerResult{entry: entry, cfg: cfg})

		nextRun := sched.entries[s].nextRunAt
		lo := due.Add(freq - 50*time.Millisecond)
		hi := due.Add(freq + 50*time.Millisecond)
		assert.True(t, nextRun.After(lo) && nextRun.Before(hi),
			"on-time shard must be scheduled at dueTime+interval (got %v, want %v..%v)",
			nextRun, lo, hi)
	})

	t.Run("SeqIncreasesWithEachResult", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		sched.onAddLocked(s)
		seqAfterAdd := sched.entries[s].seq

		const rounds = 3
		prevSeq := seqAfterAdd
		for range rounds {
			entry := simulateDispatch(sched, s)
			sched.onResultLocked(asyncSchedulerResult{entry: entry, cfg: cfg})
			newSeq := sched.entries[s].seq
			assert.Greater(t, newSeq, prevSeq,
				"seq must increase with each onResultLocked call")
			prevSeq = newSeq
		}
	})
}

// TestAsyncSchedulerNextInterval is a table-driven test covering all branches
// of nextInterval: success paths and the four error variants.
func TestAsyncSchedulerNextInterval(t *testing.T) {
	const (
		freq     = 10 * time.Second
		freqProp = 2 * time.Second
	)
	cfg := AsyncReplicationConfig{
		frequency:                 freq,
		frequencyWhilePropagating: freqProp,
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	newEntry := func(ctx context.Context) *asyncSchedulerEntry {
		return &asyncSchedulerEntry{
			shard: &Shard{asyncRepCtx: ctx},
		}
	}

	sched := newSchedulerForUnitTest(t)

	tests := []struct {
		name         string
		ctx          context.Context
		err          error
		propagated   bool
		wantInterval time.Duration
	}{
		{
			name:         "no error not propagated returns frequency",
			ctx:          context.Background(),
			wantInterval: freq,
		},
		{
			name:         "no error propagated returns frequencyWhilePropagating",
			ctx:          context.Background(),
			propagated:   true,
			wantInterval: freqProp,
		},
		{
			name:         "ErrNoDiffFound returns frequency",
			ctx:          context.Background(),
			err:          replica.ErrNoDiffFound,
			wantInterval: freq,
		},
		{
			name:         "ErrHashtreeRootUnchanged returns frequency",
			ctx:          context.Background(),
			err:          replica.ErrHashtreeRootUnchanged,
			wantInterval: freq,
		},
		{
			name:         "context cancelled returns 24h",
			ctx:          cancelledCtx,
			err:          context.Canceled,
			wantInterval: 24 * time.Hour,
		},
		{
			name:         "generic transient error returns frequency",
			ctx:          context.Background(),
			err:          errors.New("transient error"),
			wantInterval: freq,
		},
	}

	// Nil-context guard: a shard registered before initAsyncReplication sets
	// asyncRepCtx must not panic when nextInterval is called with an error.
	t.Run("nil asyncRepCtx with error does not panic", func(t *testing.T) {
		entry := &asyncSchedulerEntry{
			shard: &Shard{asyncRepCtx: nil},
		}
		result := asyncSchedulerResult{entry: entry, err: errors.New("some error")}
		assert.NotPanics(t, func() { sched.nextInterval(cfg, entry, result) })
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entry := newEntry(tc.ctx)
			result := asyncSchedulerResult{entry: entry, propagated: tc.propagated, err: tc.err}

			got := sched.nextInterval(cfg, entry, result)

			assert.Equal(t, tc.wantInterval, got)
		})
	}
}

// TestAdjustWorkersCapAtMaxMaxWorkers verifies that adjustWorkers never sets
// targetWorkers above maxMaxWorkers, even when called with a larger value.
// This is a safety property: resultCh is sized maxMaxWorkers*2 and must never
// be smaller than the number of concurrent workers.
func TestAdjustWorkersCapAtMaxMaxWorkers(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	sched.adjustWorkers(maxMaxWorkers + 100)

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()

	assert.Equal(t, maxMaxWorkers, got,
		"adjustWorkers must cap targetWorkers at maxMaxWorkers (%d)", maxMaxWorkers)

	// Clean up worker goroutines spawned by adjustWorkers so the test does
	// not leak goroutines.
	sched.cancel()
	sched.wg.Wait()
}

// TestRebuildInFlightSerializationAtomics documents and exercises the CAS
// pattern used in runEntry's defer to prevent concurrent hashtree rebuilds.
//
// When asyncRepRebuildInFlight is already true (first rebuild goroutine
// running), a second attempt must NOT start a new rebuild. Instead it must
// re-arm asyncRepNeedsRebuild so that the next completed hashbeat cycle retries.
func TestRebuildInFlightSerializationAtomics(t *testing.T) {
	s := &Shard{}

	// First rebuild goroutine acquires the flag (CAS false→true succeeds).
	firstAcquired := s.asyncRepRebuildInFlight.CompareAndSwap(false, true)
	require.True(t, firstAcquired, "first CAS must succeed when flag is clear")

	// Second concurrent attempt (the same CAS in runEntry's defer) must fail.
	secondAcquired := s.asyncRepRebuildInFlight.CompareAndSwap(false, true)
	require.False(t, secondAcquired, "second CAS must fail while first rebuild is in-flight")

	// The else-branch re-arms asyncRepNeedsRebuild so the next cycle retries.
	if !secondAcquired {
		s.asyncRepNeedsRebuild.Store(true)
	}

	assert.True(t, s.asyncRepNeedsRebuild.Load(),
		"asyncRepNeedsRebuild must be re-armed when a rebuild is already in-flight")
	assert.True(t, s.asyncRepRebuildInFlight.Load(),
		"asyncRepRebuildInFlight must remain set (first rebuild still running)")

	// First rebuild goroutine finishes; flag is cleared.
	s.asyncRepRebuildInFlight.Store(false)

	// asyncRepNeedsRebuild remains set — a future cycle will trigger a new rebuild.
	assert.True(t, s.asyncRepNeedsRebuild.Load(),
		"asyncRepNeedsRebuild must remain set after first rebuild completes")
}

// ─── dispatchDueLocked ────────────────────────────────────────────────────────

// TestDispatchDueLocked_AllWorkersBusy verifies the "all workers busy" default
// branch of dispatchDueLocked. workCh is unbuffered; with no running workers
// every send hits the default branch. That branch must call
// entry.shard.asyncRepWg.Done() to undo the earlier Add(1), otherwise
// Deregister + asyncRepWg.Wait() would block forever.
func TestDispatchDueLocked_AllWorkersBusy(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{}
	entry := &asyncSchedulerEntry{
		shard:     s,
		nextRunAt: time.Now().Add(-time.Second),
		heapIdx:   -1,
	}
	heap.Push(&sched.h, entry)
	sched.entries[s] = entry

	sched.dispatchDueLocked()

	assert.Equal(t, 1, len(sched.h), "entry must remain in heap when all workers are busy")
	assert.False(t, entry.inFlight, "entry must not be marked in-flight")

	// asyncRepWg must be balanced (Add+Done = 0). If Done was not called, Wait
	// would block until the test's own deadline fires.
	wgDone := make(chan struct{})
	go func() { s.asyncRepWg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-time.After(time.Second):
		t.Fatal("asyncRepWg.Wait() timed out — asyncRepWg.Done() was not called in the default branch")
	}
}

// TestDispatchDueLocked_InFlightEntryIsReset exercises the defensive recovery
// path: an entry with inFlight=true must never sit in the heap, but if it
// somehow does, dispatchDueLocked must reset it rather than letting the shard
// get permanently orphaned.
func TestDispatchDueLocked_InFlightEntryIsReset(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{class: &models.Class{Class: "TestInFlight"}}
	entry := &asyncSchedulerEntry{
		shard:     s,
		nextRunAt: time.Now().Add(-time.Second),
		inFlight:  true, // simulates the invariant-violation condition
		heapIdx:   -1,
	}
	heap.Push(&sched.h, entry)
	sched.entries[s] = entry

	sched.dispatchDueLocked()

	require.Equal(t, 1, len(sched.h), "entry must remain in heap after inFlight reset")
	assert.False(t, entry.inFlight, "inFlight must be cleared by the recovery path")

	// asyncRepWg must be balanced regardless of whether the re-pushed entry was
	// also dispatched (and hit the all-workers-busy default) or deferred.
	wgDone := make(chan struct{})
	go func() { s.asyncRepWg.Wait(); close(wgDone) }()
	select {
	case <-wgDone:
	case <-time.After(time.Second):
		t.Fatal("asyncRepWg is unbalanced after inFlight reset")
	}
}

// ─── onResultLocked ───────────────────────────────────────────────────────────

// TestOnResultLocked_DeregisteredMidFlight verifies that a result arriving for
// a shard that was deregistered while its cycle was in-flight is discarded
// cleanly: the entry must NOT be pushed back onto the heap.
func TestOnResultLocked_DeregisteredMidFlight(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{}
	entry := &asyncSchedulerEntry{
		shard:    s,
		inFlight: true,
		heapIdx:  -1, // already popped for dispatch
	}
	// Shard is absent from sched.entries (deregistered before result arrived).

	sched.onResultLocked(asyncSchedulerResult{entry: entry})

	assert.Empty(t, sched.h,
		"heap must be empty: deregistered-mid-flight shard must not be re-enqueued")
}

// ─── adjustWorkers ────────────────────────────────────────────────────────────

// TestAdjustWorkersClampToOne verifies that zero and negative arguments are
// clamped to 1 before updating targetWorkers.
func TestAdjustWorkersClampToOne(t *testing.T) {
	cases := []struct {
		name    string
		initial int
		arg     int
	}{
		{"zero_from_one", 1, 0},
		{"zero_from_three", 3, 0},
		{"negative_from_three", 3, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
				AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(tc.initial),
				AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(time.Hour),
				AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
			}, nil, nil)
			require.NoError(t, err)

			sched.adjustWorkers(tc.arg)

			sched.workersMu.Lock()
			got := sched.targetWorkers
			sched.workersMu.Unlock()
			assert.Equal(t, 1, got, "adjustWorkers(%d) from %d must clamp to 1", tc.arg, tc.initial)
		})
	}
}

// TestAdjustWorkersScaleDownUpdatesTargetAndSendsTokens verifies that scaling
// down from N to M (N > M) sets targetWorkers = M and sends exactly N-M
// scale-down tokens. With no running workers (Start() not called) the tokens
// accumulate in scaleDownCh so we can count them exactly.
func TestAdjustWorkersScaleDownUpdatesTargetAndSendsTokens(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:           configRuntime.NewDynamicValue(5),
		AsyncReplicationSchedulerTopologyFrequency: configRuntime.NewDynamicValue(time.Hour),
		AsyncReplicationDisabled:                   configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	sched.adjustWorkers(2) // 5 → 2: must send 3 tokens

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()
	assert.Equal(t, 2, got, "targetWorkers must reflect the scale-down target")

	tokens := 0
	for {
		select {
		case <-sched.scaleDownCh:
			tokens++
			continue
		default:
		}
		break
	}
	assert.Equal(t, 3, tokens, "must send exactly one scale-down token per removed worker")
}

// TestAdjustWorkersScaleUpDrainsStaleTokens verifies that a scale-up drains
// any stale scale-down tokens left by a prior shrink before spawning new
// workers, preventing those workers from exiting immediately.
func TestAdjustWorkersScaleUpDrainsStaleTokens(t *testing.T) {
	sched := newSchedulerForUnitTest(t) // targetWorkers = 1
	t.Cleanup(func() {
		// Cancel context so spawned workers exit, then wait for them.
		sched.cancel()
		sched.wg.Wait()
	})

	// Seed stale tokens simulating a prior shrink that was not fully consumed.
	for range 3 {
		sched.scaleDownCh <- struct{}{}
	}

	sched.adjustWorkers(3) // 1 → 3: must drain the 3 stale tokens before spawning

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()
	assert.Equal(t, 3, got, "targetWorkers must be updated to 3")

	// Stale tokens must be gone; adjustWorkers drains scaleDownCh before spawning.
	tokens := 0
	for {
		select {
		case <-sched.scaleDownCh:
			tokens++
			continue
		default:
		}
		break
	}
	assert.Equal(t, 0, tokens, "all stale scale-down tokens must be drained before new workers start")
}
