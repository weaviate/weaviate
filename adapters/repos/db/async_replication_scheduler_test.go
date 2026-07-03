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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestInitRetryBackoff verifies that initRetryBackoff never produces a negative
// or zero duration, even for large i values that caused int64 overflow in the
// naive formulation (time.Duration(1<<i)*100*time.Millisecond overflows at i=37,
// wrapping to a negative value and turning the retry loop into a busy-loop).
func TestInitRetryBackoff(t *testing.T) {
	cases := []struct {
		i    int
		want time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{10, time.Duration(1<<10) * 100 * time.Millisecond}, // 102.4 s, below the 5 min cap
		{11, time.Duration(1<<11) * 100 * time.Millisecond}, // 204.8 s, below the 5 min cap
		{12, 5 * time.Minute},                               // 2^12 × 100 ms = 409.6 s > 5 min → capped
		{36, 5 * time.Minute},                               // just below the old overflow boundary
		{37, 5 * time.Minute},                               // previously caused int64 overflow → negative duration
		{38, 5 * time.Minute},
		{100, 5 * time.Minute}, // well above any reasonable value
	}
	for _, tc := range cases {
		got := initRetryBackoff(tc.i)
		assert.Equal(t, tc.want, got, "i=%d: unexpected backoff duration", tc.i)
		assert.Greater(t, int64(got), int64(0), "i=%d: backoff must always be positive", tc.i)
	}
}

// TestAsyncRepRebuildBackoffDuration verifies the exponential backoff schedule
// for hashtree rebuild failures. The schedule is: 0 (no backoff on first call
// before any failure), 30 s, 60 s, 2 m, 4 m, 8 m, 16 m, capped at 30 m.
// It also verifies that the uint32 overflow guard prevents a negative or zero
// duration for large consecutiveFailures values.
func TestAsyncRepRebuildBackoffDuration(t *testing.T) {
	cases := []struct {
		failures uint32
		want     time.Duration
	}{
		{0, 0},                         // 0 failures → no backoff (called before any failure)
		{1, 30 * time.Second},          // 1st failure
		{2, 60 * time.Second},          // 2nd
		{3, 2 * time.Minute},           // 3rd
		{4, 4 * time.Minute},           // 4th
		{5, 8 * time.Minute},           // 5th
		{6, 16 * time.Minute},          // 6th
		{7, 30 * time.Minute},          // 7th → capped at maxBackoff
		{8, 30 * time.Minute},          // still capped
		{100, 30 * time.Minute},        // well above cap
		{^uint32(0), 30 * time.Minute}, // max uint32: overflow guard must fire
	}
	for _, tc := range cases {
		got := asyncRepRebuildBackoffDuration(tc.failures)
		assert.Equal(t, tc.want, got, "failures=%d", tc.failures)
		if tc.failures > 0 {
			assert.Greater(t, int64(got), int64(0),
				"failures=%d: backoff must always be positive", tc.failures)
		}
	}
}

// TestEffectivePropagationDelay verifies that:
//   - propagationDelay=0 set via per-class API survives Effective() when no
//     global DynamicValue is configured.
//   - a sub-second propagationDelay set via global runtime config is correctly
//     applied (it was silently ignored before the applyDurPositive fix because
//     the old applyDur required v >= a frequency floor, which propagationDelay
//     does not share).
func TestEffectivePropagationDelay(t *testing.T) {
	zero := time.Duration(0)
	fifty := 50 * time.Millisecond

	t.Run("zero_delay_via_class_api_preserved", func(t *testing.T) {
		cfg := AsyncReplicationConfig{
			propagationDelay: defaultPropagationDelay,
			classOverrides: asyncReplicationClassOverrides{
				propagationDelay: &zero,
			},
		}
		result := cfg.Effective(replication.GlobalConfig{})
		assert.Equal(t, time.Duration(0), result.propagationDelay,
			"propagationDelay=0 set via per-class API must survive Effective()")
	})

	t.Run("sub_100ms_delay_via_global_config_applied", func(t *testing.T) {
		cfg := AsyncReplicationConfig{
			propagationDelay: defaultPropagationDelay,
		}
		globals := replication.GlobalConfig{
			AsyncReplicationPropagationDelay: configRuntime.NewDynamicValue(fifty),
		}
		result := cfg.Effective(globals)
		assert.Equal(t, fifty, result.propagationDelay,
			"propagationDelay=50ms via global runtime config must be applied (was silently ignored before fix)")
	})
}

// newSchedulerForUnitTest returns a fully-running scheduler whose dispatcher
// and single worker are torn down via t.Cleanup. Internal-method tests that
// hold sched.mu serialise naturally with the dispatcher; tests with empty or
// future-dated heaps see no background dispatch.
func newSchedulerForUnitTest(t *testing.T) *AsyncReplicationScheduler {
	t.Helper()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)
	t.Cleanup(sched.Close)
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

// TestRegisterDeregisterLifecycle verifies the public Register/Deregister contract:
// entries appear after Register, disappear after Deregister, double-Register is
// idempotent, and Deregister of an unregistered shard is a no-op.
func TestRegisterDeregisterLifecycle(t *testing.T) {
	t.Run("RegisterCreatesEntry", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		require.NoError(t, sched.Register(s))

		sched.mu.Lock()
		n := len(sched.entries)
		_, ok := sched.entries[s]
		sched.mu.Unlock()
		assert.Equal(t, 1, n)
		assert.True(t, ok, "shard must be present in entries after Register")
	})

	t.Run("RegisterIsIdempotent", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		require.NoError(t, sched.Register(s))
		require.NoError(t, sched.Register(s))

		sched.mu.Lock()
		n := len(sched.entries)
		sched.mu.Unlock()
		assert.Equal(t, 1, n, "double Register must not create a second entry")
	})

	t.Run("RegisterMultipleShards", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s1, s2 := &Shard{}, &Shard{}
		require.NoError(t, sched.Register(s1))
		require.NoError(t, sched.Register(s2))

		sched.mu.Lock()
		n := len(sched.entries)
		sched.mu.Unlock()
		assert.Equal(t, 2, n)
	})

	t.Run("DeregisterRemovesEntry", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		s := &Shard{}
		require.NoError(t, sched.Register(s))
		require.NoError(t, sched.Deregister(s))

		sched.mu.Lock()
		n := len(sched.entries)
		sched.mu.Unlock()
		assert.Equal(t, 0, n)
	})

	t.Run("DeregisterUnregisteredIsNoOp", func(t *testing.T) {
		sched := newSchedulerForUnitTest(t)
		assert.NoError(t, sched.Deregister(&Shard{}))
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

// Deleted: TestOnResultLockedEpochRelativeScheduling.
//
// The epoch-relative scheduling property (a late shard climbs the heap back
// against on-time shards; the look-back floor caps catch-up bursts) is a
// timing invariant of the internal onResultLocked path that has no direct
// behavioural observation point. The standalone heap tests (TestAsyncSchedulerHeap,
// TestHeapFIFOTieBreakingSeq) cover the underlying ordering guarantees, and the
// integration tests in async_replication_scheduler_integration_test.go exercise
// the dispatcher with real shards. Removed during the constructor-spawns-workers
// migration where direct mutation of sched.h races with the live dispatcher.

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
			result := asyncSchedulerResult{entry: entry, propagated: tc.propagated, err: tc.err, ctx: tc.ctx}

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
	sched := newSchedulerForUnitTest(t) // helper registers t.Cleanup(sched.Close)

	sched.adjustWorkers(maxMaxWorkers + 100)

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()

	assert.Equal(t, maxMaxWorkers, got,
		"adjustWorkers must cap targetWorkers at maxMaxWorkers (%d)", maxMaxWorkers)
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

// Deleted: TestDispatchDueLocked_AllWorkersBusy, TestDispatchDueLocked_InFlightEntryIsReset,
// TestOnResultLocked_DeregisteredMidFlight.
//
// These tests poked the heap and inFlight state directly to exercise three
// internal paths:
//   - workCh-full backpressure with asyncRepWg balance
//   - defensive recovery from an inFlight-in-heap invariant violation
//   - discarding a result for a deregistered shard
//
// The first property is covered behaviorally by TestWorkerDrainsWorkChOnCtxCancel
// and TestSchedulerCloseWithConcurrentDispatches (Close completes promptly with
// asyncRepWg balanced). The second is dead-code recovery for a state that
// cannot occur via the public API. The third is covered transitively by the
// concurrent Register/Deregister/Close integration test
// (TestAsyncSchedulerConcurrentRegisterDeregisterAndClose) — a deregistered
// shard whose cycle was in-flight will not be re-enqueued or the test would
// deadlock during shutdown. Removed during the constructor-spawns-workers
// migration where direct mutation of sched.h races with the live dispatcher.

// ─── adjustWorkers ────────────────────────────────────────────────────────────

// TestAdjustWorkersClampToDefault verifies that zero and negative arguments are
// mapped to defaultAsyncReplicationSchedulerWorkers, matching the construction-time
// behaviour of NewAsyncReplicationScheduler.
func TestAdjustWorkersClampToDefault(t *testing.T) {
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
				AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(tc.initial),
				AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
			}, nil, nil)
			require.NoError(t, err)
			t.Cleanup(sched.Close)

			sched.adjustWorkers(tc.arg)

			sched.workersMu.Lock()
			got := sched.targetWorkers
			sched.workersMu.Unlock()
			assert.Equal(t, defaultAsyncReplicationSchedulerWorkers, got,
				"adjustWorkers(%d) from %d must map to defaultAsyncReplicationSchedulerWorkers", tc.arg, tc.initial)
		})
	}
}

// TestAdjustWorkersUpdatesTargetWorkers verifies the observable contract of
// adjustWorkers: targetWorkers is updated to the requested value (clamped to
// the valid range). The previous tests asserted exact pending-token counts in
// scaleDownCh, which depended on no-running-workers semantics that no longer
// exist. With a live worker pool, tokens are consumed concurrently and only
// the post-condition (targetWorkers == requested) is race-free.
func TestAdjustWorkersUpdatesTargetWorkers(t *testing.T) {
	t.Run("ScaleDown", func(t *testing.T) {
		sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
			AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(5),
			AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
		}, nil, nil)
		require.NoError(t, err)
		t.Cleanup(sched.Close)

		sched.adjustWorkers(2)

		sched.workersMu.Lock()
		got := sched.targetWorkers
		sched.workersMu.Unlock()
		assert.Equal(t, 2, got, "targetWorkers must reflect the scale-down target")
	})

	t.Run("ScaleUp", func(t *testing.T) {
		sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
			AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(2),
			AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
		}, nil, nil)
		require.NoError(t, err)
		t.Cleanup(sched.Close)

		sched.adjustWorkers(8)

		sched.workersMu.Lock()
		got := sched.targetWorkers
		sched.workersMu.Unlock()
		assert.Equal(t, 8, got, "targetWorkers must reflect the scale-up target")
	})

	t.Run("RapidDownUp", func(t *testing.T) {
		sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
			AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(5),
			AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
		}, nil, nil)
		require.NoError(t, err)
		t.Cleanup(sched.Close)

		sched.adjustWorkers(2)
		sched.adjustWorkers(8)

		sched.workersMu.Lock()
		got := sched.targetWorkers
		sched.workersMu.Unlock()
		assert.Equal(t, 8, got, "after down-then-up, targetWorkers must be 8")
	})
}

// Deleted: TestAdjustWorkersScaleDownUpdatesTargetAndSendsTokens,
// TestAdjustWorkersScaleUpDrainsStaleTokens, TestAdjustWorkersRapidDownUp.
//
// These asserted exact scaleDownCh token counts, which is only meaningful when
// no worker goroutines are running to consume them. With the constructor now
// spawning the worker pool, tokens are consumed concurrently and the count
// becomes racy. The observable post-condition (targetWorkers reaches the
// requested value, tests above) is preserved.

// TestAdjustWorkersCapWarnsWithLogger verifies that when adjustWorkers clamps a
// requested count above maxMaxWorkers, a warning is logged (logger non-nil path).
func TestAdjustWorkersCapWarnsWithLogger(t *testing.T) {
	logger, hook := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, logger)
	require.NoError(t, err)
	t.Cleanup(sched.Close)

	sched.adjustWorkers(maxMaxWorkers + 5)

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()
	assert.Equal(t, maxMaxWorkers, got, "targetWorkers must be capped at maxMaxWorkers")

	var warned bool
	for _, entry := range hook.Entries {
		if entry.Level == logrus.WarnLevel {
			warned = true
			break
		}
	}
	assert.True(t, warned, "a warning must be logged when the worker count is clamped")
}

// TestRebuildHashtreeEnableFailureRearmsNeedsRebuild verifies that when
// enableAsyncReplication fails during a rebuild (e.g. the scheduler reference is
// nil), asyncRepNeedsRebuild is re-armed so the next hashbeat cycle retries.
func TestRebuildHashtreeEnableFailureRearmsNeedsRebuild(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	// Index with nil scheduler → enableAsyncReplication returns an error immediately.
	idx := &Index{Config: IndexConfig{ClassName: "TestClass"}}
	s := &Shard{
		class: &models.Class{Class: "TestClass"},
		index: idx,
		// hashtree is nil → disableAsyncReplication is a no-op (idempotent).
	}

	require.False(t, s.asyncRepNeedsRebuild.Load(), "precondition: rebuild not needed yet")

	sched.rebuildHashtree(s)

	assert.True(t, s.asyncRepNeedsRebuild.Load(),
		"asyncRepNeedsRebuild must be re-armed when enableAsyncReplication fails")
}

// TestNextIntervalErrNoDiffFoundWithPropagatedTrue verifies that ErrNoDiffFound
// always returns the base frequency — even when propagated=true — because an
// empty diff means there is nothing to propagate in the next cycle.
func TestNextIntervalErrNoDiffFoundWithPropagatedTrue(t *testing.T) {
	const (
		freq     = 10 * time.Second
		freqProp = 2 * time.Second
	)
	cfg := AsyncReplicationConfig{
		frequency:                 freq,
		frequencyWhilePropagating: freqProp,
	}
	sched := newSchedulerForUnitTest(t)

	entry := &asyncSchedulerEntry{shard: &Shard{asyncRepCtx: context.Background()}}
	result := asyncSchedulerResult{
		entry:      entry,
		err:        replica.ErrNoDiffFound,
		propagated: true, // propagated=true must NOT override the error path
		ctx:        context.Background(),
	}

	got := sched.nextInterval(cfg, entry, result)
	assert.Equal(t, freq, got,
		"ErrNoDiffFound must return base frequency regardless of propagated flag")
}

// TestAdjustWorkersConcurrent verifies that concurrent adjustWorkers calls do
// not produce data races (run with -race). Final targetWorkers must be within
// the valid range [1, maxMaxWorkers].
func TestAdjustWorkersConcurrent(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(3),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)
	t.Cleanup(sched.Close)

	const goroutines = 10
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		n := i + 1 // vary between 1 and goroutines
		go func() {
			defer wg.Done()
			<-ready
			sched.adjustWorkers(n)
		}()
	}
	close(ready)
	wg.Wait()

	sched.workersMu.Lock()
	got := sched.targetWorkers
	sched.workersMu.Unlock()
	assert.GreaterOrEqual(t, got, 1, "targetWorkers must be >= 1 after concurrent adjustWorkers")
	assert.LessOrEqual(t, got, maxMaxWorkers, "targetWorkers must be <= maxMaxWorkers after concurrent adjustWorkers")
}

// TestAsyncReplicationConfigFromModelFrequencyFloor verifies that
// asyncReplicationConfigFromModel clamps sub-minimum frequency /
// frequencyWhilePropagating values up to the minimum (logging a Warn) and
// preserves values at or above it. The lenient clamp-and-warn behavior is
// required for backwards compatibility with collections persisted before the
// minimums were raised.
func TestAsyncReplicationConfigFromModelFrequencyFloor(t *testing.T) {
	// pick boundary inputs relative to each field's own minimum.
	freqBelow := int64(minFrequency/time.Millisecond) - 1 // just below 5s
	freqExact := int64(minFrequency / time.Millisecond)   // exactly at 5s
	freqAbove := int64(minFrequency/time.Millisecond) + 1 // just above 5s

	fwpBelow := int64(minFrequencyWhilePropagating/time.Millisecond) - 1 // just below 1s
	fwpExact := int64(minFrequencyWhilePropagating / time.Millisecond)   // exactly at 1s
	fwpAbove := int64(minFrequencyWhilePropagating/time.Millisecond) + 1 // just above 1s

	newLogger := func() (logrus.FieldLogger, *test.Hook) {
		l, h := test.NewNullLogger()
		return l, h
	}

	t.Run("frequency_below_min_clamped_and_warned", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &freqBelow,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequency)
		assert.Equal(t, minFrequency, *cfg.classOverrides.frequency,
			"sub-minimum frequency must be clamped to minFrequency")
		require.Len(t, hook.Entries, 1, "exactly one Warn entry expected")
		assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
		assert.Equal(t, "frequency", hook.LastEntry().Data["field"])
	})

	t.Run("frequency_at_min_accepted_no_warn", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &freqExact,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequency)
		assert.Equal(t, minFrequency, *cfg.classOverrides.frequency)
		assert.Empty(t, hook.Entries, "no warning expected at minimum")
	})

	t.Run("frequency_above_min_accepted_no_warn", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &freqAbove,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequency)
		assert.Equal(t, time.Duration(freqAbove)*time.Millisecond, *cfg.classOverrides.frequency)
		assert.Empty(t, hook.Entries, "no warning expected above minimum")
	})

	t.Run("frequencyWhilePropagating_below_min_clamped_and_warned", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &fwpBelow,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequencyWhilePropagating)
		assert.Equal(t, minFrequencyWhilePropagating, *cfg.classOverrides.frequencyWhilePropagating,
			"sub-minimum frequencyWhilePropagating must be clamped to minFrequencyWhilePropagating")
		require.Len(t, hook.Entries, 1, "exactly one Warn entry expected")
		assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
		assert.Equal(t, "frequencyWhilePropagating", hook.LastEntry().Data["field"])
	})

	t.Run("frequencyWhilePropagating_at_min_accepted_no_warn", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &fwpExact,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequencyWhilePropagating)
		assert.Equal(t, minFrequencyWhilePropagating, *cfg.classOverrides.frequencyWhilePropagating)
		assert.Empty(t, hook.Entries, "no warning expected at minimum")
	})

	t.Run("frequencyWhilePropagating_above_min_accepted_no_warn", func(t *testing.T) {
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &fwpAbove,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequencyWhilePropagating)
		assert.Equal(t, time.Duration(fwpAbove)*time.Millisecond, *cfg.classOverrides.frequencyWhilePropagating)
		assert.Empty(t, hook.Entries, "no warning expected above minimum")
	})

	// Policy for raw API inputs (ReplicationAsyncConfig has no minimum checks
	// of its own on these int64 fields, so any value can reach this helper):
	//   - zero: treated as below-minimum, clamped up to the minimum with a Warn,
	//   - negative: rejected with an error (no meaningful interpretation),
	//   - overflow (ms value > maxDurationMillis): rejected to avoid wrapping
	//     to a negative time.Duration and silently clamping to the minimum.
	t.Run("frequency_zero_clamped_and_warned", func(t *testing.T) {
		zero := int64(0)
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &zero,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequency)
		assert.Equal(t, minFrequency, *cfg.classOverrides.frequency)
		require.Len(t, hook.Entries, 1, "exactly one Warn entry expected")
		assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
		assert.Equal(t, "frequency", hook.LastEntry().Data["field"])
	})

	t.Run("frequency_negative_rejected", func(t *testing.T) {
		neg := int64(-1)
		logger, hook := newLogger()
		_, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &neg,
		}, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "frequency must be >= 0")
		assert.Empty(t, hook.Entries, "no clamp warning expected for negative input")
	})

	t.Run("frequencyWhilePropagating_zero_clamped_and_warned", func(t *testing.T) {
		zero := int64(0)
		logger, hook := newLogger()
		cfg, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &zero,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, cfg.classOverrides.frequencyWhilePropagating)
		assert.Equal(t, minFrequencyWhilePropagating, *cfg.classOverrides.frequencyWhilePropagating)
		require.Len(t, hook.Entries, 1, "exactly one Warn entry expected")
		assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
		assert.Equal(t, "frequencyWhilePropagating", hook.LastEntry().Data["field"])
	})

	t.Run("frequencyWhilePropagating_negative_rejected", func(t *testing.T) {
		neg := int64(-100)
		logger, hook := newLogger()
		_, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &neg,
		}, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "frequencyWhilePropagating must be >= 0")
		assert.Empty(t, hook.Entries, "no clamp warning expected for negative input")
	})

	// Inputs whose value in milliseconds does not fit in time.Duration would
	// silently wrap to a negative duration on conversion and then be clamped
	// to the minimum. Reject them explicitly so callers don't end up with the
	// fastest cadence when they asked for the slowest.
	t.Run("frequency_overflow_rejected", func(t *testing.T) {
		overflow := int64(math.MaxInt64)
		logger, hook := newLogger()
		_, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			Frequency: &overflow,
		}, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "frequency too large")
		assert.Empty(t, hook.Entries, "no clamp warning expected for overflow input")
	})

	t.Run("frequencyWhilePropagating_overflow_rejected", func(t *testing.T) {
		overflow := int64(math.MaxInt64)
		logger, hook := newLogger()
		_, err := asyncReplicationConfigFromModel(false, &models.ReplicationAsyncConfig{
			FrequencyWhilePropagating: &overflow,
		}, logger)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "frequencyWhilePropagating too large")
		assert.Empty(t, hook.Entries, "no clamp warning expected for overflow input")
	})
}

// TestAsyncReplicationClampWarner verifies the warner's consecutive-duplicate
// suppression for positive-but-below-minimum runtime overrides on the two
// Frequency fields. A stable sub-minimum value logs once; changing to a new
// sub-minimum value re-arms the warning. Only the last warned value per field
// is remembered, so an A→B→A alternation would log A twice — see the type's
// doc comment for the tradeoff.
func TestAsyncReplicationClampWarner(t *testing.T) {
	makeWarner := func() (*asyncReplicationClampWarner, *test.Hook) {
		l, h := test.NewNullLogger()
		return &asyncReplicationClampWarner{logger: l}, h
	}

	t.Run("subfloor_frequency_warns_once_per_unique_value", func(t *testing.T) {
		w, hook := makeWarner()
		subfloor := minFrequency / 2
		globals := replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(subfloor),
		}
		w.checkGlobals(globals)
		w.checkGlobals(globals)
		w.checkGlobals(globals)
		require.Len(t, hook.Entries, 1, "repeat checks with the same value must dedup")
		assert.Equal(t, "frequency", hook.LastEntry().Data["field"])
		assert.Equal(t, subfloor, hook.LastEntry().Data["requested"])
		assert.Equal(t, minFrequency, hook.LastEntry().Data["applied"])
		assert.Contains(t, hook.LastEntry().Message, "below minimum")
	})

	t.Run("changed_subfloor_value_warns_again", func(t *testing.T) {
		w, hook := makeWarner()
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(minFrequency / 2),
		})
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(minFrequency / 3),
		})
		require.Len(t, hook.Entries, 2, "a different sub-minimum value must re-arm the warning")
	})

	t.Run("above_floor_value_does_not_warn", func(t *testing.T) {
		w, hook := makeWarner()
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(minFrequency * 2),
		})
		assert.Empty(t, hook.Entries)
	})

	t.Run("zero_value_does_not_warn", func(t *testing.T) {
		// Zero is the DynamicValue "not configured" sentinel — not a sub-minimum
		// override that warrants a Warn.
		w, hook := makeWarner()
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(time.Duration(0)),
		})
		assert.Empty(t, hook.Entries)
	})

	t.Run("negative_value_does_not_warn", func(t *testing.T) {
		// Negative runtime overrides are silently ignored by the apply helpers
		// (the prior cadence is preserved). The warner intentionally does not
		// cover them — the right place to reject typos is the env-var / YAML
		// loader at startup.
		w, hook := makeWarner()
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(-1 * time.Second),
		})
		assert.Empty(t, hook.Entries)
	})

	t.Run("each_field_warns_with_its_own_floor", func(t *testing.T) {
		// Pick a value above minFrequencyWhilePropagating but below minFrequency.
		// Both fields share the same value, but only frequency is considered
		// sub-minimum.
		w, hook := makeWarner()
		shared := 2 * time.Second
		w.checkGlobals(replication.GlobalConfig{
			AsyncReplicationFrequency:                 configRuntime.NewDynamicValue(shared),
			AsyncReplicationFrequencyWhilePropagating: configRuntime.NewDynamicValue(shared),
		})
		require.Len(t, hook.Entries, 1)
		assert.Equal(t, "frequency", hook.LastEntry().Data["field"])
	})
}

// TestRebuildHashtreeCancelledContext verifies that rebuildHashtree returns
// safely without panicking or modifying asyncRepNeedsRebuild when the
// scheduler's context is already cancelled.
func TestRebuildHashtreeCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sched, err := NewAsyncReplicationScheduler(ctx, replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	// Cancel the context before calling rebuildHashtree.
	cancel()

	idx := &Index{Config: IndexConfig{ClassName: "TestClass"}}
	s := &Shard{
		class: &models.Class{Class: "TestClass"},
		index: idx,
	}

	require.False(t, s.asyncRepNeedsRebuild.Load(), "precondition: rebuild not needed yet")

	assert.NotPanics(t, func() { sched.rebuildHashtree(s) },
		"rebuildHashtree must not panic with a cancelled context")

	// With a cancelled context, both early-return guards fire before
	// enableAsyncReplication is reached, so asyncRepNeedsRebuild is not re-armed.
	assert.False(t, s.asyncRepNeedsRebuild.Load(),
		"asyncRepNeedsRebuild must not be set when context is already cancelled at entry")
}

// TestRegisterAfterCloseReturnsErrSchedulerClosed verifies that Register returns
// ErrSchedulerClosed promptly (does not deadlock) once Close has been called.
func TestRegisterAfterCloseReturnsErrSchedulerClosed(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)
	sched.Close()

	type result struct{ err error }
	resultCh := make(chan result, 1)
	go func() {
		resultCh <- result{err: sched.Register(&Shard{})}
	}()

	select {
	case res := <-resultCh:
		assert.ErrorIs(t, res.err, ErrSchedulerClosed)

		sched.mu.Lock()
		n := len(sched.entries)
		sched.mu.Unlock()
		assert.Equal(t, 0, n, "shard must not appear in registry when Register returns an error")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Register blocked — closed guard did not fire after Close()")
	}
}

// TestDeregisterAfterCloseReturnsErrSchedulerClosed verifies that Deregister
// returns ErrSchedulerClosed promptly (does not deadlock) once Close has been called.
func TestDeregisterAfterCloseReturnsErrSchedulerClosed(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)
	sched.Close()

	type result struct{ err error }
	resultCh := make(chan result, 1)
	go func() {
		resultCh <- result{err: sched.Deregister(&Shard{})}
	}()

	select {
	case res := <-resultCh:
		assert.ErrorIs(t, res.err, ErrSchedulerClosed)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Deregister blocked — closed guard did not fire after Close()")
	}
}

// TestCloseIsIdempotent verifies that calling Close() twice is safe: the
// second call returns without panic and does not change observable state.
func TestCloseIsIdempotent(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	sched.Close()
	assert.True(t, sched.closed.Load(), "closed must be set after first Close()")

	// Second call must not panic, must complete promptly.
	done := make(chan struct{})
	go func() { sched.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second Close() blocked")
	}
	assert.True(t, sched.closed.Load(), "closed must remain set after second Close()")
}

// entriesContains is a small helper that snapshots whether `s` is present in
// sched.entries under sched.mu. Used by the strict-postcondition tests.
func entriesContains(sched *AsyncReplicationScheduler, s *Shard) bool {
	sched.mu.Lock()
	_, ok := sched.entries[s]
	sched.mu.Unlock()
	return ok
}

// TestRegisterStrictPostconditionPresent verifies the documented postcondition:
// Register returning nil implies the shard is present in the scheduler's
// internal registry once the call returns.
func TestRegisterStrictPostconditionPresent(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{}
	require.NoError(t, sched.Register(s))
	assert.True(t, entriesContains(sched, s),
		"Register returning nil must imply the shard is in entries")
}

// TestDeregisterStrictPostconditionAbsent verifies the documented postcondition:
// Deregister returning nil implies the shard is not present in the scheduler's
// internal registry once the call returns.
func TestDeregisterStrictPostconditionAbsent(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{}
	require.NoError(t, sched.Register(s))
	require.True(t, entriesContains(sched, s), "precondition: shard must be registered")

	require.NoError(t, sched.Deregister(s))
	assert.False(t, entriesContains(sched, s),
		"Deregister returning nil must imply the shard is not in entries")
}

// TestRegisterReregisterAfterDeregister verifies that a Register/Deregister/
// Register sequence on the same shard works as expected and leaves the shard
// registered after the second Register.
func TestRegisterReregisterAfterDeregister(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	s := &Shard{}
	require.NoError(t, sched.Register(s))
	require.NoError(t, sched.Deregister(s))
	require.False(t, entriesContains(sched, s))

	require.NoError(t, sched.Register(s))
	assert.True(t, entriesContains(sched, s),
		"shard must be registered again after Deregister + Register")
}

// TestConcurrentRegisterDeregisterStress hammers the public Register/Deregister
// API from many goroutines and checks two end-state invariants:
//   - No panic / deadlock under sustained concurrent use.
//   - After all goroutines finish and a final reconciliation pass deregisters
//     every shard, the registry is empty (no leaked entries).
func TestConcurrentRegisterDeregisterStress(t *testing.T) {
	sched := newSchedulerForUnitTest(t)

	const goroutines = 16
	const iterationsPerGoroutine = 50

	// Each goroutine flips a private shard between registered and not, so a
	// `Register` returning nil on one goroutine cannot be undone by another.
	shards := make([]*Shard, goroutines)
	for i := range shards {
		shards[i] = &Shard{}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	for i := range goroutines {
		s := shards[i]
		go func() {
			defer wg.Done()
			<-start
			for range iterationsPerGoroutine {
				if err := sched.Register(s); err == nil {
					// Strict: post-Register, shard must be registered.
					if !entriesContains(sched, s) {
						t.Errorf("Register returned nil but shard is not in entries")
						return
					}
				}
				if err := sched.Deregister(s); err == nil {
					// Strict: post-Deregister, shard must not be registered.
					if entriesContains(sched, s) {
						t.Errorf("Deregister returned nil but shard is still in entries")
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	// Final reconciliation: every shard left in any state must be deregister-able.
	for _, s := range shards {
		_ = sched.Deregister(s)
	}
	sched.mu.Lock()
	final := len(sched.entries)
	sched.mu.Unlock()
	assert.Equal(t, 0, final, "no entries must remain after final Deregister sweep")
}

// TestRegisterRacingCloseStrict spawns Register goroutines concurrently with
// Close and verifies that Register's strict postcondition holds across the
// dispatcher race window:
//
//   - Every Register that returned nil → its shard is in entries.
//   - Every Register that returned ErrSchedulerClosed → its shard is NOT in
//     entries (the dispatcher's addCh branch dropped it because closed was
//     set before onAddLocked could run).
//
// Close does not clear entries, so the post-condition is checkable after
// every goroutine has returned and Close has completed.
func TestRegisterRacingCloseStrict(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(2),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	const n = 100
	shards := make([]*Shard, n)
	for i := range shards {
		shards[i] = &Shard{}
	}
	results := make([]error, n)

	var registerWg sync.WaitGroup
	registerWg.Add(n)
	start := make(chan struct{})
	for i := range n {
		idx := i
		go func() {
			defer registerWg.Done()
			<-start
			results[idx] = sched.Register(shards[idx])
		}()
	}
	close(start)

	closeDone := make(chan struct{})
	go func() {
		// Brief jitter so some Registers complete before Close starts; the
		// rest race against closed.Store(true).
		time.Sleep(time.Millisecond)
		sched.Close()
		close(closeDone)
	}()

	registerWg.Wait()
	<-closeDone

	// Snapshot the registry once. It is only mutated by the dispatcher
	// goroutine, which has exited by this point (Close waits on wg).
	sched.mu.Lock()
	finalEntries := make(map[*Shard]struct{}, len(sched.entries))
	for s := range sched.entries {
		finalEntries[s] = struct{}{}
	}
	sched.mu.Unlock()

	for i, err := range results {
		_, present := finalEntries[shards[i]]
		switch {
		case err == nil:
			assert.True(t, present,
				"Register returning nil must imply the shard is in entries (strict postcondition)")
		case errors.Is(err, ErrSchedulerClosed):
			// Asymmetric contract: the strict postcondition is one-sided —
			// nil ⟹ shard added. The error branch makes no positive claim
			// because the inner <-ctx.Done() panic backstop can fire after
			// the dispatcher already ran onAddLocked, producing a "false
			// negative" (operation succeeded, error reported). The shard
			// being either present or absent is consistent with the API.
		default:
			t.Errorf("unexpected error from Register: %v", err)
		}
	}
}

// TestDeregisterRacingCloseStrict mirrors TestRegisterRacingCloseStrict for
// Deregister: each goroutine pre-registers its shard, then races a Deregister
// call against Close. Strict nil-postcondition: nil → shard absent. The error
// branch makes no positive claim — the inner ctx.Done() backstop can fire
// after the dispatcher ran onRemoveLocked, producing a benign false negative.
func TestDeregisterRacingCloseStrict(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(2),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	const n = 100
	shards := make([]*Shard, n)
	for i := range shards {
		shards[i] = &Shard{}
		require.NoError(t, sched.Register(shards[i]))
	}
	results := make([]error, n)

	var deregisterWg sync.WaitGroup
	deregisterWg.Add(n)
	start := make(chan struct{})
	for i := range n {
		idx := i
		go func() {
			defer deregisterWg.Done()
			<-start
			results[idx] = sched.Deregister(shards[idx])
		}()
	}
	close(start)

	closeDone := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond)
		sched.Close()
		close(closeDone)
	}()

	deregisterWg.Wait()
	<-closeDone

	sched.mu.Lock()
	finalEntries := make(map[*Shard]struct{}, len(sched.entries))
	for s := range sched.entries {
		finalEntries[s] = struct{}{}
	}
	sched.mu.Unlock()

	for i, err := range results {
		_, present := finalEntries[shards[i]]
		switch {
		case err == nil:
			assert.False(t, present,
				"Deregister returning nil must imply the shard is NOT in entries (strict postcondition)")
		case errors.Is(err, ErrSchedulerClosed):
			// Asymmetric contract — see the Register variant above.
		default:
			t.Errorf("unexpected error from Deregister: %v", err)
		}
	}
}

// TestEffectiveThreeTierPrecedence verifies the three-tier override hierarchy
// of Effective(): global runtime config > per-class schema override > code default.
//
// Uses frequency as the test field because it has both an enforced minimum
// (minFrequency) and no non-zero code default for easy isolation.
func TestEffectiveThreeTierPrecedence(t *testing.T) {
	const codeDefault = 5 * time.Second
	const classOverride = 2 * time.Minute
	const globalOverride = 3 * time.Minute

	t.Run("code_default_wins_when_no_overrides", func(t *testing.T) {
		cfg := AsyncReplicationConfig{frequency: codeDefault}
		result := cfg.Effective(replication.GlobalConfig{})
		assert.Equal(t, codeDefault, result.frequency,
			"code default must be preserved when no class or global override is set")
	})

	t.Run("class_override_beats_code_default", func(t *testing.T) {
		cfg := AsyncReplicationConfig{
			frequency: codeDefault,
			classOverrides: asyncReplicationClassOverrides{
				frequency: durationPtr(classOverride),
			},
		}
		result := cfg.Effective(replication.GlobalConfig{})
		assert.Equal(t, classOverride, result.frequency,
			"per-class override must beat the code default")
	})

	t.Run("global_config_beats_class_override", func(t *testing.T) {
		cfg := AsyncReplicationConfig{
			frequency: codeDefault,
			classOverrides: asyncReplicationClassOverrides{
				frequency: durationPtr(classOverride),
			},
		}
		globals := replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(globalOverride),
		}
		result := cfg.Effective(globals)
		assert.Equal(t, globalOverride, result.frequency,
			"global runtime config must beat both class override and code default")
	})

	t.Run("global_config_zero_preserves_class_override", func(t *testing.T) {
		// A zero DynamicValue means "not configured at cluster level" and must not
		// overwrite a class override.
		cfg := AsyncReplicationConfig{
			frequency: codeDefault,
			classOverrides: asyncReplicationClassOverrides{
				frequency: durationPtr(classOverride),
			},
		}
		globals := replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(time.Duration(0)),
		}
		result := cfg.Effective(globals)
		assert.Equal(t, classOverride, result.frequency,
			"zero global DynamicValue must not override a class-level setting")
	})

	t.Run("global_config_subfloor_clamps_to_floor", func(t *testing.T) {
		// A positive but sub-minimum global DynamicValue must clamp up to the
		// minimum rather than being silently dropped — otherwise an operator who
		// tried to dial frequency down past the minimum would get the previous
		// (class or default) value with no signal that their override was
		// ineffective.
		cfg := AsyncReplicationConfig{
			frequency: codeDefault,
			classOverrides: asyncReplicationClassOverrides{
				frequency: durationPtr(classOverride),
			},
		}
		globals := replication.GlobalConfig{
			AsyncReplicationFrequency: configRuntime.NewDynamicValue(minFrequency - time.Second),
		}
		result := cfg.Effective(globals)
		assert.Equal(t, minFrequency, result.frequency,
			"sub-minimum global DynamicValue must clamp to minFrequency, not be ignored")
	})

	t.Run("global_config_frequencyWhilePropagating_subfloor_clamps_to_its_own_floor", func(t *testing.T) {
		// Symmetric to frequency, but guards against a regression where a future
		// edit might clamp this field against minFrequency instead of its own
		// distinct minFrequencyWhilePropagating minimum.
		const fwpClassOverride = 10 * time.Second
		cfg := AsyncReplicationConfig{
			frequencyWhilePropagating: defaultFrequencyWhilePropagating,
			classOverrides: asyncReplicationClassOverrides{
				frequencyWhilePropagating: durationPtr(fwpClassOverride),
			},
		}
		globals := replication.GlobalConfig{
			AsyncReplicationFrequencyWhilePropagating: configRuntime.NewDynamicValue(minFrequencyWhilePropagating - time.Millisecond),
		}
		result := cfg.Effective(globals)
		assert.Equal(t, minFrequencyWhilePropagating, result.frequencyWhilePropagating,
			"sub-minimum global DynamicValue must clamp to minFrequencyWhilePropagating, not minFrequency, and not be ignored")
	})
}

// TestSchedulerCloseWithConcurrentDispatches verifies that Close() returns
// promptly and does not deadlock when workers are actively dispatching entries.
// This is a regression test for the unconditional resultCh send introduced in
// runEntry(): before the fix, the select-with-ctx.Done() alternative could drop
// results on shutdown, leaving entry.inFlight stuck as true. While the stuck
// state had no practical impact (the scheduler shuts down entirely), it
// represented a logical invariant violation and the select created an
// unnecessary 50/50 race between the send and ctx cancellation.
func TestSchedulerCloseWithConcurrentDispatches(t *testing.T) {
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(5),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	// Register shards with nil asyncRepCtx so runEntry returns immediately
	// via the "shard not yet initialized" fast path, then unconditionally sends
	// a result. The scheduler re-dispatches them at minDispatchInterval, keeping
	// workers active and maximising the chance that a send races with ctx
	// cancellation when Close() fires.
	const n = 20
	for range n {
		require.NoError(t, sched.Register(&Shard{}))
	}

	// Wait until the dispatcher has placed at least one entry into the worker
	// pipeline. The shards have nil asyncRepCtx so runEntry returns immediately,
	// but we must still observe that a dispatch cycle has started before calling
	// Close() to exercise the concurrent-dispatch code path.
	require.Eventually(t, func() bool {
		return len(sched.workCh) > 0 || len(sched.resultCh) > 0
	}, 200*time.Millisecond, time.Millisecond,
		"dispatcher did not produce any dispatches within 200ms")

	done := make(chan struct{})
	go func() {
		sched.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close() returned: no goroutine leak, no deadlock in result-send path.
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked — goroutine leak or deadlock in result-send path")
	}
}

// Deleted: TestSingleCyclePerShard_InFlightEntryNotRedispatched.
//
// The single-cycle-per-shard invariant (an entry cannot be dispatched twice
// concurrently) was asserted by directly poking entry.inFlight and the heap.
// The invariant is exercised end-to-end by the integration tests that register
// real shards and run real cycles; a violation would manifest as concurrent
// runHashbeatCycle invocations and shard-state corruption visible in those
// tests. Removed during the constructor-spawns-workers migration where direct
// heap mutation races with the live dispatcher.

// TestWorkerDrainsWorkChOnCtxCancel is a regression test for the asyncRepWg
// imbalance that occurs when Close() fires while work items are buffered in
// workCh. Before the fix, workers exited on ctx.Done() without draining the
// channel, leaving Add(1) calls without a matching Done(). Any subsequent
// asyncRepWg.Wait() — in particular the one inside rebuildHashtree — would
// block forever, stalling DB.Shutdown().
//
// The test verifies that after Close() returns, asyncRepWg.Wait() on every
// shard completes within a short deadline, proving the WaitGroup is balanced.
func TestWorkerDrainsWorkChOnCtxCancel(t *testing.T) {
	const numShards = 10
	const numWorkers = 2

	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(numWorkers),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, nil)
	require.NoError(t, err)

	shards := make([]*Shard, numShards)
	for i := range shards {
		shards[i] = &Shard{}
	}

	// Register all shards so the dispatcher enqueues them immediately.
	for _, s := range shards {
		require.NoError(t, sched.Register(s))
	}

	// Wait until the dispatcher has buffered at least one entry in workCh,
	// ensuring Close() fires while some items are still in the worker pipeline.
	// With nil asyncRepCtx, runEntry returns immediately; items cycle quickly,
	// so we poll until we observe a non-empty buffer rather than sleeping.
	require.Eventually(t, func() bool {
		return len(sched.workCh) > 0 || len(sched.resultCh) > 0
	}, 200*time.Millisecond, time.Millisecond,
		"dispatcher did not buffer any work items within 200ms")

	// Close cancels the scheduler context. Workers must drain any remaining
	// buffered workCh items and call Done() for each to keep asyncRepWg balanced.
	done := make(chan struct{})
	go func() { sched.Close(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked — scheduler goroutine leak or deadlock")
	}

	// After Close() the WaitGroup for every shard must reach zero promptly.
	// A timeout here means a worker exited ctx.Done() without calling Done()
	// for a buffered item, leaving the WaitGroup permanently positive.
	for i, s := range shards {
		wgDone := make(chan struct{})
		go func() { s.asyncRepWg.Wait(); close(wgDone) }()
		select {
		case <-wgDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("asyncRepWg.Wait() timed out for shard %d — WaitGroup is unbalanced", i)
		}
	}
}

// TestSchedulerClose_BoundedAndCancelsContextsWhenShardsStillRegistered verifies
// two safety properties of Close() when called without prior deregistration:
//
//  1. Close() completes within a reasonable bound (it must not block forever).
//  2. Close() force-cancels the asyncReplicationCancelFunc of every shard still
//     registered at shutdown time, so that any in-flight hashbeat RPC using the
//     per-shard context aborts promptly instead of blocking until
//     diffPerNodeTimeout.
//  3. Close() logs an error to surface the ordering bug.
//
// The shards are constructed with nil asyncRepCtx (so runEntry returns
// immediately via the "not yet initialized" fast path) but with a live
// cancellable context wired into asyncReplicationCancelFunc, mirroring the
// state a real shard has after initAsyncReplication but before mayStopAsyncReplication.
func TestSchedulerClose_BoundedAndCancelsContextsWhenShardsStillRegistered(t *testing.T) {
	logger, hook := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(3),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, logger)
	require.NoError(t, err)

	const n = 5
	shardCtxs := make([]context.Context, n)
	shards := make([]*Shard, n)
	for i := range n {
		ctx, cancel := context.WithCancel(context.Background())
		shardCtxs[i] = ctx
		// asyncRepCtx is nil: runEntry exits immediately via "not yet initialized".
		// asyncReplicationCancelFunc is non-nil: Close() must call it to bound RPCs.
		shards[i] = &Shard{asyncReplicationCancelFunc: cancel}
		require.NoError(t, sched.Register(shards[i]))
	}

	// No synchronization needed before Close(): Close() iterates sched.entries
	// to force-cancel asyncReplicationCancelFunc for every registered shard,
	// regardless of whether the entry is currently in-flight or queued in the
	// heap. The assertion below validates this property.

	// Close without deregistering — the invariant-violation path.
	done := make(chan struct{})
	go func() { sched.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked — scheduler goroutines did not exit within deadline")
	}

	// Close() must have cancelled each shard's context via asyncReplicationCancelFunc.
	for i, ctx := range shardCtxs {
		assert.Error(t, ctx.Err(),
			"shard %d: asyncReplicationCancelFunc must be called by Close() to abort in-flight RPCs", i)
	}

	// Verify the invariant-violation error was logged.
	var errorLogged bool
	for _, entry := range hook.Entries {
		if entry.Level == logrus.ErrorLevel {
			errorLogged = true
			break
		}
	}
	assert.True(t, errorLogged, "Close() must log an error when shards are still registered")
}

// durationPtr is a helper that returns a pointer to a time.Duration value.
func durationPtr(d time.Duration) *time.Duration { return &d }

// newBareScheduler builds a scheduler struct without starting any goroutines, so
// dispatchDueLocked/effectiveBatchSize can be exercised deterministically.
func newBareScheduler(batchSize, workChCap int) *AsyncReplicationScheduler {
	s := &AsyncReplicationScheduler{
		entries:                  make(map[*Shard]*asyncSchedulerEntry),
		dispatchBuckets:          make(map[*Index][]*asyncSchedulerEntry),
		workCh:                   make(chan *[]*asyncSchedulerEntry, workChCap),
		asyncReplicationDisabled: configRuntime.NewDynamicValue(false),
		rootPrefilterBatchSize:   configRuntime.NewDynamicValue(batchSize),
		logger:                   logrus.New(),
	}
	s.batchPool.New = func() any {
		b := make([]*asyncSchedulerEntry, 0, 64)
		return &b
	}
	heap.Init(&s.h)
	return s
}

func drainBatchSizes(ch chan *[]*asyncSchedulerEntry) []int {
	var sizes []int
	for {
		select {
		case bp := <-ch:
			sizes = append(sizes, len(*bp))
		default:
			return sizes
		}
	}
}

func drainBatches(ch chan *[]*asyncSchedulerEntry) []*[]*asyncSchedulerEntry {
	var batches []*[]*asyncSchedulerEntry
	for {
		select {
		case bp := <-ch:
			batches = append(batches, bp)
		default:
			return batches
		}
	}
}

func drainResults(ch chan asyncSchedulerResult) []asyncSchedulerResult {
	var results []asyncSchedulerResult
	for {
		select {
		case r := <-ch:
			results = append(results, r)
		default:
			return results
		}
	}
}

// TestRunBatchEmitsOneResultPerEntry verifies that every entry in a multi-entry
// batch produces exactly one result via runEntry — the invariant that keeps
// asyncRepWg balanced (runEntry alone owns each entry's Done()+result).
func TestRunBatchEmitsOneResultPerEntry(t *testing.T) {
	sched := newBareScheduler(512, 1)
	sched.ctx = context.Background()
	sched.resultCh = make(chan asyncSchedulerResult, 8)

	entries := make([]*asyncSchedulerEntry, 3)
	for i := range entries {
		s := &Shard{class: &models.Class{Class: "C"}}
		s.asyncRepWg.Add(1)
		entries[i] = &asyncSchedulerEntry{shard: s}
	}

	sched.runBatch(&entries, newBatchScratch())

	got := map[*asyncSchedulerEntry]int{}
	for range entries {
		select {
		case r := <-sched.resultCh:
			got[r.entry]++
		case <-time.After(time.Second):
			t.Fatal("missing result for a batch entry")
		}
	}
	for _, e := range entries {
		assert.Equal(t, 1, got[e], "each entry must yield exactly one result")
		e.shard.asyncRepWg.Wait()
	}
}

// TestAsyncSchedulerConcurrentBatchedDispatch drives the real scheduler's
// multi-entry batched path (coalesce → pooled hand-off → runBatch → per-entry
// runEntry, plus rollback and shutdown drain) while Register/Deregister race
// against Close. Shards share one MT index so due entries coalesce into batches
// of >1; uninitialised hashtrees keep classifyBatch from issuing RPCs. Run under
// -race, it guards the §concurrency-safety invariants: no data race on the batch
// machinery, no deadlock, and balanced asyncRepWg on every path.
func TestAsyncSchedulerConcurrentBatchedDispatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:       configRuntime.NewDynamicValue(4),
		AsyncReplicationDisabled:               configRuntime.NewDynamicValue(false),
		AsyncReplicationRootPrefilterBatchSize: configRuntime.NewDynamicValue(3),
	}, nil, logger)
	require.NoError(t, err)

	idx := &Index{Config: IndexConfig{ClassName: "MT"}, partitioningEnabled: true}
	shards := make([]*Shard, 30)
	for i := range shards {
		shards[i] = &Shard{index: idx, class: &models.Class{Class: "MT"}, name: fmt.Sprintf("t%02d", i)}
	}

	var wg sync.WaitGroup
	for _, s := range shards {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sched.Register(s)
			time.Sleep(5 * time.Millisecond)
			_ = sched.Deregister(s)
		}()
	}

	time.Sleep(8 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		sched.Close()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("scheduler did not terminate — possible deadlock in batched dispatch")
	}

	for _, s := range shards {
		s.asyncRepWg.Wait()
	}
}

// TestOnResultDiscardsZombieEntryAfterReRegister covers a shard deregistered
// then re-registered (same *Shard) while its cycle result is in flight: the
// stale entry must be discarded, not re-pushed alongside the fresh one.
func TestOnResultDiscardsZombieEntryAfterReRegister(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deferDescent bool
	}{
		{name: "normal re-push"},
		{name: "deferDescent re-push", deferDescent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sched := newBareScheduler(512, 1)

			s := &Shard{index: &Index{Config: IndexConfig{ClassName: "C"}}, class: &models.Class{Class: "C"}}
			sched.onAddLocked(s)
			e1 := sched.entries[s]

			// Simulate dispatch (Add+inFlight+Pop) then the worker's pre-send Done().
			s.asyncRepWg.Add(1)
			e1.inFlight = true
			heap.Pop(&sched.h)
			s.asyncRepWg.Done()

			sched.onRemoveLocked(s)
			sched.onAddLocked(s)
			e2 := sched.entries[s]
			require.NotSame(t, e1, e2)

			sched.onResultLocked(asyncSchedulerResult{entry: e1, deferDescent: tc.deferDescent})

			require.Len(t, sched.h, 1, "stale entry must not be re-pushed")
			assert.Same(t, e2, sched.h[0])
			assert.Equal(t, -1, e1.heapIdx, "stale entry stays out of the heap")
			s.asyncRepWg.Wait()
		})
	}
}

// TestAsyncSchedulerMassDivergenceDeferDescent stresses the mass-divergence path
// (node-restart scenario): one batch larger than the resultCh buffer, all
// diverging, so a single runBatch blocks mid-loop on resultCh while Deregister→
// Register race the same shards. Under -race it guards against deadlock, data
// races, duplicate heap entries, and asyncRepWg imbalance in the widened window.
func TestAsyncSchedulerMassDivergenceDeferDescent(t *testing.T) {
	logger, _ := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:       configRuntime.NewDynamicValue(4),
		AsyncReplicationDisabled:               configRuntime.NewDynamicValue(false),
		AsyncReplicationRootPrefilterBatchSize: configRuntime.NewDynamicValue(512),
	}, nil, logger)
	require.NoError(t, err)

	// n > resultCh buffer (maxMaxWorkers*2 = 200) so one runBatch overflows it.
	idx := &Index{Config: IndexConfig{ClassName: "MT"}, partitioningEnabled: true}
	const n = 256
	shards := make([]*Shard, n)
	for i := range shards {
		shards[i] = &Shard{index: idx, class: &models.Class{Class: "MT"}, name: fmt.Sprintf("t%03d", i)}
		_ = sched.Register(shards[i])
	}

	var wg sync.WaitGroup
	for _, s := range shards {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				_ = sched.Deregister(s)
				_ = sched.Register(s)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		sched.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("scheduler did not terminate — possible deadlock in mass-divergence defer path")
	}

	for _, s := range shards {
		s.asyncRepWg.Wait()
	}
}

// TestClassifyBatchExcludesIneligible verifies that shards with active
// target-node overrides or an uninitialised hashtree are excluded from the
// batched pre-filter (no RPC issued) and default to a full descent.
func TestClassifyBatchExcludesIneligible(t *testing.T) {
	sched := newBareScheduler(512, 1)
	sched.ctx = context.Background()

	override := &Shard{class: &models.Class{Class: "C"}}
	override.targetNodeOverrides = additional.AsyncReplicationTargetNodeOverrides{{TargetNode: "n2"}}
	uninit := &Shard{class: &models.Class{Class: "C"}}

	batch := []*asyncSchedulerEntry{{shard: override}, {shard: uninit}}
	scratch := newBatchScratch()

	sched.classifyBatch(batch, scratch)

	assert.Empty(t, scratch.roots, "no eligible shard ⇒ no roots collected")
	assert.Empty(t, scratch.eligible)
	for _, e := range batch {
		assert.False(t, scratch.skip[e], "ineligible shards must take the full descent")
	}
}

func TestEffectiveBatchSize(t *testing.T) {
	t.Run("configured value is used", func(t *testing.T) {
		sched := newBareScheduler(256, 1)
		assert.Equal(t, 256, sched.effectiveBatchSize())
	})

	t.Run("invalid falls back to default", func(t *testing.T) {
		sched := newBareScheduler(0, 1)
		assert.Equal(t, fallbackRootPrefilterBatchSize, sched.effectiveBatchSize())
		sched = newBareScheduler(-5, 1)
		assert.Equal(t, fallbackRootPrefilterBatchSize, sched.effectiveBatchSize())
	})

	t.Run("over cap is clamped", func(t *testing.T) {
		sched := newBareScheduler(maxRootPrefilterBatchSize+1000, 1)
		assert.Equal(t, maxRootPrefilterBatchSize, sched.effectiveBatchSize())
	})
}

func TestDispatchDueCoalescing(t *testing.T) {
	t.Run("coalesces up to batch size with a smaller final batch", func(t *testing.T) {
		sched := newBareScheduler(3, 16)
		idx := &Index{Config: IndexConfig{ClassName: "C"}}
		for range 7 {
			sched.onAddLocked(&Shard{index: idx, class: &models.Class{Class: "C"}})
		}

		sched.dispatchDueLocked()

		assert.ElementsMatch(t, []int{3, 3, 1}, drainBatchSizes(sched.workCh))
		assert.Empty(t, sched.h, "all due entries dispatched")
	})

	t.Run("batch size 1 dispatches singletons", func(t *testing.T) {
		sched := newBareScheduler(1, 16)
		idx := &Index{Config: IndexConfig{ClassName: "C"}}
		for range 4 {
			sched.onAddLocked(&Shard{index: idx, class: &models.Class{Class: "C"}})
		}

		sched.dispatchDueLocked()

		assert.ElementsMatch(t, []int{1, 1, 1, 1}, drainBatchSizes(sched.workCh))
	})

	t.Run("distinct indexes are batched separately", func(t *testing.T) {
		sched := newBareScheduler(512, 16)
		a := &Index{Config: IndexConfig{ClassName: "A"}}
		b := &Index{Config: IndexConfig{ClassName: "B"}}
		for range 3 {
			sched.onAddLocked(&Shard{index: a, class: &models.Class{Class: "A"}})
		}
		for range 2 {
			sched.onAddLocked(&Shard{index: b, class: &models.Class{Class: "B"}})
		}

		sched.dispatchDueLocked()

		assert.ElementsMatch(t, []int{3, 2}, drainBatchSizes(sched.workCh))
	})

	t.Run("full channel rolls unsent entries back into the heap", func(t *testing.T) {
		sched := newBareScheduler(1, 2)
		idx := &Index{Config: IndexConfig{ClassName: "C"}}
		for range 5 {
			sched.onAddLocked(&Shard{index: idx, class: &models.Class{Class: "C"}})
		}

		sched.dispatchDueLocked()

		assert.Len(t, drainBatchSizes(sched.workCh), 2, "channel capacity bounds dispatched batches")
		assert.Len(t, sched.h, 3, "unsent entries rolled back into the heap")
		for _, e := range sched.h {
			assert.False(t, e.inFlight, "rolled-back entries must not be in-flight")
		}
	})
}

// TestDeferDescentReDispatchesSingletons verifies the descent-spreading path:
// a coalesced multi-entry batch of diverging shards is not descended inline;
// each diverging entry is deferred back to the dispatcher (deferDescent) and
// re-dispatched as a standalone singleton so descent spreads across the pool.
// Uninitialised hashtrees make classifyBatch classify every shard as diverging
// without issuing the pre-filter RPC.
func TestDeferDescentReDispatchesSingletons(t *testing.T) {
	sched := newBareScheduler(512, 16)
	sched.ctx = context.Background()
	sched.resultCh = make(chan asyncSchedulerResult, 32)

	idx := &Index{Config: IndexConfig{ClassName: "MT"}, partitioningEnabled: true}
	const n = 5
	for i := range n {
		sched.onAddLocked(&Shard{index: idx, class: &models.Class{Class: "MT"}, name: fmt.Sprintf("t%d", i)})
	}

	sched.dispatchDueLocked()
	batches := drainBatches(sched.workCh)
	require.Len(t, batches, 1, "the due shards coalesce into a single pre-filter batch")
	require.Len(t, *batches[0], n)

	sched.runBatch(batches[0], newBatchScratch())

	results := drainResults(sched.resultCh)
	require.Len(t, results, n, "one result per entry")
	for _, r := range results {
		assert.True(t, r.deferDescent, "diverging entries are deferred, not descended inline")
		assert.NoError(t, r.err)
		sched.onResultLocked(r)
	}

	require.Len(t, sched.entries, n)
	for _, e := range sched.entries {
		assert.True(t, e.descendDirect, "deferred entries are marked for direct descent")
	}

	sched.dispatchDueLocked()
	assert.ElementsMatch(t, []int{1, 1, 1, 1, 1}, drainBatchSizes(sched.workCh),
		"diverging shards re-dispatch as singletons, never re-coalesced into one batch")
	for _, e := range sched.entries {
		assert.False(t, e.descendDirect, "descendDirect cleared once the singleton is sent")
	}
}

// TestDescendDirectSurvivesFullChannel verifies that when workCh fills mid-round,
// unsent descendDirect entries roll back retaining the flag (so they retry as
// singletons next round) and are never re-coalesced into a batch or lost.
func TestDescendDirectSurvivesFullChannel(t *testing.T) {
	sched := newBareScheduler(512, 2)
	idx := &Index{Config: IndexConfig{ClassName: "MT"}, partitioningEnabled: true}
	const n = 5
	for i := range n {
		s := &Shard{index: idx, class: &models.Class{Class: "MT"}, name: fmt.Sprintf("t%d", i)}
		sched.onAddLocked(s)
		sched.entries[s].descendDirect = true
	}

	sched.dispatchDueLocked()

	assert.ElementsMatch(t, []int{1, 1}, drainBatchSizes(sched.workCh),
		"channel capacity bounds the singletons dispatched this round")
	assert.Len(t, sched.h, n-2, "unsent divergers rolled back into the heap")
	for _, e := range sched.h {
		assert.True(t, e.descendDirect, "rolled-back divergers retain descendDirect")
		assert.False(t, e.inFlight, "rolled-back divergers must not be in-flight")
	}

	sched.dispatchDueLocked()
	assert.ElementsMatch(t, []int{1, 1}, drainBatchSizes(sched.workCh),
		"remaining divergers keep dispatching as singletons")
}
