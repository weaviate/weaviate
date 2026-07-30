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

package replication_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	clusterTypes "github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	deletedMetric    = "weaviate_replication_operation_cleanup_deleted_total"
	failuresMetric   = "weaviate_replication_operation_cleanup_failures_total"
	ineligibleMetric = "weaviate_replication_operation_cleanup_ineligible"
)

// recordingRemover is the hand-rolled fake for StaleOpRemover. onCall runs before
// the result is returned, which is how the demotion-mid-tick row flips leadership.
type recordingRemover struct {
	mu     sync.Mutex
	calls  [][]uint64
	err    error
	onCall func(callNo int)
}

func (r *recordingRemover) ForceDeleteReplicationsByIds(ctx context.Context, ids []uint64) error {
	r.mu.Lock()
	r.calls = append(r.calls, append([]uint64(nil), ids...))
	callNo := len(r.calls)
	r.mu.Unlock()
	if r.onCall != nil {
		r.onCall(callNo)
	}
	return r.err
}

func (r *recordingRemover) recorded() [][]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]uint64, len(r.calls))
	copy(out, r.calls)
	return out
}

// cleanerHarness owns the mutable knobs a test flips between ticks. Leadership is
// a single atomic.Bool behind both leadership closures.
type cleanerHarness struct {
	cleaner  *replication.OpCleaner
	fsm      *replication.ShardReplicationFSM
	remover  *recordingRemover
	clock    *clockwork.FakeClock
	registry *prometheus.Registry
	logs     *logrustest.Hook

	leader           *atomic.Bool
	enabled          *runtime.DynamicValue[bool]
	maxAge           *runtime.DynamicValue[time.Duration]
	interval         *runtime.DynamicValue[time.Duration]
	includeCancelled *runtime.DynamicValue[bool]
}

func newCleanerHarness(t *testing.T) *cleanerHarness {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	h := &cleanerHarness{
		fsm:              replication.NewShardReplicationFSM(prometheus.NewPedanticRegistry()),
		remover:          &recordingRemover{},
		clock:            clockwork.NewFakeClock(),
		registry:         prometheus.NewPedanticRegistry(),
		logs:             hook,
		leader:           &atomic.Bool{},
		enabled:          runtime.NewDynamicValue(true),
		maxAge:           runtime.NewDynamicValue(24 * time.Hour),
		interval:         runtime.NewDynamicValue(time.Hour),
		includeCancelled: runtime.NewDynamicValue(false),
	}
	h.leader.Store(true)

	cleaner, err := replication.NewOpCleaner(h.params(logger))
	require.NoError(t, err)
	h.cleaner = cleaner
	return h
}

func (h *cleanerHarness) params(logger *logrus.Logger) replication.OpCleanerParams {
	return replication.OpCleanerParams{
		Logger:           logger,
		NodeID:           "node1",
		FSM:              h.fsm,
		Remover:          h.remover,
		Clock:            h.clock,
		Registerer:       h.registry,
		ReadyToSweep:     h.leader.Load,
		IsLeader:         h.leader.Load,
		Enabled:          h.enabled.Get,
		MaxAge:           h.maxAge.Get,
		Interval:         h.interval.Get,
		IncludeCancelled: h.includeCancelled.Get,
		Jitter:           func(d time.Duration) time.Duration { return d },
	}
}

// seedAncient seeds n ops with ids startID..startID+n-1, aged far past the
// harness's 24h max age.
func (h *cleanerHarness) seedAncient(t testing.TB, startID uint64, n int, state api.ShardReplicationState, flagged bool) {
	t.Helper()
	specs := make([]seedSpec, 0, n)
	for i := 0; i < n; i++ {
		specs = append(specs, seedSpec{
			id:           startID + uint64(i),
			state:        state,
			stateStartMs: h.clock.Now().Add(-365 * 24 * time.Hour).UnixMilli(),
			shouldDelete: flagged,
			collection:   "TestClass",
		})
	}
	seedViaRestore(t, h.fsm, specs...)
}

// seedMixed replaces the FSM contents in one Restore call: Restore clears first,
// so two calls would drop the first batch.
func (h *cleanerHarness) seedMixed(t testing.TB, ready, cancelled int) {
	t.Helper()
	specs := make([]seedSpec, 0, ready+cancelled)
	ancient := h.clock.Now().Add(-365 * 24 * time.Hour).UnixMilli()
	for i := 0; i < ready; i++ {
		specs = append(specs, seedSpec{id: uint64(i + 1), state: api.READY, stateStartMs: ancient})
	}
	for i := 0; i < cancelled; i++ {
		specs = append(specs, seedSpec{id: uint64(ready + i + 1), state: api.CANCELLED, stateStartMs: ancient})
	}
	seedViaRestore(t, h.fsm, specs...)
}

// metricValue reads one gathered metric by name and label values, returning 0 when
// the series has never been touched.
func metricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metric:
		for _, m := range family.GetMetric() {
			for k, v := range labels {
				found := false
				for _, l := range m.GetLabel() {
					if l.GetName() == k && l.GetValue() == v {
						found = true
					}
				}
				if !found {
					continue metric
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			if m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func TestNewOpCleaner_RejectsNilGetters(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	cases := []struct {
		name   string
		mangle func(p *replication.OpCleanerParams)
	}{
		{"nil Logger", func(p *replication.OpCleanerParams) { p.Logger = nil }},
		{"nil FSM", func(p *replication.OpCleanerParams) { p.FSM = nil }},
		{"nil Remover", func(p *replication.OpCleanerParams) { p.Remover = nil }},
		{"nil Clock", func(p *replication.OpCleanerParams) { p.Clock = nil }},
		{"nil Registerer", func(p *replication.OpCleanerParams) { p.Registerer = nil }},
		{"nil ReadyToSweep", func(p *replication.OpCleanerParams) { p.ReadyToSweep = nil }},
		{"nil IsLeader", func(p *replication.OpCleanerParams) { p.IsLeader = nil }},
		{"nil Enabled", func(p *replication.OpCleanerParams) { p.Enabled = nil }},
		{"nil MaxAge", func(p *replication.OpCleanerParams) { p.MaxAge = nil }},
		{"nil Interval", func(p *replication.OpCleanerParams) { p.Interval = nil }},
		{"nil IncludeCancelled", func(p *replication.OpCleanerParams) { p.IncludeCancelled = nil }},
		{"nil Jitter", func(p *replication.OpCleanerParams) { p.Jitter = nil }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh harness per row: promauto panics on duplicate registration.
			h := newCleanerHarness(t)
			params := h.params(logger)
			params.Registerer = prometheus.NewPedanticRegistry()
			tc.mangle(&params)

			cleaner, err := replication.NewOpCleaner(params)
			require.Error(t, err, "a nil dependency must be rejected, not silently disable the sweep")
			require.Nil(t, cleaner)
		})
	}
}

func TestOpCleaner_NotReady(t *testing.T) {
	t.Run("a follower does nothing", func(t *testing.T) {
		h := newCleanerHarness(t)
		h.seedAncient(t, 1, 10, api.READY, false)
		h.leader.Store(false)

		removed, err := h.cleaner.Tick(context.Background())
		require.NoError(t, err)
		require.Zero(t, removed)
		require.Empty(t, h.remover.recorded())
	})

	t.Run("promotion between ticks is picked up", func(t *testing.T) {
		h := newCleanerHarness(t)
		h.seedAncient(t, 1, 10, api.READY, false)
		h.leader.Store(false)

		removed, err := h.cleaner.Tick(context.Background())
		require.NoError(t, err)
		require.Zero(t, removed)

		h.leader.Store(true)
		removed, err = h.cleaner.Tick(context.Background())
		require.NoError(t, err)
		require.Equal(t, 10, removed, "the gate must be read on every tick, not captured at construction")
	})

	t.Run("demotion mid-tick stops at the next chunk boundary", func(t *testing.T) {
		h := newCleanerHarness(t)
		h.seedAncient(t, 1, 3000, api.READY, false)
		h.remover.onCall = func(callNo int) {
			if callNo == 1 {
				h.leader.Store(false)
			}
		}

		removed, err := h.cleaner.Tick(context.Background())
		require.NoError(t, err, "a demotion is a deferral, not a failure")
		require.Equal(t, 1000, removed)
		require.Len(t, h.remover.recorded(), 1, "the remaining chunks must be deferred to the new leader's own tick")
		require.Zero(t, metricValue(t, h.registry, failuresMetric, nil))
	})
}

func TestOpCleaner_DisableSwitch(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 10, api.READY, false)
	h.enabled.SetValue(false)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Empty(t, h.remover.recorded())

	h.enabled.SetValue(true)
	removed, err = h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, removed, "the enabled flag must be read on every tick")
}

// TestOpCleaner_ZeroMaxAgeDoesNotDeleteEverything is the highest-consequence row in
// this file: the naive cutoff = now - 0 implementation would delete every READY op
// in the cluster and pass every other test here.
func TestOpCleaner_ZeroMaxAgeDoesNotDeleteEverything(t *testing.T) {
	for _, maxAge := range []time.Duration{0, -time.Hour} {
		t.Run(fmt.Sprintf("max age %s", maxAge), func(t *testing.T) {
			h := newCleanerHarness(t)
			h.seedAncient(t, 1, 100, api.READY, false)
			h.maxAge.SetValue(maxAge)

			removed, err := h.cleaner.Tick(context.Background())
			require.NoError(t, err)
			require.Zero(t, removed)
			require.Empty(t, h.remover.recorded(), "a non-positive max age disables the sweep, it never means delete everything")
		})
	}
}

func TestOpCleaner_Pacing(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 25_000, api.READY, false)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10_000, removed)

	calls := h.remover.recorded()
	require.Len(t, calls, 10)
	firstTickMax := uint64(0)
	for _, ids := range calls {
		require.Len(t, ids, 1000)
		for _, id := range ids {
			if id > firstTickMax {
				firstTickMax = id
			}
		}
	}

	// The fake remover does not mutate the FSM, so drop the first tick's ids from
	// it by hand to model the applied removals.
	require.NoError(t, h.fsm.ForceDeleteByIds(flatten(calls)))

	removed, err = h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10_000, removed)

	for _, ids := range h.remover.recorded()[10:] {
		for _, id := range ids {
			require.Greater(t, id, firstTickMax, "selection is lowest-id-first, so the second tick takes strictly later ops")
		}
	}
}

// TestOpCleaner_PacingWithCancelledIncluded pins that the per-tick budget is per
// tick, not per state: widening the predicate must not raise the RAFT volume.
func TestOpCleaner_PacingWithCancelledIncluded(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedMixed(t, 15_000, 15_000)
	h.includeCancelled.SetValue(true)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10_000, removed)
	require.Len(t, h.remover.recorded(), 10)
}

func TestOpCleaner_PartialBatch(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 1500, api.READY, false)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1500, removed)

	calls := h.remover.recorded()
	require.Len(t, calls, 2)
	require.Len(t, calls[0], 1000)
	require.Len(t, calls[1], 500)
}

func TestOpCleaner_StopsOnRemoverError(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 3000, api.READY, false)
	h.remover.err = errors.New("raft is unhappy")

	removed, err := h.cleaner.Tick(context.Background())
	require.Error(t, err)
	require.Zero(t, removed)
	require.Len(t, h.remover.recorded(), 1, "the tick stops on the first failure")
	require.Equal(t, 1.0, metricValue(t, h.registry, failuresMetric, nil))
	require.Zero(t, metricValue(t, h.registry, deletedMetric, map[string]string{"state": "READY"}))
}

// TestOpCleaner_LostElectionIsNotAFailure: a demotion discovered inside Execute is
// a deferral to the new leader, not an error worth alerting on.
func TestOpCleaner_LostElectionIsNotAFailure(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 3000, api.READY, false)
	h.remover.err = fmt.Errorf("submit batch: %w", clusterTypes.ErrNotLeader)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Len(t, h.remover.recorded(), 1)
	require.Zero(t, metricValue(t, h.registry, failuresMetric, nil))
}

func TestOpCleaner_DeletedMetricSplitsByState(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedMixed(t, 7, 3)
	h.includeCancelled.SetValue(true)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, removed)

	require.Equal(t, 7.0, metricValue(t, h.registry, deletedMetric, map[string]string{"state": "READY"}))
	require.Equal(t, 3.0, metricValue(t, h.registry, deletedMetric, map[string]string{"state": "CANCELLED"}))
}

// TestOpCleaner_ReportsIneligibleFlaggedOps: the flagged population is the only
// explanation an operator gets for a READY gauge that plateaus above zero.
func TestOpCleaner_ReportsIneligibleFlaggedOps(t *testing.T) {
	h := newCleanerHarness(t)

	ancient := h.clock.Now().Add(-365 * 24 * time.Hour).UnixMilli()
	specs := make([]seedSpec, 0, 17)
	for i := 1; i <= 10; i++ {
		specs = append(specs, seedSpec{id: uint64(i), state: api.READY, stateStartMs: ancient})
	}
	for i := 11; i <= 17; i++ {
		specs = append(specs, seedSpec{id: uint64(i), state: api.READY, stateStartMs: ancient, shouldDelete: true})
	}
	seedViaRestore(t, h.fsm, specs...)

	removed, err := h.cleaner.Tick(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, removed)

	require.Equal(t, 7.0, metricValue(t, h.registry, ineligibleMetric, map[string]string{"reason": "flagged"}))
	require.Equal(t, [][]uint64{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}}, h.remover.recorded())

	var logged bool
	for _, entry := range h.logs.AllEntries() {
		if entry.Level == logrus.InfoLevel && strings.Contains(entry.Message, "7 age-eligible but flagged") {
			logged = true
		}
	}
	require.True(t, logged, "the flagged count must reach the operator-facing log line")
}

// TestOpCleaner_HotReloadsInterval pins that Interval() is re-read every cycle,
// which is the whole hot-reload mechanism (there is no ticker to Reset).
func TestOpCleaner_HotReloadsInterval(t *testing.T) {
	h := newCleanerHarness(t)
	h.seedAncient(t, 1, 1, api.READY, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.cleaner.Run(ctx)
	}()

	requireParked(t, h.clock)

	// One tick at the original interval, which is also when the new interval is read.
	h.interval.SetValue(10 * time.Minute)
	h.clock.Advance(time.Hour)
	requireParked(t, h.clock)
	require.Eventually(t, func() bool { return len(h.remover.recorded()) == 1 }, time.Second, 5*time.Millisecond)

	h.clock.Advance(10*time.Minute - time.Millisecond)
	requireParked(t, h.clock)
	require.Len(t, h.remover.recorded(), 1, "the loop must still be waiting out the new, shorter interval")

	h.clock.Advance(time.Millisecond)
	require.Eventually(t, func() bool { return len(h.remover.recorded()) == 2 }, time.Second, 5*time.Millisecond)

	cancel()
	<-done
}

// TestOpCleaner_NonPositiveInterval is the operability sibling of the zero-max-age
// warning in Tick: a knob left at 0 must not silently substitute the built-in
// re-check period. A disabled sweep is not misconfigured, so it stays quiet.
func TestOpCleaner_NonPositiveInterval(t *testing.T) {
	cases := []struct {
		name     string
		enabled  bool
		wantWarn bool
	}{
		{name: "enabled with a zero interval warns", enabled: true, wantWarn: true},
		{name: "disabled with a zero interval stays quiet", enabled: false, wantWarn: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCleanerHarness(t)
			h.seedAncient(t, 1, 1, api.READY, false)
			h.enabled.SetValue(tc.enabled)
			h.interval.SetValue(0)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = h.cleaner.Run(ctx)
			}()

			// A non-positive interval also drives the first wait, which is the
			// unexported defaultCleanupInterval, i.e. an hour.
			requireParked(t, h.clock)
			h.clock.Advance(time.Hour)
			// Parked again ⇒ the loop has been all the way round, so the log and the
			// remover can be read without racing it.
			requireParked(t, h.clock)

			require.Equal(t, tc.wantWarn, loggedWarning(h.logs, "REPLICA_MOVEMENT_CLEANUP_INTERVAL"),
				"the substituted re-check period must be visible to an operator exactly when the sweep is on")
			require.Empty(t, h.remover.recorded(), "a non-positive interval must skip the sweep entirely")

			cancel()
			<-done
		})
	}
}

func loggedWarning(hook *logrustest.Hook, substr string) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, substr) {
			return true
		}
	}
	return false
}

func TestOpCleaner_RunStopsOnContextCancel(t *testing.T) {
	h := newCleanerHarness(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.cleaner.Run(ctx)
	}()

	requireParked(t, h.clock)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "Run did not return promptly on context cancel")
	}
	require.False(t, clockHasWaiter(h.clock), "nothing may be left parked on the clock after Run returns")
}

// requireParked waits until the loop is parked on the fake clock. A bare Advance
// races: between the timer firing and Run re-arming via clock.After the fake clock
// has zero waiters.
func requireParked(t *testing.T, clock *clockwork.FakeClock) {
	t.Helper()
	require.Eventually(t, func() bool { return clockHasWaiter(clock) }, time.Second, 2*time.Millisecond,
		"the cleanup loop never parked on the fake clock")
}

// clockHasWaiter probes whether the FakeClock has at least one waiter.
// BlockUntilContext(ctx, 1) returns nil immediately when one is registered and
// context.DeadlineExceeded when there are none; clockwork exports no NumWaiters,
// so this asymmetric probe is the only way to tell the two apart.
func clockHasWaiter(clock *clockwork.FakeClock) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	return clock.BlockUntilContext(ctx, 1) == nil
}

func flatten(calls [][]uint64) []uint64 {
	var out []uint64
	for _, ids := range calls {
		out = append(out, ids...)
	}
	return out
}
