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

package monitoring

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring/metricstest"
)

func newTestStartupMetrics(t *testing.T) (*StartupMetrics, *prometheus.Registry, time.Time) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	processStart := time.Now().Add(-time.Minute)
	return newStartupMetrics(reg, processStart), reg, processStart
}

func TestStartupMetrics_PhaseStarted(t *testing.T) {
	for _, phase := range AllStartupPhases() {
		t.Run(string(phase), func(t *testing.T) {
			m, _, _ := newTestStartupMetrics(t)

			done := m.PhaseStarted(phase)
			require.Equal(t, float64(0), testutil.ToFloat64(m.phaseDuration.WithLabelValues(string(phase))),
				"duration is only published once the phase ends")

			time.Sleep(2 * time.Millisecond)
			done()
			require.Greater(t, testutil.ToFloat64(m.phaseDuration.WithLabelValues(string(phase))), float64(0))

			for _, other := range AllStartupPhases() {
				if other == phase {
					continue
				}
				require.Equal(t, float64(0), testutil.ToFloat64(m.phaseDuration.WithLabelValues(string(other))),
					"other phases must not move")
			}
		})
	}
}

func TestStartupMetrics_PhaseStartedRecordsLastRun(t *testing.T) {
	m, _, _ := newTestStartupMetrics(t)

	done := m.PhaseStarted(StartupPhaseDBReload)
	time.Sleep(5 * time.Millisecond)
	done()
	first := testutil.ToFloat64(m.phaseDuration.WithLabelValues(string(StartupPhaseDBReload)))

	m.PhaseStarted(StartupPhaseDBReload)()
	second := testutil.ToFloat64(m.phaseDuration.WithLabelValues(string(StartupPhaseDBReload)))

	require.Less(t, second, first, "a re-run replaces the previous duration rather than accumulating")
}

func TestStartupMetrics_SetReady(t *testing.T) {
	m, _, processStart := newTestStartupMetrics(t)

	require.Equal(t, float64(0), testutil.ToFloat64(m.startupDuration), "0 until ready")
	require.Equal(t, float64(0), testutil.ToFloat64(m.readyTimestamp), "0 until ready")

	before := time.Now()
	m.SetReady()
	after := time.Now()

	ts := testutil.ToFloat64(m.readyTimestamp)
	require.GreaterOrEqual(t, ts, float64(before.UnixNano())/float64(time.Second))
	require.LessOrEqual(t, ts, float64(after.UnixNano())/float64(time.Second))

	dur := testutil.ToFloat64(m.startupDuration)
	require.InDelta(t, ts-float64(processStart.UnixNano())/float64(time.Second), dur, 0.01,
		"startup duration is measured from process start")

	time.Sleep(2 * time.Millisecond)
	m.SetReady()
	require.Equal(t, ts, testutil.ToFloat64(m.readyTimestamp), "only the first readiness counts")
	require.Equal(t, dur, testutil.ToFloat64(m.startupDuration), "only the first readiness counts")
}

func TestStartupMetrics_ObserveShardLoad(t *testing.T) {
	tests := []struct {
		registration ShardRegistration
		other        ShardRegistration
	}{
		{registration: ShardRegistrationEager, other: ShardRegistrationLazy},
		{registration: ShardRegistrationLazy, other: ShardRegistrationEager},
	}
	for _, tt := range tests {
		t.Run(string(tt.registration), func(t *testing.T) {
			m, reg, _ := newTestStartupMetrics(t)

			m.ObserveShardLoad(tt.registration, 1500*time.Millisecond)

			count, err := metricstest.SampleCount(reg, "weaviate_shard_load_duration_seconds",
				prometheus.Labels{"registration": string(tt.registration)})
			require.NoError(t, err)
			require.Equal(t, uint64(1), count)

			sum, err := metricstest.SampleSum(reg, "weaviate_shard_load_duration_seconds",
				prometheus.Labels{"registration": string(tt.registration)})
			require.NoError(t, err)
			require.InDelta(t, 1.5, sum, 1e-9, "observed in seconds")

			count, err = metricstest.SampleCount(reg, "weaviate_shard_load_duration_seconds",
				prometheus.Labels{"registration": string(tt.other)})
			require.NoError(t, err)
			require.Equal(t, uint64(0), count, "the other registration is pre-registered but untouched")
		})
	}
}

func TestStartupMetrics_ObserveVectorIndexRestore(t *testing.T) {
	m, reg, _ := newTestStartupMetrics(t)

	m.ObserveVectorIndexRestore(VectorIndexTypeHNSW, 250*time.Millisecond)

	count, err := metricstest.SampleCount(reg, "weaviate_vector_index_restore_duration_seconds",
		prometheus.Labels{"index_type": "hnsw"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	sum, err := metricstest.SampleSum(reg, "weaviate_vector_index_restore_duration_seconds",
		prometheus.Labels{"index_type": "hnsw"})
	require.NoError(t, err)
	require.InDelta(t, 0.25, sum, 1e-9)
}

func TestStartupMetrics_PrefillStarted(t *testing.T) {
	tests := []struct {
		name      string
		indexType VectorIndexType
		mode      PrefillMode
		err       error
		wantCount uint64
	}{
		{name: "hnsw sync completes", indexType: VectorIndexTypeHNSW, mode: PrefillModeSync, wantCount: 1},
		{name: "hnsw async completes", indexType: VectorIndexTypeHNSW, mode: PrefillModeAsync, wantCount: 1},
		{name: "flat sync completes", indexType: VectorIndexTypeFlat, mode: PrefillModeSync, wantCount: 1},
		{name: "hfresh async completes", indexType: VectorIndexTypeHFresh, mode: PrefillModeAsync, wantCount: 1},
		{name: "failed prefill is not observed", indexType: VectorIndexTypeHNSW, mode: PrefillModeSync, err: errors.New("boom"), wantCount: 0},
		{name: "aborted prefill is not observed", indexType: VectorIndexTypeHNSW, mode: PrefillModeAsync, err: errors.New("context canceled"), wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg, _ := newTestStartupMetrics(t)
			labels := prometheus.Labels{"index_type": string(tt.indexType), "mode": string(tt.mode)}

			done := m.PrefillStarted(tt.indexType, tt.mode)
			require.Equal(t, float64(1), testutil.ToFloat64(m.prefillActive.With(labels)),
				"active while the prefill runs")

			done(tt.err)
			require.Equal(t, float64(0), testutil.ToFloat64(m.prefillActive.With(labels)),
				"active drops whatever the outcome")

			count, err := metricstest.SampleCount(reg, "weaviate_vector_cache_prefill_duration_seconds", labels)
			require.NoError(t, err)
			require.Equal(t, tt.wantCount, count)
		})
	}
}

func TestStartupMetrics_PrefillActiveCountsConcurrentRuns(t *testing.T) {
	m, _, _ := newTestStartupMetrics(t)
	labels := prometheus.Labels{"index_type": "hnsw", "mode": "async"}

	done1 := m.PrefillStarted(VectorIndexTypeHNSW, PrefillModeAsync)
	done2 := m.PrefillStarted(VectorIndexTypeHNSW, PrefillModeAsync)
	require.Equal(t, float64(2), testutil.ToFloat64(m.prefillActive.With(labels)))
	done1(nil)
	require.Equal(t, float64(1), testutil.ToFloat64(m.prefillActive.With(labels)))
	done2(nil)
	require.Equal(t, float64(0), testutil.ToFloat64(m.prefillActive.With(labels)))
}

func TestStartupMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *StartupMetrics

	require.NotPanics(t, func() {
		m.PhaseStarted(StartupPhaseDBReload)()
		m.SetReady()
		m.ObserveShardLoad(ShardRegistrationEager, time.Second)
		m.ObserveVectorIndexRestore(VectorIndexTypeHNSW, time.Second)
		m.PrefillStarted(VectorIndexTypeHNSW, PrefillModeSync)(nil)
		m.PrefillStarted(VectorIndexTypeHNSW, PrefillModeSync)(errors.New("boom"))
	})
}

// Every label combination is pre-registered so a fresh node scrapes zeros
// rather than omitting series, and the set is fixed regardless of how many
// collections or tenants the node holds.
func TestStartupMetrics_PreRegisteredSeries(t *testing.T) {
	m, reg, _ := newTestStartupMetrics(t)

	// Which phase is running was a gauge of its own once; a stuck node is
	// still visible as a phase whose duration stays 0 while the process is
	// up, and the logs name the phase, so its five series were not worth
	// their cost.
	t.Run("no phase active gauge", func(t *testing.T) {
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, family := range families {
			require.NotEqual(t, "weaviate_startup_phase_active", family.GetName())
		}
	})

	tests := []struct {
		name      string
		collector prometheus.Collector
		want      int
	}{
		{name: "phase duration", collector: m.phaseDuration, want: len(AllStartupPhases())},
		{name: "startup duration", collector: m.startupDuration, want: 1},
		{name: "ready timestamp", collector: m.readyTimestamp, want: 1},
		{name: "shard load", collector: m.shardLoad, want: 2},
		{name: "vector index restore", collector: m.vectorIndexRestore, want: 1},
		{name: "prefill duration", collector: m.prefillDuration, want: 4},
		{name: "prefill active", collector: m.prefillActive, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, testutil.CollectAndCount(tt.collector))
		})
	}
}

// The timing metrics deliberately expose only _sum and _count: a fixed cost of
// two series per label combination on every node, instead of a bucket set
// that multiplies by node count in hosted setups.
func TestStartupMetrics_TimingMetricsExposeOnlySumAndCount(t *testing.T) {
	m, reg, _ := newTestStartupMetrics(t)
	m.ObserveShardLoad(ShardRegistrationEager, time.Second)
	m.ObserveVectorIndexRestore(VectorIndexTypeHNSW, time.Second)
	m.PrefillStarted(VectorIndexTypeHNSW, PrefillModeSync)(nil)

	families, err := reg.Gather()
	require.NoError(t, err)
	byName := map[string]*dto.MetricFamily{}
	for _, family := range families {
		byName[family.GetName()] = family
	}

	for _, name := range []string{
		"weaviate_shard_load_duration_seconds",
		"weaviate_vector_index_restore_duration_seconds",
		"weaviate_vector_cache_prefill_duration_seconds",
	} {
		t.Run(name, func(t *testing.T) {
			family, ok := byName[name]
			require.True(t, ok, "metric must be registered")
			require.Equal(t, dto.MetricType_SUMMARY, family.GetType(), "sum/count only, no buckets")
			for _, metric := range family.GetMetric() {
				require.Empty(t, metric.GetSummary().GetQuantile(), "no quantile series either")
			}
		})
	}
}

func TestStartupMetrics_Singleton(t *testing.T) {
	require.NotNil(t, GetStartupMetrics())
	require.Same(t, GetStartupMetrics(), GetStartupMetrics())
}

// TrackReady turns the readiness predicate into the time-to-ready gauges.
// Nothing polls that predicate outside the kubernetes probe, so the tracker
// has to.
func TestStartupMetrics_TrackReady(t *testing.T) {
	tests := []struct {
		name       string
		readyAfter int32 // not-ready answers before the first ready one
		cancel     bool
		want       bool
	}{
		{name: "ready on the first check", readyAfter: 0, want: true},
		{name: "ready after a few checks", readyAfter: 3, want: true},
		{name: "cancelled before ready", readyAfter: 1 << 30, cancel: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _, _ := newTestStartupMetrics(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var calls atomic.Int32
			isReady := func() bool {
				n := calls.Add(1)
				if tt.cancel && n == 2 {
					cancel()
				}
				return n > tt.readyAfter
			}

			got := m.TrackReady(ctx, isReady, time.Millisecond)

			require.Equal(t, tt.want, got)
			if tt.want {
				require.Equal(t, tt.readyAfter+1, calls.Load(), "returns on the first ready answer")
				require.Greater(t, testutil.ToFloat64(m.readyTimestamp), float64(0), "the first ready answer is recorded")
			} else {
				require.Equal(t, float64(0), testutil.ToFloat64(m.readyTimestamp), "a cancelled tracker records nothing")
			}
		})
	}

	t.Run("nil receiver returns false without polling", func(t *testing.T) {
		var m *StartupMetrics
		require.False(t, m.TrackReady(context.Background(), func() bool { return true }, time.Millisecond))
	})
}
