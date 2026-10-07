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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring/testinghelpers"
)

func newTestStartupMetrics(t *testing.T) (*StartupMetrics, *prometheus.Registry, time.Time) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	processStart := time.Now().Add(-time.Minute)
	return newStartupMetrics(reg, processStart), reg, processStart
}

func TestStartupMetrics_SetReady(t *testing.T) {
	m, _, processStart := newTestStartupMetrics(t)

	require.Equal(t, float64(0), testutil.ToFloat64(m.startupDuration), "0 until ready")

	before := time.Now()
	m.SetReady()
	after := time.Now()

	dur := testutil.ToFloat64(m.startupDuration)
	require.GreaterOrEqual(t, dur, before.Sub(processStart).Seconds(), "measured from process start")
	require.LessOrEqual(t, dur, after.Sub(processStart).Seconds(), "measured from process start")

	time.Sleep(2 * time.Millisecond)
	m.SetReady()
	require.Equal(t, dur, testutil.ToFloat64(m.startupDuration), "only the first readiness counts")
}

// TrackReady turns the readiness check into the time-to-ready gauge; nothing
// else polls that check, so the tracker has to.
func TestStartupMetrics_TrackReady(t *testing.T) {
	tests := []struct {
		name       string
		readyAfter int32 // the check answers true from call readyAfter+1 on
		cancel     bool  // cancel the context on the second call instead
		want       bool
	}{
		{name: "ready on the first poll", readyAfter: 0, want: true},
		{name: "ready after three polls", readyAfter: 3, want: true},
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
				require.Greater(t, testutil.ToFloat64(m.startupDuration), float64(0), "the first ready answer is recorded")
			} else {
				require.Equal(t, float64(0), testutil.ToFloat64(m.startupDuration), "a cancelled tracker records nothing")
			}
		})
	}
}

func TestStartupMetrics_ObserveVectorIndexRestore(t *testing.T) {
	m, reg, _ := newTestStartupMetrics(t)

	m.ObserveVectorIndexRestore(1500 * time.Millisecond)

	count, err := testinghelpers.SampleCount(reg, "weaviate_vector_index_restore_duration_seconds", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	sum, err := testinghelpers.SampleSum(reg, "weaviate_vector_index_restore_duration_seconds", nil)
	require.NoError(t, err)
	require.InDelta(t, 1.5, sum, 1e-9, "observed in seconds")
}

func TestStartupMetrics_PrefillStarted(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantCount uint64
	}{
		{name: "a completed prefill is observed", wantCount: 1},
		{name: "a failed prefill is not observed", err: errors.New("boom"), wantCount: 0},
		{name: "an aborted prefill is not observed", err: context.Canceled, wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, reg, _ := newTestStartupMetrics(t)

			done := m.PrefillStarted()
			done(tt.err)

			count, err := testinghelpers.SampleCount(reg, "weaviate_vector_cache_prefill_duration_seconds", nil)
			require.NoError(t, err)
			require.Equal(t, tt.wantCount, count)
		})
	}
}

func TestStartupMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *StartupMetrics

	require.NotPanics(t, func() {
		m.SetReady()
		m.ObserveVectorIndexRestore(time.Second)
		m.PrefillStarted()(nil)
		m.PrefillStarted()(errors.New("boom"))
		require.False(t, m.TrackReady(context.Background(), func() bool { return true }, time.Millisecond))
	})
}

// The whole set is five unlabelled series per node: one gauge and two
// summaries that expose only _sum and _count, no quantiles and no buckets.
func TestStartupMetrics_FiveSeriesPerNode(t *testing.T) {
	m, reg, _ := newTestStartupMetrics(t)
	m.SetReady()
	m.ObserveVectorIndexRestore(time.Second)
	m.PrefillStarted()(nil)

	families, err := reg.Gather()
	require.NoError(t, err)

	series := 0
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.Empty(t, metric.GetLabel(), "%s carries no labels", family.GetName())
			switch {
			case metric.GetGauge() != nil:
				series++
			case metric.GetSummary() != nil:
				require.Empty(t, metric.GetSummary().GetQuantile(), "%s exposes no quantiles", family.GetName())
				series += 2 // _sum and _count
			default:
				t.Fatalf("%s is neither a gauge nor a summary", family.GetName())
			}
		}
	}
	require.Equal(t, 5, series)
}
