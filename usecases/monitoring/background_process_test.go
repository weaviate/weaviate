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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// histogramSampleCount returns how many durations have been observed for a
// process. GetMetricWithLabelValues lazily creates an empty (count 0) series,
// so it is safe to call before any observation.
func histogramSampleCount(t *testing.T, m *BackgroundProcessMetrics, process string) uint64 {
	t.Helper()
	obs, err := m.duration.GetMetricWithLabelValues(process)
	require.NoError(t, err)
	var out dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&out))
	return out.GetHistogram().GetSampleCount()
}

func TestBackgroundProcessMetrics_StartedDone(t *testing.T) {
	tests := []struct {
		name       string
		run        func(m *BackgroundProcessMetrics)
		wantActive float64
	}{
		{
			name: "single run leaves gauge at zero when done",
			run: func(m *BackgroundProcessMetrics) {
				done := m.Started(ProcessBackup)
				done()
			},
			wantActive: 0,
		},
		{
			name: "running instance keeps gauge raised",
			run: func(m *BackgroundProcessMetrics) {
				_ = m.Started(ProcessBackup)
			},
			wantActive: 1,
		},
		{
			name: "concurrent runs accumulate",
			run: func(m *BackgroundProcessMetrics) {
				done1 := m.Started(ProcessBackup)
				_ = m.Started(ProcessBackup) // still running
				done1()
			},
			wantActive: 1,
		},
		{
			name:       "no runs",
			run:        func(m *BackgroundProcessMetrics) {},
			wantActive: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newBackgroundProcessMetrics(prometheus.NewPedanticRegistry())
			tt.run(m)
			assert.Equal(t, tt.wantActive,
				testutil.ToFloat64(m.active.WithLabelValues("backup")))
		})
	}
}

func TestBackgroundProcessMetrics_DurationRecordedOnDone(t *testing.T) {
	m := newBackgroundProcessMetrics(prometheus.NewPedanticRegistry())

	// A run still in progress has recorded no duration yet.
	done := m.Started(ProcessBackup)
	assert.Equal(t, uint64(0), histogramSampleCount(t, m, "backup"))

	// Completing the run records exactly one observation and drops the gauge.
	done()
	assert.Equal(t, uint64(1), histogramSampleCount(t, m, "backup"))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.active.WithLabelValues("backup")))

	// A second run adds a second observation, keyed to its own process.
	m.Started(ProcessCompaction)()
	assert.Equal(t, uint64(1), histogramSampleCount(t, m, "compaction"))
	assert.Equal(t, uint64(1), histogramSampleCount(t, m, "backup"))
}

func TestBackgroundProcessMetrics_Failed(t *testing.T) {
	m := newBackgroundProcessMetrics(prometheus.NewPedanticRegistry())

	// Failures are independent of liveness/duration: a run can finish and still
	// be recorded as failed.
	m.Started(ProcessBackup)()
	assert.Equal(t, float64(0), testutil.ToFloat64(m.failures.WithLabelValues("backup")))

	m.Failed(ProcessBackup)
	m.Failed(ProcessBackup)
	assert.Equal(t, float64(2), testutil.ToFloat64(m.failures.WithLabelValues("backup")))

	// Keyed per process; unrelated processes stay at zero.
	assert.Equal(t, float64(0), testutil.ToFloat64(m.failures.WithLabelValues("restore")))
}

func TestBackgroundProcessMetrics_IncDecActive(t *testing.T) {
	m := newBackgroundProcessMetrics(prometheus.NewPedanticRegistry())

	m.IncActive(ProcessReplicaMovement)
	m.IncActive(ProcessReplicaMovement)
	assert.Equal(t, float64(2), testutil.ToFloat64(m.active.WithLabelValues("replica_movement")))

	m.DecActive(ProcessReplicaMovement)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.active.WithLabelValues("replica_movement")))
}

func TestBackgroundProcessMetrics_ProcessesAreIndependent(t *testing.T) {
	m := newBackgroundProcessMetrics(prometheus.NewPedanticRegistry())

	_ = m.Started(ProcessCompaction)
	_ = m.Started(ProcessRestore)

	assert.Equal(t, float64(1), testutil.ToFloat64(m.active.WithLabelValues("compaction")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.active.WithLabelValues("restore")))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.active.WithLabelValues("backup")))
}

func TestBackgroundProcessMetrics_NilSafe(t *testing.T) {
	var m *BackgroundProcessMetrics

	assert.NotPanics(t, func() {
		done := m.Started(ProcessBackup)
		m.IncActive(ProcessBackup)
		m.DecActive(ProcessBackup)
		done()
	})
}
