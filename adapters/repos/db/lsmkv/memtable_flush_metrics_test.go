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

package lsmkv

import (
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type flushFailureCommitLogger struct {
	memtableCommitLogger
	failClose  bool
	failDelete bool
}

func (cl *flushFailureCommitLogger) close() error {
	if cl.failClose {
		return errors.New("close failed")
	}
	return cl.memtableCommitLogger.close()
}

func (cl *flushFailureCommitLogger) delete() error {
	if cl.failDelete {
		return errors.New("delete failed")
	}
	return cl.memtableCommitLogger.delete()
}

func newTestFlushMetrics() *memtableMetrics {
	return &memtableMetrics{
		flushingCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_flush_total"}, []string{"strategy"}),
		flushingInProgress: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{Name: "test_flush_in_progress"}, []string{"strategy"}),
		flushingFailureCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "test_flush_failures_total"}, []string{"strategy"}),
		flushingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "test_flush_duration"}, []string{"strategy"}),
		flushMemtableSize: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{Name: "test_flush_size"}, []string{"strategy"}),
		flushMemtableBytesWritten: func(int64) {},
	}
}

// TestSegmentWriteMetrics pins which series a segment write moves and which a
// commit-log failure moves.
func TestSegmentWriteMetrics(t *testing.T) {
	tests := []struct {
		name       string
		failClose  bool
		failDelete bool
		breakWrite bool
		empty      bool
		// when set, the memtable is written through writeSegmentTo rather than
		// flush, which is how a replay stages a chunk
		suffix       string
		wantErr      bool
		wantCount    float64
		wantFailures float64
		wantSeries   int
	}{
		{
			name: "close fails before any segment write", failClose: true, wantErr: true,
			wantCount: 1, wantFailures: 1, wantSeries: 0,
		},
		{
			name: "delete fails after the segment is written", failDelete: true, wantErr: true,
			wantCount: 1, wantFailures: 1, wantSeries: 1,
		},
		{
			name: "empty memtable writes no segment", empty: true,
			wantCount: 0, wantFailures: 0, wantSeries: 0,
		},
		{
			name: "segment write fails", breakWrite: true, wantErr: true,
			wantCount: 1, wantFailures: 1, wantSeries: 0,
		},
		{
			name: "empty memtable whose commit log will not delete", empty: true, failDelete: true,
			wantErr: true, wantCount: 1, wantFailures: 1, wantSeries: 0,
		},
		{name: "flush", wantCount: 1, wantFailures: 0, wantSeries: 1},
		{
			// the path a replay takes for a chunk: it writes a segment without
			// touching the commit log, so the WAL it is reading stays open
			name: "written with a suffix, bypassing flush", suffix: DeleteMarkerSuffix,
			wantCount: 1, wantFailures: 0, wantSeries: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetricsTestMemtable(t, tt.failClose, tt.failDelete, !tt.empty)
			if tt.breakWrite {
				// the commit log keeps the handle it already opened, so only the
				// segment write meets the missing directory
				m.path = filepath.Join(filepath.Dir(m.path), "gone", "segment-1")
			}

			var err error
			if tt.suffix != "" {
				_, err = m.writeSegmentTo(m.path, tt.suffix)
			} else {
				_, err = m.flush()
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			requireSegmentWriteMetrics(t, m, tt.wantCount, tt.wantFailures, tt.wantSeries)

			if tt.suffix != "" {
				require.FileExists(t, m.path+".wal",
					"a replay writes its chunks while the WAL it is reading is still open")
			}
		})
	}
}

func newMetricsTestMemtable(t *testing.T, failClose, failDelete, withEntry bool) *Memtable {
	t.Helper()

	path := filepath.Join(t.TempDir(), "segment-1")
	real, err := newCommitLogger(path, StrategyReplace, 0)
	require.NoError(t, err)

	m, err := newMemtable(&flushFailureCommitLogger{
		memtableCommitLogger: real,
		failClose:            failClose,
		failDelete:           failDelete,
	}, nil, nullLogger(), nil, memtableConfig{path: path, strategy: StrategyReplace})
	require.NoError(t, err)

	if withEntry {
		require.NoError(t, m.put([]byte("key"), []byte("value")))
	}
	// the fake has no per-operation observers, so it cannot be installed before put
	m.metrics = newTestFlushMetrics()

	return m
}

func requireSegmentWriteMetrics(t *testing.T, m *Memtable, count, failures float64, wantSeries int) {
	t.Helper()

	require.Equal(t, count,
		testutil.ToFloat64(m.metrics.flushingCount.WithLabelValues(StrategyReplace)),
		"lsm_memtable_flush_total counts segment writes")
	require.Equal(t, failures,
		testutil.ToFloat64(m.metrics.flushingFailureCount.WithLabelValues(StrategyReplace)),
		"lsm_memtable_flush_failures_total covers a failed segment write and a failed commit-log step")
	require.Equal(t, float64(0),
		testutil.ToFloat64(m.metrics.flushingInProgress.WithLabelValues(StrategyReplace)),
		"in-progress must return to zero")
	require.LessOrEqual(t, failures, count,
		"a success rate computed as (total-failures)/total goes negative once failures outruns total")
	require.Equal(t, wantSeries, testutil.CollectAndCount(m.metrics.flushingDuration),
		"the duration histogram has a child for this strategy")
	require.Equal(t, wantSeries, testutil.CollectAndCount(m.metrics.flushMemtableSize),
		"the size histogram has a child for this strategy")
}
