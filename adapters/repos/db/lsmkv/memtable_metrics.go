//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

var (
	flushingDurationBuckets = prometheus.ExponentialBuckets(0.01, 2, 15)    // from 10ms up to ~2min 44s
	flushingSizeBuckets     = prometheus.ExponentialBuckets(64*1024, 2, 15) // 64KB → ~1GB
)

type memtableMetrics struct {
	// flushing metrics
	flushingCount        *prometheus.CounterVec
	flushingInProgress   *prometheus.GaugeVec
	flushingFailureCount *prometheus.CounterVec
	flushingDuration     *prometheus.HistogramVec
	flushMemtableSize    *prometheus.HistogramVec

	flushMemtableBytesWritten BytesWriteObserver

	// per-operation metrics (old-style, to be migrated)
	put             NsObserver
	setTombstone    NsObserver
	append          NsObserver
	appendMapSorted NsObserver
	get             NsObserver
	getBySecondary  NsObserver
	getMap          NsObserver
	getCollection   NsObserver
	size            Setter
}

// newMemtableMetrics curries the prometheus-functions just once to make sure
// they don't have to be curried on the hotpath where we this would lead to a
// lot of allocations.
func newMemtableMetrics(metrics *Metrics, path, strategy string) (*memtableMetrics, error) {
	if metrics == nil {
		return nil, nil
	}

	flushingCount, err := monitoring.EnsureRegisteredMetric(metrics.register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_memtable_flush_total",
				Help:      "Total number of LSM memtable flushes, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_memtable_flush_total: %w", err)
	}

	flushingInProgress, err := monitoring.EnsureRegisteredMetric(metrics.register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_memtable_flush_in_progress",
				Help:      "Number of LSM memtable flushes in progress, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_memtable_flush_in_progress: %w", err)
	}

	flushingFailureCount, err := monitoring.EnsureRegisteredMetric(metrics.register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_memtable_flush_failures_total",
				Help:      "Total number of failed LSM memtable flushes, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_memtable_flush_failures_total: %w", err)
	}

	flushingDuration, err := monitoring.EnsureRegisteredMetric(metrics.register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_memtable_flush_duration_seconds",
				Help:      "Duration of LSM memtable flush in seconds, labeled by segment strategy",
				Buckets:   flushingDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_memtable_flush_duration_seconds: %w", err)
	}

	flushMemtableSize, err := monitoring.EnsureRegisteredMetric(metrics.register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_memtable_flush_size_bytes",
				Help:      "Size of LSM memtable at flushing time, in bytes",
				Buckets:   flushingSizeBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_memtable_flush_size_bytes: %w", err)
	}

	return &memtableMetrics{
		flushingCount:        flushingCount,
		flushingInProgress:   flushingInProgress,
		flushingFailureCount: flushingFailureCount,
		flushingDuration:     flushingDuration,
		flushMemtableSize:    flushMemtableSize,

		flushMemtableBytesWritten: metrics.MemtableWriteObserver(strategy, "flushMemtable"),

		put:             metrics.MemtableOpObserver(path, strategy, "put"),
		setTombstone:    metrics.MemtableOpObserver(path, strategy, "setTombstone"),
		append:          metrics.MemtableOpObserver(path, strategy, "append"),
		appendMapSorted: metrics.MemtableOpObserver(path, strategy, "appendMapSorted"),
		get:             metrics.MemtableOpObserver(path, strategy, "get"),
		getBySecondary:  metrics.MemtableOpObserver(path, strategy, "getBySecondary"),
		getMap:          metrics.MemtableOpObserver(path, strategy, "getMap"),
		getCollection:   metrics.MemtableOpObserver(path, strategy, "getCollection"),
		size:            metrics.MemtableSizeSetter(path, strategy),
	}, nil
}

func (m *memtableMetrics) incFlushingCount(strategy string) {
	if m == nil {
		return
	}
	m.flushingCount.WithLabelValues(strategy).Inc()
}

func (m *memtableMetrics) incFlushingInProgress(strategy string) {
	if m == nil {
		return
	}
	m.flushingInProgress.WithLabelValues(strategy).Inc()
}

func (m *memtableMetrics) decFlushingInProgress(strategy string) {
	if m == nil {
		return
	}
	m.flushingInProgress.WithLabelValues(strategy).Dec()
}

func (m *memtableMetrics) incFlushingFailureCount(strategy string) {
	if m == nil {
		return
	}
	m.flushingFailureCount.WithLabelValues(strategy).Inc()
}

func (m *memtableMetrics) observeFlushingDuration(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.flushingDuration.WithLabelValues(strategy).Observe(duration.Seconds())
}

func (m *memtableMetrics) observeFlushMemtableSize(strategy string, size uint64) {
	if m == nil {
		return
	}
	m.flushMemtableSize.WithLabelValues(strategy).Observe(float64(size))
}

func (m *memtableMetrics) observeFlushMemtableBytesWritten(n int64) {
	if m == nil {
		return
	}
	m.flushMemtableBytesWritten(n)
}

func (m *memtableMetrics) observePut(ns int64) {
	if m == nil {
		return
	}
	m.put(ns)
}

func (m *memtableMetrics) observeSetTombstone(ns int64) {
	if m == nil {
		return
	}
	m.setTombstone(ns)
}

func (m *memtableMetrics) observeAppend(ns int64) {
	if m == nil {
		return
	}
	m.append(ns)
}

func (m *memtableMetrics) observeAppendMapSorted(ns int64) {
	if m == nil {
		return
	}
	m.appendMapSorted(ns)
}

func (m *memtableMetrics) observeGet(ns int64) {
	if m == nil {
		return
	}
	m.get(ns)
}

func (m *memtableMetrics) observeGetBySecondary(ns int64) {
	if m == nil {
		return
	}
	m.getBySecondary(ns)
}

func (m *memtableMetrics) observeGetMap(ns int64) {
	if m == nil {
		return
	}
	m.getMap(ns)
}

func (m *memtableMetrics) observeGetCollection(ns int64) {
	if m == nil {
		return
	}
	m.getCollection(ns)
}

func (m *memtableMetrics) observeSize(size uint64) {
	if m == nil {
		return
	}
	m.size(size)
}
