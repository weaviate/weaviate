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

type (
	NsObserver         func(ns int64)
	BytesWriteObserver func(bytes int64)
	BytesReadObserver  func(bytes int64, nanoseconds int64)
	Setter             func(val uint64)
	TimeObserver       func(start time.Time)
)

var compactionDurationBuckets = prometheus.ExponentialBuckets(0.01, 2, 18) // 0.01s → 0.02s → ... → ~163.84s

type Metrics struct {
	register prometheus.Registerer

	// compaction-related metrics
	compactionCount        *prometheus.CounterVec
	compactionInProgress   *prometheus.GaugeVec
	compactionFailureCount *prometheus.CounterVec
	compactionNoOpCount    *prometheus.CounterVec
	compactionDuration     *prometheus.HistogramVec

	// old-style metrics, to be migrated
	ActiveSegments               *prometheus.GaugeVec
	ObjectsBucketSegments        *prometheus.GaugeVec
	CompressedVecsBucketSegments *prometheus.GaugeVec
	SegmentObjects               *prometheus.GaugeVec
	SegmentSize                  *prometheus.GaugeVec
	SegmentCount                 *prometheus.GaugeVec
	SegmentUnloaded              *prometheus.GaugeVec
	startupDurations             prometheus.ObserverVec
	startupDiskIO                prometheus.ObserverVec
	objectCount                  prometheus.Gauge
	memtableDurations            prometheus.ObserverVec
	memtableSize                 *prometheus.GaugeVec
	DimensionSum                 *prometheus.GaugeVec
	IOWrite                      *prometheus.SummaryVec
	IORead                       *prometheus.SummaryVec
	LazySegmentUnLoad            prometheus.Gauge
	LazySegmentLoad              prometheus.Gauge
	LazySegmentClose             prometheus.Gauge
	LazySegmentInit              prometheus.Gauge

	groupClasses        bool
	criticalBucketsOnly bool
}

func NewMetrics(promMetrics *monitoring.PrometheusMetrics, className,
	shardName string,
) (*Metrics, error) {
	if promMetrics.Group {
		className = "n/a"
		shardName = "n/a"
	}

	register := promMetrics.Registerer

	// compaction-related metrics
	compactionCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_compaction_count",
				Help:      "Total number of LSM bucket compactions requested, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_compaction_count: %w", err)
	}

	compactionInProgress, err := monitoring.EnsureRegisteredMetric(register, prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "lsm_bucket_compaction_in_progress",
			Help:      "Number of LSM bucket compactions currently in progress, labeled by segment strategy",
		},
		[]string{"strategy"},
	))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_compaction_in_progress: %w", err)
	}

	compactionFailureCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_compaction_failure_count",
				Help:      "Number of failed LSM bucket compactions, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_compaction_failure_count: %w", err)
	}

	compactionNoOpCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_compaction_noop_count",
				Help:      "Number of times the periodic LSM bucket compaction task ran but found nothing to compact, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_compaction_noop_count: %w", err)
	}

	compactionDuration, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_compaction_duration_seconds",
				Help:      "Duration of LSM bucket compaction in seconds, labeled by segment strategy",
				Buckets:   compactionDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_compaction_duration_seconds: %w", err)
	}

	// old-style metrics, to be migrated

	lazySegmentInit := monitoring.GetMetrics().AsyncOperations.With(prometheus.Labels{
		"operation":  "lazySegmentInit",
		"class_name": className,
		"shard_name": shardName,
		"path":       "n/a",
	})

	lazySegmentLoad := monitoring.GetMetrics().AsyncOperations.With(prometheus.Labels{
		"operation":  "lazySegmentLoad",
		"class_name": className,
		"shard_name": shardName,
		"path":       "n/a",
	})

	lazySegmentClose := monitoring.GetMetrics().AsyncOperations.With(prometheus.Labels{
		"operation":  "lazySegmentClose",
		"class_name": className,
		"shard_name": shardName,
		"path":       "n/a",
	})
	lazySegmentUnload := monitoring.GetMetrics().AsyncOperations.With(prometheus.Labels{
		"operation":  "lazySegmentUnLoad",
		"class_name": className,
		"shard_name": shardName,
		"path":       "n/a",
	})

	return &Metrics{
		register:            register,
		groupClasses:        promMetrics.Group,
		criticalBucketsOnly: promMetrics.LSMCriticalBucketsOnly,

		compactionNoOpCount:    compactionNoOpCount,
		compactionCount:        compactionCount,
		compactionInProgress:   compactionInProgress,
		compactionFailureCount: compactionFailureCount,
		compactionDuration:     compactionDuration,

		// old-style metrics, to be migrated
		ActiveSegments: promMetrics.LSMSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		ObjectsBucketSegments: promMetrics.LSMObjectsBucketSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		CompressedVecsBucketSegments: promMetrics.LSMCompressedVecsBucketSegmentCount.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentObjects: promMetrics.LSMSegmentObjects.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentSize: promMetrics.LSMSegmentSize.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentCount: promMetrics.LSMSegmentCountByLevel.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		SegmentUnloaded: promMetrics.LSMSegmentUnloaded.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		startupDiskIO: promMetrics.StartupDiskIO.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		startupDurations: promMetrics.StartupDurations.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		objectCount: promMetrics.ObjectCount.With(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		memtableDurations: promMetrics.LSMMemtableDurations.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		memtableSize: promMetrics.LSMMemtableSize.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		DimensionSum: promMetrics.VectorDimensionsSum.MustCurryWith(prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
		}),
		IOWrite:           promMetrics.FileIOWrites,
		IORead:            promMetrics.FileIOReads,
		LazySegmentLoad:   lazySegmentLoad,
		LazySegmentClose:  lazySegmentClose,
		LazySegmentInit:   lazySegmentInit,
		LazySegmentUnLoad: lazySegmentUnload,
	}, nil
}

// compaction metrics

func (m *Metrics) IncCompactionCount(strategy string) {
	if m == nil {
		return
	}
	m.compactionCount.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncCompactionInProgress(strategy string) {
	if m == nil {
		return
	}
	m.compactionInProgress.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecCompactionInProgress(strategy string) {
	if m == nil {
		return
	}
	m.compactionInProgress.WithLabelValues(strategy).Dec()
}

func (m *Metrics) IncCompactionNoOp(strategy string) {
	if m == nil {
		return
	}
	m.compactionNoOpCount.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncCompactionFailureCount(strategy string) {
	if m == nil {
		return
	}
	m.compactionFailureCount.WithLabelValues(strategy).Inc()
}

func (m *Metrics) ObserveCompactionDuration(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.compactionDuration.WithLabelValues(strategy).Observe(duration.Seconds())
}

func noOpNsObserver(startNs int64) {
	// do nothing
}

func noOpNsReadObserver(startNs int64, time int64) {
	// do nothing
}

func noOpSetter(val uint64) {
	// do nothing
}

func (m *Metrics) MemtableOpObserver(path, strategy, op string) NsObserver {
	if m == nil {
		return noOpNsObserver
	}

	if m.groupClasses {
		path = "n/a"
	}

	curried := m.memtableDurations.With(prometheus.Labels{
		"operation": op,
		"path":      path,
		"strategy":  strategy,
	})

	return func(startNs int64) {
		took := float64(time.Now().UnixNano()-startNs) / float64(time.Millisecond)
		curried.Observe(took)
	}
}

func (m *Metrics) MemtableWriteObserver(strategy, op string) BytesWriteObserver {
	if m == nil {
		return noOpNsObserver
	}

	curried := m.IOWrite.With(prometheus.Labels{
		"operation": op,
		"strategy":  strategy,
	})

	return func(bytes int64) {
		curried.Observe(float64(bytes))
	}
}

func (m *Metrics) ReadObserver(op string) BytesReadObserver {
	if m == nil {
		return noOpNsReadObserver
	}

	curried := m.IORead.With(prometheus.Labels{
		"operation": op,
	})

	return func(n int64, nanoseconds int64) { curried.Observe(float64(n)) }
}

func (m *Metrics) MemtableSizeSetter(path, strategy string) Setter {
	if m == nil || m.groupClasses {
		// this metric would set absolute values, that's not possible in
		// grouped mode, each call would essentially overwrite the last
		return noOpSetter
	}

	curried := m.memtableSize.With(prometheus.Labels{
		"path":     path,
		"strategy": strategy,
	})

	return func(size uint64) {
		curried.Set(float64(size))
	}
}

func (m *Metrics) TrackStartupReadWALDiskIO(read int64, nanoseconds int64) {
	if m == nil {
		return
	}

	seconds := float64(nanoseconds) / float64(time.Second)
	throughput := float64(read) / float64(seconds)
	m.startupDiskIO.With(prometheus.Labels{"operation": "lsm_recover_wal"}).Observe(throughput)
}

func (m *Metrics) TrackStartupBucket(start time.Time) {
	if m == nil {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "lsm_startup_bucket"}).Observe(took)
}

func (m *Metrics) TrackStartupBucketRecovery(start time.Time) {
	if m == nil {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "lsm_startup_bucket_recovery"}).Observe(took)
}

func (m *Metrics) ObjectCount(count int) {
	if m == nil {
		return
	}

	m.objectCount.Set(float64(count))
}
