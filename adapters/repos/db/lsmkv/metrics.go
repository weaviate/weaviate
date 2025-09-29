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

var (
	lifecycleDurationBuckets  = prometheus.ExponentialBuckets(0.01, 2, 15)     // 0.01s → 0.02s → ... → ~163.84s
	cursorDurationBuckets     = prometheus.ExponentialBuckets(0.01, 2, 16)     // 0.01s → ... → ~327.68s
	operationDurationBuckets  = prometheus.ExponentialBuckets(0.01, 2, 12)     // 0.01s → 0.02s → ... → ~40.96s
	segmentSizeBuckets        = prometheus.ExponentialBuckets(100*1024, 2, 20) // 100KB → 200KB → 400KB → ... → ~52GB
	compactionDurationBuckets = prometheus.ExponentialBuckets(0.01, 2, 15)     // 0.01s → 0.02s → ... → ~163.84s
)

type Metrics struct {
	register prometheus.Registerer

	// bucket metrics
	bucketInitCountByStrategy        *prometheus.CounterVec
	bucketInitInProgressByStrategy   *prometheus.GaugeVec
	bucketInitFailureCountByStrategy *prometheus.CounterVec
	bucketInitDurationByStrategy     *prometheus.HistogramVec

	bucketShutdownCountByStrategy        *prometheus.CounterVec
	bucketShutdownInProgressByStrategy   *prometheus.GaugeVec
	bucketShutdownDurationByStrategy     *prometheus.HistogramVec
	bucketShutdownFailureCountByStrategy *prometheus.CounterVec

	bucketOpenedCursorsByStrategy  *prometheus.CounterVec
	bucketOpenCursorsByStrategy    *prometheus.GaugeVec
	bucketCursorDurationByStrategy *prometheus.HistogramVec

	bucketReadOpCountByComponent        *prometheus.CounterVec
	bucketReadOpOngoingByComponent      *prometheus.GaugeVec
	bucketReadOpFailureCountByComponent *prometheus.CounterVec
	bucketReadOpDurationByComponent     *prometheus.HistogramVec

	bucketWriteOpCount        *prometheus.CounterVec
	bucketWriteOpOngoing      *prometheus.GaugeVec
	bucketWriteOpFailureCount *prometheus.CounterVec
	bucketWriteOpDuration     *prometheus.HistogramVec

	// segment metrics
	segmentTotalByStrategy *prometheus.GaugeVec
	segmentSizeByStrategy  *prometheus.HistogramVec

	// wal recovery metrics
	walRecoveryCount        *prometheus.CounterVec
	walRecoveryInProgress   *prometheus.GaugeVec
	walRecoveryFailureCount *prometheus.CounterVec
	walRecoveryDuration     *prometheus.HistogramVec

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

	// bucket metrics

	bucketInitCountByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_init_count",
				Help:      "Total number of LSM bucket initializations requested, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_init_count: %w", err)
	}

	bucketInitInProgressByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_init_in_progress",
				Help:      "Number of LSM bucket initializations currently in progress, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_init_in_progress: %w", err)
	}

	bucketInitFailureCountByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_init_failure_count",
				Help:      "Number of failed LSM bucket initializations, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_init_failure_count: %w", err)
	}

	bucketInitDurationByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_init_duration_seconds",
				Help:      "Duration of LSM bucket initialization in seconds, labeled by segment strategy",
				Buckets:   lifecycleDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_init_duration_seconds: %w", err)
	}

	bucketShutdownCountByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_shutdown_count",
				Help:      "Total number of LSM bucket shutdowns requested, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_shutdown_count: %w", err)
	}

	bucketShutdownInProgressByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_shutdown_in_progress",
				Help:      "Number of LSM bucket shutdowns currently in progress, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_shutdown_in_progress: %w", err)
	}

	bucketShutdownDurationByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_shutdown_duration_seconds",
				Help:      "Duration of LSM bucket shutdown in seconds, labeled by segment strategy",
				Buckets:   lifecycleDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_shutdown_duration_seconds: %w", err)
	}

	bucketShutdownFailureCountByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_shutdown_failure_count",
				Help:      "Number of failed LSM bucket shutdowns, labeled by segment strategy",
			}, []string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_shutdown_failure_count: %w", err)
	}

	bucketOpenedCursorsByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_opened_cursors",
				Help:      "Number of opened LSM bucket cursors, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_opened_cursors: %w", err)
	}

	bucketOpenCursorsByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_open_cursors",
				Help:      "Number of currently open LSM bucket cursors, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_open_cursors: %w", err)
	}

	bucketCursorDurationByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_cursor_duration_seconds",
				Help:      "Duration of LSM bucket cursor operations in seconds, labeled by segment strategy",
				Buckets:   cursorDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_cursor_duration_seconds: %w", err)
	}

	bucketReadOpCountByComponent, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_read_operation_count",
				Help:      "Total number of LSM bucket read operations requested, labeled by operation and component",
			},
			[]string{"operation", "component"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_read_operation_count: %w", err)
	}

	bucketReadOpOngoingByComponent, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_read_operation_ongoing",
				Help:      "Number of LSM bucket read operations currently in progress, labeled by operation and component",
			},
			[]string{"operation", "component"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_read_operation_ongoing: %w", err)
	}

	bucketReadOpFailureCountByComponent, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_read_operation_failure_count",
				Help:      "Number of failed LSM bucket read operations, labeled by operation and component",
			},
			[]string{"operation", "component"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_read_operation_failure_count: %w", err)
	}

	bucketReadOpDurationByComponent, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_read_operation_duration_seconds",
				Help:      "Duration of LSM bucket read operations in seconds, labeled by operation and component",
				Buckets:   operationDurationBuckets,
			},
			[]string{"operation", "component"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_read_operation_duration_seconds: %w", err)
	}

	bucketWriteOpCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_write_operation_count",
				Help:      "Total number of LSM bucket write operations requested, labeled by operation",
			},
			[]string{"operation"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_write_operation_count: %w", err)
	}

	bucketWriteOpOngoing, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_write_operation_ongoing",
				Help:      "Number of LSM bucket write operations currently in progress, labeled by operation",
			},
			[]string{"operation"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_write_operation_ongoing: %w", err)
	}

	bucketWriteOpFailureCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_write_operation_failure_count",
				Help:      "Number of failed LSM bucket write operations, labeled by operation",
			},
			[]string{"operation"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_write_operation_failure_count: %w", err)
	}

	bucketWriteOpDuration, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_write_operation_duration_seconds",
				Help:      "Duration of LSM bucket write operations in seconds, labeled by operation",
				Buckets:   operationDurationBuckets,
			},
			[]string{"operation"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_write_operation_duration_seconds: %w", err)
	}

	// segment metrics
	segmentTotalByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_segment_total",
				Help:      "Total number of LSM bucket segments, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_segment_total: %w", err)
	}

	segmentSizeByStrategy, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_segment_size_bytes",
				Help:      "Size of LSM bucket segments in bytes, labeled by segment strategy",
				Buckets:   segmentSizeBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_segment_size_bytes: %w", err)
	}

	// wal recovery metrics
	walRecoveryCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_wal_recovery_count",
				Help:      "Total number of LSM bucket WAL recoveries requested, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_wal_recovery_count: %w", err)
	}

	walRecoveryInProgress, err := monitoring.EnsureRegisteredMetric(register, prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "lsm_bucket_wal_recovery_in_progress",
			Help:      "Number of LSM bucket WAL recoveries currently in progress",
		},
		[]string{"strategy"},
	))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_wal_recovery_in_progress: %w", err)
	}

	walRecoveryFailureCount, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_wal_recovery_failure_count",
				Help:      "Number of failed LSM bucket WAL recoveries, labeled by segment strategy",
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_wal_recovery_failure_count: %w", err)
	}

	walRecoveryDuration, err := monitoring.EnsureRegisteredMetric(register,
		prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "weaviate",
				Name:      "lsm_bucket_wal_recovery_duration_seconds",
				Help:      "Duration of LSM bucket WAL recovery in seconds, labeled by segment strategy",
				Buckets:   compactionDurationBuckets,
			},
			[]string{"strategy"},
		))
	if err != nil {
		return nil, fmt.Errorf("register lsm_bucket_wal_recovery_duration_seconds: %w", err)
	}

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

		// bucket metrics
		bucketInitCountByStrategy:        bucketInitCountByStrategy,
		bucketInitInProgressByStrategy:   bucketInitInProgressByStrategy,
		bucketInitFailureCountByStrategy: bucketInitFailureCountByStrategy,
		bucketInitDurationByStrategy:     bucketInitDurationByStrategy,

		bucketShutdownCountByStrategy:        bucketShutdownCountByStrategy,
		bucketShutdownInProgressByStrategy:   bucketShutdownInProgressByStrategy,
		bucketShutdownDurationByStrategy:     bucketShutdownDurationByStrategy,
		bucketShutdownFailureCountByStrategy: bucketShutdownFailureCountByStrategy,

		bucketOpenedCursorsByStrategy:  bucketOpenedCursorsByStrategy,
		bucketOpenCursorsByStrategy:    bucketOpenCursorsByStrategy,
		bucketCursorDurationByStrategy: bucketCursorDurationByStrategy,

		bucketReadOpCountByComponent:        bucketReadOpCountByComponent,
		bucketReadOpOngoingByComponent:      bucketReadOpOngoingByComponent,
		bucketReadOpFailureCountByComponent: bucketReadOpFailureCountByComponent,
		bucketReadOpDurationByComponent:     bucketReadOpDurationByComponent,

		bucketWriteOpCount:        bucketWriteOpCount,
		bucketWriteOpOngoing:      bucketWriteOpOngoing,
		bucketWriteOpFailureCount: bucketWriteOpFailureCount,
		bucketWriteOpDuration:     bucketWriteOpDuration,

		// segment metrics
		segmentTotalByStrategy: segmentTotalByStrategy,
		segmentSizeByStrategy:  segmentSizeByStrategy,

		// wal recovery metrics
		walRecoveryCount:        walRecoveryCount,
		walRecoveryInProgress:   walRecoveryInProgress,
		walRecoveryFailureCount: walRecoveryFailureCount,
		walRecoveryDuration:     walRecoveryDuration,

		// compaction-related metrics
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

// bucket metrics
func (m *Metrics) IncBucketInitCountByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketInitCountByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncBucketInitInProgressByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketInitInProgressByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecBucketInitInProgressByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketInitInProgressByStrategy.WithLabelValues(strategy).Dec()
}

func (m *Metrics) IncBucketInitFailureCountByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketInitFailureCountByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) ObserveBucketInitDurationByStrategy(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.bucketInitDurationByStrategy.WithLabelValues(strategy).Observe(duration.Seconds())
}

func (m *Metrics) IncBucketShutdownCountByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketShutdownCountByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncBucketShutdownInProgressByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketShutdownInProgressByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecBucketShutdownInProgressByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketShutdownInProgressByStrategy.WithLabelValues(strategy).Dec()
}

func (m *Metrics) IncBucketShutdownFailureCountByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketShutdownFailureCountByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) ObserveBucketShutdownDurationByStrategy(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.bucketShutdownDurationByStrategy.WithLabelValues(strategy).Observe(duration.Seconds())
}

func (m *Metrics) IncBucketOpenedCursorsByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketOpenedCursorsByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncBucketOpenCursorsByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketOpenCursorsByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecBucketOpenCursorsByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.bucketOpenCursorsByStrategy.WithLabelValues(strategy).Dec()
}

func (m *Metrics) ObserveBucketCursorDurationByStrategy(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.bucketCursorDurationByStrategy.WithLabelValues(strategy).Observe(duration.Seconds())
}

func (m *Metrics) IncBucketReadOpCountByComponent(op, component string) {
	if m == nil {
		return
	}
	m.bucketReadOpCountByComponent.WithLabelValues(op, component).Inc()
}

func (m *Metrics) IncBucketReadOpOngoingByComponent(op, component string) {
	if m == nil {
		return
	}
	m.bucketReadOpOngoingByComponent.WithLabelValues(op, component).Inc()
}

func (m *Metrics) DecBucketReadOpOngoingByComponent(op, component string) {
	if m == nil {
		return
	}
	m.bucketReadOpOngoingByComponent.WithLabelValues(op, component).Dec()
}

func (m *Metrics) IncBucketReadOpFailureCountByComponent(op, component string) {
	if m == nil {
		return
	}
	m.bucketReadOpFailureCountByComponent.WithLabelValues(op, component).Inc()
}

func (m *Metrics) ObserveBucketReadOpDurationByComponent(op, component string, duration time.Duration) {
	if m == nil {
		return
	}
	m.bucketReadOpDurationByComponent.WithLabelValues(op, component).Observe(duration.Seconds())
}

func (m *Metrics) IncBucketWriteOpCount(op string) {
	if m == nil {
		return
	}
	m.bucketWriteOpCount.WithLabelValues(op).Inc()
}

func (m *Metrics) IncBucketWriteOpOngoing(op string) {
	if m == nil {
		return
	}
	m.bucketWriteOpOngoing.WithLabelValues(op).Inc()
}

func (m *Metrics) DecBucketWriteOpOngoing(op string) {
	if m == nil {
		return
	}
	m.bucketWriteOpOngoing.WithLabelValues(op).Dec()
}

func (m *Metrics) IncBucketWriteOpFailureCount(op string) {
	if m == nil {
		return
	}
	m.bucketWriteOpFailureCount.WithLabelValues(op).Inc()
}

func (m *Metrics) ObserveBucketWriteOpDuration(op string, duration time.Duration) {
	if m == nil {
		return
	}
	m.bucketWriteOpDuration.WithLabelValues(op).Observe(duration.Seconds())
}

// segment metrics

func (m *Metrics) IncSegmentTotalByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.segmentTotalByStrategy.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecSegmentTotalByStrategy(strategy string) {
	if m == nil {
		return
	}
	m.segmentTotalByStrategy.WithLabelValues(strategy).Dec()
}

func (m *Metrics) ObserveSegmentSize(strategy string, sizeBytes int64) {
	if m == nil {
		return
	}
	m.segmentSizeByStrategy.WithLabelValues(strategy).Observe(float64(sizeBytes))
}

// wal recovery metrics
func (m *Metrics) IncWalRecoveryCount(strategy string) {
	if m == nil {
		return
	}
	m.walRecoveryCount.WithLabelValues(strategy).Inc()
}

func (m *Metrics) IncWalRecoveryInProgress(strategy string) {
	if m == nil {
		return
	}
	m.walRecoveryInProgress.WithLabelValues(strategy).Inc()
}

func (m *Metrics) DecWalRecoveryInProgress(strategy string) {
	if m == nil {
		return
	}
	m.walRecoveryInProgress.WithLabelValues(strategy).Dec()
}

func (m *Metrics) IncWalRecoveryFailureCount(strategy string) {
	if m == nil {
		return
	}
	m.walRecoveryFailureCount.WithLabelValues(strategy).Inc()
}

func (m *Metrics) ObserveWalRecoveryDuration(strategy string, duration time.Duration) {
	if m == nil {
		return
	}
	m.walRecoveryDuration.WithLabelValues(strategy).Observe(duration.Seconds())
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

func (m *Metrics) ObjectCount(count int) {
	if m == nil {
		return
	}

	m.objectCount.Set(float64(count))
}
