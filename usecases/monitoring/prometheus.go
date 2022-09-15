//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package monitoring

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	BatchTime                          *prometheus.HistogramVec
	BatchDeleteTime                    *prometheus.HistogramVec
	ObjectsTime                        *prometheus.HistogramVec
	LSMBloomFilters                    *prometheus.HistogramVec
	AsyncOperations                    *prometheus.GaugeVec
	LSMSegmentCount                    *prometheus.GaugeVec
	LSMSegmentCountByLevel             *prometheus.GaugeVec
	LSMSegmentObjects                  *prometheus.GaugeVec
	LSMSegmentSize                     *prometheus.GaugeVec
	LSMMemtableSize                    *prometheus.GaugeVec
	LSMMemtableDurations               *prometheus.HistogramVec
	VectorIndexTombstones              *prometheus.GaugeVec
	VectorIndexTombstoneCleanupThreads *prometheus.GaugeVec
	VectorIndexTombstoneCleanedCount   *prometheus.CounterVec
	VectorIndexOperations              *prometheus.GaugeVec
	VectorIndexDimensionOperations     *prometheus.CounterVec
	VectorIndexDurations               *prometheus.HistogramVec
	VectorIndexSize                    *prometheus.GaugeVec
	VectorIndexMaintenanceDurations    *prometheus.HistogramVec
	ObjectCount                        *prometheus.GaugeVec
	QueriesCount                       *prometheus.GaugeVec
	QueryDimensions                    *prometheus.CounterVec
	GoroutinesCount                    *prometheus.GaugeVec
	BackupRestoreDurations             *prometheus.HistogramVec
	BackupStoreDurations               *prometheus.HistogramVec
	BucketPauseDurations               *prometheus.HistogramVec
	BackupRestoreClassDurations        *prometheus.HistogramVec
	BackupRestoreBackupInitDurations   *prometheus.HistogramVec
	BackupRestoreFromStorageDurations  *prometheus.HistogramVec
	BackupRestoreDataTransferred       *prometheus.CounterVec
	BackupStoreDataTransferred         *prometheus.CounterVec
	DimensionSum                       *prometheus.GaugeVec

	StartupProgress  *prometheus.GaugeVec
	StartupDurations *prometheus.HistogramVec
	StartupDiskIO    *prometheus.HistogramVec
}

var (
	msBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000}
	metrics   *PrometheusMetrics
)

func GetMetrics() *PrometheusMetrics {
	if metrics == nil {
		NewPrometheusMetrics()
	}
	return metrics
}

func NewPrometheusMetrics() *PrometheusMetrics {
	metrics = &PrometheusMetrics{
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),
		BatchDeleteTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_delete_durations_ms",
			Help:    "Duration in ms of a single delete batch",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),

		ObjectsTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "objects_durations_ms",
			Help:    "Duration of an individual object operation. Also as part of batches.",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 25),
		}, []string{"operation", "step", "class_name", "shard_name"}),
		ObjectCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "object_count",
			Help: "Number of currently ongoing async operations",
		}, []string{"class_name", "shard_name"}),

		QueriesCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "concurrent_queries_count",
			Help: "Number of concurrently running query operations",
		}, []string{"class_name", "query_type"}),

		GoroutinesCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "concurrent_goroutines",
			Help: "Number of concurrently running goroutines",
		}, []string{"class_name", "query_type"}),

		AsyncOperations: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "async_operations_running",
			Help: "Number of currently ongoing async operations",
		}, []string{"operation", "class_name", "shard_name", "path"}),

		LSMSegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_active_segments",
			Help: "Number of currently present segments per shard",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMBloomFilters: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lsm_bloom_filters_duration_ms",
			Help:    "Duration of bloom filter operations",
			Buckets: prometheus.ExponentialBuckets(0.001, 1.25, 60),
		}, []string{"operation", "strategy", "class_name", "shard_name"}),
		LSMSegmentObjects: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_segment_objects",
			Help: "Number of objects/entries of segment by level",
		}, []string{"strategy", "class_name", "shard_name", "path", "level"}),
		LSMSegmentSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_segment_size",
			Help: "Size of segment by level and unit",
		}, []string{"strategy", "class_name", "shard_name", "path", "level", "unit"}),
		LSMSegmentCountByLevel: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_segment_count",
			Help: "Number of segments by level",
		}, []string{"strategy", "class_name", "shard_name", "path", "level"}),
		LSMMemtableSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_memtable_size",
			Help: "Size of memtable by path",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMMemtableDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "lsm_memtable_durations_ms",
			Help:    "Time in ms for a bucket operation to complete",
			Buckets: msBuckets,
		}, []string{"strategy", "class_name", "shard_name", "path", "operation"}),

		VectorIndexTombstones: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_tombstones",
			Help: "Number of active vector index tombstones",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCleanupThreads: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_tombstone_cleanup_threads",
			Help: "Number of threads in use to clean up tombstones",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCleanedCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "vector_index_tombstone_cleaned",
			Help: "Total number of deleted objects that have been cleaned up",
		}, []string{"class_name", "shard_name"}),
		VectorIndexOperations: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_operations",
			Help: "Total number of mutating operations on the vector index",
		}, []string{"operation", "class_name", "shard_name"}),
		VectorIndexDimensionOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "vector_index_dimensions_total",
			Help: "Total number of mutating operations multiplied with dimensions on the vector index",
		}, []string{"operation", "class_name", "shard_name"}),
		VectorIndexSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_size",
			Help: "The size of the vector index. Typically larger than number of vectors, as it grows proactively.",
		}, []string{"class_name", "shard_name"}),
		VectorIndexMaintenanceDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vector_index_maintenance_durations_ms",
			Help:    "Duration of a sync or async vector index maintenance operation",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"operation", "class_name", "shard_name"}),
		VectorIndexDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "vector_index_durations_ms",
			Help:    "Duration of typical vector index operations (insert, delete)",
			Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 30),
		}, []string{"operation", "step", "class_name", "shard_name"}),

		StartupProgress: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "startup_progress",
			Help: "A ratio (percentage) of startup progress for a particular component in a shard",
		}, []string{"operation", "class_name", "shard_name"}),
		StartupDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "startup_durations_ms",
			Help:    "Duration of inidividual startup operations in ms",
			Buckets: prometheus.ExponentialBuckets(100, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),
		StartupDiskIO: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "startup_diskio_throughput",
			Help:    "Disk I/O throuhput in bytes per second",
			Buckets: prometheus.ExponentialBuckets(1, 2, 40),
		}, []string{"operation", "class_name", "shard_name"}),
		QueryDimensions: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "query_dimensions_total",
			Help: "The vector dimensions used by any read-query that involves vectors",
		}, []string{"query_type", "operation", "class_name"}),

		BackupRestoreDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backup_restore_ms",
			Help:    "Duration of a backup restore",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"backend_name", "class_name"}),
		BackupRestoreClassDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backup_restore_class_ms",
			Help:    "Duration restoring class",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"class_name"}),
		BackupRestoreBackupInitDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backup_restore_init_ms",
			Help:    "startup phase of a backup restore",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"backend_name", "class_name"}),
		BackupRestoreFromStorageDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backup_restore_from_backend_ms",
			Help:    "file transfer stage of a backup restore",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"backend_name", "class_name"}),
		BackupStoreDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "backup_store_to_backend_ms",
			Help:    "file transfer stage of a backup restore",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"backend_name", "class_name"}),
		BucketPauseDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "bucket_pause_durations_ms",
			Help:    "bucket pause durations",
			Buckets: prometheus.ExponentialBuckets(1, 1.5, 30),
		}, []string{"bucket_dir"}),
		BackupRestoreDataTransferred: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "backup_restore_data_transferred",
			Help: "Total number of bytes transferred during a backup restore",
		}, []string{"backend_name", "class_name"}),
		BackupStoreDataTransferred: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "backup_store_data_transferred",
			Help: "Total number of bytes transferred during a backup store",
		}, []string{"backend_name", "class_name"}),
		DimensionSum: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dimension_sum",
			Help: "Total dimensions in a shard",
		}, []string{"class_name", "shard_name"}),
	}

	return metrics
}

type OnceUponATimer struct {
	sync.Once
	Timer *prometheus.Timer
}

func NewOnceTimer(promTimer *prometheus.Timer) *OnceUponATimer {
	o := OnceUponATimer{}
	o.Timer = promTimer
	return &o
}

func (o *OnceUponATimer) ObserveDurationOnce() {
	o.Do(func() {
		o.Timer.ObserveDuration()
	})
}
