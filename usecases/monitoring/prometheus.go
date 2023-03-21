//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package monitoring

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	BatchTime                          *prometheus.HistogramVec
	BatchDeleteTime                    *prometheus.SummaryVec
	ObjectsTime                        *prometheus.SummaryVec
	LSMBloomFilters                    *prometheus.SummaryVec
	AsyncOperations                    *prometheus.GaugeVec
	LSMSegmentCount                    *prometheus.GaugeVec
	LSMSegmentCountByLevel             *prometheus.GaugeVec
	LSMSegmentObjects                  *prometheus.GaugeVec
	LSMSegmentSize                     *prometheus.GaugeVec
	LSMMemtableSize                    *prometheus.GaugeVec
	LSMMemtableDurations               *prometheus.SummaryVec
	VectorIndexTombstones              *prometheus.GaugeVec
	VectorIndexTombstoneCleanupThreads *prometheus.GaugeVec
	VectorIndexTombstoneCleanedCount   *prometheus.CounterVec
	VectorIndexOperations              *prometheus.GaugeVec
	VectorIndexDurations               *prometheus.SummaryVec
	VectorIndexSize                    *prometheus.GaugeVec
	VectorIndexMaintenanceDurations    *prometheus.SummaryVec
	ObjectCount                        *prometheus.GaugeVec
	QueriesCount                       *prometheus.GaugeVec
	QueriesDurations                   *prometheus.HistogramVec
	QueriesFilteredVectorDurations     *prometheus.SummaryVec
	QueryDimensions                    *prometheus.CounterVec
	GoroutinesCount                    *prometheus.GaugeVec
	BackupRestoreDurations             *prometheus.SummaryVec
	BackupStoreDurations               *prometheus.SummaryVec
	BucketPauseDurations               *prometheus.SummaryVec
	BackupRestoreClassDurations        *prometheus.SummaryVec
	BackupRestoreBackupInitDurations   *prometheus.SummaryVec
	BackupRestoreFromStorageDurations  *prometheus.SummaryVec
	BackupRestoreDataTransferred       *prometheus.CounterVec
	BackupStoreDataTransferred         *prometheus.CounterVec
	VectorDimensionsSum                *prometheus.GaugeVec

	StartupProgress  *prometheus.GaugeVec
	StartupDurations *prometheus.SummaryVec
	StartupDiskIO    *prometheus.SummaryVec
}

var (
	msBuckets                    = []float64{10, 50, 100, 500, 1000, 5000}
	metrics   *PrometheusMetrics = nil
)

func init() {
	metrics = newPrometheusMetrics()
}

func GetMetrics() *PrometheusMetrics {
	return metrics
}

func newPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: msBuckets,
		}, []string{"operation", "class_name", "shard_name"}),
		BatchDeleteTime: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_delete_durations_ms",
			Help: "Duration in ms of a single delete batch",
		}, []string{"operation", "class_name", "shard_name"}),

		ObjectsTime: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "objects_durations_ms",
			Help: "Duration of an individual object operation. Also as part of batches.",
		}, []string{"operation", "step", "class_name", "shard_name"}),
		ObjectCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "object_count",
			Help: "Number of currently ongoing async operations",
		}, []string{"class_name", "shard_name"}),

		QueriesCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "concurrent_queries_count",
			Help: "Number of concurrently running query operations",
		}, []string{"class_name", "query_type"}),

		QueriesDurations: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "queries_durations_ms",
			Help:    "Duration of queries in milliseconds",
			Buckets: msBuckets,
		}, []string{"class_name", "query_type"}),

		QueriesFilteredVectorDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "queries_filtered_vector_durations_ms",
			Help: "Duration of queries in milliseconds",
		}, []string{"class_name", "shard_name", "operation"}),

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
		LSMBloomFilters: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "lsm_bloom_filters_duration_ms",
			Help: "Duration of bloom filter operations",
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
		LSMMemtableDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "lsm_memtable_durations_ms",
			Help: "Time in ms for a bucket operation to complete",
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
		VectorIndexSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_size",
			Help: "The size of the vector index. Typically larger than number of vectors, as it grows proactively.",
		}, []string{"class_name", "shard_name"}),
		VectorIndexMaintenanceDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "vector_index_maintenance_durations_ms",
			Help: "Duration of a sync or async vector index maintenance operation",
		}, []string{"operation", "class_name", "shard_name"}),
		VectorIndexDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "vector_index_durations_ms",
			Help: "Duration of typical vector index operations (insert, delete)",
		}, []string{"operation", "step", "class_name", "shard_name"}),
		VectorDimensionsSum: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_dimensions_sum",
			Help: "Total dimensions in a shard",
		}, []string{"class_name", "shard_name"}),

		StartupProgress: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "startup_progress",
			Help: "A ratio (percentage) of startup progress for a particular component in a shard",
		}, []string{"operation", "class_name", "shard_name"}),
		StartupDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "startup_durations_ms",
			Help: "Duration of individual startup operations in ms",
		}, []string{"operation", "class_name", "shard_name"}),
		StartupDiskIO: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "startup_diskio_throughput",
			Help: "Disk I/O throuhput in bytes per second",
		}, []string{"operation", "class_name", "shard_name"}),
		QueryDimensions: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "query_dimensions_total",
			Help: "The vector dimensions used by any read-query that involves vectors",
		}, []string{"query_type", "operation", "class_name"}),

		BackupRestoreDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "backup_restore_ms",
			Help: "Duration of a backup restore",
		}, []string{"backend_name", "class_name"}),
		BackupRestoreClassDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "backup_restore_class_ms",
			Help: "Duration restoring class",
		}, []string{"class_name"}),
		BackupRestoreBackupInitDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "backup_restore_init_ms",
			Help: "startup phase of a backup restore",
		}, []string{"backend_name", "class_name"}),
		BackupRestoreFromStorageDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "backup_restore_from_backend_ms",
			Help: "file transfer stage of a backup restore",
		}, []string{"backend_name", "class_name"}),
		BackupStoreDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "backup_store_to_backend_ms",
			Help: "file transfer stage of a backup restore",
		}, []string{"backend_name", "class_name"}),
		BucketPauseDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "bucket_pause_durations_ms",
			Help: "bucket pause durations",
		}, []string{"bucket_dir"}),
		BackupRestoreDataTransferred: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "backup_restore_data_transferred",
			Help: "Total number of bytes transferred during a backup restore",
		}, []string{"backend_name", "class_name"}),
		BackupStoreDataTransferred: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "backup_store_data_transferred",
			Help: "Total number of bytes transferred during a backup store",
		}, []string{"backend_name", "class_name"}),
	}
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
