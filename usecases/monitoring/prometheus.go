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

package monitoring

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	DefaultMetricsNamespace = "weaviate"
)

type Config struct {
	Enabled                    bool   `json:"enabled" yaml:"enabled" long:"enabled"`
	Tool                       string `json:"tool" yaml:"tool"`
	Port                       int    `json:"port" yaml:"port" long:"port" default:"8081"`
	Group                      bool   `json:"group_classes" yaml:"group_classes"`
	MonitorCriticalBucketsOnly bool   `json:"monitor_critical_buckets_only" yaml:"monitor_critical_buckets_only"`

	// Metrics namespace group the metrics with common prefix.
	MetricsNamespace string `json:"metrics_namespace" yaml:"metrics_namespace" long:"metrics_namespace" default:""`
}

// NOTE: Do not add any new metrics to this global `PrometheusMetrics` struct.
// Instead add your metrics close the corresponding component.
type PrometheusMetrics struct {
	Registerer prometheus.Registerer

	BatchTime                           *prometheus.HistogramVec
	BatchSizeBytes                      *prometheus.SummaryVec
	BatchSizeObjects                    prometheus.Summary
	BatchSizeTenants                    prometheus.Summary
	BatchDeleteTime                     *prometheus.SummaryVec
	BatchCount                          *prometheus.CounterVec
	BatchCountBytes                     *prometheus.CounterVec
	ObjectsTime                         *prometheus.SummaryVec
	LSMBloomFilters                     *prometheus.SummaryVec
	AsyncOperations                     *prometheus.GaugeVec
	LSMSegmentCount                     *prometheus.GaugeVec
	LSMObjectsBucketSegmentCount        *prometheus.GaugeVec
	LSMCompressedVecsBucketSegmentCount *prometheus.GaugeVec
	LSMSegmentCountByLevel              *prometheus.GaugeVec
	LSMSegmentUnloaded                  *prometheus.GaugeVec
	LSMSegmentObjects                   *prometheus.GaugeVec
	LSMSegmentSize                      *prometheus.GaugeVec
	LSMMemtableSize                     *prometheus.GaugeVec
	LSMMemtableDurations                *prometheus.SummaryVec
	ObjectCount                         *prometheus.GaugeVec
	QueriesCount                        *prometheus.GaugeVec
	RequestsTotal                       *prometheus.GaugeVec
	QueriesDurations                    *prometheus.HistogramVec
	QueriesFilteredVectorDurations      *prometheus.SummaryVec
	QueryDimensions                     *prometheus.CounterVec
	QueryDimensionsCombined             prometheus.Counter
	GoroutinesCount                     *prometheus.GaugeVec
	BackupRestoreDurations              *prometheus.SummaryVec
	BackupStoreDurations                *prometheus.SummaryVec
	BucketPauseDurations                *prometheus.SummaryVec
	BackupRestoreClassDurations         *prometheus.SummaryVec
	BackupRestoreBackupInitDurations    *prometheus.SummaryVec
	BackupRestoreFromStorageDurations   *prometheus.SummaryVec
	BackupRestoreDataTransferred        *prometheus.CounterVec
	BackupStoreDataTransferred          *prometheus.CounterVec
	FileIOWrites                        *prometheus.SummaryVec
	FileIOReads                         *prometheus.SummaryVec
	MmapOperations                      *prometheus.CounterVec
	MmapProcMaps                        prometheus.Gauge

	// offload metric
	QueueSize                        *prometheus.GaugeVec
	QueueDiskUsage                   *prometheus.GaugeVec
	QueuePaused                      *prometheus.GaugeVec
	QueueCount                       *prometheus.GaugeVec
	QueuePartitionProcessingDuration *prometheus.HistogramVec

	VectorIndexQueueInsertCount *prometheus.CounterVec
	VectorIndexQueueDeleteCount *prometheus.CounterVec

	VectorIndexTombstones              *prometheus.GaugeVec
	VectorIndexTombstoneCleanupThreads *prometheus.GaugeVec
	VectorIndexTombstoneCleanedCount   *prometheus.CounterVec
	VectorIndexTombstoneUnexpected     *prometheus.CounterVec
	VectorIndexTombstoneCycleStart     *prometheus.GaugeVec
	VectorIndexTombstoneCycleEnd       *prometheus.GaugeVec
	VectorIndexTombstoneCycleProgress  *prometheus.GaugeVec
	VectorIndexOperations              *prometheus.GaugeVec
	VectorIndexDurations               *prometheus.SummaryVec
	VectorIndexSize                    *prometheus.GaugeVec
	VectorIndexMaintenanceDurations    *prometheus.SummaryVec
	VectorDimensionsSum                *prometheus.GaugeVec
	VectorSegmentsSum                  *prometheus.GaugeVec

	StartupProgress  *prometheus.GaugeVec
	StartupDurations *prometheus.SummaryVec
	StartupDiskIO    *prometheus.SummaryVec

	ShardsLoaded    prometheus.Gauge
	ShardsUnloaded  prometheus.Gauge
	ShardsLoading   prometheus.Gauge
	ShardsUnloading prometheus.Gauge

	// RAFT-based schema metrics
	SchemaWrites         *prometheus.SummaryVec
	SchemaReadsLocal     *prometheus.SummaryVec
	SchemaReadsLeader    *prometheus.SummaryVec
	SchemaWaitForVersion *prometheus.SummaryVec

	TombstoneFindLocalEntrypoint  *prometheus.CounterVec
	TombstoneFindGlobalEntrypoint *prometheus.CounterVec
	TombstoneReassignNeighbors    *prometheus.CounterVec
	TombstoneDeleteListSize       *prometheus.GaugeVec

	Group bool
	// Keeping metering to only the critical buckets (objects, vectors_compressed)
	// helps cut down on noise when monitoring
	LSMCriticalBucketsOnly bool

	// Deprecated metrics, keeping around because the classification features
	// seems to sill use the old logic. However, those metrics are not actually
	// used for the schema anymore, but only for the classification features.
	SchemaTxOpened   *prometheus.CounterVec
	SchemaTxClosed   *prometheus.CounterVec
	SchemaTxDuration *prometheus.SummaryVec

	// Vectorization
	T2VBatches            *prometheus.GaugeVec
	T2VBatchQueueDuration *prometheus.HistogramVec
	T2VRequestDuration    *prometheus.HistogramVec
	T2VTokensInBatch      *prometheus.HistogramVec
	T2VTokensInRequest    *prometheus.HistogramVec
	T2VRateLimitStats     *prometheus.GaugeVec
	T2VRepeatStats        *prometheus.GaugeVec
	T2VRequestsPerBatch   *prometheus.HistogramVec

	TokenizerDuration           *prometheus.HistogramVec
	TokenizerRequests           *prometheus.CounterVec
	TokenizerInitializeDuration *prometheus.HistogramVec
	TokenCount                  *prometheus.CounterVec
	TokenCountPerRequest        *prometheus.HistogramVec

	// Currently targeted at OpenAI, the metrics will have to be added to every vectorizer for complete coverage
	ModuleExternalRequests           *prometheus.CounterVec
	ModuleExternalRequestDuration    *prometheus.HistogramVec
	ModuleExternalBatchLength        *prometheus.HistogramVec
	ModuleExternalRequestSingleCount *prometheus.CounterVec
	ModuleExternalRequestBatchCount  *prometheus.CounterVec
	ModuleExternalRequestSize        *prometheus.HistogramVec
	ModuleExternalResponseSize       *prometheus.HistogramVec
	ModuleExternalResponseStatus     *prometheus.CounterVec
	VectorizerRequestTokens          *prometheus.HistogramVec
	ModuleExternalError              *prometheus.CounterVec
	ModuleCallError                  *prometheus.CounterVec
	ModuleBatchError                 *prometheus.CounterVec
}

func NewTenantOffloadMetrics(cfg Config, reg prometheus.Registerer) *TenantOffloadMetrics {
	r := promauto.With(reg)
	return &TenantOffloadMetrics{
		FetchedBytes: r.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "tenant_offload_fetched_bytes_total",
		}),
		TransferredBytes: r.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "tenant_offload_transferred_bytes_total",
		}),
		OpsDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.MetricsNamespace,
			Name:      "tenant_offload_operation_duration_seconds",
			Buckets:   LatencyBuckets,
		}, []string{"operation", "status"}), // status can be "success" or "failure"
	}
}

type TenantOffloadMetrics struct {
	// NOTE: These ops are not GET or PUT requests to object storage.
	// these are one of the `download`, `upload` or `delete`. Because we use s5cmd to talk
	// to object storage currently. Which supports these operations at high level.
	FetchedBytes     prometheus.Counter
	TransferredBytes prometheus.Counter
	OpsDuration      *prometheus.HistogramVec
}

// NewHTPServerMetrics return the ServerMetrics that can be used in any of the grpc or http servers.
func NewHTTPServerMetrics(namespace string, reg prometheus.Registerer) *HTTPServerMetrics {
	r := promauto.With(reg)

	return &HTTPServerMetrics{
		RequestDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "Time (in seconds) spent serving requests.",
			Buckets:   LatencyBuckets,
		}, []string{"method", "route", "status_code"}),
		RequestBodySize: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_size_bytes",
			Help:      "Size (in bytes) of the request received.",
			Buckets:   sizeBuckets,
		}, []string{"method", "route"}),
		ResponseBodySize: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_response_size_bytes",
			Help:      "Size (in bytes) of the response sent.",
			Buckets:   sizeBuckets,
		}, []string{"method", "route"}),
		InflightRequests: r.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "http_requests_inflight",
			Help:      "Current number of inflight requests.",
		}, []string{"method", "route"}),
	}
}

// HTTPServerMetrics exposes set of prometheus metrics for http servers.
type HTTPServerMetrics struct {
	TCPActiveConnections *prometheus.GaugeVec
	RequestDuration      *prometheus.HistogramVec
	RequestBodySize      *prometheus.HistogramVec
	ResponseBodySize     *prometheus.HistogramVec
	InflightRequests     *prometheus.GaugeVec
}

// GRPCServerMetrics exposes set of prometheus metrics for grpc servers.
type GRPCServerMetrics struct {
	RequestDuration  *prometheus.HistogramVec
	RequestBodySize  *prometheus.HistogramVec
	ResponseBodySize *prometheus.HistogramVec
	InflightRequests *prometheus.GaugeVec
}

func NewGRPCServerMetrics(namespace string, reg prometheus.Registerer) *GRPCServerMetrics {
	r := promauto.With(reg)
	return &GRPCServerMetrics{
		RequestDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "grpc_server_request_duration_seconds",
			Help:      "Time (in seconds) spent serving requests.",
			Buckets:   LatencyBuckets,
		}, []string{"grpc_service", "method", "status"}),
		RequestBodySize: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "grpc_server_request_size_bytes",
			Help:      "Size (in bytes) of the request received.",
			Buckets:   sizeBuckets,
		}, []string{"grpc_service", "method"}),
		ResponseBodySize: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "grpc_server_response_size_bytes",
			Help:      "Size (in bytes) of the response sent.",
			Buckets:   sizeBuckets,
		}, []string{"grpc_service", "method"}),
		InflightRequests: r.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "grpc_server_requests_inflight",
			Help:      "Current number of inflight requests.",
		}, []string{"grpc_service", "method"}),
	}
}

// Delete Shard deletes existing label combinations that match both
// the shard and class name. If a metric is not collected at the shard
// level it is unaffected. This is to make sure that deleting a single
// shard (e.g. multi-tenancy) does not affect metrics for existing
// shards.
//
// In addition, there are some metrics that we explicitly keep, such
// as vector_dimensions_sum as they can be used in billing decisions.
func (pm *PrometheusMetrics) DeleteShard(className, shardName string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	}
	pm.BatchTime.DeletePartialMatch(labels)
	pm.BatchDeleteTime.DeletePartialMatch(labels)
	pm.ObjectsTime.DeletePartialMatch(labels)
	pm.ObjectCount.DeletePartialMatch(labels)
	pm.QueriesFilteredVectorDurations.DeletePartialMatch(labels)
	pm.AsyncOperations.DeletePartialMatch(labels)
	pm.LSMBloomFilters.DeletePartialMatch(labels)
	pm.LSMMemtableDurations.DeletePartialMatch(labels)
	pm.LSMMemtableSize.DeletePartialMatch(labels)
	pm.LSMMemtableDurations.DeletePartialMatch(labels)
	pm.LSMSegmentCount.DeletePartialMatch(labels)
	pm.LSMSegmentSize.DeletePartialMatch(labels)
	pm.LSMSegmentCountByLevel.DeletePartialMatch(labels)
	pm.QueueSize.DeletePartialMatch(labels)
	pm.QueueDiskUsage.DeletePartialMatch(labels)
	pm.QueuePaused.DeletePartialMatch(labels)
	pm.QueueCount.DeletePartialMatch(labels)
	pm.QueuePartitionProcessingDuration.DeletePartialMatch(labels)
	pm.VectorIndexQueueInsertCount.DeletePartialMatch(labels)
	pm.VectorIndexQueueDeleteCount.DeletePartialMatch(labels)
	pm.VectorIndexTombstones.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneCleanupThreads.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneCleanedCount.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneUnexpected.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneCycleStart.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneCycleEnd.DeletePartialMatch(labels)
	pm.VectorIndexTombstoneCycleProgress.DeletePartialMatch(labels)
	pm.VectorIndexOperations.DeletePartialMatch(labels)
	pm.VectorIndexMaintenanceDurations.DeletePartialMatch(labels)
	pm.VectorIndexDurations.DeletePartialMatch(labels)
	pm.VectorIndexSize.DeletePartialMatch(labels)
	pm.StartupProgress.DeletePartialMatch(labels)
	pm.StartupDurations.DeletePartialMatch(labels)
	pm.StartupDiskIO.DeletePartialMatch(labels)
	return nil
}

// DeleteClass deletes all metrics that match the class name, but do
// not have a shard-specific label. See [DeleteShard] for more
// information.
func (pm *PrometheusMetrics) DeleteClass(className string) error {
	if pm == nil {
		return nil
	}

	labels := prometheus.Labels{
		"class_name": className,
	}
	pm.QueriesCount.DeletePartialMatch(labels)
	pm.QueriesDurations.DeletePartialMatch(labels)
	pm.GoroutinesCount.DeletePartialMatch(labels)
	pm.BackupRestoreClassDurations.DeletePartialMatch(labels)
	pm.BackupRestoreBackupInitDurations.DeletePartialMatch(labels)
	pm.BackupRestoreFromStorageDurations.DeletePartialMatch(labels)
	pm.BackupStoreDurations.DeletePartialMatch(labels)
	pm.BackupRestoreDataTransferred.DeletePartialMatch(labels)
	pm.BackupStoreDataTransferred.DeletePartialMatch(labels)
	pm.QueriesFilteredVectorDurations.DeletePartialMatch(labels)

	return nil
}

const mb = 1024 * 1024

var (
	// msBuckets and sBuckets are deprecated. Use `LatencyBuckets` and `sizeBuckets` instead.
	msBuckets = []float64{10, 50, 100, 500, 1000, 5000, 10000, 60000, 300000}
	sBuckets  = []float64{0.01, 0.1, 1, 10, 20, 30, 60, 120, 180, 500}

	// LatencyBuckets is default histogram bucket for response time (in seconds).
	// It also includes request that served *very* fast and *very* slow
	LatencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 50, 100}

	// sizeBuckets defines buckets for request/response body sizes (in bytes).
	// TODO(kavi): Check with real data once deployed on prod and tweak accordingly.
	sizeBuckets = []float64{1 * mb, 2.5 * mb, 5 * mb, 10 * mb, 25 * mb, 50 * mb, 100 * mb, 250 * mb}

	metrics *PrometheusMetrics = nil
)

func init() {
	metrics = newPrometheusMetrics()
}

func InitConfig(cfg Config) {
	metrics.Group = cfg.Group
	metrics.LSMCriticalBucketsOnly = cfg.MonitorCriticalBucketsOnly
}

func GetMetrics() *PrometheusMetrics {
	return metrics
}

func newPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		Registerer: prometheus.DefaultRegisterer,
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: msBuckets,
		}, []string{"operation", "class_name", "shard_name"}),
		BatchSizeBytes: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_size_bytes",
			Help: "Size of a raw batch request batch in bytes",
		}, []string{"api"}),
		BatchSizeObjects: promauto.NewSummary(prometheus.SummaryOpts{
			Name: "batch_size_objects",
			Help: "Number of objects in a batch",
		}),
		BatchSizeTenants: promauto.NewSummary(prometheus.SummaryOpts{
			Name: "batch_size_tenants",
			Help: "Number of unique tenants referenced in a batch",
		}),

		BatchDeleteTime: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_delete_durations_ms",
			Help: "Duration in ms of a single delete batch",
		}, []string{"operation", "class_name", "shard_name"}),

		BatchCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "batch_objects_processed_total",
			Help: "Number of objects processed in a batch",
		}, []string{"class_name", "shard_name"}),

		BatchCountBytes: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "batch_objects_processed_bytes",
			Help: "Number of bytes processed in a batch",
		}, []string{"class_name", "shard_name"}),

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

		RequestsTotal: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "requests_total",
			Help: "Number of all requests made",
		}, []string{"status", "class_name", "api", "query_type"}),

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

		// LSM metrics
		LSMSegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_active_segments",
			Help: "Number of currently present segments per shard",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMObjectsBucketSegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_objects_bucket_segment_count",
			Help: "Number of segments per shard in the objects bucket",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMCompressedVecsBucketSegmentCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_compressed_vecs_bucket_segment_count",
			Help: "Number of segments per shard in the vectors_compressed bucket",
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
		LSMSegmentUnloaded: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_segment_unloaded",
			Help: "Number of unloaded segments",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMMemtableSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "lsm_memtable_size",
			Help: "Size of memtable by path",
		}, []string{"strategy", "class_name", "shard_name", "path"}),
		LSMMemtableDurations: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "lsm_memtable_durations_ms",
			Help: "Time in ms for a bucket operation to complete",
		}, []string{"strategy", "class_name", "shard_name", "path", "operation"}),
		FileIOWrites: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "file_io_writes_total_bytes",
			Help: "Total number of bytes written to disk",
		}, []string{"operation", "strategy"}),
		FileIOReads: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "file_io_reads_total_bytes",
			Help: "Total number of bytes read from disk",
		}, []string{"operation"}),
		MmapOperations: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "mmap_operations_total",
			Help: "Total number of mmap operations",
		}, []string{"operation", "strategy"}),
		MmapProcMaps: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "mmap_proc_maps",
			Help: "Number of entries in /proc/self/maps",
		}),

		// Queue metrics
		QueueSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_size",
			Help: "Number of records in the queue",
		}, []string{"class_name", "shard_name"}),
		QueueDiskUsage: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_disk_usage",
			Help: "Disk usage of the queue",
		}, []string{"class_name", "shard_name"}),
		QueuePaused: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_paused",
			Help: "Whether the queue is paused",
		}, []string{"class_name", "shard_name"}),
		QueueCount: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_count",
			Help: "Number of queues",
		}, []string{"class_name", "shard_name"}),
		QueuePartitionProcessingDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name: "queue_partition_processing_duration_ms",
			Help: "Duration in ms of a single partition processing",
		}, []string{"class_name", "shard_name"}),

		// Async indexing metrics
		VectorIndexQueueInsertCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "vector_index_queue_insert_count",
			Help: "Number of insert operations added to the vector index queue",
		}, []string{"class_name", "shard_name", "target_vector"}),
		VectorIndexQueueDeleteCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "vector_index_queue_delete_count",
			Help: "Number of delete operations added to the vector index queue",
		}, []string{"class_name", "shard_name", "target_vector"}),

		// Vector index metrics
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
		VectorIndexTombstoneUnexpected: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "vector_index_tombstone_unexpected_total",
			Help: "Total number of unexpected tombstones that were found, for example because a vector was not found for an existing id in the index",
		}, []string{"class_name", "shard_name", "operation"}),
		VectorIndexTombstoneCycleStart: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_tombstone_cycle_start_timestamp_seconds",
			Help: "Unix epoch timestamp of the start of the current tombstone cleanup cycle",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCycleEnd: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_tombstone_cycle_end_timestamp_seconds",
			Help: "Unix epoch timestamp of the end of the last tombstone cleanup cycle. A negative value indicates that the cycle is still running",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCycleProgress: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_index_tombstone_cycle_progress",
			Help: "A ratio (percentage) of the progress of the current tombstone cleanup cycle. 0 indicates the very beginning, 1 is a complete cycle.",
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
		VectorSegmentsSum: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "vector_segments_sum",
			Help: "Total segments in a shard if quantization enabled",
		}, []string{"class_name", "shard_name"}),

		// Startup metrics
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
		QueryDimensionsCombined: promauto.NewCounter(prometheus.CounterOpts{
			Name: "query_dimensions_combined_total",
			Help: "The vector dimensions used by any read-query that involves vectors, aggregated across all classes and shards. The sum of all labels for query_dimensions_total should always match this labelless metric",
		}),

		// Backup/restore metrics
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

		// Shard metrics
		ShardsLoaded: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "shards_loaded",
			Help: "Number of shards loaded",
		}),
		ShardsUnloaded: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "shards_unloaded",
			Help: "Number of shards on not loaded",
		}),
		ShardsLoading: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "shards_loading",
			Help: "Number of shards in process of loading",
		}),
		ShardsUnloading: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "shards_unloading",
			Help: "Number of shards in process of unloading",
		}),

		// Schema TX-metrics. Can be removed when RAFT is ready
		SchemaTxOpened: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "schema_tx_opened_total",
			Help: "Total number of opened schema transactions",
		}, []string{"ownership"}),
		SchemaTxClosed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "schema_tx_closed_total",
			Help: "Total number of closed schema transactions. A close must be either successful or failed",
		}, []string{"ownership", "status"}),
		SchemaTxDuration: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "schema_tx_duration_seconds",
			Help: "Mean duration of a tx by status",
		}, []string{"ownership", "status"}),

		// RAFT-based schema metrics
		SchemaWrites: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "schema_writes_seconds",
			Help: "Duration of schema writes (which always involve the leader)",
		}, []string{"type"}),
		SchemaReadsLocal: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "schema_reads_local_seconds",
			Help: "Duration of local schema reads that do not involve the leader",
		}, []string{"type"}),
		SchemaReadsLeader: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "schema_reads_leader_seconds",
			Help: "Duration of schema reads that are passed to the leader",
		}, []string{"type"}),
		SchemaWaitForVersion: promauto.NewSummaryVec(prometheus.SummaryOpts{
			Name: "schema_wait_for_version_seconds",
			Help: "Duration of waiting for a schema version to be reached",
		}, []string{"type"}),

		TombstoneFindLocalEntrypoint: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "tombstone_find_local_entrypoint",
			Help: "Total number of tombstone delete local entrypoint calls",
		}, []string{"class_name", "shard_name"}),
		TombstoneFindGlobalEntrypoint: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "tombstone_find_global_entrypoint",
			Help: "Total number of tombstone delete global entrypoint calls",
		}, []string{"class_name", "shard_name"}),
		TombstoneReassignNeighbors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "tombstone_reassign_neighbors",
			Help: "Total number of tombstone reassign neighbor calls",
		}, []string{"class_name", "shard_name"}),
		TombstoneDeleteListSize: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tombstone_delete_list_size",
			Help: "Delete list size of tombstones",
		}, []string{"class_name", "shard_name"}),

		T2VBatches: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "t2v_concurrent_batches",
			Help: "Number of batches currently running",
		}, []string{"vectorizer"}),
		T2VBatchQueueDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t2v_batch_queue_duration_seconds",
			Help:    "Time of a batch spend in specific portions of the queue",
			Buckets: sBuckets,
		}, []string{"vectorizer", "operation"}),
		T2VRequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t2v_request_duration_seconds",
			Help:    "Duration of an individual request to the vectorizer",
			Buckets: sBuckets,
		}, []string{"vectorizer"}),
		T2VTokensInBatch: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t2v_tokens_in_batch",
			Help:    "Number of tokens in a user-defined batch",
			Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
		}, []string{"vectorizer"}),
		T2VTokensInRequest: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t2v_tokens_in_request",
			Help:    "Number of tokens in an individual request sent to the vectorizer",
			Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
		}, []string{"vectorizer"}),
		T2VRateLimitStats: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "t2v_rate_limit_stats",
			Help: "Rate limit stats for the vectorizer",
		}, []string{"vectorizer", "stat"}),
		T2VRepeatStats: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "t2v_repeat_stats",
			Help: "Why batch scheduling is repeated",
		}, []string{"vectorizer", "stat"}),
		T2VRequestsPerBatch: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "t2v_requests_per_batch",
			Help:    "Number of requests required to process an entire (user) batch",
			Buckets: []float64{1, 2, 5, 10, 100, 1000},
		}, []string{"vectorizer"}),
		TokenizerDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tokenizer_duration_seconds",
			Help:    "Duration of a tokenizer operation",
			Buckets: LatencyBuckets,
		}, []string{"tokenizer"}),
		TokenizerRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "tokenizer_requests_total",
			Help: "Number of tokenizer requests",
		}, []string{"tokenizer"}),
		TokenizerInitializeDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tokenizer_initialize_duration_seconds",
			Help:    "Duration of a tokenizer initialization operation",
			Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10},
		}, []string{"tokenizer"}),
		TokenCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "token_count_total",
			Help: "Number of tokens processed",
		}, []string{"tokenizer"}),
		TokenCountPerRequest: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "token_count_per_request",
			Help:    "Number of tokens processed per request",
			Buckets: []float64{1, 10, 50, 100, 500, 1000, 10000, 100000, 1000000},
		}, []string{"tokenizer"}),
		ModuleExternalRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_requests_total",
			Help: "Number of module requests to external APIs",
		}, []string{"op", "api"}),
		ModuleExternalRequestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_module_request_duration_seconds",
			Help:    "Duration of an individual request to a module external API",
			Buckets: LatencyBuckets,
		}, []string{"op", "api"}),
		ModuleExternalBatchLength: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_module_requests_per_batch",
			Help:    "Number of items in a batch",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608},
		}, []string{"op", "api"}),
		ModuleExternalRequestSize: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_module_request_size_bytes",
			Help:    "Size (in bytes) of the request sent to an external API",
			Buckets: []float64{256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608},
		}, []string{"op", "api"}),
		ModuleExternalResponseSize: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_module_response_size_bytes",
			Help:    "Size (in bytes) of the response received from an external API",
			Buckets: []float64{256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152, 4194304, 8388608},
		}, []string{"op", "api"}),
		VectorizerRequestTokens: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "weaviate_vectorizer_request_tokens",
			Help:    "Number of tokens in the request sent to an external vectorizer",
			Buckets: []float64{0, 1, 10, 50, 100, 500, 1000, 5000, 10000, 100000, 1000000},
		}, []string{"inout", "api"}),
		ModuleExternalRequestSingleCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_request_single_count",
			Help: "Number of single-item external API requests",
		}, []string{"op", "api"}),
		ModuleExternalRequestBatchCount: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_request_batch_count",
			Help: "Number of batched module requests",
		}, []string{"op", "api"}),
		ModuleExternalError: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_error_total",
			Help: "Number of OpenAI errors",
		}, []string{"op", "module", "endpoint", "status_code"}),
		ModuleCallError: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_call_error_total",
			Help: "Number of module errors (related to external calls)",
		}, []string{"module", "endpoint", "status_code"}),
		ModuleExternalResponseStatus: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_response_status_total",
			Help: "Number of API response statuses",
		}, []string{"op", "endpoint", "status"}),
		ModuleBatchError: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_module_batch_error_total",
			Help: "Number of batch errors",
		}, []string{"operation", "class_name"}),
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
