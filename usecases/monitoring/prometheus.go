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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	BatchTime                          *prometheus.HistogramVec
	ObjectsTime                        *prometheus.HistogramVec
	LSMBloomFilters                    *prometheus.HistogramVec
	AsyncOperations                    *prometheus.GaugeVec
	LSMSegmentCount                    *prometheus.GaugeVec
	LSMSegmentCountByLevel             *prometheus.GaugeVec
	LSMSegmentObjects                  *prometheus.GaugeVec
	LSMSegmentSize                     *prometheus.GaugeVec
	VectorIndexTombstones              *prometheus.GaugeVec
	VectorIndexTombstoneCleanupThreads *prometheus.GaugeVec
	VectorIndexTombstoneCleanedCount   *prometheus.CounterVec
	VectorIndexOperations              *prometheus.GaugeVec
}

func NewPrometheusMetrics() *PrometheusMetrics { // TODO don't rely on global state for registration
	return &PrometheusMetrics{
		BatchTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "batch_durations_ms",
			Help:    "Duration in ms of a single batch",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 40),
		}, []string{"operation", "class_name", "shard_name"}),

		ObjectsTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "objects_durations_ms",
			Help:    "Duration of an individual object operation. Also as part of batches.",
			Buckets: prometheus.ExponentialBuckets(10, 1.25, 25),
		}, []string{"operation", "step", "class_name", "shard_name"}),

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
	}
}
