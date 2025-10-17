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

package hnsw

import (
	"fmt"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// createTestPrometheusMetrics creates a PrometheusMetrics instance with all required metrics initialized for testing
func createTestPrometheusMetrics() *monitoring.PrometheusMetrics {
	return &monitoring.PrometheusMetrics{
		VectorIndexTombstones: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_tombstones",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCleanupThreads: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_tombstone_cleanup_threads",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCleanedCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_vector_index_tombstone_cleaned_count",
		}, []string{"class_name", "shard_name"}),
		VectorIndexOperations: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_operations",
		}, []string{"class_name", "shard_name", "operation"}),
		VectorIndexDurations: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "test_vector_index_durations",
		}, []string{"class_name", "shard_name", "operation", "step"}),
		VectorIndexSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_size",
		}, []string{"class_name", "shard_name"}),
		VectorIndexMaintenanceDurations: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "test_vector_index_maintenance_durations",
		}, []string{"class_name", "shard_name", "operation"}),
		StartupProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_startup_progress",
		}, []string{"class_name", "shard_name", "operation"}),
		StartupDurations: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "test_startup_durations",
		}, []string{"class_name", "shard_name", "operation"}),
		StartupDiskIO: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "test_startup_disk_io",
		}, []string{"class_name", "shard_name", "operation"}),
		TombstoneReassignNeighbors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_tombstone_reassign_neighbors",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneUnexpected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_vector_index_tombstone_unexpected",
		}, []string{"class_name", "shard_name", "operation"}),
		VectorIndexTombstoneCycleStart: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_tombstone_cycle_start",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCycleEnd: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_tombstone_cycle_end",
		}, []string{"class_name", "shard_name"}),
		VectorIndexTombstoneCycleProgress: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_tombstone_cycle_progress",
		}, []string{"class_name", "shard_name"}),
		TombstoneFindGlobalEntrypoint: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_tombstone_find_global_entrypoint",
		}, []string{"class_name", "shard_name"}),
		TombstoneFindLocalEntrypoint: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_tombstone_find_local_entrypoint",
		}, []string{"class_name", "shard_name"}),
		TombstoneDeleteListSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_tombstone_delete_list_size",
		}, []string{"class_name", "shard_name"}),
		VectorIndexBatchMemoryEstimationRatio: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "test_vector_index_batch_memory_estimation_ratio",
			Help: "Test metric for memory estimation ratio",
		}, []string{"class_name", "shard_name"}),
		VectorIndexMemoryAllocationRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "test_vector_index_memory_allocation_rejected_total",
			Help: "Test metric for memory allocation rejections",
		}, []string{"class_name", "shard_name"}),
	}
}

// fakeMemStatsReader is a test implementation of memStatsReader that simulates memory allocation.
type fakeMemStatsReader struct {
	allocValues []uint64
	callCount   int
}

// ReadMemStats simulates reading memory statistics by returning pre-configured values.
func (f *fakeMemStatsReader) ReadMemStats(m *runtime.MemStats) {
	if f.callCount >= len(f.allocValues) {
		panic(fmt.Sprintf("fakeMemStatsReader.ReadMemStats called too many times: call %d exceeds configured values (length %d)", f.callCount+1, len(f.allocValues)))
	}
	m.Alloc = f.allocValues[f.callCount]
	f.callCount++
}
