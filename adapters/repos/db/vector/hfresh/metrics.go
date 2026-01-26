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

package hfresh

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	enabled          bool
	size             prometheus.Gauge
	insert           prometheus.Gauge
	insertTime       prometheus.Observer
	delete           prometheus.Gauge
	deleteTime       prometheus.Observer
	postings         prometheus.Gauge
	postingSize      prometheus.Observer
	analyzePending   prometheus.Gauge
	analyze          prometheus.Observer
	analyzeCount     prometheus.Gauge
	splitsPending    prometheus.Gauge
	split            prometheus.Observer
	splitCount       prometheus.Gauge
	mergesPending    prometheus.Gauge
	merge            prometheus.Observer
	mergeCount       prometheus.Gauge
	reassignsPending prometheus.Gauge
	reassign         prometheus.Observer
	reassignCount    prometheus.Gauge
	centroids        prometheus.Observer
	storeGet         prometheus.Observer
	storeAppend      prometheus.Observer
	storePut         prometheus.Observer
}

func NewMetrics(prom *monitoring.PrometheusMetrics,
	className, shardName string,
) *Metrics {
	if prom == nil {
		return &Metrics{enabled: false}
	}

	if prom.Group {
		className = "n/a"
		shardName = "n/a"
	}

	size := prom.VectorIndexSize.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	insert := prom.VectorIndexOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "create",
	})

	insertTime := prom.VectorIndexDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "create",
		"step":       "n/a",
	})

	del := prom.VectorIndexOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "delete",
	})

	deleteTime := prom.VectorIndexDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "delete",
		"step":       "n/a",
	})

	postings := prom.VectorIndexPostings.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	postingSize := prom.VectorIndexPostingSize.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	analyzePending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "analyze",
	})

	analyze := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "analyze",
	})

	analyzeCount := prom.VectorIndexBackgroundOperationsCount.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "analyze",
	})

	splitsPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "split",
	})

	split := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "split",
	})

	splitCount := prom.VectorIndexBackgroundOperationsCount.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "split",
	})

	mergesPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "merge",
	})

	merge := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "merge",
	})

	mergeCount := prom.VectorIndexBackgroundOperationsCount.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "merge",
	})

	reassignsPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "reassign",
	})

	reassign := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "reassign",
	})

	reassignCount := prom.VectorIndexBackgroundOperationsCount.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "reassign",
	})

	centroids := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "centroid_search",
	})

	storeGet := prom.VectorIndexStoreOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "get",
	})

	storeAppend := prom.VectorIndexStoreOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "append",
	})

	storePut := prom.VectorIndexStoreOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "put",
	})

	return &Metrics{
		enabled:          true,
		size:             size,
		insert:           insert,
		insertTime:       insertTime,
		delete:           del,
		deleteTime:       deleteTime,
		postings:         postings,
		postingSize:      postingSize,
		analyzePending:   analyzePending,
		analyze:          analyze,
		analyzeCount:     analyzeCount,
		splitsPending:    splitsPending,
		split:            split,
		splitCount:       splitCount,
		mergesPending:    mergesPending,
		merge:            merge,
		mergeCount:       mergeCount,
		reassignsPending: reassignsPending,
		reassign:         reassign,
		reassignCount:    reassignCount,
		centroids:        centroids,
		storeGet:         storeGet,
		storeAppend:      storeAppend,
		storePut:         storePut,
	}
}

func (m *Metrics) SetSize(size int) {
	if !m.enabled {
		return
	}

	m.size.Set(float64(size))
}

func (m *Metrics) InsertVector(start time.Time) {
	if !m.enabled {
		return
	}

	m.insertTime.Observe(float64(time.Since(start).Milliseconds()))
	m.insert.Inc()
}

func (m *Metrics) DeleteVector(start time.Time) {
	if !m.enabled {
		return
	}

	m.deleteTime.Observe(float64(time.Since(start).Milliseconds()))
	m.delete.Inc()
}

func (m *Metrics) SetPostings(count int) {
	if !m.enabled {
		return
	}

	m.postings.Set(float64(count))
}

func (m *Metrics) ObservePostingSize(size float64) {
	if !m.enabled {
		return
	}

	m.postingSize.Observe(size)
}

func (m *Metrics) EnqueueAnalyzeTask() {
	if !m.enabled {
		return
	}

	m.analyzePending.Inc()
}

func (m *Metrics) DequeueAnalyzeTask() {
	if !m.enabled {
		return
	}

	m.analyzePending.Dec()
}

func (m *Metrics) AnalyzeDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.analyze.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) IncAnalyzeCount() {
	if !m.enabled {
		return
	}

	m.analyzeCount.Inc()
}

func (m *Metrics) SetAnalyzeCount(count int64) {
	if !m.enabled {
		return
	}

	m.analyzePending.Add(float64(count))
}

func (m *Metrics) EnqueueSplitTask() {
	if !m.enabled {
		return
	}

	m.splitsPending.Inc()
}

func (m *Metrics) DequeueSplitTask() {
	if !m.enabled {
		return
	}

	m.splitsPending.Dec()
}

func (m *Metrics) SplitDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.split.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) IncSplitCount() {
	if !m.enabled {
		return
	}

	m.splitCount.Inc()
}

func (m *Metrics) SetSplitCount(count int64) {
	if !m.enabled {
		return
	}

	m.splitsPending.Set(float64(count))
}

func (m *Metrics) EnqueueMergeTask() {
	if !m.enabled {
		return
	}

	m.mergesPending.Inc()
}

func (m *Metrics) DequeueMergeTask() {
	if !m.enabled {
		return
	}

	m.mergesPending.Dec()
}

func (m *Metrics) MergeDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.merge.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) IncMergeCount() {
	if !m.enabled {
		return
	}

	m.mergeCount.Inc()
}

func (m *Metrics) SetMergeCount(count int64) {
	if !m.enabled {
		return
	}

	m.mergesPending.Set(float64(count))
}

func (m *Metrics) EnqueueReassignTask() {
	if !m.enabled {
		return
	}

	m.reassignsPending.Inc()
}

func (m *Metrics) DequeueReassignTask() {
	if !m.enabled {
		return
	}

	m.reassignsPending.Dec()
}

func (m *Metrics) ReassignDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.reassign.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) IncReassignCount() {
	if !m.enabled {
		return
	}

	m.reassignCount.Inc()
}

func (m *Metrics) SetReassignCount(count int64) {
	if !m.enabled {
		return
	}

	m.reassignsPending.Set(float64(count))
}

func (m *Metrics) CentroidSearchDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.centroids.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) StoreGetDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.storeGet.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) StoreAppendDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.storeAppend.Observe(float64(time.Since(start).Milliseconds()))
}

func (m *Metrics) StorePutDuration(start time.Time) {
	if !m.enabled {
		return
	}

	m.storePut.Observe(float64(time.Since(start).Milliseconds()))
}
