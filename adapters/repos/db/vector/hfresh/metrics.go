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
	enabled bool
	// group mirrors PrometheusMetrics.Group. When true, every shard / named
	// vector on this node collapses to the same (class=n/a, shard=n/a)
	// label set, and per-shard .Set() on gauges becomes last-writer-wins.
	// SetPostings is no-op'd in that mode; the sole writer is the
	// node-wide sweep (db/node_wide_metrics.go observeHFreshPostings),
	// which sums PostingMap.Size() across every loaded hfresh index.
	group            bool
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

	baseLabels := prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	}
	opLabels := func(op string) prometheus.Labels {
		return prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
			"operation":  op,
		}
	}
	opStepLabels := func(op, step string) prometheus.Labels {
		return prometheus.Labels{
			"class_name": className,
			"shard_name": shardName,
			"operation":  op,
			"step":       step,
		}
	}

	size := prom.VectorIndexSize.With(baseLabels)
	insert := prom.VectorIndexOperations.With(opLabels("create"))
	insertTime := prom.VectorIndexDurations.With(opStepLabels("create", "n/a"))
	del := prom.VectorIndexOperations.With(opLabels("delete"))
	deleteTime := prom.VectorIndexDurations.With(opStepLabels("delete", "n/a"))
	postings := prom.VectorIndexPostings.With(baseLabels)
	postingSize := prom.VectorIndexPostingSize.With(baseLabels)

	analyzePending := prom.VectorIndexPendingBackgroundOperations.With(opLabels("analyze"))
	analyze := prom.VectorIndexBackgroundOperationsDurations.With(opLabels("analyze"))
	analyzeCount := prom.VectorIndexBackgroundOperationsCount.With(opLabels("analyze"))

	splitsPending := prom.VectorIndexPendingBackgroundOperations.With(opLabels("split"))
	split := prom.VectorIndexBackgroundOperationsDurations.With(opLabels("split"))
	splitCount := prom.VectorIndexBackgroundOperationsCount.With(opLabels("split"))

	mergesPending := prom.VectorIndexPendingBackgroundOperations.With(opLabels("merge"))
	merge := prom.VectorIndexBackgroundOperationsDurations.With(opLabels("merge"))
	mergeCount := prom.VectorIndexBackgroundOperationsCount.With(opLabels("merge"))

	reassignsPending := prom.VectorIndexPendingBackgroundOperations.With(opLabels("reassign"))
	reassign := prom.VectorIndexBackgroundOperationsDurations.With(opLabels("reassign"))
	reassignCount := prom.VectorIndexBackgroundOperationsCount.With(opLabels("reassign"))

	centroids := prom.VectorIndexBackgroundOperationsDurations.With(opLabels("centroid_search"))

	storeGet := prom.VectorIndexStoreOperationsDurations.With(opLabels("get"))
	storeAppend := prom.VectorIndexStoreOperationsDurations.With(opLabels("append"))
	storePut := prom.VectorIndexStoreOperationsDurations.With(opLabels("put"))

	return &Metrics{
		enabled:          true,
		group:            prom.Group,
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

// SetPostings sets this index's contribution to the postings gauge.
//
// Per-shard write path: when grouping is disabled (PROMETHEUS_MONITORING_GROUP
// unset), each (class, shard) hfresh index has its own gauge series; this
// just updates it. When grouping is enabled, every shard/named-vector on the
// node collapses to (class=n/a, shard=n/a), and per-shard .Set() becomes
// last-writer-wins — the reported value is whichever shard wrote most
// recently rather than the node-wide total, which made the recall-after-
// restart e2e test flaky (pre and post counts drifted across a graceful
// restart because the "last writer" identity changed).
//
// Under grouping, this is a no-op. The node-wide sweep in
// db.nodeWideMetricsObserver.observeHFreshPostings owns the (n/a, n/a)
// series and sums PostingMap.Size() across every loaded hfresh index every
// 30s. PostingMap.Size() is used as the source on both sides of a restart
// (it's the in-memory count, and HFresh.Flush() persists in-memory
// FastAddVectorID-only entries to LSMKV so Restore() recovers them), so
// the metric stays stable.
func (m *Metrics) SetPostings(count int) {
	if !m.enabled || m.group {
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

func (m *Metrics) SetPendingAnalyzeTasks(count int64) {
	if !m.enabled {
		return
	}

	m.analyzePending.Set(float64(count))
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

func (m *Metrics) SetPendingSplitTasks(count int64) {
	if !m.enabled {
		return
	}

	m.splitsPending.Set(float64(count))
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

func (m *Metrics) SetPendingMergeTasks(count int64) {
	if !m.enabled {
		return
	}

	m.mergesPending.Set(float64(count))
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

func (m *Metrics) SetPendingReassignTasks(count int64) {
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
