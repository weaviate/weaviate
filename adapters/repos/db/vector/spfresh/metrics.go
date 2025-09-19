//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	enabled          bool
	size             prometheus.Gauge
	insert           prometheus.Gauge
	insertTime       prometheus.ObserverVec
	delete           prometheus.Gauge
	deleteTime       prometheus.ObserverVec
	postings         prometheus.Gauge
	splitsPending    prometheus.Gauge
	split            prometheus.Observer
	mergesPending    prometheus.Gauge
	merge            prometheus.Observer
	reassignsPending prometheus.Gauge
	reassign         prometheus.Observer
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

	insertTime := prom.VectorIndexDurations.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "create",
	})

	del := prom.VectorIndexOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "delete",
	})

	deleteTime := prom.VectorIndexDurations.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "delete",
	})

	postings := prom.VectorIndexPostings.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	splitsPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "split",
	})

	split := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	mergesPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "merge",
	})

	merge := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	reassignsPending := prom.VectorIndexPendingBackgroundOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "reassign",
	})

	reassign := prom.VectorIndexBackgroundOperationsDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	return &Metrics{
		enabled:          true,
		size:             size,
		insert:           insert,
		insertTime:       insertTime,
		delete:           del,
		deleteTime:       deleteTime,
		postings:         postings,
		splitsPending:    splitsPending,
		split:            split,
		mergesPending:    mergesPending,
		merge:            merge,
		reassignsPending: reassignsPending,
		reassign:         reassign,
	}
}

func (m *Metrics) SetSize(size int) {
	if !m.enabled {
		return
	}

	m.size.Set(float64(size))
}

func (m *Metrics) InsertVector() {
	if !m.enabled {
		return
	}

	m.insert.Inc()
}

func (m *Metrics) TrackInsertObserver(step string) Observer {
	if !m.enabled {
		return noOpObserver
	}

	curried := m.insertTime.With(prometheus.Labels{"step": step})

	return func(start time.Time) {
		took := float64(time.Since(start)) / float64(time.Millisecond)
		curried.Observe(took)
	}
}

func (m *Metrics) DeleteVector() {
	if !m.enabled {
		return
	}

	m.delete.Inc()
}

func (m *Metrics) TrackDelete(start time.Time, step string) {
	if !m.enabled {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.deleteTime.With(prometheus.Labels{"step": step}).Observe(took)
}

func (m *Metrics) AddPosting() {
	if !m.enabled {
		return
	}

	m.postings.Inc()
}

func (m *Metrics) SetPostings(count int) {
	if !m.enabled {
		return
	}

	m.postings.Set(float64(count))
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

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.split.Observe(took)
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

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.merge.Observe(took)
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

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.reassign.Observe(took)
}

type Observer func(start time.Time)

func noOpObserver(start time.Time) {
	// do nothing
}
