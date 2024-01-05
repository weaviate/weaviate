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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type Metrics struct {
	enabled          bool
	tombstones       prometheus.Gauge
	threads          prometheus.Gauge
	insert           prometheus.Gauge
	insertTime       prometheus.ObserverVec
	delete           prometheus.Gauge
	deleteTime       prometheus.ObserverVec
	cleaned          prometheus.Counter
	size             prometheus.Gauge
	grow             prometheus.Observer
	startupProgress  prometheus.Gauge
	startupDurations prometheus.ObserverVec
	startupDiskIO    prometheus.ObserverVec
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

	tombstones := prom.VectorIndexTombstones.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	threads := prom.VectorIndexTombstoneCleanupThreads.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	cleaned := prom.VectorIndexTombstoneCleanedCount.With(prometheus.Labels{
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

	size := prom.VectorIndexSize.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	grow := prom.VectorIndexMaintenanceDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "grow",
	})

	startupProgress := prom.StartupProgress.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "hnsw_read_commitlogs",
	})

	startupDurations := prom.StartupDurations.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	startupDiskIO := prom.StartupDiskIO.MustCurryWith(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	return &Metrics{
		enabled:          true,
		tombstones:       tombstones,
		threads:          threads,
		cleaned:          cleaned,
		insert:           insert,
		insertTime:       insertTime,
		delete:           del,
		deleteTime:       deleteTime,
		size:             size,
		grow:             grow,
		startupProgress:  startupProgress,
		startupDurations: startupDurations,
		startupDiskIO:    startupDiskIO,
	}
}

func (m *Metrics) AddTombstone() {
	if !m.enabled {
		return
	}

	m.tombstones.Inc()
}

func (m *Metrics) RemoveTombstone() {
	if !m.enabled {
		return
	}

	m.tombstones.Dec()
}

func (m *Metrics) StartCleanup(threads int) {
	if !m.enabled {
		return
	}

	m.threads.Add(float64(threads))
}

func (m *Metrics) EndCleanup(threads int) {
	if !m.enabled {
		return
	}

	m.threads.Sub(float64(threads))
}

func (m *Metrics) CleanedUp() {
	if !m.enabled {
		return
	}

	m.cleaned.Inc()
}

func (m *Metrics) InsertVector() {
	if !m.enabled {
		return
	}

	m.insert.Inc()
}

func (m *Metrics) DeleteVector() {
	if !m.enabled {
		return
	}

	m.delete.Inc()
}

func (m *Metrics) SetSize(size int) {
	if !m.enabled {
		return
	}

	m.size.Set(float64(size))
}

func (m *Metrics) GrowDuration(start time.Time) {
	if !m.enabled {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.grow.Observe(took)
}

type Observer func(start time.Time)

func noOpObserver(start time.Time) {
	// do nothing
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

func (m *Metrics) TrackDelete(start time.Time, step string) {
	if !m.enabled {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.deleteTime.With(prometheus.Labels{"step": step}).Observe(took)
}

func (m *Metrics) StartupProgress(ratio float64) {
	if !m.enabled {
		return
	}

	m.startupProgress.Set(ratio)
}

func (m *Metrics) TrackStartupTotal(start time.Time) {
	if !m.enabled {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "hnsw_read_all_commitlogs"}).Observe(took)
}

func (m *Metrics) TrackStartupIndividual(start time.Time) {
	if !m.enabled {
		return
	}

	took := float64(time.Since(start)) / float64(time.Millisecond)
	m.startupDurations.With(prometheus.Labels{"operation": "hnsw_read_single_commitlog"}).Observe(took)
}

func (m *Metrics) TrackStartupReadCommitlogDiskIO(read int64, nanoseconds int64) {
	if !m.enabled {
		return
	}

	seconds := float64(nanoseconds) / float64(time.Second)
	throughput := float64(read) / float64(seconds)
	m.startupDiskIO.With(prometheus.Labels{"operation": "hnsw_read_commitlog"}).Observe(throughput)
}
