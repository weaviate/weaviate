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

package hnsw

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	enabled    bool
	tombstones prometheus.Gauge
	threads    prometheus.Gauge
	insert     prometheus.Gauge
	delete     prometheus.Gauge
	cleaned    prometheus.Counter
}

func NewMetrics(prom *monitoring.PrometheusMetrics,
	className, shardName string) *Metrics {
	if prom == nil {
		return &Metrics{enabled: false}
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

	del := prom.VectorIndexOperations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "delete",
	})

	return &Metrics{
		enabled:    true,
		tombstones: tombstones,
		threads:    threads,
		cleaned:    cleaned,
		insert:     insert,
		delete:     del,
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
