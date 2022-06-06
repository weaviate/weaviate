package hnsw

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
)

type Metrics struct {
	enabled    bool
	tombstones prometheus.Gauge
	threads    prometheus.Gauge
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

	return &Metrics{
		enabled:    true,
		tombstones: tombstones,
		threads:    threads,
		cleaned:    cleaned,
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
