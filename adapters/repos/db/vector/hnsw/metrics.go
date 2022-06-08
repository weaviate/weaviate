package hnsw

import (
	"time"

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
	size       prometheus.Gauge
	grow       prometheus.Observer
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

	size := prom.VectorIndexSize.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
	})

	grow := prom.VectorIndexMaintenanceDurations.With(prometheus.Labels{
		"class_name": className,
		"shard_name": shardName,
		"operation":  "grow",
	})

	return &Metrics{
		enabled:    true,
		tombstones: tombstones,
		threads:    threads,
		cleaned:    cleaned,
		insert:     insert,
		delete:     del,
		size:       size,
		grow:       grow,
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
