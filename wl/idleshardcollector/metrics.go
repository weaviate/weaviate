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

package idleshardcollector

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// result label values; renaming one empties dashboards with no error.
const (
	resultOK     = "ok"
	resultFailed = "failed"
)

// metrics carry no per-shard labels; namespace labels only the loaded gauge, and only when namespaces are on.
type metrics struct {
	loaded    *prometheus.GaugeVec
	unloading prometheus.Gauge
	unloads   *prometheus.CounterVec

	unloadDuration    prometheus.Histogram
	reloadAfterUnload prometheus.Histogram
}

// newMetrics registers with reg; a nil reg leaves the metrics working but unregistered.
func newMetrics(reg prometheus.Registerer, namespaces bool, logger logrus.FieldLogger) *metrics {
	var nsLabel []string
	if namespaces {
		nsLabel = []string{"namespace"}
	}
	m := &metrics{
		loaded: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_idle_shard_collector_loaded_shards",
			Help: "Loaded lazy shards on this node.",
		}, nsLabel),
		unloading: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_idle_shard_collector_unloading_shards",
			Help: "Lazy shard unloads in flight on this node.",
		}),
		unloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_idle_shard_collector_unloads_total",
			Help: "Idle shard unload attempts by result: ok when the shard was unloaded, failed when it stayed loaded.",
		}, []string{"result"}),
		unloadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_idle_shard_collector_unload_duration_seconds",
			Help:    "Duration of successful lazy shard unloads (flush and close).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		}),
		reloadAfterUnload: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_idle_shard_collector_reload_after_unload_seconds",
			Help:    "Time between unloading a lazy shard and its next load; low values mean thrashing.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		}),
	}
	if reg != nil {
		m.loaded = register(reg, m.loaded, logger)
		m.unloading = register(reg, m.unloading, logger)
		m.unloads = register(reg, m.unloads, logger)
		m.unloadDuration = register(reg, m.unloadDuration, logger)
		m.reloadAfterUnload = register(reg, m.reloadAfterUnload, logger)
	}
	return m
}

func register[T prometheus.Collector](reg prometheus.Registerer, c T, logger logrus.FieldLogger) T {
	got, _, err := monitoring.EnsureRegisteredMetric(reg, c)
	if err != nil {
		logger.WithField("action", "idle_shard_collector_metrics").
			Warnf("metric stays unregistered: %v", err)
		return c
	}
	return got
}

func (m *metrics) observeUnload(unloaded bool) {
	result := resultFailed
	if unloaded {
		result = resultOK
	}
	m.unloads.WithLabelValues(result).Inc()
}
