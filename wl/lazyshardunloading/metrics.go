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

package lazyshardunloading

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// reason labels; renaming one empties dashboards with no error.
type reason string

const (
	reasonMakeRoom  reason = "make_room"
	reasonWatermark reason = "watermark"
	reasonIdle      reason = "idle"
)

const (
	admissionUnderLimit = "under_limit"
	admissionMadeRoom   = "made_room"
	admissionOvercommit = "overcommit"
)

// metrics carry no per-shard labels; namespace is a label only when namespaces are on.
type metrics struct {
	namespaces bool

	loaded    *prometheus.GaugeVec
	unloading prometheus.Gauge
	limit     prometheus.Gauge
	overLimit prometheus.Gauge
	unloads   *prometheus.CounterVec

	admitUnder, admitMadeRoom, admitOvercommit prometheus.Counter

	unloadDuration    prometheus.Histogram
	reloadAfterUnload prometheus.Histogram
}

// newMetrics registers with reg; a nil reg leaves the metrics working but unregistered.
func newMetrics(reg prometheus.Registerer, namespaces bool, logger logrus.FieldLogger) *metrics {
	var nsLabel []string
	unloadLabels := []string{"reason", "outcome"}
	if namespaces {
		nsLabel = []string{"namespace"}
		unloadLabels = append(unloadLabels, "namespace")
	}
	admissions := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "weaviate_lazy_shard_unloading_admissions_total",
		Help: "Lazy shard loads by admission outcome (under_limit|made_room|overcommit). Overcommit means the load went over the limit because no shard could be unloaded to make room.",
	}, []string{"outcome"})
	m := &metrics{
		namespaces: namespaces,
		loaded: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_lazy_shard_unloading_loaded_shards",
			Help: "Loaded lazy shards on this node.",
		}, nsLabel),
		unloading: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_lazy_shard_unloading_unloading_shards",
			Help: "Lazy shard unloads in flight on this node.",
		}),
		limit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_lazy_shard_unloading_limit",
			Help: "The configured max_loaded_shards; 0 means no limit.",
		}),
		overLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_lazy_shard_unloading_over_limit",
			Help: "Loaded lazy shards above max_loaded_shards.",
		}),
		unloads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_lazy_shard_unloading_unloads_total",
			Help: "Lazy shard unload attempts by reason (make_room|watermark|idle) and outcome (ok, or why the shard was kept).",
		}, unloadLabels),
		unloadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_lazy_shard_unloading_unload_duration_seconds",
			Help:    "Duration of successful lazy shard unloads (flush and close).",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		}),
		reloadAfterUnload: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "weaviate_lazy_shard_unloading_reload_after_unload_seconds",
			Help:    "Time between unloading a lazy shard and its next load; low values mean thrashing.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 16),
		}),
	}
	if reg != nil {
		m.loaded = register(reg, m.loaded, logger)
		m.unloading = register(reg, m.unloading, logger)
		m.limit = register(reg, m.limit, logger)
		m.overLimit = register(reg, m.overLimit, logger)
		m.unloads = register(reg, m.unloads, logger)
		admissions = register(reg, admissions, logger)
		m.unloadDuration = register(reg, m.unloadDuration, logger)
		m.reloadAfterUnload = register(reg, m.reloadAfterUnload, logger)
	}
	m.admitUnder = admissions.WithLabelValues(admissionUnderLimit)
	m.admitMadeRoom = admissions.WithLabelValues(admissionMadeRoom)
	m.admitOvercommit = admissions.WithLabelValues(admissionOvercommit)
	return m
}

func register[T prometheus.Collector](reg prometheus.Registerer, c T, logger logrus.FieldLogger) T {
	got, _, err := monitoring.EnsureRegisteredMetric(reg, c)
	if err != nil {
		logger.WithField("action", "lazy_shard_unloading_metrics").
			Warnf("metric stays unregistered: %v", err)
		return c
	}
	return got
}

func (m *metrics) observeUnload(r reason, outcome, namespace string) {
	if m.namespaces {
		m.unloads.WithLabelValues(string(r), outcome, namespace).Inc()
		return
	}
	m.unloads.WithLabelValues(string(r), outcome).Inc()
}
