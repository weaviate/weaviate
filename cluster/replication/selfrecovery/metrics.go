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

package selfrecovery

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds Prometheus collectors for the self-recovery subsystem.
// No (collection, shard) labels: a wiped node restoring many shards
// would otherwise inflate cardinality. Per-shard drill-down lives in
// /replication/replicate/list and structured logs.
type Metrics struct {
	InProgress                 prometheus.Gauge
	StartedTotal               *prometheus.CounterVec   // labels: source_node
	CompletedTotal             *prometheus.CounterVec   // labels: result (success|failure|cancelled)
	DurationSeconds            *prometheus.HistogramVec // labels: result (success|failure|empty_fallback|cancelled)
	NoDataEmptyTotal           prometheus.Counter
	NoDataDuringBootstrapTotal prometheus.Counter
	UnreachablePeerTotal       *prometheus.CounterVec // labels: peer
	GiveupTotal                prometheus.Counter
	AcceptEmptyTotal           prometheus.Counter
	SubmitDroppedTotal         prometheus.Counter
}

var (
	metricsOnce sync.Once
	metricsInst *Metrics
)

// GlobalMetrics returns the process-wide Metrics singleton, registering
// on the default Prometheus registry on first call.
func GlobalMetrics() *Metrics {
	metricsOnce.Do(func() {
		metricsInst = &Metrics{
			InProgress: promauto.NewGauge(prometheus.GaugeOpts{
				Name: "weaviate_self_recovery_in_progress",
				Help: "Number of self-recovery operations currently in progress on this node.",
			}),
			StartedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_started_total",
				Help: "Total number of self-recovery operations started, by source peer.",
			}, []string{"source_node"}),
			CompletedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_completed_total",
				Help: "Total number of self-recovery operations completed, by terminal result (success|failure|cancelled).",
			}, []string{"result"}),
			DurationSeconds: promauto.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "weaviate_self_recovery_duration_seconds",
				Help:    "End-to-end duration of a self-recovery operation, by terminal result (success|failure|empty_fallback|cancelled).",
				Buckets: prometheus.ExponentialBuckets(10, 2, 10), // 10s, 20s, 40s, ... ~1.4h
			}, []string{"result"}),
			NoDataEmptyTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_no_data_empty_total",
				Help: "Self-recovery occurrences where all probed peers definitively reported no data after RAFT bootstrap completed (catastrophic-wipe). Alert on this.",
			}),
			NoDataDuringBootstrapTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_no_data_during_bootstrap_total",
				Help: "Self-recovery occurrences during the RAFT bootstrap window where all probed peers reported no data (likely a class added during this node's downtime, not data loss).",
			}),
			UnreachablePeerTotal: promauto.NewCounterVec(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_unreachable_peer_total",
				Help: "Probes that failed to reach a peer (transport/timeout), by peer.",
			}, []string{"peer"}),
			GiveupTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_giveup_total",
				Help: "Self-recovery attempts that exhausted retries without reaching READY.",
			}),
			AcceptEmptyTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_accept_empty_total",
				Help: "Operator invocations of the accept-empty escape hatch.",
			}),
			SubmitDroppedTotal: promauto.NewCounter(prometheus.CounterOpts{
				Name: "weaviate_self_recovery_submit_dropped_total",
				Help: "Submissions dropped because the in-process worker queue was full (will be retried on next node restart).",
			}),
		}
	})
	return metricsInst
}
