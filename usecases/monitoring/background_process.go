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

package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BackgroundProcess is the value of the closed `process` label — the only
// label, so cardinality stays constant regardless of shard/tenant count.
type BackgroundProcess string

const (
	ProcessBackup           BackgroundProcess = "backup"
	ProcessRestore          BackgroundProcess = "restore"
	ProcessOffload          BackgroundProcess = "offload"
	ProcessUsageCollection  BackgroundProcess = "usage_collection"
	ProcessCompaction       BackgroundProcess = "compaction"
	ProcessTombstoneCleanup BackgroundProcess = "tombstone_cleanup"
	ProcessAsyncReplication BackgroundProcess = "async_replication"
	ProcessAsyncIndexing    BackgroundProcess = "async_indexing"
	ProcessTTLDeletion      BackgroundProcess = "ttl_deletion"
	ProcessReplicaMovement  BackgroundProcess = "replica_movement"
)

// BackgroundProcessMetrics reports heavy background processes per node, keyed by
// `process`. Nil-safe and concurrency-safe.
type BackgroundProcessMetrics struct {
	// active: concurrently running instances (0 = idle).
	active *prometheus.GaugeVec
	// duration: finished-run seconds; _bucket/_count/_sum give percentiles,
	// throughput and busy-time. Only Started runs are timed.
	duration *prometheus.HistogramVec
}

var backgroundProcessMetrics *BackgroundProcessMetrics

func init() {
	backgroundProcessMetrics = newBackgroundProcessMetrics(prometheus.DefaultRegisterer)
}

// GetBackgroundProcessMetrics returns the singleton on the default registerer.
func GetBackgroundProcessMetrics() *BackgroundProcessMetrics {
	return backgroundProcessMetrics
}

func newBackgroundProcessMetrics(reg prometheus.Registerer) *BackgroundProcessMetrics {
	return &BackgroundProcessMetrics{
		active: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_background_process_active",
			Help: "Number of currently running instances of a background process (0 = idle)",
		}, []string{"process"}),
		duration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name: "weaviate_background_process_duration_seconds",
			Help: "Wall-clock duration of finished background process runs, in seconds",
			// 50ms → ~7.3h: covers a sub-second compaction and a multi-hour backup.
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 20),
		}, []string{"process"}),
	}
}

// IncActive/DecActive raise and lower the active gauge — the primitive for
// split-callback sites (e.g. the replication engine) that can't hold a closure.
// Pair each IncActive with one DecActive; elsewhere prefer Started.
func (m *BackgroundProcessMetrics) IncActive(process BackgroundProcess) {
	if m == nil {
		return
	}
	m.active.WithLabelValues(string(process)).Inc()
}

// DecActive is the counterpart of IncActive.
func (m *BackgroundProcessMetrics) DecActive(process BackgroundProcess) {
	if m == nil {
		return
	}
	m.active.WithLabelValues(string(process)).Dec()
}

// Started marks the process active and returns a done-callback (call once, e.g.
// `defer Started(p)()`) that clears it and records the run's duration. Wrap the
// working burst, not an idle loop, so the gauge reflects real activity.
func (m *BackgroundProcessMetrics) Started(process BackgroundProcess) func() {
	if m == nil {
		return func() {}
	}

	m.IncActive(process)
	start := time.Now()

	return func() {
		m.DecActive(process)
		m.duration.WithLabelValues(string(process)).Observe(time.Since(start).Seconds())
	}
}
