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
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// processStart anchors weaviate_startup_duration_seconds: package
// initialisation runs before main, and unlike process_start_time_seconds it
// does not depend on platform support in the Go process collector.
var processStart = time.Now()

// StartupMetrics reports how long a node takes to become ready and how much
// time goes into rebuilding vector indexes and prefilling their caches. No
// series carries a label, so the cost is five series per node whatever the
// node holds.
//
// Time to ready is a gauge: it happens once per process, and a gauge keeps the
// figure for the life of the process. Restores and prefills recur (lazy
// shards, tenant activation, replica movement), so they are summaries without
// quantiles: only _sum and _count, which gives totals, counts and averages.
//
// Nil-safe and concurrency-safe.
type StartupMetrics struct {
	startupDuration    prometheus.Gauge
	vectorIndexRestore prometheus.Summary
	prefillDuration    prometheus.Summary

	processStart time.Time
	readyOnce    sync.Once
}

var startupMetrics *StartupMetrics

func init() {
	startupMetrics = newStartupMetrics(prometheus.DefaultRegisterer, processStart)
}

// GetStartupMetrics returns the singleton on the default registerer.
func GetStartupMetrics() *StartupMetrics {
	return startupMetrics
}

func newStartupMetrics(reg prometheus.Registerer, processStart time.Time) *StartupMetrics {
	r := promauto.With(reg)
	return &StartupMetrics{
		startupDuration: r.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_startup_duration_seconds",
			Help: "Seconds from process start until this node first passed the readiness endpoint's check (/v1/.well-known/ready), polled once the API server is configured. 0 until ready.",
		}),
		// no Objectives on purpose: that leaves only _sum and _count
		vectorIndexRestore: r.NewSummary(prometheus.SummaryOpts{
			Name: "weaviate_vector_index_restore_duration_seconds",
			Help: "Seconds to rebuild a vector index from its on-disk state (snapshot, commit logs, compressed vectors), whenever a shard opens: at boot, on tenant activation, on replica movement. Only observed when there was state to restore. Sum and count only.",
		}),
		prefillDuration: r.NewSummary(prometheus.SummaryOpts{
			Name: "weaviate_vector_cache_prefill_duration_seconds",
			Help: "Seconds a vector cache prefill took to complete, whenever a shard opens: at boot, on tenant activation, on replica movement. Aborted and failed prefills, and shards with nothing to prefill, are not observed. Sum and count only.",
		}),
		processStart: processStart,
	}
}

// SetReady records the moment the node first became ready. Later calls are
// no-ops.
func (m *StartupMetrics) SetReady() {
	if m == nil {
		return
	}

	m.readyOnce.Do(func() {
		m.startupDuration.Set(time.Since(m.processStart).Seconds())
	})
}

// TrackReady polls isReady every period until it answers true or ctx is
// cancelled, records the first true answer with SetReady, and reports whether
// it did. Nothing else polls the readiness check, so a tracker has to.
func (m *StartupMetrics) TrackReady(ctx context.Context, isReady func() bool, period time.Duration) bool {
	if m == nil {
		return false
	}

	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if isReady() {
			m.SetReady()
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}

// ObserveVectorIndexRestore records one successful restore of a vector index
// that had on-disk state. Callers skip it for a fresh index.
func (m *StartupMetrics) ObserveVectorIndexRestore(took time.Duration) {
	if m == nil {
		return
	}

	m.vectorIndexRestore.Observe(took.Seconds())
}

// PrefillStarted starts timing a prefill and returns a done callback (call
// once) that, when err is nil, records how long it took. A prefill cut short
// by shutdown would record a misleadingly short sample, so callers pass the
// abort or failure error instead.
func (m *StartupMetrics) PrefillStarted() func(err error) {
	if m == nil {
		return func(error) {}
	}

	start := time.Now()
	return func(err error) {
		if err != nil {
			return
		}
		m.prefillDuration.Observe(time.Since(start).Seconds())
	}
}
