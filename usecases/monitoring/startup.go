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

// processStart anchors weaviate_startup_duration_seconds; process_start_time_seconds needs platform support.
var processStart = time.Now()

// StartupMetrics reports time to ready plus vector index restore and cache
// prefill durations: one gauge and two summaries (_sum and _count only, since
// restores and prefills recur on tenant activation and replica movement), no
// labels, five series per node. Nil-safe and concurrency-safe.
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

// SetReady records the first readiness; later calls are no-ops.
func (m *StartupMetrics) SetReady() {
	if m == nil {
		return
	}

	m.readyOnce.Do(func() {
		m.startupDuration.Set(time.Since(m.processStart).Seconds())
	})
}

// TrackReady polls isReady every period until it answers true or ctx is
// cancelled, records the first true answer and reports whether it did.
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

// ObserveVectorIndexRestore records one restore of an index that had on-disk state.
func (m *StartupMetrics) ObserveVectorIndexRestore(took time.Duration) {
	if m == nil {
		return
	}

	m.vectorIndexRestore.Observe(took.Seconds())
}

// PrefillStarted starts timing a prefill; the returned callback records the
// duration only when err is nil, so aborted and failed prefills leave no sample.
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
