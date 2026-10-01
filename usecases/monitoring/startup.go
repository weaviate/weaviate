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

// processStart anchors weaviate_startup_duration_seconds. Package
// initialisation runs before main, so this is within milliseconds of the
// moment the process started, and unlike process_start_time_seconds it does
// not depend on platform support in the Go process collector.
var processStart = time.Now()

// StartupPhase is the value of the closed `phase` label on the startup phase
// gauges: one stretch of the boot sequence that runs once per process.
//
// Phases nest rather than add up. db_reload runs inside raft_open when the
// node restores from a raft snapshot, or inside cluster_open's wait when the
// raft log catches up after a join, so summing phases double counts.
type StartupPhase string

const (
	// StartupPhaseModulesInit covers registering and initialising the enabled
	// modules, which includes waiting for their sidecars (vectorizers,
	// rerankers) to answer.
	StartupPhaseModulesInit StartupPhase = "modules_init"
	// StartupPhaseClusterOpen covers the whole cluster service open, from
	// starting the internal RPC server until the local DB is restored.
	StartupPhaseClusterOpen StartupPhase = "cluster_open"
	// StartupPhaseRaftOpen covers opening the raft store, including restoring
	// the latest raft snapshot and, when that snapshot is ahead of what the DB
	// last applied, reloading the DB from it.
	StartupPhaseRaftOpen StartupPhase = "raft_open"
	// StartupPhaseRaftBootstrap covers joining the existing cluster or
	// bootstrapping a new one, bounded by RAFT_BOOTSTRAP_TIMEOUT.
	StartupPhaseRaftBootstrap StartupPhase = "raft_bootstrap"
	// StartupPhaseDBReload covers loading every local collection and its
	// eager shards from the schema. This is the phase that scales with data
	// volume. It can run again later if raft installs a newer snapshot.
	StartupPhaseDBReload StartupPhase = "db_reload"
)

// AllStartupPhases lists every phase in boot order, so every series can be
// pre-registered and scrapes zero before the phase has run.
func AllStartupPhases() []StartupPhase {
	return []StartupPhase{
		StartupPhaseModulesInit,
		StartupPhaseClusterOpen,
		StartupPhaseRaftOpen,
		StartupPhaseRaftBootstrap,
		StartupPhaseDBReload,
	}
}

// VectorIndexType is the value of the closed `index_type` label on the vector
// index load metrics. A dynamic index reports as whichever index it currently
// wraps; hfresh's centroid graph and geo-property indexes report as hnsw
// because that is what restores and prefills them.
type VectorIndexType string

const (
	VectorIndexTypeHNSW   VectorIndexType = "hnsw"
	VectorIndexTypeFlat   VectorIndexType = "flat"
	VectorIndexTypeHFresh VectorIndexType = "hfresh"
)

// PrefillMode is the value of the closed `mode` label on the cache prefill
// metrics: sync when the prefill ran inside the shard load and therefore
// delayed readiness, async when it ran in the background after the shard was
// already serving.
type PrefillMode string

const (
	PrefillModeSync  PrefillMode = "sync"
	PrefillModeAsync PrefillMode = "async"
)

// prefillCombos are the (index_type, mode) pairs a prefill actually runs as,
// and the only ones pre-registered: hnsw honours the wait-for-cache flag,
// flat always preloads synchronously and hfresh warms its version map in the
// background.
var prefillCombos = [][]string{
	{string(VectorIndexTypeHNSW), string(PrefillModeSync)},
	{string(VectorIndexTypeHNSW), string(PrefillModeAsync)},
	{string(VectorIndexTypeFlat), string(PrefillModeSync)},
	{string(VectorIndexTypeHFresh), string(PrefillModeAsync)},
}

// StartupMetrics reports how long a node takes to become ready and where that
// time goes: the boot phases, each shard load, each vector index restore and
// each vector cache prefill. Every series is node-level with a closed label
// set, so the cost is fixed regardless of how many collections or tenants the
// node holds, and PROMETHEUS_MONITORING_GROUP has no effect on it.
//
// Phases are gauges rather than histograms: they happen once per process, and
// a gauge keeps the last boot's figure for the life of the process instead of
// decaying out of a rate() window. Shard loads and prefills recur (lazy
// shards, tenant activation), so they are summaries, deliberately without
// quantiles: only _sum and _count are exposed, two series per label
// combination, so the per-node cost stays at a few dozen series in hosted
// setups with many nodes. That gives totals, counts and averages but no
// percentiles. Because a node observes them only as it loads, the cumulative
// _sum/_count since boot is the startup total and needs no rate().
//
// Nil-safe and concurrency-safe.
type StartupMetrics struct {
	phaseDuration *prometheus.GaugeVec

	startupDuration prometheus.Gauge
	readyTimestamp  prometheus.Gauge

	shardLoad          *prometheus.SummaryVec
	vectorIndexRestore *prometheus.SummaryVec

	prefillDuration *prometheus.SummaryVec
	prefillActive   *prometheus.GaugeVec

	processStart time.Time
	readyOnce    sync.Once
}

var startupMetrics *StartupMetrics

func init() {
	startupMetrics = newStartupMetrics(prometheus.DefaultRegisterer, processStart)
}

// GetStartupMetrics returns the singleton on the default registerer. It is
// registered whether or not monitoring is enabled; when it is not, /metrics is
// never served and the only cost is the fixed handful of series.
func GetStartupMetrics() *StartupMetrics {
	return startupMetrics
}

func newStartupMetrics(reg prometheus.Registerer, processStart time.Time) *StartupMetrics {
	r := promauto.With(reg)
	m := &StartupMetrics{
		phaseDuration: r.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_startup_phase_duration_seconds",
			Help: "Wall-clock seconds the last run of a startup phase took on this node. Phases nest (db_reload runs inside raft_open or cluster_open), so they are not additive. 0 until the phase has completed once.",
		}, []string{"phase"}),
		startupDuration: r.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_startup_duration_seconds",
			Help: "Seconds from process start until this node first satisfied the readiness probe's predicate (the same check as /v1/.well-known/ready), polled once the API server is configured. 0 until ready.",
		}),
		readyTimestamp: r.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_startup_ready_timestamp_seconds",
			Help: "Unix time at which this node first satisfied the readiness probe's predicate. 0 until ready.",
		}),
		// The three summaries below set no Objectives on purpose: that leaves
		// only _sum and _count, no quantile series and no buckets.
		shardLoad: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_shard_load_duration_seconds",
			Help: "Seconds to load an existing shard from disk: LSM buckets and WAL recovery, inverted indexes, vector index restore and any synchronous cache prefill. registration is eager for shards opened at startup and lazy for shards opened on first access. Creating a new shard and failed loads are not observed. Sum and count only.",
		}, []string{"registration"}),
		vectorIndexRestore: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_vector_index_restore_duration_seconds",
			Help: "Seconds to rebuild a vector index from its on-disk state (snapshot, commit logs, compressed vectors). Only observed when there was state to restore. Sum and count only.",
		}, []string{"index_type"}),
		prefillDuration: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_vector_cache_prefill_duration_seconds",
			Help: "Seconds a vector cache prefill took to complete. mode is sync when it ran inside the shard load and delayed readiness, async when it ran in the background. Aborted and failed prefills are not observed. Sum and count only.",
		}, []string{"index_type", "mode"}),
		prefillActive: r.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_vector_cache_prefill_active",
			Help: "Number of vector cache prefills currently running on this node",
		}, []string{"index_type", "mode"}),
		processStart: processStart,
	}

	for _, phase := range AllStartupPhases() {
		m.phaseDuration.WithLabelValues(string(phase)).Set(0)
	}
	for _, registration := range []ShardRegistration{ShardRegistrationEager, ShardRegistrationLazy} {
		m.shardLoad.WithLabelValues(string(registration))
	}
	m.vectorIndexRestore.WithLabelValues(string(VectorIndexTypeHNSW))
	for _, combo := range prefillCombos {
		m.prefillDuration.WithLabelValues(combo...)
		m.prefillActive.WithLabelValues(combo...).Set(0)
	}

	return m
}

// PhaseStarted starts timing the phase and returns a done callback (call
// once, e.g. `defer PhaseStarted(p)()`) that publishes the elapsed wall time.
// A phase that runs again replaces its previous duration. Until the callback
// runs the phase reads 0, which is how a node stuck in a phase shows up.
func (m *StartupMetrics) PhaseStarted(phase StartupPhase) func() {
	if m == nil {
		return func() {}
	}

	start := time.Now()
	return func() {
		m.phaseDuration.WithLabelValues(string(phase)).Set(time.Since(start).Seconds())
	}
}

// SetReady records the moment the node first became ready. Later calls are
// no-ops, so a poll loop can call it without guarding.
func (m *StartupMetrics) SetReady() {
	if m == nil {
		return
	}

	m.readyOnce.Do(func() {
		now := time.Now()
		m.readyTimestamp.Set(float64(now.UnixNano()) / float64(time.Second))
		m.startupDuration.Set(now.Sub(m.processStart).Seconds())
	})
}

// TrackReady polls isReady every period until it answers true or ctx is
// cancelled, records the first true answer with SetReady, and reports whether
// it did. Nothing polls the readiness predicate outside the kubernetes probe,
// so a tracker has to; it returns after the first true answer, so steady state
// costs nothing.
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

// ObserveShardLoad records one successful load of an existing shard. Callers
// skip it for a shard that was just created, so creation churn (tenant
// creation, class creation) does not skew the load distribution.
func (m *StartupMetrics) ObserveShardLoad(registration ShardRegistration, took time.Duration) {
	if m == nil {
		return
	}

	m.shardLoad.WithLabelValues(string(registration)).Observe(took.Seconds())
}

// ObserveVectorIndexRestore records one successful restore of a vector index
// that had on-disk state. Callers skip it for a fresh index.
func (m *StartupMetrics) ObserveVectorIndexRestore(indexType VectorIndexType, took time.Duration) {
	if m == nil {
		return
	}

	m.vectorIndexRestore.WithLabelValues(string(indexType)).Observe(took.Seconds())
}

// PrefillStarted counts a running prefill and returns a done callback (call
// once) that stops counting it and, when err is nil, records how long it took.
// Pass the abort or failure error otherwise: a prefill cut short by shutdown
// would record a misleadingly short sample.
func (m *StartupMetrics) PrefillStarted(indexType VectorIndexType, mode PrefillMode) func(err error) {
	if m == nil {
		return func(error) {}
	}

	labels := []string{string(indexType), string(mode)}
	m.prefillActive.WithLabelValues(labels...).Inc()
	start := time.Now()

	return func(err error) {
		m.prefillActive.WithLabelValues(labels...).Dec()
		if err != nil {
			return
		}
		m.prefillDuration.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	}
}
