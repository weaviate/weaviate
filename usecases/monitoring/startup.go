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

// StartupPhase is the value of the closed `phase` label: one stretch of the
// boot sequence that runs once per process. Phases nest rather than add up:
// db_reload runs inside raft_open when the node restores from a raft snapshot,
// or inside cluster_open's wait when the raft log catches up after a join.
type StartupPhase string

const (
	StartupPhaseModulesInit   StartupPhase = "modules_init"
	StartupPhaseClusterOpen   StartupPhase = "cluster_open"
	StartupPhaseRaftOpen      StartupPhase = "raft_open"
	StartupPhaseRaftBootstrap StartupPhase = "raft_bootstrap"
	StartupPhaseDBReload      StartupPhase = "db_reload"
)

// AllStartupPhases lists every phase in boot order.
func AllStartupPhases() []StartupPhase {
	return []StartupPhase{
		StartupPhaseModulesInit,
		StartupPhaseClusterOpen,
		StartupPhaseRaftOpen,
		StartupPhaseRaftBootstrap,
		StartupPhaseDBReload,
	}
}

// VectorIndexType is the value of the closed `index_type` label. A dynamic
// index reports as whichever index it currently wraps; hfresh's centroid graph
// and geo-property indexes report as hnsw because that is what restores and
// prefills them.
type VectorIndexType string

const (
	VectorIndexTypeHNSW   VectorIndexType = "hnsw"
	VectorIndexTypeFlat   VectorIndexType = "flat"
	VectorIndexTypeHFresh VectorIndexType = "hfresh"
)

// PrefillMode is the value of the closed `mode` label: sync when the prefill
// ran inside the shard load and therefore delayed readiness, async when it ran
// in the background.
type PrefillMode string

const (
	PrefillModeSync  PrefillMode = "sync"
	PrefillModeAsync PrefillMode = "async"
)

// ShardLoadTrigger is the value of the closed `trigger` label on the shard
// load summary: why a shard that already had files on disk was opened. Only
// startup loads delay readiness; warmup is the background sweep that follows a
// lazy boot; runtime is every load on demand after that (first access, tenant
// activation, replica movement, onload), which a day of tenant churn would
// otherwise pass off as boot cost.
type ShardLoadTrigger string

const (
	ShardLoadTriggerStartup ShardLoadTrigger = "startup"
	ShardLoadTriggerWarmup  ShardLoadTrigger = "warmup"
	ShardLoadTriggerRuntime ShardLoadTrigger = "runtime"
)

// AllShardLoadTriggers lists every trigger, so every series can be
// pre-registered.
func AllShardLoadTriggers() []ShardLoadTrigger {
	return []ShardLoadTrigger{ShardLoadTriggerStartup, ShardLoadTriggerWarmup, ShardLoadTriggerRuntime}
}

// prefillCombos are the only (index_type, mode) pairs a prefill runs as, and
// the only ones pre-registered.
var prefillCombos = [][]string{
	{string(VectorIndexTypeHNSW), string(PrefillModeSync)},
	{string(VectorIndexTypeHNSW), string(PrefillModeAsync)},
	{string(VectorIndexTypeFlat), string(PrefillModeSync)},
	{string(VectorIndexTypeHFresh), string(PrefillModeAsync)},
}

// StartupMetrics reports how long a node takes to become ready and where that
// time goes. Every series has a closed label set, so the cost is fixed
// regardless of how many collections or tenants the node holds.
//
// Phases are gauges: they happen once per process, and a gauge keeps the last
// boot's figure instead of decaying out of a rate() window. Shard loads and
// prefills recur (lazy shards, tenant activation), so they are summaries
// without quantiles: only _sum and _count are exposed, which gives totals,
// counts and averages at two series per label combination.
//
// Nil-safe and concurrency-safe.
type StartupMetrics struct {
	phaseDuration *prometheus.GaugeVec

	startupDuration prometheus.Gauge

	shardLoad          *prometheus.SummaryVec
	vectorIndexRestore *prometheus.SummaryVec
	prefillDuration    *prometheus.SummaryVec

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
	m := &StartupMetrics{
		phaseDuration: r.NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_startup_phase_duration_seconds",
			Help: "Wall-clock seconds the last run of a startup phase took on this node. Phases nest (db_reload runs inside raft_open or cluster_open), so they are not additive. 0 until the phase has completed once.",
		}, []string{"phase"}),
		startupDuration: r.NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_startup_duration_seconds",
			Help: "Seconds from process start until this node first passed the readiness endpoint's check (/v1/.well-known/ready), polled once the API server is configured. 0 until ready.",
		}),
		// no Objectives on purpose: that leaves only _sum and _count
		shardLoad: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_shard_load_duration_seconds",
			Help: "Seconds to open a shard that already has files on disk: LSM buckets and WAL recovery, inverted indexes, vector index restore and any synchronous cache prefill. trigger is startup for a shard opened while its collection's index was built at boot, warmup for one opened by the background sweep that follows a lazy boot, and runtime for one opened on demand afterwards: first access, tenant activation, replica movement or onload. Creating a new shard and failed loads are not observed. Sum and count only.",
		}, []string{"trigger"}),
		vectorIndexRestore: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_vector_index_restore_duration_seconds",
			Help: "Seconds to rebuild a vector index from its on-disk state (snapshot, commit logs, compressed vectors). Only observed when there was state to restore. Sum and count only.",
		}, []string{"index_type"}),
		prefillDuration: r.NewSummaryVec(prometheus.SummaryOpts{
			Name: "weaviate_vector_cache_prefill_duration_seconds",
			Help: "Seconds a vector cache prefill took to complete. mode is sync when it ran inside the shard load and delayed readiness, async when it ran in the background. Aborted and failed prefills are not observed. Sum and count only.",
		}, []string{"index_type", "mode"}),
		processStart: processStart,
	}

	// pre-register every series so it scrapes as zero before it has run
	for _, phase := range AllStartupPhases() {
		m.phaseDuration.WithLabelValues(string(phase)).Set(0)
	}
	for _, trigger := range AllShardLoadTriggers() {
		m.shardLoad.WithLabelValues(string(trigger))
	}
	m.vectorIndexRestore.WithLabelValues(string(VectorIndexTypeHNSW))
	for _, combo := range prefillCombos {
		m.prefillDuration.WithLabelValues(combo...)
	}

	return m
}

// PhaseStarted starts timing the phase and returns a done callback (call
// once, e.g. `defer PhaseStarted(p)()`). A phase that runs again replaces its
// previous duration; until the callback runs the phase reads 0.
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

// ObserveShardLoad records one successful load of an existing shard under the
// trigger that opened it. Callers skip it for a shard that was just created, so
// creation churn does not skew the load distribution.
func (m *StartupMetrics) ObserveShardLoad(trigger ShardLoadTrigger, took time.Duration) {
	if m == nil {
		return
	}

	m.shardLoad.WithLabelValues(string(trigger)).Observe(took.Seconds())
}

// ObserveVectorIndexRestore records one successful restore of a vector index
// that had on-disk state. Callers skip it for a fresh index.
func (m *StartupMetrics) ObserveVectorIndexRestore(indexType VectorIndexType, took time.Duration) {
	if m == nil {
		return
	}

	m.vectorIndexRestore.WithLabelValues(string(indexType)).Observe(took.Seconds())
}

// PrefillStarted starts timing a prefill and returns a done callback (call
// once) that, when err is nil, records how long it took. A prefill cut short
// by shutdown would record a misleadingly short sample, so callers pass the
// abort or failure error instead.
func (m *StartupMetrics) PrefillStarted(indexType VectorIndexType, mode PrefillMode) func(err error) {
	if m == nil {
		return func(error) {}
	}

	labels := []string{string(indexType), string(mode)}
	start := time.Now()

	return func(err error) {
		if err != nil {
			return
		}
		m.prefillDuration.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	}
}
