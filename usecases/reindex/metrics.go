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

package reindex

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Both label vocabularies are closed sets. A label carrying runtime data — a
// collection, a node, a shard, a task id — mints one series per value, and a
// multi-tenant collection has one shard per tenant.
const (
	GateSubmit  = "submit"
	GateBackup  = "backup"
	GateRestore = "restore"
	GateOverlap = "overlap"
	// A shard transfer meets the same rung a backup does, but an operator
	// chasing a stalled replica movement must not have to read it as a backup.
	GateTransfer = "transfer"
)

const (
	VerdictBackupBusy  = "backup_busy"
	VerdictRestoreBusy = "restore_busy"
	VerdictUnreachable = "unreachable"
	VerdictLiveTask    = "live_task"
	// Told apart from VerdictLiveTask because an operator acts on them
	// differently: one is waited out, the other is a cluster to repair.
	VerdictTaskListUnreadable = "task_list_unreadable"
	VerdictHoldSubmit         = "hold_submit"
	VerdictHoldCleanup        = "hold_cleanup"
	VerdictHoldUnknown        = "hold_unrecognized"
	VerdictOverlap            = "overlap_observed"
	VerdictOverlapUnsure      = "overlap_undetermined"
)

type GateMetrics struct {
	refusals  *prometheus.CounterVec
	rollbacks *prometheus.CounterVec
}

// The gauges read openHolds at scrape, which is what makes a hold visible while
// it is open; the counter only ever reports windows that already closed. The
// caller names the kinds, so one cannot be added without deciding on a series.
func NewGateMetrics(reg prometheus.Registerer, openHolds map[string]func() int,
	rollbackOutcomes []string,
) *GateMetrics {
	factory := promauto.With(reg)
	for hold, count := range openHolds {
		factory.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "weaviate_reindex_open_holds",
			Help:        "Reindex holds currently closing the backup and restore gates over some collection, by kind.",
			ConstLabels: prometheus.Labels{"hold": hold},
		}, func() float64 { return float64(count()) })
	}

	metrics := &GateMetrics{
		refusals: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_reindex_gate_refusals_total",
			Help: "Operations refused by a runtime-reindex gate, by the gate that refused and what it found.",
		}, []string{"gate", "verdict"}),
		rollbacks: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "weaviate_reindex_submit_rollbacks_total",
			Help: "Reindex submissions rolled back after losing the race to a backup, by how the rollback ended.",
		}, []string{"outcome"}),
	}

	// A CounterVec with no children emits nothing, and "no data" is a different
	// alerting state from zero. Every series is enumerable, so all start at zero.
	for gate, verdicts := range reachableVerdicts {
		for _, verdict := range verdicts {
			metrics.refusals.WithLabelValues(gate, verdict)
		}
	}
	for _, outcome := range rollbackOutcomes {
		metrics.rollbacks.WithLabelValues(outcome)
	}
	return metrics
}

// A pair missing here is absent from /metrics until the first time it fires.
var reachableVerdicts = map[string][]string{
	GateSubmit:  {VerdictBackupBusy, VerdictRestoreBusy, VerdictUnreachable},
	GateBackup:  {VerdictLiveTask, VerdictTaskListUnreadable, VerdictHoldSubmit, VerdictHoldCleanup, VerdictHoldUnknown},
	GateRestore: {VerdictLiveTask, VerdictTaskListUnreadable, VerdictHoldSubmit, VerdictHoldCleanup, VerdictHoldUnknown},
	GateOverlap: {VerdictOverlap, VerdictOverlapUnsure},
	// The same rung as the backup gate, so the same findings.
	GateTransfer: {VerdictLiveTask, VerdictTaskListUnreadable, VerdictHoldSubmit, VerdictHoldCleanup, VerdictHoldUnknown},
}

// Refused counts one refusal whatever it covers: a gate that closed over sixty
// shards refused one operation, not sixty.
//
// A nil receiver is a no-op, which is what a fixture that wired no metrics has.
func (m *GateMetrics) Refused(gate, verdict string) {
	if m == nil {
		return
	}
	m.refusals.WithLabelValues(gate, verdict).Inc()
}

// An outcome that leaves a migration running while a capture is in flight is
// what an operator pages on, and a log line is not something to page on.
func (m *GateMetrics) RolledBack(outcome string) {
	if m == nil {
		return
	}
	m.rollbacks.WithLabelValues(outcome).Inc()
}
