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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// SegmentEditOpsMetrics exposes the Prometheus series for LSM segment edit
// operations; the background cleanup that strips a dropped vector index is
// the first user. The
// collectors are process-global and shared across shards via
// EnsureRegisteredMetric, so it is safe to construct one instance per shard.
// The class and shard names are bound per instance so call sites pass only the
// op type or op id.
//
// The GAUGES carry {class_name, shard_name}; the duration histogram does not.
// The gauges describe the state of a shard, and there the shard dimension is
// load-bearing twice over: an op id is the drop EPOCH, shared by every shard
// taking part in the same drop cluster-wide, so a series without it is written
// by all of them and reads as whichever wrote last; and in a multi-tenant
// collection the shard name IS the tenant name, unique only within its class.
// Those labels are also what DeleteOwnShard reaps by. The histogram
// describes the cost of the work instead, which is node-level.
//
// A nil *SegmentEditOpsMetrics is a valid no-op receiver: when monitoring is
// disabled the holding component carries nil and callers need not branch.
type SegmentEditOpsMetrics struct {
	class string
	shard string
	// grouped mirrors PrometheusMetrics.Group. The per-shard GAUGES are
	// suppressed in that mode: their labels have already collapsed to "n/a", so
	// every shard would write one shared series and the readings would be
	// meaningless — 100 tenants owing 3 segments each would read 3, not 300,
	// and the first to drain would zero it for all of them. Absent beats wrong.
	// The cumulative series carry no shard dimension anyway, so they stay on.
	grouped bool

	active          *prometheus.GaugeVec
	pendingSegments *prometheus.GaugeVec
	transformerDur  *prometheus.HistogramVec
	segmentsOwed    *prometheus.GaugeVec
}

// NewSegmentEditOpsMetrics registers (or reuses) the edit-ops collectors against
// reg and binds them to className/shardName. Returns a nil-safe instance.
// Callers pass the names AFTER the monitoring-group collapse (NewMetrics turns
// both into "n/a" when PROMETHEUS_MONITORING_GROUP is set) and the grouped flag
// alongside, because for these gauges collapsing is not enough — see the
// grouped field.
func NewSegmentEditOpsMetrics(reg prometheus.Registerer, className, shardName string, grouped bool) (*SegmentEditOpsMetrics, error) {
	active, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "lsm_segment_edit_ops_active",
		Help:      "Number of LSM segment edit operations armed on this shard, by op type. An op stays armed until its drop completes cluster-wide, so active > 0 with segments_owed = 0 is the ordinary state of a shard that finished its own rewrites and is waiting on peers - not a stall.",
	}, []string{"op_type", "class_name", "shard_name"}))
	if err != nil {
		return nil, fmt.Errorf("register lsm_segment_edit_ops_active: %w", err)
	}

	pendingSegments, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "lsm_segment_edit_ops_pending_segments",
		Help:      "Segments still queued for rewrite for an edit op on this shard. Excludes segments quarantined after exhausting their retry budget; compare with weaviate_lsm_segment_edit_ops_segments_owed to see those.",
	}, []string{"op_id", "class_name", "shard_name"}))
	if err != nil {
		return nil, fmt.Errorf("register lsm_segment_edit_ops_pending_segments: %w", err)
	}

	transformerDur, _, err := EnsureRegisteredMetric(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "lsm_segment_edit_ops_transformer_duration_seconds",
		Help:      "Wall-clock seconds one dedicated cleaner pass spent rewriting one segment under an edit op, by op type: the full rewrite loop - cursor scan, shadowed-key checks, transform, buffered writes - excluding the fsync, the segment swap and the bookkeeping commits. Compaction runs the same transformer for its own reasons and is not timed.",
		// Matches the sibling lsmkv compaction-duration histogram rather than the
		// shared LatencyBuckets, which stop at 100s. Segment size is unbounded, so
		// a rewrite can run longer than that, and every such rewrite — precisely
		// the ones worth seeing — would land in the same +Inf bucket.
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 15), // 0.01s → ~163.84s
	}, []string{"op_type"}))
	if err != nil {
		return nil, fmt.Errorf("register lsm_segment_edit_ops_transformer_duration_seconds: %w", err)
	}

	segmentsOwed, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "lsm_segment_edit_ops_segments_owed",
		Help:      "Segments an edit op still owes on this shard: queued plus quarantined. Stays above weaviate_lsm_segment_edit_ops_pending_segments exactly while segments sit in quarantine. A new round returns quarantined segments to the queue with a fresh retry budget, but only a bounded number of times; once a segment exhausts that budget it stays quarantined for good. So a brief divergence is a drop mid-retry, and one that persists across rounds is a drop that will not finish without intervention.",
	}, []string{"op_id", "class_name", "shard_name"}))
	if err != nil {
		return nil, fmt.Errorf("register lsm_segment_edit_ops_segments_owed: %w", err)
	}

	return &SegmentEditOpsMetrics{
		class:           className,
		shard:           shardName,
		grouped:         grouped,
		active:          active,
		pendingSegments: pendingSegments,
		transformerDur:  transformerDur,
		segmentsOwed:    segmentsOwed,
	}, nil
}

// PerShardGaugesEnabled reports whether the per-shard gauges are actually
// collected. False in grouped mode, where they are suppressed — callers use it
// to skip the work of computing values nothing will record.
func (m *SegmentEditOpsMetrics) PerShardGaugesEnabled() bool {
	return m != nil && !m.grouped
}

// SetActive records the number of active ops of opType on this shard.
func (m *SegmentEditOpsMetrics) SetActive(opType string, n int) {
	if m == nil || m.grouped {
		return
	}
	m.active.WithLabelValues(opType, m.class, m.shard).Set(float64(n))
}

// SetPendingSegments records how many segments still await rewrite for opID on
// this shard.
func (m *SegmentEditOpsMetrics) SetPendingSegments(opID string, n int) {
	if m == nil || m.grouped {
		return
	}
	m.pendingSegments.WithLabelValues(opID, m.class, m.shard).Set(float64(n))
}

// SetSegmentsOwed records how many segments opID still owes on this shard —
// queued plus quarantined. Quarantined segments are excluded from
// SetPendingSegments, so a stalled drop holds this gauge above pending instead
// of both reading zero like a finished one (see the exported Help for why the
// divergence must persist across rounds to mean stalled).
func (m *SegmentEditOpsMetrics) SetSegmentsOwed(opID string, n int) {
	if m == nil || m.grouped {
		return
	}
	m.segmentsOwed.WithLabelValues(opID, m.class, m.shard).Set(float64(n))
}

// ObserveTransformerDuration records the wall-clock seconds spent applying the
// transformer to one segment for opType.
func (m *SegmentEditOpsMetrics) ObserveTransformerDuration(opType string, seconds float64) {
	if m == nil {
		return
	}
	m.transformerDur.WithLabelValues(opType).Observe(seconds)
}

// ForgetOp drops THIS shard's series for an op that no longer exists in the
// sidecar — completed or orphan-swept — so vanished ops do not linger as stale
// gauges. Scoped to the shard: an op id is the drop epoch and every
// participating shard shares it, so dropping by op id alone would delete
// sibling shards' live series. The three labels are the gauges' complete set,
// so this is an exact Delete — a partial match would scan every series on the
// node under the vec's write lock, which all shards' gauge writes contend on.
func (m *SegmentEditOpsMetrics) ForgetOp(opID string) {
	if m == nil {
		return
	}
	labels := prometheus.Labels{"op_id": opID, "class_name": m.class, "shard_name": m.shard}
	m.pendingSegments.Delete(labels)
	m.segmentsOwed.Delete(labels)
}

// DeleteOwnShard drops every series bound to this instance's class/shard, so
// an unloaded or deleted shard leaves no permanent series behind on the node.
// Reached from SegmentGroup.reapEditOpsMetrics — via shutdown (Shard.drop
// guarantees store.Shutdown on every exit) or via the constructor's rollback
// for a group discarded before it ever had a shutdown to reach. A shard that
// was never loaded never published.
func (m *SegmentEditOpsMetrics) DeleteOwnShard() {
	if m == nil {
		return
	}
	// Gauges only. The duration histogram is cumulative and deliberately carries
	// no shard dimension: it describes the cost of the work, not the state of a
	// shard, and a cumulative series that gets reaped cannot be rebuilt — a
	// tenant deactivate/reactivate would silently reset it.
	labels := prometheus.Labels{"class_name": m.class, "shard_name": m.shard}
	m.active.DeletePartialMatch(labels)
	m.pendingSegments.DeletePartialMatch(labels)
	m.segmentsOwed.DeletePartialMatch(labels)
}
