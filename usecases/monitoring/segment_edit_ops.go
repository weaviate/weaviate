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
// operations; drop-vector-index Phase-2 cleanup is the first user. The
// collectors are process-global and shared across shards via
// EnsureRegisteredMetric, so it is safe to construct one instance per shard.
// shardName is bound per instance so call sites pass only the op type or op id.
//
// A nil *SegmentEditOpsMetrics is a valid no-op receiver: when monitoring is
// disabled the holding component carries nil and callers need not branch.
type SegmentEditOpsMetrics struct {
	shard string

	active          *prometheus.GaugeVec
	pendingSegments *prometheus.GaugeVec
	transformerDur  *prometheus.HistogramVec
	bytesReclaimed  *prometheus.CounterVec
	forcedRemaining *prometheus.GaugeVec
}

// NewSegmentEditOpsMetrics registers (or reuses) the edit-ops collectors against
// reg and binds them to shardName. Returns a nil-safe instance.
func NewSegmentEditOpsMetrics(reg prometheus.Registerer, shardName string) (*SegmentEditOpsMetrics, error) {
	active, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "segment_edit_ops_active",
		Help:      "Number of active LSM segment edit operations, by op type and shard.",
	}, []string{"op_type", "shard"}))
	if err != nil {
		return nil, fmt.Errorf("register segment_edit_ops_active: %w", err)
	}

	pendingSegments, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "segment_edit_ops_pending_segments",
		Help:      "Segments still awaiting rewrite for an edit op, by op id and shard.",
	}, []string{"op_id", "shard"}))
	if err != nil {
		return nil, fmt.Errorf("register segment_edit_ops_pending_segments: %w", err)
	}

	transformerDur, _, err := EnsureRegisteredMetric(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "segment_edit_ops_transformer_duration_seconds",
		Help:      "Wall-clock seconds spent applying an edit-op transformer to one segment, by op type.",
		Buckets:   LatencyBuckets,
	}, []string{"op_type"}))
	if err != nil {
		return nil, fmt.Errorf("register segment_edit_ops_transformer_duration_seconds: %w", err)
	}

	bytesReclaimed, _, err := EnsureRegisteredMetric(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "segment_edit_ops_bytes_reclaimed_total",
		Help:      "Bytes reclaimed by edit-op rewrites (input minus output), by op type and shard.",
	}, []string{"op_type", "shard"}))
	if err != nil {
		return nil, fmt.Errorf("register segment_edit_ops_bytes_reclaimed_total: %w", err)
	}

	forcedRemaining, _, err := EnsureRegisteredMetric(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: DefaultMetricsNamespace,
		Name:      "segment_edit_ops_forced_cleanup_segments_remaining",
		Help:      "Segments the forced cleanup driver still has to rewrite for an edit op, by op id.",
	}, []string{"op_id"}))
	if err != nil {
		return nil, fmt.Errorf("register segment_edit_ops_forced_cleanup_segments_remaining: %w", err)
	}

	return &SegmentEditOpsMetrics{
		shard:           shardName,
		active:          active,
		pendingSegments: pendingSegments,
		transformerDur:  transformerDur,
		bytesReclaimed:  bytesReclaimed,
		forcedRemaining: forcedRemaining,
	}, nil
}

// SetActive records the number of active ops of opType on this shard.
func (m *SegmentEditOpsMetrics) SetActive(opType string, n int) {
	if m == nil {
		return
	}
	m.active.WithLabelValues(opType, m.shard).Set(float64(n))
}

// SetPendingSegments records how many segments still await rewrite for opID on
// this shard.
func (m *SegmentEditOpsMetrics) SetPendingSegments(opID string, n int) {
	if m == nil {
		return
	}
	m.pendingSegments.WithLabelValues(opID, m.shard).Set(float64(n))
}

// SetForcedCleanupRemaining records how many segments the forced cleanup driver
// still has to rewrite for opID.
func (m *SegmentEditOpsMetrics) SetForcedCleanupRemaining(opID string, n int) {
	if m == nil {
		return
	}
	m.forcedRemaining.WithLabelValues(opID).Set(float64(n))
}

// ObserveTransformerDuration records the wall-clock seconds spent applying the
// transformer to one segment for opType.
func (m *SegmentEditOpsMetrics) ObserveTransformerDuration(opType string, seconds float64) {
	if m == nil {
		return
	}
	m.transformerDur.WithLabelValues(opType).Observe(seconds)
}

// AddBytesReclaimed adds the bytes a rewrite reclaimed for opType. Non-positive
// deltas are ignored so a rewrite that did not shrink the segment is not counted
// (C7: attribute only when the transformer changed bytes).
func (m *SegmentEditOpsMetrics) AddBytesReclaimed(opType string, bytes int64) {
	if m == nil || bytes <= 0 {
		return
	}
	m.bytesReclaimed.WithLabelValues(opType, m.shard).Add(float64(bytes))
}

// ForgetOp drops the op-id-labeled series once opID has fully completed, so
// finished ops do not linger as stale zero gauges.
func (m *SegmentEditOpsMetrics) ForgetOp(opID string) {
	if m == nil {
		return
	}
	m.pendingSegments.DeletePartialMatch(prometheus.Labels{"op_id": opID})
	m.forcedRemaining.DeletePartialMatch(prometheus.Labels{"op_id": opID})
}
