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
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// findMetric returns the single metric in family `name` whose labels exactly
// match `want`, or nil if none does.
func findMetric(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) *dto.Metric {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if len(labels) != len(want) {
				continue
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
					break
				}
			}
			if match {
				return m
			}
		}
	}
	return nil
}

// countMetrics returns how many child series family `name` currently exposes.
func countMetrics(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() == name {
			return len(fam.GetMetric())
		}
	}
	return 0
}

func TestMetrics_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewSegmentEditOpsMetrics(reg, "shardA")
	require.NoError(t, err)

	const opType = "remove_target_vectors"
	m.SetActive(opType, 2)
	m.SetPendingSegments("op1", 3)
	m.SetForcedCleanupRemaining("op1", 3)
	m.ObserveTransformerDuration(opType, 0.5)
	m.AddBytesReclaimed(opType, 1024)

	tests := []struct {
		name      string
		family    string
		labels    map[string]string
		gaugeVal  float64
		isGauge   bool
		isCounter bool
		sampleCnt uint64
	}{
		{
			name:     "active is labeled by op_type and shard",
			family:   "weaviate_segment_edit_ops_active",
			labels:   map[string]string{"op_type": opType, "shard": "shardA"},
			gaugeVal: 2,
			isGauge:  true,
		},
		{
			name:     "pending_segments is labeled by op_id and shard",
			family:   "weaviate_segment_edit_ops_pending_segments",
			labels:   map[string]string{"op_id": "op1", "shard": "shardA"},
			gaugeVal: 3,
			isGauge:  true,
		},
		{
			name:     "forced_cleanup_segments_remaining is labeled by op_id only",
			family:   "weaviate_segment_edit_ops_forced_cleanup_segments_remaining",
			labels:   map[string]string{"op_id": "op1"},
			gaugeVal: 3,
			isGauge:  true,
		},
		{
			name:      "bytes_reclaimed is labeled by op_type and shard",
			family:    "weaviate_segment_edit_ops_bytes_reclaimed_total",
			labels:    map[string]string{"op_type": opType, "shard": "shardA"},
			gaugeVal:  1024,
			isCounter: true,
		},
		{
			name:      "transformer_duration is labeled by op_type only",
			family:    "weaviate_segment_edit_ops_transformer_duration_seconds",
			labels:    map[string]string{"op_type": opType},
			sampleCnt: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metric := findMetric(t, reg, tt.family, tt.labels)
			require.NotNil(t, metric, "no series matched labels %v in %s", tt.labels, tt.family)
			switch {
			case tt.isGauge:
				require.Equal(t, tt.gaugeVal, metric.GetGauge().GetValue())
			case tt.isCounter:
				require.Equal(t, tt.gaugeVal, metric.GetCounter().GetValue())
			default:
				require.Equal(t, tt.sampleCnt, metric.GetHistogram().GetSampleCount())
			}
		})
	}
}

func TestBytesReclaimed_NotAttributedWhenUnchanged(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewSegmentEditOpsMetrics(reg, "shardA")
	require.NoError(t, err)

	const family = "weaviate_segment_edit_ops_bytes_reclaimed_total"

	// A rewrite that did not shrink the segment (delta == 0) or grew it (delta < 0)
	// must not be attributed: no series should be created at all.
	m.AddBytesReclaimed("remove_target_vectors", 0)
	m.AddBytesReclaimed("remove_target_vectors", -64)
	require.Equal(t, 0, countMetrics(t, reg, family))

	// A real shrink is attributed.
	m.AddBytesReclaimed("remove_target_vectors", 512)
	metric := findMetric(t, reg, family,
		map[string]string{"op_type": "remove_target_vectors", "shard": "shardA"})
	require.NotNil(t, metric)
	require.Equal(t, float64(512), metric.GetCounter().GetValue())
}

func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *SegmentEditOpsMetrics
	require.NotPanics(t, func() {
		m.SetActive("t", 1)
		m.SetPendingSegments("op", 1)
		m.SetForcedCleanupRemaining("op", 1)
		m.ObserveTransformerDuration("t", 1)
		m.AddBytesReclaimed("t", 1)
		m.ForgetOp("op")
	})
}

func TestForgetOp_DropsOpLabeledSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewSegmentEditOpsMetrics(reg, "shardA")
	require.NoError(t, err)

	m.SetPendingSegments("op1", 3)
	m.SetForcedCleanupRemaining("op1", 3)
	require.Equal(t, 1, countMetrics(t, reg, "weaviate_segment_edit_ops_pending_segments"))
	require.Equal(t, 1, countMetrics(t, reg, "weaviate_segment_edit_ops_forced_cleanup_segments_remaining"))

	m.ForgetOp("op1")
	require.Equal(t, 0, countMetrics(t, reg, "weaviate_segment_edit_ops_pending_segments"))
	require.Equal(t, 0, countMetrics(t, reg, "weaviate_segment_edit_ops_forced_cleanup_segments_remaining"))
}
