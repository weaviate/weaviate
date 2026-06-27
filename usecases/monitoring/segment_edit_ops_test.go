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
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	activeFam         = "weaviate_lsm_segment_edit_ops_active"
	pendingFam        = "weaviate_lsm_segment_edit_ops_pending_segments"
	owedFam           = "weaviate_lsm_segment_edit_ops_segments_owed"
	transformerDurFam = "weaviate_lsm_segment_edit_ops_transformer_duration_seconds"
)

// labelNamesOf returns the sorted label names of the single series in family
// `name`. Used to pin which series carry the shard dimension and which do not —
// a question testutil answers only by full-exposition comparison, which would
// also pin the help text.
func labelNamesOf(t *testing.T, reg *prometheus.Registry, name string) []string {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		require.Len(t, fam.GetMetric(), 1, "expected exactly one series in %s", name)
		var names []string
		for _, lp := range fam.GetMetric()[0].GetLabel() {
			names = append(names, lp.GetName())
		}
		sort.Strings(names)
		return names
	}
	t.Fatalf("family %s was not exported at all", name)
	return nil
}

// TestMetrics_Labels pins the label design of all four series: the GAUGES carry
// the shard dimension and the histogram deliberately does not. An op id is the
// drop epoch, shared by every shard in the same drop, so a gauge without
// {class_name, shard_name} is written by all of them and reads as whichever
// wrote last; the histogram measures the cost of the work instead, which is a
// node-level question, and keeping it unlabelled is also what lets it survive
// tenant churn that DeleteShard reaps the gauges on.
func TestMetrics_Labels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)

	const opType = "remove_target_vectors"
	m.SetActive(opType, 2)
	m.SetPendingSegments("op1", 3)
	m.SetSegmentsOwed("op1", 4)
	m.ObserveTransformerDuration(opType, 0.5)

	shardScoped := []string{"class_name", "shard_name"}
	for _, tt := range []struct {
		name   string
		family string
		labels []string
		value  float64
	}{
		{
			name:   "active is per shard, by op type",
			family: activeFam,
			labels: append([]string{"op_type"}, shardScoped...),
			value:  2,
		},
		{
			name:   "pending_segments is per shard, by op id",
			family: pendingFam,
			labels: append([]string{"op_id"}, shardScoped...),
			value:  3,
		},
		{
			name:   "segments_owed is per shard, by op id",
			family: owedFam,
			labels: append([]string{"op_id"}, shardScoped...),
			value:  4,
		},
		{
			name:   "transformer_duration is node-wide, labeled by op_type ONLY",
			family: transformerDurFam,
			labels: []string{"op_type"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want := append([]string(nil), tt.labels...)
			sort.Strings(want)
			require.Equal(t, want, labelNamesOf(t, reg, tt.family))
		})
	}

	// Values, so the setters are pinned and not merely called: every gauge is
	// read back at a DISTINCT value, which a setter writing to the wrong vec
	// (or ignoring its argument) cannot satisfy.
	require.Equal(t, float64(2), testutil.ToFloat64(m.active.WithLabelValues(opType, "ClassA", "shardA")))
	require.Equal(t, float64(3), testutil.ToFloat64(m.pendingSegments.WithLabelValues("op1", "ClassA", "shardA")))
	require.Equal(t, float64(4), testutil.ToFloat64(m.segmentsOwed.WithLabelValues("op1", "ClassA", "shardA")))
	require.Equal(t, 1, testutil.CollectAndCount(m.transformerDur, transformerDurFam))
}

func TestMetrics_NilReceiverIsNoop(t *testing.T) {
	var m *SegmentEditOpsMetrics
	require.NotPanics(t, func() {
		m.SetActive("t", 1)
		m.SetPendingSegments("op", 1)
		m.SetSegmentsOwed("op", 1)
		m.ObserveTransformerDuration("t", 1)
		m.ForgetOp("op")
		m.DeleteOwnShard()
	})
}

func TestForgetOp_DropsOpLabeledSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)

	m.SetPendingSegments("op1", 3)
	m.SetSegmentsOwed("op1", 3)
	require.Equal(t, 1, testutil.CollectAndCount(reg, pendingFam))
	require.Equal(t, 1, testutil.CollectAndCount(reg, owedFam))

	m.ForgetOp("op1")
	require.Zero(t, testutil.CollectAndCount(reg, pendingFam))
	require.Zero(t, testutil.CollectAndCount(reg, owedFam))
}

// TestForgetOp_LeavesSiblingShardsAlone pins the scoping of ForgetOp. An op id
// is the drop EPOCH, shared by every shard taking part in the same drop, so a
// DeletePartialMatch on op_id alone reaps series belonging to shards that are
// still mid-strip. Those shards then report nothing until their next refresh,
// and the operator sees a drop that looks finished on shards where it is not.
func TestForgetOp_LeavesSiblingShardsAlone(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Same registry, so both instances share the underlying collectors — this is
	// how it works in production, one instance per shard.
	a, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)
	b, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardB", false)
	require.NoError(t, err)

	const opID = "epoch-1" // one drop, both shards
	a.SetPendingSegments(opID, 3)
	a.SetSegmentsOwed(opID, 3)
	b.SetPendingSegments(opID, 7)
	b.SetSegmentsOwed(opID, 7)
	require.Equal(t, 2, testutil.CollectAndCount(reg, pendingFam))

	a.ForgetOp(opID)

	// Counted BEFORE any lookup by label: reading a child back would recreate a
	// deleted series at zero, and then "gone" and "zero" would be the same
	// assertion. The count says exactly one survived; the value says which.
	require.Equal(t, 1, testutil.CollectAndCount(reg, pendingFam),
		"the forgetting shard's series must go, and only that one")
	require.Equal(t, 1, testutil.CollectAndCount(reg, owedFam))

	require.Equal(t, float64(7), testutil.ToFloat64(b.pendingSegments.WithLabelValues(opID, "ClassA", "shardB")),
		"a sibling shard mid-strip must keep its series")
	require.Equal(t, float64(7), testutil.ToFloat64(b.segmentsOwed.WithLabelValues(opID, "ClassA", "shardB")))
}

// TestDeleteOwnShard_OnlyTheNamedShard pins that reaping one shard does not take
// its neighbours' series with it. The collectors are process-global and shared
// by every shard on the node, so a DeletePartialMatch on an empty or partial
// label set would silently blank all of them.
func TestDeleteOwnShard_OnlyTheNamedShard(t *testing.T) {
	reg := prometheus.NewRegistry()
	a, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)
	b, err := NewSegmentEditOpsMetrics(reg, "ClassA", "shardB", false)
	require.NoError(t, err)

	const opType = "remove_target_vectors"
	a.SetActive(opType, 1)
	a.SetPendingSegments("op1", 2)
	b.SetActive(opType, 5)
	b.SetPendingSegments("op1", 9)
	require.Equal(t, 2, testutil.CollectAndCount(reg, activeFam))

	a.DeleteOwnShard()

	// Counted BEFORE any lookup by label: reading a child back would recreate a
	// deleted series at zero, and then "gone" and "zero" would be the same
	// assertion. The count says exactly one survived; the value says which.
	require.Equal(t, 1, testutil.CollectAndCount(reg, activeFam),
		"exactly the reaped shard's series must go")
	require.Equal(t, 1, testutil.CollectAndCount(reg, pendingFam))

	require.Equal(t, float64(5), testutil.ToFloat64(b.active.WithLabelValues(opType, "ClassA", "shardB")),
		"reaping one shard must not blank the others")
	require.Equal(t, float64(9), testutil.ToFloat64(b.pendingSegments.WithLabelValues("op1", "ClassA", "shardB")))
}

// TestGroupedMode_SuppressesPerShardGauges pins the grouped-mode contract.
// PROMETHEUS_MONITORING_GROUP collapses class_name/shard_name to "n/a" upstream,
// so without this guard every shard on the node writes the SAME gauge series:
// 100 tenants owing 3 segments each would read 3 rather than 300, and the first
// to drain would zero it for all of them. A wrong number on a dashboard is
// worse than a missing one. The cumulative series carry no shard dimension, so
// they stay on and remain correct.
func TestGroupedMode_SuppressesPerShardGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Names arrive already collapsed, exactly as NewMetrics passes them.
	m, err := NewSegmentEditOpsMetrics(reg, "n/a", "n/a", true)
	require.NoError(t, err)

	const opType = "remove_target_vectors"
	m.SetActive(opType, 2)
	m.SetPendingSegments("op1", 3)
	m.SetSegmentsOwed("op1", 3)

	require.Zero(t, testutil.CollectAndCount(reg, activeFam),
		"per-shard gauges must not be emitted in grouped mode")
	require.Zero(t, testutil.CollectAndCount(reg, pendingFam))
	require.Zero(t, testutil.CollectAndCount(reg, owedFam))

	// Cost is node-wide either way, so grouping must not silence it.
	m.ObserveTransformerDuration(opType, 0.25)
	require.Equal(t, 1, testutil.CollectAndCount(reg, transformerDurFam))
}
