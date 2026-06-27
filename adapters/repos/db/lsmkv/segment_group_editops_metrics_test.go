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

package lsmkv

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// gaugeValue returns the value of the single series in family `name` matching
// `want`, and whether such a series exists.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) (float64, bool) {
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
			match := len(labels) == len(want)
			for k, v := range want {
				if labels[k] != v {
					match = false
				}
			}
			if match {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestRefreshEditOpsMetrics_DrivesGauges proves the segment-group instrumentation
// is wired: refreshEditOpsMetrics must publish active-op, pending-segment and
// forced-cleanup-remaining gauges derived from the live sidecar state, and must
// follow that state down as segments are marked done.
func TestRefreshEditOpsMetrics_DrivesGauges(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	segs := segIDsOf(bucket)
	require.Len(t, segs, 2)

	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segs))

	reg := prometheus.NewRegistry()
	seom, err := monitoring.NewSegmentEditOpsMetrics(reg, "shardA")
	require.NoError(t, err)
	// A minimal Metrics carrying only the edit-op series: the refresh path touches
	// nothing else. Restore the original before the helper's shutdown Cleanup runs,
	// since shutdown drives the (here unset) segment-strategy vecs.
	orig := bucket.disk.metrics
	bucket.disk.metrics = &Metrics{editOps: seom}
	defer func() { bucket.disk.metrics = orig }()

	const (
		activeFam  = "weaviate_segment_edit_ops_active"
		pendingFam = "weaviate_segment_edit_ops_pending_segments"
		forcedFam  = "weaviate_segment_edit_ops_forced_cleanup_segments_remaining"
	)
	opType := string(OpTypeRemoveTargetVectors)

	bucket.disk.refreshEditOpsMetrics()

	active, ok := gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "shard": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), active)

	pending, ok := gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "shard": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(2), pending)

	forced, ok := gaugeValue(t, reg, forcedFam, map[string]string{"op_id": "op1"})
	require.True(t, ok)
	require.Equal(t, float64(2), forced)

	// Complete one segment; the gauges must track the drop.
	require.NoError(t, editOps.MarkSegmentDone("op1", segs[0]))
	bucket.disk.refreshEditOpsMetrics()

	pending, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "shard": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), pending)

	forced, ok = gaugeValue(t, reg, forcedFam, map[string]string{"op_id": "op1"})
	require.True(t, ok)
	require.Equal(t, float64(1), forced)
}

// TestRefreshEditOpsMetrics_ReconcilesVanishedOps proves deleted ops do not
// linger as stale series: Bucket.DeleteEditOp must (via its own refresh) drop the
// op-id gauges and zero the op-type active count, so a finished drop never reads
// as still active on a dashboard.
func TestRefreshEditOpsMetrics_ReconcilesVanishedOps(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)

	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segs))

	reg := prometheus.NewRegistry()
	seom, err := monitoring.NewSegmentEditOpsMetrics(reg, "shardA")
	require.NoError(t, err)
	orig := bucket.disk.metrics
	bucket.disk.metrics = &Metrics{editOps: seom}
	defer func() { bucket.disk.metrics = orig }()

	const (
		activeFam  = "weaviate_segment_edit_ops_active"
		pendingFam = "weaviate_segment_edit_ops_pending_segments"
		forcedFam  = "weaviate_segment_edit_ops_forced_cleanup_segments_remaining"
	)
	opType := string(OpTypeRemoveTargetVectors)

	bucket.disk.refreshEditOpsMetrics()

	active, ok := gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "shard": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), active)
	_, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "shard": "shardA"})
	require.True(t, ok)

	// The bucket-level delete must refresh on its own — no manual refresh here.
	require.NoError(t, bucket.DeleteEditOp("op1"))

	_, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "shard": "shardA"})
	require.False(t, ok, "pending series must be dropped once the op is deleted")
	_, ok = gaugeValue(t, reg, forcedFam, map[string]string{"op_id": "op1"})
	require.False(t, ok, "forced-cleanup series must be dropped once the op is deleted")

	active, ok = gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "shard": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(0), active, "active count must return to zero, not stick at its last value")
}
