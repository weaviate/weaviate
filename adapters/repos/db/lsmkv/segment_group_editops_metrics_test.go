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
func gaugeValue(t *testing.T, g prometheus.Gatherer, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := g.Gather()
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
	seom, err := monitoring.NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)
	// A minimal Metrics carrying only the edit-op series: the refresh path touches
	// nothing else. Restore the original before the helper's shutdown Cleanup runs,
	// since shutdown drives the (here unset) segment-strategy vecs.
	orig := bucket.disk.metrics
	bucket.disk.metrics = &Metrics{editOps: seom}
	defer func() { bucket.disk.metrics = orig }()

	const (
		activeFam  = "weaviate_lsm_segment_edit_ops_active"
		pendingFam = "weaviate_lsm_segment_edit_ops_pending_segments"
		forcedFam  = "weaviate_lsm_segment_edit_ops_segments_owed"
	)
	opType := string(OpTypeRemoveTargetVectors)

	bucket.disk.refreshEditOpsMetrics()

	active, ok := gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), active)

	pending, ok := gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(2), pending)

	shardLabels := map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"}
	forced, ok := gaugeValue(t, reg, forcedFam, shardLabels)
	require.True(t, ok)
	require.Equal(t, float64(2), forced,
		"with nothing quarantined, owed equals queued")

	// Complete one segment; the gauges must track the drop.
	require.NoError(t, editOps.MarkSegmentDone("op1", segs[0]))
	bucket.disk.refreshEditOpsMetrics()

	pending, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), pending)

	forced, ok = gaugeValue(t, reg, forcedFam, shardLabels)
	require.True(t, ok)
	require.Equal(t, float64(1), forced)

	// Quarantine is the only permanent terminal state a drop has, and it must
	// not read like success. The segment leaves the QUEUE, so pending goes to
	// zero — but the op still OWES it, so forced holds at one. That divergence
	// is the whole signal that a drop stalled rather than finished.
	require.NoError(t, editOps.Quarantine("op1", segs[1]))
	bucket.disk.refreshEditOpsMetrics()

	pending, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Zero(t, pending, "the quarantined segment leaves the queue")

	forced, ok = gaugeValue(t, reg, forcedFam, shardLabels)
	require.True(t, ok)
	require.Equal(t, float64(1), forced, "but it is still owed, so the stall stays visible")
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
	seom, err := monitoring.NewSegmentEditOpsMetrics(reg, "ClassA", "shardA", false)
	require.NoError(t, err)
	orig := bucket.disk.metrics
	bucket.disk.metrics = &Metrics{editOps: seom}
	defer func() { bucket.disk.metrics = orig }()

	const (
		activeFam  = "weaviate_lsm_segment_edit_ops_active"
		pendingFam = "weaviate_lsm_segment_edit_ops_pending_segments"
		forcedFam  = "weaviate_lsm_segment_edit_ops_segments_owed"
	)
	opType := string(OpTypeRemoveTargetVectors)

	bucket.disk.refreshEditOpsMetrics()

	active, ok := gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(1), active)
	_, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)

	// The delete is gated on the op being drained, so retire its one row first;
	// marking done does not refresh, which keeps this a test of the delete.
	require.NoError(t, editOps.MarkSegmentDone("op1", segs[0]))

	// The bucket-level delete must refresh on its own — no manual refresh here.
	deleted, pending, quarantined, err := bucket.DeleteEditOpIfDrained("op1")
	require.NoError(t, err)
	require.True(t, deleted, "the op is drained, so it must delete (pending=%d quarantined=%d)",
		pending, quarantined)

	_, ok = gaugeValue(t, reg, pendingFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.False(t, ok, "pending series must be dropped once the op is deleted")
	_, ok = gaugeValue(t, reg, forcedFam, map[string]string{"op_id": "op1", "class_name": "ClassA", "shard_name": "shardA"})
	require.False(t, ok, "owed series must be dropped once the op is deleted")

	active, ok = gaugeValue(t, reg, activeFam, map[string]string{"op_type": opType, "class_name": "ClassA", "shard_name": "shardA"})
	require.True(t, ok)
	require.Equal(t, float64(0), active, "active count must return to zero, not stick at its last value")
}

// TestNewMetrics_WiresEditOpsSeries pins the wiring itself. Every other test in
// this file installs a hand-built Metrics{editOps: ...}, so all of them stay
// green if NewMetrics stops populating the field — the series would simply
// never be exported in production and nothing would say so.
//
// Uses the global PrometheusMetrics because that is what NewMetrics needs (a
// minimal hand-built one nil-derefs on the other lsmkv vecs), so the class name
// is deliberately unique to keep the assertion off any other test's series.
func TestNewMetrics_WiresEditOpsSeries(t *testing.T) {
	const className = "EditOpsWiringProbeClass"

	m, err := NewMetrics(monitoring.GetMetrics(), className, "shardW")
	require.NoError(t, err)
	require.NotNil(t, m.editOps, "NewMetrics must wire the edit-op series")

	m.SetEditOpsActive("remove_target_vectors", 3)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var got float64
	var found bool
	for _, fam := range families {
		if fam.GetName() != "weaviate_lsm_segment_edit_ops_active" {
			continue
		}
		for _, metric := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["class_name"] == className && labels["shard_name"] == "shardW" &&
				labels["op_type"] == "remove_target_vectors" {
				got, found = metric.GetGauge().GetValue(), true
			}
		}
	}
	require.True(t, found, "the wired series must be exported under {op_type, class_name, shard_name}")
	require.Equal(t, float64(3), got)
}

// TestRefreshEditOpsMetrics_SkipsSidecarWhenNothingCollects pins that the
// refresh does not touch the bolt sidecar when nothing will consume the result.
// It costs a bolt read tx, and it runs on every shard load, every compaction and
// every cleanup tick — so on a node with monitoring off (the default), or in
// grouped mode where the per-shard gauges are suppressed, that is pure waste.
// Grouped mode is the worse of the two: those are the nodes with the most
// shards to multiply it by.
func TestRefreshEditOpsMetrics_SkipsSidecarWhenNothingCollects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		metrics *Metrics
	}{
		// Production monitoring-off is a NIL *Metrics (shard_init_lsm only
		// builds one when promMetrics is set); the empty struct additionally
		// covers the wired-but-no-editOps clause of the guard.
		{name: "monitoring off (nil, the production shape)", metrics: nil},
		{name: "metrics wired but editOps not", metrics: &Metrics{}},
		{name: "grouped mode", metrics: func() *Metrics {
			seom, err := monitoring.NewSegmentEditOpsMetrics(prometheus.NewRegistry(), "n/a", "n/a", true)
			require.NoError(t, err)
			return &Metrics{editOps: seom}
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
			require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
			require.NoError(t, bucket.FlushAndSwitch())
			require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
			require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

			orig := bucket.disk.metrics
			bucket.disk.metrics = tc.metrics
			defer func() { bucket.disk.metrics = orig }()

			// The sidecar is healthy and holds one op, so a refresh that actually
			// ran would read it and record the seen-set. Still nil therefore means
			// it returned before touching bolt at all — which is the point.
			bucket.disk.refreshEditOpsMetrics()

			require.Nil(t, bucket.disk.editOpsSeenOpIDs,
				"a refresh nothing will consume must return before reading the sidecar")
		})
	}
}
