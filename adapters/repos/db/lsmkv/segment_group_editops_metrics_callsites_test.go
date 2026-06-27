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
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The tests in this file pin the CALL SITES of refreshEditOpsMetrics, not the
// refresh itself: a test that hand-calls the refresh at each assertion point
// (as the sibling file's do) exercises the function while leaving every
// production caller unpinned. Nothing below may call the refresh directly;
// each drives a real entry point and then reads the gauges.

const (
	editOpsActiveFam  = "weaviate_lsm_segment_edit_ops_active"
	editOpsPendingFam = "weaviate_lsm_segment_edit_ops_pending_segments"
	editOpsOwedFam    = "weaviate_lsm_segment_edit_ops_segments_owed"
)

// installEditOpsMetrics binds a REAL Metrics to the bucket's segment group. The
// hand-built Metrics{editOps: ...} the sibling file uses cannot be reused here:
// the production entry points these tests drive also touch the compaction and
// segment-strategy vecs a minimal one leaves nil, and nil-deref instead of
// asserting. That means gathering from the default registry, so every test
// takes its own class name to keep its assertions off the others' series.
func installEditOpsMetrics(t *testing.T, bucket *Bucket, className string) {
	t.Helper()
	m, err := NewMetrics(monitoring.GetMetrics(), className, "shardA")
	require.NoError(t, err)
	require.NotNil(t, m.editOps, "monitoring must be on for these to pin anything")
	orig := bucket.disk.metrics
	bucket.disk.metrics = m
	t.Cleanup(func() { bucket.disk.metrics = orig })
}

func opIDLabels(className, opID string) map[string]string {
	return map[string]string{"op_id": opID, "class_name": className, "shard_name": "shardA"}
}

func opTypeLabels(className string) map[string]string {
	return map[string]string{
		"op_type": string(OpTypeRemoveTargetVectors), "class_name": className, "shard_name": "shardA",
	}
}

// TestEditOpsMetrics_RegisterPublishes pins the refresh in
// registerEditOpAndSnapshot. Arming a drop is the moment the gauges must appear:
// without this call site a shard shows nothing until some later pass happens to
// refresh, so the start of a strip is invisible.
func TestEditOpsMetrics_RegisterPublishes(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	const className = "EditOpsCallsiteRegisterClass"
	installEditOpsMetrics(t, bucket, className)

	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))

	active, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "arming a drop must publish the active gauge")
	require.Equal(t, float64(1), active)

	pending, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.True(t, ok, "arming a drop must publish the pending gauge")
	require.Equal(t, float64(2), pending, "both segments are owed")
}

// TestEditOpsMetrics_ReArmRequeueClearsStall pins the refresh on the resume arm
// of Bucket.RegisterEditOp. A re-arm hands quarantined segments a fresh budget,
// which is exactly the transition that clears the owed-above-queued signature a
// stalled drop is read by — so a dashboard left on the stale reading would show
// a stall that the re-arm already cleared.
func TestEditOpsMetrics_ReArmRequeueClearsStall(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)
	require.NoError(t, editOps.Quarantine("op1", segs[0]))

	// Install AFTER the quarantine so the first reading below is the re-arm's,
	// not a leftover from the register above.
	const className = "EditOpsCallsiteReArmClass"
	installEditOpsMetrics(t, bucket, className)

	// Re-arm: same op id, already snapshotted, so this takes the resume arm.
	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 2}))

	pending, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.True(t, ok)
	require.Equal(t, float64(1), pending, "the requeued segment is back in the queue")

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok)
	require.Equal(t, float64(1), owed)
	require.Equal(t, pending, owed, "owed must not still read above queued: the stall is over")
}

// TestEditOpsMetrics_CleanupPassRefreshes pins the deferred refresh in
// cleanupOnceEditOps. The cleanup pass is what actually drains the queue, so
// without this call site the progress gauge never moves while the strip runs —
// which is the one thing the pending gauge exists to show.
func TestEditOpsMetrics_CleanupPassRefreshes(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	const className = "EditOpsCallsiteCleanupClass"
	installEditOpsMetrics(t, bucket, className)

	// One pass rewrites exactly one segment, so this pins that the gauge tracks
	// the drain rather than only its start and end.
	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	pending, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.True(t, ok, "a cleanup pass must publish progress on its own")
	require.Equal(t, float64(1), pending, "one of the two segments has been rewritten")
}

// TestEditOpsMetrics_CompactionRefreshes pins the refresh in the compaction
// bookkeeping. A compaction retires the pending rows of BOTH inputs at once, so
// without this call site the gauges read high until the next cleanup pass — and
// on a bucket whose rows compaction drained entirely, that is a whole
// force-cleanup interval of a finished drop reading as unfinished.
func TestEditOpsMetrics_CompactionRefreshes(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	const className = "EditOpsCallsiteCompactionClass"
	installEditOpsMetrics(t, bucket, className)

	compacted, err := bucket.disk.compactOnce(context.Background())
	require.NoError(t, err)
	require.True(t, compacted, "the two segments must merge for this to pin anything")

	// The merge ran the transformer over both inputs, so it retired the whole
	// pending set on its own. Nothing refreshed before this point — the metrics
	// went in after the snapshot — so the series EXISTING at all is what pins
	// the call site, and its value being zero is what pins that the refresh read
	// post-merge state. Without it this drop reads as unfinished until a
	// force-cleanup interval later.
	pending, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.True(t, ok, "compaction bookkeeping must refresh on its own")
	require.Zero(t, pending, "the merge drained the pending set")

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok)
	require.Zero(t, owed, "and owed nothing beyond it")
}

// TestEditOpsMetrics_RecoveryPublishesOnLoad pins the refresh call in
// recoverEditOps: a recovered shard must publish at load, without waiting for a
// cleanup pass. It does NOT pin the call being deferred — this recovery
// succeeds, so defer and tail-call are indistinguishable here. The deferral
// covers recoveries that fail in Reconcile's write phase (ENOSPC-class) with
// the sidecar still readable; producing that needs fault injection this
// harness doesn't have, so that placement is asserted by review, not by test.
func TestEditOpsMetrics_RecoveryPublishesOnLoad(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	const className = "EditOpsCallsiteRecoveryClass"
	installEditOpsMetrics(t, bucket, className)

	// No liveness provider is installed, so the orphan sweep is skipped and
	// recovery keeps the recorded pending set — the resume point of a strip
	// interrupted by the restart this simulates.
	require.NoError(t, bucket.disk.recoverEditOps(context.Background()))

	active, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "a recovered shard must publish without waiting for a pass")
	require.Equal(t, float64(1), active)

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok)
	require.Equal(t, float64(1), owed)
}

// TestEditOpsMetrics_UnloadKeepsGaugesWhileWorkIsOwed pins the unload/delete
// distinction. Tenant deactivation unloads a shard through this exact path
// (Migrator's tenants_to_cold), and a drop stalled on a cold tenant is the
// state these gauges exist to report — reaping there would make it read
// identically to no drop at all. Every sibling per-shard LSM gauge survives an
// unload for the same reason; only a delete retires them.
func TestEditOpsMetrics_UnloadKeepsGaugesWhileWorkIsOwed(t *testing.T) {
	ctx := context.Background()
	bucket, editOps := newReplaceBucketWithEditOpsNoShutdown(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	const className = "EditOpsUnloadKeepsClass"
	installEditOpsMetrics(t, bucket, className)
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))
	bucket.disk.refreshEditOpsMetrics()

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok, "precondition: the strip is published")
	require.Equal(t, float64(1), owed)

	require.NoError(t, bucket.Shutdown(ctx))

	owed, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok, "an unload with work owed must keep reporting it")
	require.Equal(t, float64(1), owed, "and must keep the value it last read")
	active, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok)
	require.Equal(t, float64(1), active)
}

// TestEditOpsMetrics_ShutdownReapsADrainedShard pins the other half: once the
// shard owes nothing, its series are stale rather than informative, so an
// unload retires them. Driven through the EARLIEST failure return (a cancelled
// context fails the callback Unregister) because the reap is deferred
// specifically so the failure paths are covered — a torn shard is never
// retried, so a reap skipped there never happens at all.
func TestEditOpsMetrics_ShutdownReapsADrainedShard(t *testing.T) {
	ctx := context.Background()
	bucket, editOps := newReplaceBucketWithEditOpsNoShutdown(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	const className = "EditOpsShutdownDrainedClass"
	installEditOpsMetrics(t, bucket, className)
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	segs := segIDsOf(bucket)
	require.NoError(t, editOps.SnapshotSegments("op1", segs))
	bucket.disk.refreshEditOpsMetrics()
	_, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "precondition: the shard is publishing before it goes down")

	// Drain it: the op completes and is deleted, leaving a zero-valued active
	// series and no live work.
	require.NoError(t, editOps.MarkSegmentDone("op1", segs[0]))
	deleted, _, _, err := bucket.DeleteEditOpIfDrained("op1")
	require.NoError(t, err)
	require.True(t, deleted)
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "precondition: the drained shard still carries a zero-valued series")

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, bucket.disk.shutdown(cancelled), "precondition: this teardown must fail")

	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "a drained shard's series must not survive its unload")
}
