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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// blockingTransformer returns a transformer factory that parks on `entered` for
// its first value and then waits for `release`, so a cleanup pass can be held
// mid-rewrite with the cycle callback observably RUNNING.
func blockingTransformer(entered chan<- struct{}, release <-chan struct{}) OpTransformerFactory {
	var signalled bool
	return func(className string, ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) {
			if !signalled {
				signalled = true
				close(entered)
				<-release
			}
			return append([]byte("X:"), v...), nil
		}
	}
}

// TestEditOpsMetrics_ReapSurvivesALivePass pins the latch against the one thing
// that can undo a reap: a cleanup pass that is already running when it happens.
// The delete path reaps while the shard is still live (Shard.drop calls
// Store.ReapEditOpsMetrics before the teardown it defers), and a pass mid-flight
// finishes afterwards and refreshes. Without the latch that refresh republishes
// everything the reap just deleted, and no second reap ever comes.
//
// A real callback group and cycle manager drive the pass, and the transformer
// parks mid-rewrite, so the overlap is the production one rather than a
// hand-placed call.
func TestEditOpsMetrics_ReapSurvivesALivePass(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	entered := make(chan struct{})
	release := make(chan struct{})

	compactionCallbacks := cyclemanager.NewCallbackGroup("compaction", logger, 1)
	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), t.TempDir(), logger, nil,
		compactionCallbacks, cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	bucket.disk.editOps = newSegmentEditOpsWithLookup(bucket.disk.dir, "TestClass",
		staticResolver(map[OpType]OpTransformerFactory{
			OpTypeRemoveTargetVectors: blockingTransformer(entered, release),
		}))

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	const className = "EditOpsLivePassReapClass"
	m, err := NewMetrics(monitoring.GetMetrics(), className, "shardA")
	require.NoError(t, err)
	bucket.disk.metrics = m

	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	bucket.disk.refreshEditOpsMetrics()
	for _, pre := range []struct {
		fam    string
		labels map[string]string
	}{
		{editOpsActiveFam, opTypeLabels(className)},
		{editOpsPendingFam, opIDLabels(className, "op1")},
		{editOpsOwedFam, opIDLabels(className, "op1")},
	} {
		_, ok := gaugeValue(t, prometheus.DefaultGatherer, pre.fam, pre.labels)
		require.True(t, ok, "precondition: %s must be published before the reap", pre.fam)
	}

	cycle := cyclemanager.NewManager("editops-reap-test",
		cyclemanager.NewFixedTicker(10*time.Millisecond), compactionCallbacks.CycleCallback, logger)
	cycle.Start()
	defer cycle.StopAndWait(ctx)

	// The pass is now inside the transformer, so the reap below lands with a
	// refresh guaranteed to follow it.
	<-entered

	bucket.disk.reapEditOpsMetrics()

	close(release)
	require.Eventually(t, func() bool {
		pending, err := bucket.disk.editOps.Pending("op1")
		return err == nil && len(pending) == 0
	}, 5*time.Second, 10*time.Millisecond, "the parked pass must run to completion after the reap")

	// One lookup per family, each under its own label schema — the active gauge
	// is keyed by op_type, the other two by op_id, and a lookup with the wrong
	// schema can never match, so a loop reusing one label set would pass
	// vacuously for two of the three.
	_, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "active series was republished after the reap")
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.False(t, ok, "pending series was republished after the reap")
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.False(t, ok, "owed series was republished after the reap")

	require.NoError(t, bucket.Shutdown(ctx))
}

// TestEditOpsMetrics_ReapSurvivesASidecarRemoval pins the second way a reap gets
// undone. Deleting the last op removes the sidecar file, after which
// MetricsSnapshot reads empty WITHOUT returning an error — indistinguishable
// from a healthy empty sidecar. A refresh landing there republishes a
// zero-valued active child over the reaped series, which then sticks forever.
func TestEditOpsMetrics_ReapSurvivesASidecarRemoval(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	const className = "EditOpsSidecarRemovalClass"
	installEditOpsMetrics(t, bucket, className)

	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	_, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "precondition: the shard is publishing")

	bucket.disk.reapEditOpsMetrics()

	// Drain and delete the only op, which removes the sidecar file underneath.
	segs := segIDsOf(bucket)
	require.NoError(t, editOps.MarkSegmentDone("op1", segs[0]))
	deleted, _, _, err := bucket.DeleteEditOpIfDrained("op1")
	require.NoError(t, err)
	require.True(t, deleted)

	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "a removed sidecar must not resurrect the reaped active series")
}

// TestEditOpsMetrics_DiscardedSegmentGroupLeavesNoSeries pins the third way a
// reap gets undone, and drives the real construction path rather than calling
// the reap by hand. recoverEditOps publishes on its own failure by design — a
// shard whose drop is stalled is exactly what the gauges are for — but a segment
// group that fails to CONSTRUCT is discarded, and a discarded group never
// reaches shutdown, so nothing else would ever reap what it published.
//
// The injected failure is a cleanup.db path that bolt cannot open, which lands
// in newSegmentCleaner immediately after recovery.
func TestEditOpsMetrics_DiscardedSegmentGroupLeavesNoSeries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	const className = "EditOpsDiscardedGroupClass"
	newMetrics := func(t *testing.T) *Metrics {
		t.Helper()
		m, err := NewMetrics(monitoring.GetMetrics(), className, "shardA")
		require.NoError(t, err)
		return m
	}
	open := func(t *testing.T) (*Bucket, error) {
		t.Helper()
		return NewBucketCreator().NewBucket(ctx, dir, dir, logger, newMetrics(t),
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace), WithClassName(className),
			WithSegmentsCleanupInterval(time.Hour))
	}

	// A first, healthy instance leaves a sidecar holding one armed op, so the
	// reopen below has something to recover and publish.
	bucket, err := open(t)
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	_, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.True(t, ok, "precondition: the healthy instance publishes")
	require.NoError(t, bucket.Shutdown(ctx))

	// Break the cleaner's bolt file: a directory in its place makes bolt.Open
	// fail, which fails newSegmentGroup just after recoverEditOps has published.
	cleanupDB := filepath.Join(dir, cleanupDbFileName)
	require.NoError(t, os.Remove(cleanupDB))
	require.NoError(t, os.Mkdir(cleanupDB, 0o755))

	_, err = open(t)
	require.Error(t, err, "precondition: construction must fail after recovery")

	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "a discarded segment group must leave no series behind")
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.False(t, ok)
}

// TestStoreReapEditOpsMetrics_RetiresEvenWithWorkOwed pins the delete-path
// reap. An unload deliberately keeps a stalled shard's gauges, so a delete
// needs its own retirement or a removed tenant's series live on forever —
// Shard.drop calls this before the teardown it defers. Driven through the real
// Store method rather than the segment group, because the store-wide loop is
// what the drop path actually invokes.
func TestStoreReapEditOpsMetrics_RetiresEvenWithWorkOwed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	const className = "EditOpsStoreReapClass"
	m, err := NewMetrics(monitoring.GetMetrics(), className, "shardA")
	require.NoError(t, err)

	store, err := New(dir, dir, logger, m, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	defer store.Shutdown(ctx)

	require.NoError(t, store.CreateOrLoadBucket(ctx, "objects",
		WithStrategy(StrategyReplace), WithClassName(className),
		WithSegmentsCleanupInterval(time.Hour)))
	bucket := store.Bucket("objects")
	require.NotNil(t, bucket)
	bucket.SetMemtableThreshold(1e9)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.RegisterEditOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok, "precondition: the strip is published")
	require.Equal(t, float64(1), owed, "precondition: work is still owed")

	store.ReapEditOpsMetrics()

	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "a deleted shard must not keep its active series")
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.False(t, ok)
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.False(t, ok, "a deleted shard must not keep its owed series")
}

// TestReapEditOpsShardSeries_RetiresAnUnloadedShard pins the store-free reap.
// A shard that owes work KEEPS its series across an unload, so deleting a
// deactivated tenant has to retire them without a Store to loop over —
// LazyLoadShard.drop's not-loaded branch is the caller. Without it, every
// tenant deactivated mid-drop and then deleted leaks three series until the
// process restarts.
func TestReapEditOpsShardSeries_RetiresAnUnloadedShard(t *testing.T) {
	const className = "EditOpsUnloadedReapClass"
	prom := monitoring.GetMetrics()

	// Publish as the shard would have before it was deactivated.
	shard, err := monitoring.NewSegmentEditOpsMetrics(prom.Registerer, className, "shardA", prom.Group)
	require.NoError(t, err)
	shard.SetActive(string(OpTypeRemoveTargetVectors), 1)
	shard.SetPendingSegments("op1", 2)
	shard.SetSegmentsOwed("op1", 3)

	owed, ok := gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.True(t, ok, "precondition: the deactivated shard is still publishing")
	require.Equal(t, float64(3), owed)

	require.NoError(t, ReapEditOpsShardSeries(prom, className, "shardA"))

	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsActiveFam, opTypeLabels(className))
	require.False(t, ok, "deleting an unloaded shard must retire its active series")
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsPendingFam, opIDLabels(className, "op1"))
	require.False(t, ok)
	_, ok = gaugeValue(t, prometheus.DefaultGatherer, editOpsOwedFam, opIDLabels(className, "op1"))
	require.False(t, ok, "deleting an unloaded shard must retire its owed series")
}
