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

package db

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Regression tests for weaviate/weaviate#12215: a non-cancellation rebuild
// failure at migration finalize must degrade to disk serving
// (WARN-and-continue), not fail the migration. Failures are injected via
// the rebuildRangeableRepFn seam since the real failure window is too
// narrow to hit black-box.

// newRangeableInMemoryTestTask installs a fault-injecting rebuildRangeableRepFn.
func newRangeableInMemoryTestTask(t *testing.T, idx *Index, className, propName string, failing *atomic.Bool, calls *atomic.Int32,
) (*ShardReindexTaskGeneric, *testFilterableToRangeableStrategyWrapper) {
	t.Helper()
	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)
	task.rebuildRangeableRepFn = func(ctx context.Context, b *lsmkv.Bucket) error {
		calls.Add(1)
		if failing.Load() {
			return fmt.Errorf("injected rebuild failure")
		}
		return b.RebuildRangeableSegmentInMemory(ctx)
	}
	return task, wrapped
}

// firstErrorEntry returns the first ERROR-level log entry captured by hook,
// or nil if none was emitted.
func firstErrorEntry(hook *test.Hook) *logrus.Entry {
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.ErrorLevel {
			return e
		}
	}
	return nil
}

// runReindexToCompletionOrError drives OnAfterLsmInit + OnAfterLsmInitAsync
// to convergence, returning the first error encountered (nil if the
// migration converged cleanly).
func runReindexToCompletionOrError(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) error {
	t.Helper()
	if err := task.OnAfterLsmInit(ctx, shard); err != nil {
		return err
	}
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		if err != nil {
			return err
		}
		if rerunAt.IsZero() {
			return nil
		}
	}
}

// setupRangeableFinalizeDegradeFixture builds a shard, injects a
// permanently failing rebuild, and drives the migration to completion: a
// non-cancellation rebuild failure must not fail the migration, so
// OnMigrationComplete still runs and range queries serve disk results.
func setupRangeableFinalizeDegradeFixture(t *testing.T, classNamePrefix string) (
	context.Context, *Shard, *Index, string, *ShardReindexTaskGeneric,
	*testFilterableToRangeableStrategyWrapper, *atomic.Bool, *atomic.Int32, *test.Hook,
) {
	t.Helper()
	const numObjects = 25
	propName := filterableToRangeablePropName

	ctx := testCtx()
	className := classNamePrefix + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	// The task captures idx.logger by value at construction, so swap in a
	// hook-capturing logger first to assert on the ERROR log this fixture drives.
	hookLogger, hook := test.NewNullLogger()
	idx.logger = hookLogger

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	failing := &atomic.Bool{}
	failing.Store(true)
	calls := &atomic.Int32{}

	task, wrapped := newRangeableInMemoryTestTask(t, idx, className, propName, failing, calls)
	require.NoError(t, runReindexToCompletionOrError(t, ctx, task, shard),
		"a non-cancellation rebuild failure must not fail the migration (finding 5)")

	return ctx, shard, idx, className, task, wrapped, failing, calls, hook
}

// TestRangeableFinalize_RebuildFailure_ServesDiskNotMissingIndexError pins
// weaviate/weaviate#12215: a non-cancellation rebuild failure at finalize
// must FINISH the migration and serve correct disk results, never a
// permanent FAILED or MissingFilterableIndexError.
func TestRangeableFinalize_RebuildFailure_ServesDiskNotMissingIndexError(t *testing.T) {
	propName := filterableToRangeablePropName
	_, shard, _, _, _, wrapped, _, calls, hook := setupRangeableFinalizeDegradeFixture(t, "RangeableRebuildDegrade_")

	assert.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must still run on a non-cancellation rebuild failure (WARN-and-continue): "+
			"in production this is exactly what flips the ready flag (setRangeableLocallyReady), so a "+
			"replica reaching this point serves via hasUsableRangeableIndex, not MissingFilterableIndexError. "+
			"This harness's test wrapper intentionally skips the real setRangeableLocallyReady side effect "+
			"(see testFilterableToRangeableStrategyWrapper's doc comment), so this flag is the load-bearing "+
			"proxy for it here; that side effect itself is unchanged, existing production code.")
	assert.GreaterOrEqual(t, calls.Load(), int32(1), "rebuildRangeableRepFn must have been invoked")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucket)
	assert.False(t, bucket.RangeableServesFromMemory(),
		"the failed rebuild must not have activated in-memory serving")
	fp := filterableToRangeableFingerprint(t, bucket)
	require.Len(t, fp, filterableToRangeableNumDistinctValues,
		"disk-path range reads must still return the full, correct term set")

	errLine := firstErrorEntry(hook)
	require.NotNil(t, errLine, "a rebuild failure must log at ERROR")
	assert.Contains(t, errLine.Message, propName)
	assert.Contains(t, errLine.Message, "built and data intact")

	className := shard.Index().Config.ClassName.String()
	metric, err := monitoring.GetMetrics().RangeableInMemoryRebuildDegraded.GetMetricWithLabelValues(className, shard.Name(), propName)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, testutil.ToFloat64(metric), float64(1), "a degrade must increment the metric (memo test 7)")
}

// TestRangeableFinalize_RebuildCancellation_RoutesTransient pins
// weaviate/weaviate#12215: cancellation must still abort the rebuild
// loudly, preserving the scheduler's transient-ack routing.
func TestRangeableFinalize_RebuildCancellation_RoutesTransient(t *testing.T) {
	const numObjects = 5
	propName := filterableToRangeablePropName
	ctx := testCtx()
	className := "RangeableRebuildCancel_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Drive the migration to completion first so the rangeable bucket
	// exists: cancellation must route through the rebuild-error branch
	// here, not the (separately tested) nil-bucket branch.
	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, runReindexToCompletionOrError(t, ctx, task, shard))

	bucketName := task.strategy.SourceBucketName(propName)
	require.NotNil(t, shard.Store().Bucket(bucketName), "precondition: bucket must exist post-swap")

	task2, _ := newFilterableToRangeableTask(t, idx, className, propName)
	task2.rebuildRangeableRepFn = func(ctx context.Context, b *lsmkv.Bucket) error {
		return fmt.Errorf("injected fault under cancellation")
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	err := task2.rebuildRangeableInMemoryReps(cancelledCtx, idx.logger, shard, []string{propName})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"a cancelled context must route directly through the rebuild-error branch too, preserving errors.Is compatibility")
}

// TestRangeableFinalize_DataWorkFailure_StillFAILED pins
// weaviate/weaviate#12215: a data-work (swap) failure, unlike a rebuild
// failure, must still fail the migration - WARN-and-continue is scoped to
// the acceleration step only.
func TestRangeableFinalize_DataWorkFailure_StillFAILED(t *testing.T) {
	const numObjects = 5
	propName := filterableToRangeablePropName
	ctx := testCtx()
	className := "RangeableDataWorkFail_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		return nil, fmt.Errorf("injected data-work swap failure")
	}

	err := runReindexToCompletionOrError(t, ctx, task, shard)
	require.Error(t, err, "a data-work (swap) failure must still fail the migration - "+
		"finding 5's WARN-and-continue is scoped to the rebuild step only")
	assert.Contains(t, err.Error(), "injected data-work swap failure")
	assert.False(t, wrapped.migrationCompleted, "OnMigrationComplete must not run when the data work itself failed")
}

// TestRangeableFinalize_MultiReplica_FailedReplicaServesCorrectDiskResults
// pins weaviate/weaviate#12215: two replicas where one's rebuild fails,
// verifying both converge to identical, correct results.
func TestRangeableFinalize_MultiReplica_FailedReplicaServesCorrectDiskResults(t *testing.T) {
	const numObjects = 25
	propName := filterableToRangeablePropName

	newReplica := func(classNamePrefix string) (context.Context, *Shard, *Index, string) {
		ctx := testCtx()
		className := classNamePrefix + uuid.NewString()[:8]
		class := newFilterableToRangeableTestClass(className)
		shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
			false, false, false,
			func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
		shard := shd.(*Shard)
		t.Cleanup(func() { shard.Shutdown(ctx) })
		for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
			require.NoError(t, shard.PutObject(ctx, obj))
		}
		return ctx, shard, idx, className
	}

	// Replica A: rebuild succeeds. Its OnMigrationComplete models the
	// cluster-wide RAFT commit of IndexRangeFilters=true that a real
	// cluster's first-successful-shard triggers.
	ctxA, shardA, idxA, classNameA := newReplica("RangeableMultiReplicaA_")
	taskA, wrappedA := newFilterableToRangeableTask(t, idxA, classNameA, propName)
	require.NoError(t, runReindexToCompletionOrError(t, ctxA, taskA, shardA))
	require.True(t, wrappedA.migrationCompleted)

	bucketA := shardA.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucketA)
	require.True(t, bucketA.RangeableServesFromMemory())

	// Replica B: rebuild is fault-injected to fail. In a real cluster the
	// schema flag is already committed cluster-wide by replica A here;
	// replica B's own OnMigrationComplete must still run so its local ready
	// flag converges too. wrappedB.migrationCompleted proxies that flag
	// since this harness's wrapper skips the real setRangeableLocallyReady
	// side effect.
	ctxB, shardB, idxB, classNameB := newReplica("RangeableMultiReplicaB_")
	failing := &atomic.Bool{}
	failing.Store(true)
	calls := &atomic.Int32{}
	taskB, wrappedB := newRangeableInMemoryTestTask(t, idxB, classNameB, propName, failing, calls)
	require.NoError(t, runReindexToCompletionOrError(t, ctxB, taskB, shardB),
		"a non-cancellation rebuild failure must not fail the migration (finding 5)")
	require.True(t, wrappedB.migrationCompleted,
		"replica B's OnMigrationComplete must still run so its local ready flag converges, "+
			"exactly as it would need to once the cluster-wide schema flag is already true")
	assert.GreaterOrEqual(t, calls.Load(), int32(1))

	bucketB := shardB.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucketB)
	assert.False(t, bucketB.RangeableServesFromMemory(),
		"replica B's failed rebuild must not have activated in-memory serving")

	fpA := filterableToRangeableFingerprint(t, bucketA)
	fpB := filterableToRangeableFingerprint(t, bucketB)
	assert.Equal(t, fpA, fpB,
		"replica B, serving from disk, must return the identical result set to replica A, "+
			"serving from memory - the staggered flip must never produce diverging results")

	// Restart replica B: boot-time population rebuilds the rep from disk,
	// restoring acceleration without any repair action.
	shardName := shardB.Name()
	require.NoError(t, shardB.Shutdown(ctxB))

	taskB2, _ := newFilterableToRangeableTask(t, idxB, classNameB, propName)
	idxB.shardReindexer = &testShardReindexer{task: taskB2}

	shdB2, err := idxB.initShard(ctxB, shardName, newFilterableToRangeableTestClass(classNameB), nil, true, true)
	require.NoError(t, err)
	shardB2 := shdB2.(*Shard)
	idxB.shards.Store(shardName, shdB2)
	t.Cleanup(func() { shardB2.Shutdown(ctxB) })

	restartedBucket := shardB2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, restartedBucket)
	assert.True(t, restartedBucket.RangeableServesFromMemory(),
		"restart must rebuild the rep from disk and restore in-memory acceleration")
}

// TestRebuildRangeableInMemoryReps_NilBucketRoutesContextCancellation pins
// weaviate/weaviate#12215: a nil bucket (a legitimate, tolerated condition)
// must not become a hard failure when the context is already done - it
// must return the context error directly so errors.Is(err, context.Canceled)
// still works.
func TestRebuildRangeableInMemoryReps_NilBucketRoutesContextCancellation(t *testing.T) {
	const numObjects = 5
	propName := filterableToRangeablePropName
	ctx := testCtx()
	className := "RangeableNilBucketCtxCancel_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	// A prop whose bucket was never created: store.Bucket(...) returns nil
	// for it regardless of context state, giving a deterministic nil-bucket
	// branch without needing to race a real shutdown.
	missingPropName := "never_created_prop"
	require.Nil(t, shard.Store().Bucket(task.strategy.SourceBucketName(missingPropName)),
		"precondition: no bucket exists for this prop")

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	err := task.rebuildRangeableInMemoryReps(cancelledCtx, idx.logger, shard, []string{missingPropName})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled,
		"a cancelled context must route directly, preserving errors.Is compatibility")
	assert.NotContains(t, err.Error(), "not found post-swap",
		"a cancelled context must short-circuit before the hard-fail message is built")
}

// TestRebuildRangeableInMemoryReps_NilBucketDegradesWithoutCancellation
// pins weaviate/weaviate#12215: absent cancellation, a nil bucket also
// degrades WARN-and-continue (ERROR log + metric) instead of failing the
// migration.
func TestRebuildRangeableInMemoryReps_NilBucketDegradesWithoutCancellation(t *testing.T) {
	const numObjects = 5
	propName := filterableToRangeablePropName
	ctx := testCtx()
	className := "RangeableNilBucketDegrade_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(idx *Index) { idx.Config.IndexRangeableInMemory = true })
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	hookLogger, hook := test.NewNullLogger()
	idx.logger = hookLogger

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)

	missingPropName := "never_created_prop"
	require.Nil(t, shard.Store().Bucket(task.strategy.SourceBucketName(missingPropName)))

	err := task.rebuildRangeableInMemoryReps(ctx, idx.logger, shard, []string{missingPropName})
	require.NoError(t, err, "a nil bucket without cancellation must degrade WARN-and-continue too (finding 5), not hard-fail")
	assert.False(t, errors.Is(err, context.Canceled))

	errLine := firstErrorEntry(hook)
	require.NotNil(t, errLine)
	assert.Contains(t, errLine.Message, "not found post-swap")

	className2 := shard.Index().Config.ClassName.String()
	metric, err2 := monitoring.GetMetrics().RangeableInMemoryRebuildDegraded.GetMetricWithLabelValues(className2, shard.Name(), missingPropName)
	require.NoError(t, err2)
	assert.GreaterOrEqual(t, testutil.ToFloat64(metric), float64(1))
}
