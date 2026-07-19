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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Regression tests for weaviate/weaviate#12199: a rangeable in-memory
// rebuild failure at migration finalize must drive the migration to FAILED,
// not a silent FINISHED that leaves the shard serving from disk.
//
// Failures are injected via the rebuildRangeableRepFn seam rather than
// reproduced black-box, since the real failure window is too narrow to hit
// reliably.

// newRangeableInMemoryTestTask installs a fault-injecting rebuildRangeableRepFn.
// failing is a pointer so callers can flip it on/off on the same task
// instance; calls counts invocations.
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

// assertRebuildFailureMessage checks the FAILED message names the property
// and states that in-memory activation, not the migration data, failed.
func assertRebuildFailureMessage(t *testing.T, err error, propName string) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), propName)
	assert.Contains(t, err.Error(), "could not be activated for in-memory serving")
}

// setupRangeableFinalizeFailureFixture builds a shard with objects, installs
// a failing rebuildRangeableRepFn, drives the migration through
// OnAfterLsmInit/OnAfterLsmInitAsync until the injected failure surfaces, and
// asserts the resulting FAILED message. classNamePrefix lets each caller
// keep its own class-name prefix in the generated class name.
func setupRangeableFinalizeFailureFixture(t *testing.T, classNamePrefix string) (
	context.Context, *Shard, *Index, string, *ShardReindexTaskGeneric,
	*testFilterableToRangeableStrategyWrapper, *atomic.Bool, *atomic.Int32,
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

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	failing := &atomic.Bool{}
	failing.Store(true)
	calls := &atomic.Int32{}

	task, wrapped := newRangeableInMemoryTestTask(t, idx, className, propName, failing, calls)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	var runErr error
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		if err != nil {
			runErr = err
			break
		}
		if rerunAt.IsZero() {
			break
		}
	}
	assertRebuildFailureMessage(t, runErr, propName)

	return ctx, shard, idx, className, task, wrapped, failing, calls
}

// TestFilterableToRangeable_RebuildFailure_RuntimeSwapAndRecoveryPath pins:
// a rebuild failure returns a property-named error, blocks OnMigrationComplete,
// and leaves on-disk swap/tidy state intact for retry - on both the
// runtimeSwap and finalizeMigrationAfterRecovery call sites.
func TestFilterableToRangeable_RebuildFailure_RuntimeSwapAndRecoveryPath(t *testing.T) {
	propName := filterableToRangeablePropName
	ctx, shard, idx, className, task, wrapped, failing, calls := setupRangeableFinalizeFailureFixture(t, "RangeableRebuildFail_")

	assert.False(t, wrapped.migrationCompleted,
		"OnMigrationComplete (the schema flip) must NOT fire when the rebuild failed")
	assert.GreaterOrEqual(t, calls.Load(), int32(1), "rebuildRangeableRepFn must have been invoked")

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.True(t, rt.IsSwapped(), "on-disk swap must complete even though the rebuild failed")
	assert.True(t, rt.IsTidied(), "on-disk tidy must complete even though the rebuild failed - "+
		"only in-memory activation is blocked, not the already-committed swap")

	// Simulates a restart: fresh task instance dispatches to
	// finalizeMigrationAfterRecovery since IsTidied() is already true.
	callsBeforeRecovery := calls.Load()
	task2, wrapped2 := newRangeableInMemoryTestTask(t, idx, className, propName, failing, calls)
	err = task2.RunSwapOnShard(ctx, shard)
	assertRebuildFailureMessage(t, err, propName)
	assert.False(t, wrapped2.migrationCompleted)
	assert.Greater(t, calls.Load(), callsBeforeRecovery,
		"finalizeMigrationAfterRecovery must re-invoke the rebuild, not skip it because tidied.mig is already set")

	// Repair: fix the fault and retry - on-disk state from the failed
	// attempts must converge.
	failing.Store(false)
	task3, wrapped3 := newRangeableInMemoryTestTask(t, idx, className, propName, failing, calls)
	require.NoError(t, task3.RunSwapOnShard(ctx, shard),
		"recovery must succeed once the rebuild is fixed")
	assert.True(t, wrapped3.migrationCompleted, "OnMigrationComplete must fire once the rebuild succeeds")

	bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, bucket)
	fp := filterableToRangeableFingerprint(t, bucket)
	require.Len(t, fp, filterableToRangeableNumDistinctValues,
		"post-convergence rangeable bucket must serve the full, correct term set")
}

// TestFilterableToRangeable_RebuildFailure_OnAfterLsmInitAsyncIsTidiedBranch
// pins: retrying via OnAfterLsmInitAsync's IsTidied-on-entry branch (distinct
// from RunSwapOnShard's recovery path) must re-check the rebuild before
// firing OnMigrationComplete, not bypass it.
func TestFilterableToRangeable_RebuildFailure_OnAfterLsmInitAsyncIsTidiedBranch(t *testing.T) {
	ctx, shard, _, _, task, wrapped, failing, calls := setupRangeableFinalizeFailureFixture(t, "RangeableRebuildFailTidiedBranch_")

	assert.False(t, wrapped.migrationCompleted)

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsTidied(),
		"precondition: the retry below must land on the IsTidied-on-entry branch, not runtimeSwap")

	// Retry on the same task instance - the call shape that used to bypass
	// the rebuild check.
	callsBeforeRetry := calls.Load()
	failing.Store(false)
	rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
	require.NoError(t, err, "retry through the IsTidied-on-entry branch must succeed once the rebuild is fixed")
	assert.True(t, rerunAt.IsZero())
	assert.True(t, wrapped.migrationCompleted, "OnMigrationComplete must fire once the rebuild succeeds")
	assert.Greater(t, calls.Load(), callsBeforeRetry,
		"the IsTidied-on-entry branch must re-invoke the rebuild before OnMigrationComplete, not skip it")
}
