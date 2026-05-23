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
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Torn-state recovery guard — Sev-1 silent-data-loss protection
// -----------------------------------------------------------------------------
//
// [ShardReindexTaskGeneric] has a torn-state guard wired into both
// OnBeforeLsmInit and OnAfterLsmInit. The guard fires when reindexed.mig
// is on disk, none of IsPrepended/IsMerged/IsSwapped/IsTidied are set
// (so the reindex bucket dirs MUST still exist), and at least one
// per-property reindex bucket dir is missing.
//
// On a fire the guard unmarks reindexed.mig so the next pass re-iterates
// instead of running runtime swap against an EMPTY reindex bucket.
// Without it the schema flag would flip on top of no data — the Sev-1
// silent failure tracked at weaviate/0-weaviate-issues#240 Symptom A.
//
// This file pins both arms: two positive cases (guard fires) and three
// negative cases (guard correctly leaves state alone), plus end-to-end
// convergence after a guard-triggered recovery (which exercises the
// weaviate/0-weaviate-issues#244 fix).

const (
	tornGuardNumObjects = 25
	tornGuardPropName   = "title"
)

// runTornStateMigrationToReindexed drives a fresh shard to the
// IsReindexed state (and NO further sentinels) so the torn-state
// guard's preconditions hold. The returned shard's pathLSM is the
// callable target for the synthetic dir-removal step that the
// positive cases perform.
func runTornStateMigrationToReindexed(t *testing.T, ctx context.Context, className string) (*Shard, *Index) {
	t.Helper()
	class := newTestClassWithProps(className, []string{tornGuardPropName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	for _, obj := range makeConvergenceTestObjects(t, tornGuardNumObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	task.skipSwapOnFinish.Store(true) // halt at IsReindexed, BEFORE runtimeSwap
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	// Verify the precondition: we're at IsReindexed and no further
	// sentinels are set.
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed(), "precondition: IsReindexed must be true")
	require.False(t, rt.IsPrepended(), "precondition: IsPrepended must be false")
	require.False(t, rt.IsMerged(), "precondition: IsMerged must be false")
	require.False(t, rt.IsSwapped(), "precondition: IsSwapped must be false")
	require.False(t, rt.IsTidied(), "precondition: IsTidied must be false")

	return shard, idx
}

// TestTornState_OnAfterLsmInit_GuardFires_ResetsReindexed pins the
// positive case: when the preconditions hold AND the reindex bucket
// dir is missing, OnAfterLsmInit's torn-state guard MUST unmark
// reindexed.mig so the next pass re-iterates.
//
// Reproduction shape:
//  1. Drive a clean migration to IsReindexed only (via skipSwapOnFinish).
//  2. Synthetically `rm -rf` the reindex bucket dir while keeping
//     `reindexed.mig` (simulates a crash between markReindexed and the
//     first runtimeSwap step where dirs were never created — or, in
//     production, a corrupt restore from backup).
//  3. Run a fresh task's OnAfterLsmInit on the SAME shard.
//  4. Assert: reindexed.mig is GONE post-call (guard fired and unmarked).
func TestTornState_OnAfterLsmInit_GuardFires_ResetsReindexed(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardA_" + uuid.NewString()[:8]
	shard, idx := runTornStateMigrationToReindexed(t, ctx, className)
	defer shard.Shutdown(ctx)

	// Synthetically remove the reindex bucket dir, leaving reindexed.mig
	// on disk. Per the guard godoc this is the exact "forged sentinel /
	// died before any iteration data reached disk" shape.
	strategy0 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task0 := newTestTask(idx.logger, strategy0)
	reindexBucketDir := shard.pathLSM() + "/" + task0.reindexBucketName(tornGuardPropName)
	require.DirExists(t, reindexBucketDir, "fixture: reindex bucket dir must exist before we remove it")
	require.NoError(t, os.RemoveAll(reindexBucketDir),
		"synthetic torn-state: remove reindex bucket dir while sentinel survives")

	// Run a fresh task's OnAfterLsmInit. The guard at
	// inverted_reindex_task_generic.go:1091 must detect the torn state
	// and unmark reindexed.
	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	require.NoError(t, task2.OnAfterLsmInit(ctx, shard),
		"OnAfterLsmInit must not error on torn-state recovery")

	rt, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.False(t, rt.IsReindexed(),
		"torn-state guard MUST unmark reindexed.mig when the reindex bucket dir is missing in the pre-prepend window")
}

// TestTornState_OnAfterLsmInit_RecoveryConvergesToBaseline pins the
// end-to-end positive case: after the guard resets reindexed.mig,
// driving the async loop to completion must produce a fingerprint
// identical to a clean baseline run. Pins the fix for
// weaviate/0-weaviate-issues#244 (unmarkReindexed now also clears
// every progress.mig.<N> checkpoint so the resumed iteration starts
// from the beginning instead of the stale lastProcessedKey).
func TestTornState_OnAfterLsmInit_RecoveryConvergesToBaseline(t *testing.T) {
	baseline := computeBaselineFingerprint(t, tornGuardPropName, tornGuardNumObjects)
	require.NotEmpty(t, baseline)

	ctx := testCtx()
	className := "TornGuardB_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{tornGuardPropName})
	shard, idx := runTornStateMigrationToReindexed(t, ctx, className)

	// Synthetically remove the reindex bucket dir while reindexed.mig
	// survives. Then simulate a restart (the production code path
	// production code goes through after a crash) — Shutdown +
	// idx.initShard with a fresh task installed on the index. This is
	// the same restart primitive PR #11415 uses for its convergence
	// matrix; it's the only faithful way to exercise the guard +
	// recovery end-to-end because OnAfterLsmInit alone doesn't
	// re-create the buckets — the proper init path inside `initShard`
	// does (OnBeforeLsmInit runs first, then LSM init, then
	// OnAfterLsmInit).
	strategy0 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task0 := newTestTask(idx.logger, strategy0)
	reindexBucketDir := shard.pathLSM() + "/" + task0.reindexBucketName(tornGuardPropName)
	require.NoError(t, os.RemoveAll(reindexBucketDir))

	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init must succeed after torn-state recovery")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	bucketName := helpers.BucketSearchableFromPropNameLSM(tornGuardPropName)
	bucket := shard2.store.Bucket(bucketName)
	require.NotNil(t, bucket)
	require.Equal(t, lsmkv.StrategyInverted, bucket.Strategy())

	got := fingerprintInvertedBucket(t, bucket)
	assert.Equal(t, len(baseline), len(got),
		"post-torn-state-recovery fingerprint must have same term count as baseline (guard let iteration re-run)")
	for term, expectedIDs := range baseline {
		gotIDs, ok := got[term]
		assert.Truef(t, ok, "term %q in baseline missing post-recovery", term)
		assert.Equalf(t, expectedIDs, gotIDs,
			"term %q post-recovery doc-id list diverges from baseline", term)
	}
}

// TestTornState_OnAfterLsmInit_DirsPresent_GuardNoOp pins the
// negative case: when reindexed.mig is set AND the reindex bucket
// dirs are present (normal IsReindexed state), the guard MUST NOT
// fire. A false-positive here would unmark a perfectly-legitimate
// sentinel and force a wasted re-iteration on every restart.
func TestTornState_OnAfterLsmInit_DirsPresent_GuardNoOp(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardC_" + uuid.NewString()[:8]
	shard, idx := runTornStateMigrationToReindexed(t, ctx, className)
	defer shard.Shutdown(ctx)

	// DO NOT remove the dirs — they are legitimately present.
	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(true) // stay at IsReindexed; we only want to test the guard's no-op
	require.NoError(t, task2.OnAfterLsmInit(ctx, shard))

	rt, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.True(t, rt.IsReindexed(),
		"torn-state guard MUST NOT unmark reindexed.mig when the reindex bucket dir is still present")
}

// TestTornState_OnAfterLsmInit_PostPrependedDirsGone_GuardNoOp pins
// the second negative case: once IsPrepended is set, runtimeSwap has
// intentionally removed the reindex bucket dirs (the segments live
// in the ingest bucket from then on). A missing dir past that point
// is correct, not torn — the guard's window-precondition must
// exclude this state.
//
// Without this exclusion, the guard would unmark reindexed.mig on
// every restart of an IsPrepended-state migration, sending the
// task back to iteration when it should be continuing forward to
// IsMerged.
func TestTornState_OnAfterLsmInit_PostPrependedDirsGone_GuardNoOp(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardD_" + uuid.NewString()[:8]
	shard, idx := runTornStateMigrationToReindexed(t, ctx, className)
	defer shard.Shutdown(ctx)

	// Drive past IsReindexed → IsMerged via runtimePrepare (production
	// code path; same atomic-method as the convergence test uses).
	strategy0 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task0 := newTestTask(idx.logger, strategy0)
	rt0, err := task0.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	props, err := task0.readPropsToReindex(rt0)
	require.NoError(t, err)
	require.NoError(t, task0.runtimePrepare(ctx, idx.logger, shard, rt0, props),
		"runtimePrepare must succeed (it writes markPrepended + cleanup + markMerged)")

	// Precondition: IsPrepended AND IsMerged set; reindex bucket dir
	// SHOULD be gone (runtimePrepare cleaned it up).
	require.True(t, rt0.IsPrepended())
	require.True(t, rt0.IsMerged())
	reindexBucketDir := shard.pathLSM() + "/" + task0.reindexBucketName(tornGuardPropName)
	assert.NoDirExists(t, reindexBucketDir,
		"runtimePrepare must have removed the reindex bucket dir; if this fails the test's"+
			" precondition is wrong, not the guard")

	// Now call OnAfterLsmInit with a FRESH task. The guard must NOT
	// fire because we're not in the pre-prepend window — the missing
	// dir is correct.
	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	require.NoError(t, task2.OnAfterLsmInit(ctx, shard))

	rt, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.True(t, rt.IsReindexed(),
		"guard MUST NOT unmark reindexed when IsPrepended is set (missing dir is correct post-runtimePrepare)")
	assert.True(t, rt.IsPrepended(),
		"prepended sentinel must survive the guard's no-op")
	assert.True(t, rt.IsMerged(),
		"merged sentinel must survive the guard's no-op")
}

// TestTornState_OnAfterLsmInit_NoReindexedSentinel_GuardNoOp pins the
// third negative case: when reindexed.mig is NOT set, the guard's
// precondition is false and it must not fire. This covers the
// fresh-migration case where the shard has nothing on disk yet.
func TestTornState_OnAfterLsmInit_NoReindexedSentinel_GuardNoOp(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardE_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{tornGuardPropName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, tornGuardNumObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Fresh task, no prior sentinels on disk.
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	task.skipSwapOnFinish.Store(true) // stop early so we don't progress past IsReindexed
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	// The guard's check requires reindexed.mig EXISTS. With no prior
	// sentinels the guard's preconditions are false on the first
	// OnAfterLsmInit call (before any markReindexed). The test is mostly
	// a documentation pin: in this state, the migration proceeds normally.
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.False(t, rt.IsReindexed(),
		"prior to iteration, IsReindexed must be false — this is the no-op-by-precondition case")
}

// TestTornState_OnBeforeLsmInit_GuardFires_ResetsReindexed pins the
// SAME guard on the OnBeforeLsmInit code path. OnBeforeLsmInit runs
// strictly before OnAfterLsmInit at shard init, so a torn state must
// be caught there too — otherwise the LSM init proceeds with a
// stale sentinel and OnAfterLsmInit's catch is the second line of
// defense.
//
// This test is the symmetric pin: ensure the guard at
// inverted_reindex_task_generic.go:885 also fires.
func TestTornState_OnBeforeLsmInit_GuardFires_ResetsReindexed(t *testing.T) {
	ctx := testCtx()
	className := "TornGuardF_" + uuid.NewString()[:8]
	shard, idx := runTornStateMigrationToReindexed(t, ctx, className)
	defer shard.Shutdown(ctx)

	strategy0 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task0 := newTestTask(idx.logger, strategy0)
	reindexBucketDir := shard.pathLSM() + "/" + task0.reindexBucketName(tornGuardPropName)
	require.NoError(t, os.RemoveAll(reindexBucketDir))

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	require.NoError(t, task2.OnBeforeLsmInit(ctx, shard),
		"OnBeforeLsmInit must not error on torn-state recovery")

	rt, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	assert.False(t, rt.IsReindexed(),
		"OnBeforeLsmInit torn-state guard MUST unmark reindexed.mig when the reindex bucket dir is missing")
}
