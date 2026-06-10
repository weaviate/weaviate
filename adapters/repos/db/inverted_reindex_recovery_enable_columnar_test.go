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

//go:build integrationTest

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Recovery-convergence matrix for EnableColumnar
// -----------------------------------------------------------------------------
//
// Port of the FilterableToRangeable matrix (see
// inverted_reindex_recovery_filterable_to_rangeable_test.go) to the
// enable-columnar migration: drive a shard to each of the 5 sentinel states a
// crashed replica can land in, restart, and assert the post-recovery columnar
// bucket fingerprint converges to the clean baseline.
//
// EnableColumnar shares the structural property that made the rangeable
// matrix necessary: its target (columnar) bucket is created by the strategy's
// PreReindexHook, not by createPropertyValueIndex, so the recovery branches
// in OnBeforeLsmInit must re-fire the hook or the replica comes back without
// the target bucket loaded (weaviate/0-weaviate-issues#246 failure shape).
//
// Builds on the test scaffolding from inverted_reindex_enable_columnar_test.go
// (class/object generators, task wrapper, columnar fingerprint helper).

// computeEnableColumnarBaseline runs a clean inline migration on a throw-away
// shard and returns the post-migration columnar-bucket fingerprint
// (docID → int64 value). Every recovery-from-state case asserts convergence
// against this. Sibling of computeFilterableToRangeableBaseline.
func computeEnableColumnarBaseline(t *testing.T, propName string, numObjects int) map[uint64]int64 {
	t.Helper()
	ctx := testCtx()
	shard, idx, _, className := setupEnableColumnarShard(t, ctx, "EnableColumnarBaselineRef", numObjects)

	task, _ := newEnableColumnarTask(t, idx, className, propName)
	runEnableColumnarTask(t, ctx, task, shard)

	return enableColumnarFingerprint(t,
		shard.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_EnableColumnar_FromEachState pins the #240
// Symptom B invariant for the EnableColumnar strategy: from any on-disk
// state a replica could land in after a mid-migration restart, the recovery
// code path converges on columnar bucket content equivalent to the clean
// baseline run.
//
// Five sentinel states, matching the MapToBlockmax / FilterableToRangeable
// matrix shape — EnableColumnar is non-semantic (inline runtimeSwap path).
func TestRecoveryConvergence_EnableColumnar_FromEachState(t *testing.T) {
	const numObjects = 25
	propName := enableColumnarPropName

	baseline := computeEnableColumnarBaseline(t, propName, numObjects)
	require.Len(t, baseline, numObjects, "baseline fingerprint must hold one row per object")

	cases := recoveryConvergenceStateCases("EnableColumnar")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			shard, idx, class, className := setupEnableColumnarShard(t, ctx, "EnableColumnarCase", numObjects)

			task, _ := newEnableColumnarTask(t, idx, className, propName)

			tc.driveToState(t, ctx, shard, task)

			// Verify driveToState actually landed at the intended on-disk
			// state. Without this guard a buggy driveToState would let
			// recovery from a different state appear to "converge".
			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			for name, want := range tc.expectedPostStateSentinels {
				var got bool
				switch name {
				case "reindexed":
					got = rt.IsReindexed()
				case "prepended":
					got = rt.IsPrepended()
				case "merged":
					got = rt.IsMerged()
				case "swapped":
					got = rt.IsSwapped()
				case "tidied":
					got = rt.IsTidied()
				}
				assert.Equalf(t, want, got, "after driveToState, sentinel %q (case %q)", name, tc.name)
			}

			// Simulated restart: graceful shutdown, fresh task, then
			// idx.initShard re-runs FinalizeCompletedMigrations →
			// OnBeforeLsmInit → LSM init → OnAfterLsmInit. Same restart
			// primitive as the rangeable matrix.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newEnableColumnarTask(t, idx, className, propName)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Drive the async loop to completion. EnableColumnar is
			// non-semantic, so the inline runtimeSwap path completes the
			// migration within OnAfterLsmInitAsync.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err, "recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			bucket := shard2.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
			require.NotNilf(t, bucket, "post-recovery columnar bucket must exist (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyColumnar, bucket.Strategy(),
				"post-recovery columnar bucket must be StrategyColumnar (case %q)", tc.name)

			got := enableColumnarFingerprint(t, bucket)

			// Catch divergence at docID granularity for actionable failure
			// output (which row holds the wrong value / is missing).
			assert.Equalf(t, len(baseline), len(got),
				"post-recovery columnar row count diverges from baseline (case %q)", tc.name)
			for docID, expected := range baseline {
				gotVal, ok := got[docID]
				if !ok {
					assert.Failf(t, "missing row",
						"docID %d present in baseline but missing post-recovery (case %q)", docID, tc.name)
					continue
				}
				assert.Equalf(t, expected, gotVal,
					"docID %d post-recovery value diverges from baseline (case %q)", docID, tc.name)
			}
		})
	}
}

// TestRecoveryConvergence_EnableColumnar_IsTidied_OnBeforeLsmInitFiresHook
// pins the same contract the rangeable sibling pins (the narrow markTidied →
// PreReindexHook crash window): when [ShardReindexTaskGeneric.OnBeforeLsmInit]
// is re-entered with the tracker already at IsTidied (the previous process
// crashed between markTidied and the recovery-tidy hook fire), the
// early-return branch MUST re-fire PreReindexHook so the columnar target
// bucket is loaded. Without it, OnAfterLsmInitAsync's IsTidied safety check
// refuses OnMigrationComplete and the replica is stuck.
//
// FinalizeCompletedMigrations normally clears the tidied tracker at shard
// init, which masks the bug at the integration tier — this test calls
// OnBeforeLsmInit directly with explicit sentinel state to pin the branch
// independently. weaviate/0-weaviate-issues#246 narrow window.
func TestRecoveryConvergence_EnableColumnar_IsTidied_OnBeforeLsmInitFiresHook(t *testing.T) {
	const propName = enableColumnarPropName

	ctx := testCtx()
	className := "EnableColumnarIsTidiedHook_" + uuid.NewString()[:8]
	class := newEnableColumnarTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	task, wrapper := newEnableColumnarTask(t, idx, className, propName)

	// Synthesize the on-disk state of a crash between markTidied and the
	// recovery-branch PreReindexHook fire: every sentinel present.
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	ftr := rt.(*fileReindexTracker)
	require.NoError(t, os.MkdirAll(ftr.config.migrationPath, 0o755))
	for _, name := range []string{
		ftr.config.filenameStarted,
		ftr.config.filenameReindexed,
		ftr.config.filenamePrepended,
		ftr.config.filenameMerged,
		ftr.config.filenameSwapped,
		ftr.config.filenameTidied,
	} {
		require.NoError(t, os.WriteFile(
			filepath.Join(ftr.config.migrationPath, name),
			[]byte(ftr.encodeTimeNow()), 0o644))
	}
	// Properties sentinel so readPropsToReindex doesn't return empty.
	require.NoError(t, os.WriteFile(
		filepath.Join(ftr.config.migrationPath, ftr.config.filenameProperties),
		[]byte(propName), 0o644))

	// Counter is 0 — no migration ran in this process.
	require.Equal(t, 0, wrapper.preReindexHookCount, "precondition")

	require.NoError(t, task.OnBeforeLsmInit(ctx, shard),
		"OnBeforeLsmInit must succeed on the synthesized IsTidied state")

	// The contract: OnBeforeLsmInit's IsTidied-on-entry branch fires
	// PreReindexHook so the target bucket is loaded into the in-memory
	// store. Without it the counter stays 0 and the replica is stuck on
	// the migration.
	assert.GreaterOrEqual(t, wrapper.preReindexHookCount, 1,
		"OnBeforeLsmInit IsTidied-on-entry branch MUST fire PreReindexHook "+
			"(weaviate/0-weaviate-issues#246 narrow window)")

	// And the bucket is actually in the store, with the columnar strategy.
	bucket := shard.store.Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
	require.NotNil(t, bucket, "PreReindexHook must have created the columnar bucket")
	assert.Equal(t, lsmkv.StrategyColumnar, bucket.Strategy())
	assert.NotNil(t, bucket.ColumnarSchema(),
		"PreReindexHook must wire the per-property columnar schema")
}
