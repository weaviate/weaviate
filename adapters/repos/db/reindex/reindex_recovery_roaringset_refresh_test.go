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

package reindex_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for RoaringSetRefresh — the same-strategy
// refresh of a filterable RoaringSet bucket (production code path
// behind `repair-filterable`). Inline runtimeSwap path, so the matrix
// mirrors MapToBlockmax_FromEachState verbatim with the bucket type as
// the only difference.

// newRoaringSetRefreshTask wraps reindex.RoaringSetRefreshStrategy.
func newRoaringSetRefreshTask(t *testing.T, logger logrus.FieldLogger) (*reindex.ShardReindexTaskGeneric, *roaringSetRefreshStrategyWrapper) {
	t.Helper()
	wrapped := &roaringSetRefreshStrategyWrapper{
		RoaringSetRefreshStrategy: reindex.RoaringSetRefreshStrategy{
			Generation: 1,
		},
	}
	task := reindex.NewShardReindexTaskGeneric(
		"RoaringSetRefresh", logger, wrapped,
		reindex.ReindexTaskConfig{
			SwapBuckets:                   true,
			TidyBuckets:                   true,
			Concurrency:                   2,
			MemtableOptFactor:             4,
			BackupMemtableOptFactor:       1,
			ProcessingDuration:            10 * time.Minute,
			PauseDuration:                 1 * time.Second,
			CheckProcessingEveryNoObjects: 1000,
		},
		&reindex.UuidKeyParser{}, reindex.UuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// roaringSetRefreshStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert completion. The real strategy's
// OnMigrationComplete is already a no-op (same-strategy refresh needs
// no schema update); this wrapper is essentially an observer.
type roaringSetRefreshStrategyWrapper struct {
	reindex.RoaringSetRefreshStrategy
	migrationCompleted bool
}

func (s *roaringSetRefreshStrategyWrapper) OnMigrationComplete(_ context.Context, _ reindex.ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// computeRoaringSetRefreshBaseline runs a clean RoaringSetRefresh
// migration on a throw-away shard and returns its post-migration
// filterable-bucket fingerprint. Every recovery-from-state case asserts
// bit-equal convergence against this baseline. Sibling of
// computeBaselineFingerprint (MapToBlockmax).
func computeRoaringSetRefreshBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "RoaringSetRefreshBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newRoaringSetRefreshTask(t, f.Logger())
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	return fingerprintRoaringSetBucket(t,
		shard.Store().Bucket(helpers.BucketFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_RoaringSetRefresh_Baseline establishes that the
// production migration code path runs end-to-end on the test fixture and
// produces a non-empty filterable-bucket fingerprint. Sanity check
// before the matrix: if this fails, every cell in the matrix would fail
// for the same root cause.
func TestRecoveryConvergence_RoaringSetRefresh_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "RoaringSetRefreshBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	preBucket := shard.Store().Bucket(filtBucketName)
	require.NotNil(t, preBucket, "pre-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, preBucket.Strategy(),
		"pre-migration filterable bucket must be StrategyRoaringSet")
	preFP := fingerprintRoaringSetBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration filterable fingerprint must be non-empty")

	task, wrapped := newRoaringSetRefreshTask(t, f.Logger())
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.Store().Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must remain StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty")
	// Same-strategy refresh: a clean rebuild must produce exactly the
	// same per-term posting list set as the pre-migration bucket (the
	// objects didn't change). This is the definitional invariant for a
	// "repair" strategy.
	require.Equalf(t, len(preFP), len(postFP),
		"refresh changed term count: pre=%d post=%d", len(preFP), len(postFP))
	for term, preIDs := range preFP {
		postIDs, ok := postFP[term]
		require.Truef(t, ok, "term %q present pre-migration but missing post-migration", term)
		require.Equalf(t, preIDs, postIDs,
			"term %q posting list changed across same-strategy refresh", term)
	}

	rt, err := task.NewReindexTracker(shard.PathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_RoaringSetRefresh_FromEachState pins the
// #240 Symptom B invariant for the RoaringSetRefresh strategy: from any
// on-disk state a replica could land in after a mid-migration restart,
// the recovery code path converges on filterable-bucket content
// bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either the production
// OnAfterLsmInit+OnAfterLsmInitAsync loop (with skipSwapOnFinish to halt
// at IsReindexed) plus direct runtimePrepare invocations for the
// intermediate states, or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 uses for MapToBlockmax.
func TestRecoveryConvergence_RoaringSetRefresh_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeRoaringSetRefreshBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "RoaringSetRefresh_IsReindexed_via_skipSwapOnFinish",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				task.SkipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": false,
				"merged":    false,
				"swapped":   false,
				"tidied":    false,
			},
		},
		{
			name: "RoaringSetRefresh_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				// runtimePrepare writes markPrepended + cleanup +
				// markMerged in one atomic method, so we can't reach
				// IsPrepended-without-IsMerged via production code
				// alone. Drive to IsMerged via runtimePrepare, then
				// remove merged.mig by hand to simulate a crash
				// between markPrepended() and markMerged().
				task.SkipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				rt, err := task.NewReindexTracker(shard.PathLSM())
				require.NoError(t, err)
				props, err := task.ReadPropsToReindex(rt)
				require.NoError(t, err)
				require.NoError(t, task.RuntimePrepare(ctx, task.Logger, shard, rt, props))
				ftr := rt.(*reindex.FileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.Config.MigrationPath, ftr.Config.FilenameMerged)),
					"removing merged.mig to synthesize IsPrepended-only state")
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": true,
				"merged":    false,
				"swapped":   false,
				"tidied":    false,
			},
		},
		{
			name: "RoaringSetRefresh_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				// runtimeSwap writes markSwapped + tidy + markTidied
				// atomically. Drive the migration to completion, then
				// remove tidied.mig by hand to simulate a crash between
				// markSwapped() and markTidied().
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				rt, err := task.NewReindexTracker(shard.PathLSM())
				require.NoError(t, err)
				ftr := rt.(*reindex.FileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.Config.MigrationPath, ftr.Config.FilenameTidied)),
					"removing tidied.mig to synthesize IsSwapped-only state")
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": true,
				"merged":    true,
				"swapped":   true,
				"tidied":    false,
			},
		},
		{
			name: "RoaringSetRefresh_IsMerged_via_runtimePrepare_no_runtimeSwap",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				// Step 1: drive iteration to markReindexed via the
				// production barrier path.
				task.SkipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				// Step 2: call runtimePrepare directly (production code
				// path; same package access). This writes markPrepended
				// + cleanup + markMerged in one atomic method. We stop
				// before runtimeSwap.
				rt, err := task.NewReindexTracker(shard.PathLSM())
				require.NoError(t, err)
				props, err := task.ReadPropsToReindex(rt)
				require.NoError(t, err)
				require.NoError(t, task.RuntimePrepare(ctx, task.Logger, shard, rt, props))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": true,
				"merged":    true,
				"swapped":   false,
				"tidied":    false,
			},
		},
		{
			name: "RoaringSetRefresh_IsTidied_full_migration",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": true,
				"merged":    true,
				"swapped":   true,
				"tidied":    true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RoaringSetRefreshCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*db.Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			// Phase 1: drive the migration to the case-specific state
			// using the production code path.
			task, _ := newRoaringSetRefreshTask(t, f.Logger())
			tc.driveToState(t, ctx, shard, task)

			// Verify driveToState actually landed at the intended
			// on-disk state. Without this guard a buggy driveToState
			// would let recovery from a different state appear to
			// "converge".
			rt, err := task.NewReindexTracker(shard.PathLSM())
			require.NoError(t, err)
			actualSentinels := map[string]bool{
				"reindexed": rt.IsReindexed(),
				"prepended": rt.IsPrepended(),
				"merged":    rt.IsMerged(),
				"swapped":   rt.IsSwapped(),
				"tidied":    rt.IsTidied(),
			}
			for name, want := range tc.expectedPostStateSentinels {
				assert.Equalf(t, want, actualSentinels[name],
					"after driveToState, sentinel %q expected=%v got=%v (case %q)",
					name, want, actualSentinels[name], tc.name)
			}

			// Phase 2: simulate restart — full shutdown + shard re-init
			// + fresh task. This is the real-world restart sequence:
			// shard_init runs FinalizeCompletedMigrations, then
			// OnBeforeLsmInit, then LSM init, then OnAfterLsmInit, then
			// OnAfterLsmInitAsync loop on the background scheduler.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newRoaringSetRefreshTask(t, f.Logger())
			task2.SkipSwapOnFinish.Store(false)
			f.SetShardReindexer(&testShardReindexer{task: task2})

			shd2, err := f.InitShard(ctx, shardName, class, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*db.Shard)
			defer shard2.Shutdown(ctx)
			f.StoreShard(shardName, shd2)

			// Drive the async loop to completion in case recovery is
			// only partially handled by OnBeforeLsmInit + OnAfterLsmInit.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err,
					"recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Phase 3: convergence check against baseline fingerprint.
			bucket := shard2.Store().Bucket(helpers.BucketFromPropNameLSM(propName))
			require.NotNilf(t, bucket, "post-recovery filterable bucket must exist (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyRoaringSet, bucket.Strategy(),
				"post-recovery filterable bucket must remain StrategyRoaringSet (case %q)", tc.name)

			got := fingerprintRoaringSetBucket(t, bucket)

			// Catch divergence at term granularity for actionable
			// failure output (which token has the wrong posting list).
			assert.Equalf(t, len(baseline), len(got),
				"post-recovery filterable term count diverges from baseline (case %q)", tc.name)
			for term, expectedIDs := range baseline {
				gotIDs, ok := got[term]
				if !ok {
					assert.Failf(t, "missing term",
						"term %q present in baseline but missing post-recovery (case %q)", term, tc.name)
					continue
				}
				assert.Equalf(t, expectedIDs, gotIDs,
					"term %q post-recovery doc-id list diverges from baseline (case %q)\n  baseline (%d): %v\n  got      (%d): %v",
					term, tc.name, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
			}
		})
	}
}
