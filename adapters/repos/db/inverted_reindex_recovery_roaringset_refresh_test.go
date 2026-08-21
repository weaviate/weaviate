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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for RoaringSetRefresh — the same-strategy
// refresh of a filterable RoaringSet bucket (production code path
// behind `repair-filterable`). Inline runtimeSwap path.

// newRoaringSetRefreshTask wraps RoaringSetRefreshStrategy.
func newRoaringSetRefreshTask(t *testing.T, idx *Index) (*ShardReindexTaskGeneric, *roaringSetRefreshStrategyWrapper) {
	t.Helper()
	wrapped := &roaringSetRefreshStrategyWrapper{
		RoaringSetRefreshStrategy: RoaringSetRefreshStrategy{
			generation: 1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"RoaringSetRefresh", idx.logger, wrapped,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	// Without an identity the task's record key is incomplete and every
	// transition would refuse to write itself.
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-roaringset-refresh", Version: 1},
		"shard-1__node-0",
		&ReindexTaskPayload{MigrationType: ReindexTypeRepairFilterable},
	)
	return task, wrapped
}

// roaringSetRefreshStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert completion. The real strategy's
// OnMigrationComplete is already a no-op (same-strategy refresh needs
// no schema update); this wrapper is essentially an observer.
type roaringSetRefreshStrategyWrapper struct {
	RoaringSetRefreshStrategy
	migrationCompleted bool
}

func (s *roaringSetRefreshStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
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

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newRoaringSetRefreshTask(t, idx)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	return fingerprintRoaringSetBucket(t,
		shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)))
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

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(filtBucketName)
	require.NotNil(t, preBucket, "pre-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, preBucket.Strategy(),
		"pre-migration filterable bucket must be StrategyRoaringSet")
	preFP := fingerprintRoaringSetBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration filterable fingerprint must be non-empty")

	task, wrapped := newRoaringSetRefreshTask(t, idx)
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

	postBucket := shard.store.Bucket(filtBucketName)
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

	// Swapped rather than Promoted: the flip is durable, and the
	// staged-to-canonical rename is deliberately left to the next load.
	rec, ok := task.migrationRecord(shard)
	require.True(t, ok, "the migration must have left a record")
	require.Equal(t, MigrationStateSwapped, rec.State())
	require.Equal(t, []string{propName}, rec.(MigrationRecordSwapped).Flipped())
}

// TestRecoveryConvergence_RoaringSetRefresh_FromEachState pins the
// #240 Symptom B invariant for the RoaringSetRefresh strategy: from any
// recorded state a replica could land in after a mid-migration restart,
// the recovery code path converges on filterable-bucket content
// bit-equivalent to the clean baseline run.
//
// Each state is reached through production code: the
// OnAfterLsmInit+OnAfterLsmInitAsync loop (with skipSwapOnFinish to halt
// once the rebuild is recorded complete), plus a direct runtimePrepare
// call for the merged state.
func TestRecoveryConvergence_RoaringSetRefresh_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeRoaringSetRefreshBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "RoaringSetRefresh_Iterated_via_skipSwapOnFinish",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				task.skipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedState: MigrationStateIterated,
		},
		{
			name: "RoaringSetRefresh_Merged_via_runtimePrepare_no_runtimeSwap",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				task.skipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				// runtimePrepare stops one step short of runtimeSwap, so
				// the staged data is complete and no flip is decided yet.
				rec, ok := task.migrationRecord(shard)
				require.True(t, ok)
				require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rec.Subject().Properties))
			},
			expectedState: MigrationStateMerged,
		},
		{
			name: "RoaringSetRefresh_Swapped_full_migration",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
			},
			expectedState: MigrationStateSwapped,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RoaringSetRefreshCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			// Phase 1: drive the migration to the case-specific state
			// using the production code path.
			task, _ := newRoaringSetRefreshTask(t, idx)
			tc.driveToState(t, ctx, shard, task)

			// Verify driveToState actually landed at the intended
			// state. Without this guard a buggy driveToState would let
			// recovery from a different state appear to "converge".
			rec, ok := task.migrationRecord(shard)
			require.Truef(t, ok, "driveToState must leave a record (case %q)", tc.name)
			assert.Equalf(t, tc.expectedState, rec.State(),
				"after driveToState (case %q)", tc.name)

			// Phase 2: simulate restart — full shutdown + shard re-init
			// + fresh task. This is the real-world restart sequence:
			// shard_init reconciles the records, then LSM init, then
			// OnAfterLsmInit, then the OnAfterLsmInitAsync loop on the
			// background scheduler.
			// Whether a merged migration should become live is a cluster
			// fact. With no task map the verdict is "leave", and the row
			// that exists to prove promotion happens at load would pass
			// against the pre-migration bucket instead.
			subject := rec.Subject()
			require.NotNil(t, idx.db, "test shard fixture must wire idx.db")
			idx.db.SetMigrationLocalTaskSource(func() ([]*distributedtask.Task, bool) {
				return []*distributedtask.Task{{
					Namespace: ReindexNamespace,
					TaskDescriptor: distributedtask.TaskDescriptor{
						ID: subject.TaskID, Version: subject.Key.TaskVersion,
					},
					Status: distributedtask.TaskStatusFinished,
				}}, true
			})

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newRoaringSetRefreshTask(t, idx)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Drive the async loop to completion in case recovery is
			// only partially handled by OnAfterLsmInit.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err,
					"recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Phase 3: convergence check against baseline fingerprint.
			bucket := shard2.store.Bucket(helpers.BucketFromPropNameLSM(propName))
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
