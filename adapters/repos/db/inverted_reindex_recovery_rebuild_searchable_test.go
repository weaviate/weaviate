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
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for RebuildSearchable — rebuild an
// existing BlockMax bucket from objects. Source and target are both
// StrategyInverted, so the test class needs UsingBlockMaxWAND=true.
// Driven via the trio Run*OnShard methods even though production
// dispatches via RunOnShard; recovery dispatches off the on-disk
// sentinel, indifferent to invocation route.

// newRebuildSearchableTestClass mirrors newTestClassWithProps but flips
// UsingBlockMaxWAND to true so the searchable bucket for each property
// starts at StrategyInverted (BlockMax) — RebuildSearchable's source
// strategy is StrategyInverted, so without this flip the test would
// instead set up a MapCollection searchable bucket and the strategy
// would find no properties to rebuild.
func newRebuildSearchableTestClass(className string, propNames []string) *models.Class {
	props := make([]*models.Property, len(propNames))
	for i, name := range propNames {
		props[i] = &models.Property{
			Name:         name,
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		}
	}
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      true, // start at StrategyInverted searchable buckets
		},
		Properties: props,
	}
}

// newRebuildSearchableTask wraps a RebuildSearchableStrategy in the
// test infrastructure. Mirrors newSearchableRetokenizeTask /
// newFilterableRetokenizeTask but the strategy only carries propNames +
// generation (no targetTokenization, no bucketStrategy — rebuild is
// schema-stable). Config mirrors blockmaxSearchableTaskConfig with
// selection enabled so getPropsToReindex picks up the requested
// property even though discovery-by-strategy would also find it.
func newRebuildSearchableTask(t *testing.T, idx *Index, className, propName string) (*ShardReindexTaskGeneric, *testRebuildSearchableStrategyWrapper) {
	t.Helper()
	wrapped := &testRebuildSearchableStrategyWrapper{
		RebuildSearchableStrategy: RebuildSearchableStrategy{
			propNames:  []string{propName},
			generation: 1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"RebuildSearchable", idx.logger, wrapped,
		reindexTaskConfig{
			swapBuckets:                   true,
			tidyBuckets:                   true,
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,

			selectionEnabled: true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: {propName: {}},
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil,
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testRebuildSearchableStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert completion. The real
// strategy's OnMigrationComplete is already a no-op (rebuild preserves
// the property's existing schema flags), so this wrapper is purely an
// observer. Mirrors testSearchableRetokenizeStrategyWrapper /
// testFilterableRetokenizeStrategyWrapper.
type testRebuildSearchableStrategyWrapper struct {
	RebuildSearchableStrategy
	migrationCompleted bool
}

func (s *testRebuildSearchableStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// computeRebuildSearchableBaseline runs a clean rebuild on a throw-away
// shard and returns its post-migration fingerprint. Every
// recovery-from-state case asserts bit-equal convergence against this
// baseline. Sibling of computeSearchableRetokenizeBaseline /
// computeFilterableRetokenizeBaseline.
func computeRebuildSearchableBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "RebuildSearchableBaselineRef_" + uuid.NewString()[:8]
	class := newRebuildSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newRebuildSearchableTask(t, idx, className, propName)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return fingerprintInvertedBucket(t,
		shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_RebuildSearchable_Baseline establishes that
// the production migration code path can drive a fully-clean rebuild
// of an existing BlockMax searchable bucket on this fixture. Sanity
// check before the matrix: if this fails, every cell in the matrix
// would fail for the same root cause and the failure output here is
// far more actionable than five matrix rows failing in parallel.
func TestRecoveryConvergence_RebuildSearchable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "RebuildSearchableBaseline_" + uuid.NewString()[:8]
	class := newRebuildSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(searchBucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, preBucket.Strategy(),
		"pre-migration searchable bucket must be StrategyInverted (UsingBlockMaxWAND=true)")
	preFP := fingerprintInvertedBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration searchable fingerprint must be non-empty (objects already inserted)")

	task, wrapped := newRebuildSearchableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.store.Bucket(searchBucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must remain StrategyInverted")
	postFP := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration searchable fingerprint must be non-empty (objects re-tokenized)")

	// RebuildSearchable preserves tokenization (the dedicated retokenize
	// strategies handle that case). With word tokenization unchanged,
	// the pre- and post-migration term sets must match exactly.
	require.Equalf(t, len(preFP), len(postFP),
		"rebuild must preserve term count: pre=%d post=%d", len(preFP), len(postFP))
	for term, preIDs := range preFP {
		postIDs, ok := postFP[term]
		require.Truef(t, ok, "term %q present pre-migration but missing post-migration", term)
		require.Equalf(t, preIDs, postIDs,
			"term %q post-migration doc-id list diverges from pre-migration\n  pre  (%d): %v\n  post (%d): %v",
			term, len(preIDs), preIDs, len(postIDs), postIDs)
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_RebuildSearchable_FromEachState pins the
// #240 Symptom B invariant for the RebuildSearchable strategy: from
// any on-disk state a replica could land in after a mid-migration
// restart, the recovery code path converges on bucket content
// bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either production code (the
// Run*OnShard trio) or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 used for SearchableRetokenize.
func TestRecoveryConvergence_RebuildSearchable_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeRebuildSearchableBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "RebuildSearchable_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "RebuildSearchable_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.config.migrationPath, ftr.config.filenameMerged)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "RebuildSearchable_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": false,
			},
		},
		{
			name: "RebuildSearchable_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "RebuildSearchable_IsTidied_via_full_trio",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RebuildSearchableCase_" + uuid.NewString()[:8]
			class := newRebuildSearchableTestClass(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newRebuildSearchableTask(t, idx, className, propName)

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
			// primitive PR #11415 uses for the searchable half.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newRebuildSearchableTask(t, idx, className, propName)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Drive the async loop. For RebuildSearchable the
			// in-process OnAfterLsmInitAsync path stops at IsReindexed
			// when skipSwapOnFinish is set; for non-set cases we still
			// drain it in case any work is pending.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err, "recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Trio-style migrations require an explicit RunSwapOnShard
			// to move past IsReindexed (in production OnGroupCompleted
			// does this on re-ack for semantic migrations; for the
			// non-semantic inline path runtimeSwap fires inside the
			// async loop above and this is a no-op). Mirrors the
			// SearchableRetokenize / FilterableRetokenize recovery
			// finalization at convergence_test.go:790-795.
			rt2, err := task2.newReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

			bucket := shard2.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			require.NotNilf(t, bucket, "post-recovery searchable bucket must exist (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
				"post-recovery searchable bucket must remain StrategyInverted (case %q)", tc.name)

			got := fingerprintInvertedBucket(t, bucket)

			// Catch divergence at term granularity for actionable
			// failure output (which token has the wrong posting list).
			assert.Equalf(t, len(baseline), len(got),
				"post-recovery searchable term count diverges from baseline (case %q)", tc.name)
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
