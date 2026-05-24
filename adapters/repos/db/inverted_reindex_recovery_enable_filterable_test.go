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
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for EnableFilterable — the from-scratch
// filterable-bucket migration. Class starts at IndexFilterable=false;
// PreReindexHook creates the bucket and the analyzer overlay
// (ForceFilterable=true) drives the backfill despite the live schema
// flag still being false. Matrix shape mirrors
// FilterableRetokenize_FromEachState.

// newEnableFilterableTask wraps reindex.EnableFilterableStrategy. Selection is
// mandatory: the strategy can't discover targets via the schema-flag
// scan because that flag is still false at migration time.
func newEnableFilterableTask(t *testing.T, idx *Index, className, propName string) (*reindex.ShardReindexTaskGeneric, *testEnableFilterableStrategyWrapper) {
	t.Helper()
	wrapped := &testEnableFilterableStrategyWrapper{
		EnableFilterableStrategy: reindex.EnableFilterableStrategy{
			PropNames:  []string{propName},
			Generation: 1,
		},
	}
	selectedProps := map[string]struct{}{propName: {}}
	task := reindex.NewShardReindexTaskGeneric(
		"EnableFilterable", idx.logger, wrapped,
		reindex.ReindexTaskConfig{
			SwapBuckets:                   true,
			TidyBuckets:                   true,
			Concurrency:                   2,
			MemtableOptFactor:             4,
			BackupMemtableOptFactor:       1,
			ProcessingDuration:            10 * time.Minute,
			PauseDuration:                 1 * time.Second,
			CheckProcessingEveryNoObjects: 1000,

			SelectionEnabled: true,
			SelectedPropsByCollection: map[string]map[string]struct{}{
				className: selectedProps,
			},
			SelectedShardsByCollection: map[string]map[string]struct{}{
				className: nil, // nil = all shards
			},
		},
		&reindex.UuidKeyParser{}, reindex.UuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testEnableFilterableStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert completion. The real strategy's
// OnMigrationComplete is already a no-op (cluster-wide schema flip lives
// in OnTaskCompleted), so this wrapper is essentially an observer.
// Mirrors testFilterableRetokenizeStrategyWrapper.
type testEnableFilterableStrategyWrapper struct {
	reindex.EnableFilterableStrategy
	migrationCompleted bool
}

func (s *testEnableFilterableStrategyWrapper) OnMigrationComplete(_ context.Context, _ reindex.ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// newEnableFilterableTestClass builds a class fixture for the
// EnableFilterable matrix: one Word-tokenized text property with
// IndexFilterable=false (so the filterable bucket genuinely does not
// exist pre-migration). The default newTestClassWithProps leaves
// IndexFilterable nil (defaults to true) — not what we want here.
func newEnableFilterableTestClass(className, propName string) *models.Class {
	class := newTestClassWithProps(className, []string{propName})
	class.Properties[0].IndexFilterable = boolPtr(false)
	return class
}

// computeEnableFilterableBaseline runs a clean enable-filterable
// migration on a throw-away shard and returns its post-migration
// fingerprint. Every recovery-from-state case asserts bit-equal
// convergence against this baseline. Sibling of
// computeFilterableRetokenizeBaseline.
func computeEnableFilterableBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "EnableFilterableBaselineRef_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newEnableFilterableTask(t, idx, className, propName)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return fingerprintRoaringSetBucket(t,
		shard.Store().Bucket(helpers.BucketFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_EnableFilterable_Baseline establishes that the
// production enable-filterable migration code path drives a class' from
// no filterable bucket to a fully-populated RoaringSet bucket against
// the same scaffolding PR #11415 used for the searchable half. Sanity
// check before the matrix: if this fails, every cell in the matrix
// would fail for the same root cause.
func TestRecoveryConvergence_EnableFilterable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "EnableFilterableBaseline_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Pre-migration: the filterable bucket must NOT exist. With
	// IndexFilterable=false on the class, createPropertyValueIndex
	// (`shard_init_properties.go:471`) skips creating the bucket.
	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	preBucket := shard.Store().Bucket(filtBucketName)
	require.Nilf(t, preBucket,
		"pre-migration filterable bucket must be absent (IndexFilterable=false on class)")

	task, wrapped := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.Store().Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must be StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty (analyzer-overlay backfill)")

	// Every word-tokenized dictionary token should be present given
	// numObjects=25 and the 3-word cycling pattern (each token appears
	// as one of the 3 words for some doc).
	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := postFP[tok]
		require.Truef(t, ok,
			"post-migration filterable fingerprint missing token %q (every dictionary word should appear)", tok)
		require.NotEmptyf(t, docIDs,
			"post-migration filterable token %q has no docIDs (posting list is empty)", tok)
	}

	rt, err := task.NewReindexTracker(shard.PathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_EnableFilterable_FromEachState pins the #240
// Symptom B invariant for the enable-filterable (from-scratch
// bucket-creation) trio path: from any on-disk state a replica could
// land in after a mid-migration restart, the recovery code path
// converges on bucket content bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either production code (the
// Run*OnShard trio) or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 used for SearchableRetokenize, with the
// only differences being:
//   - bucket strategy (RoaringSet, not Inverted/MapCollection),
//   - the from-scratch pre-state (no canonical bucket yet),
//   - the wrapped strategy struct.
func TestRecoveryConvergence_EnableFilterable_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeEnableFilterableBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "EnableFilterable_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "EnableFilterable_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				rt, err := task.NewReindexTracker(shard.PathLSM())
				require.NoError(t, err)
				ftr := rt.(*reindex.FileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.Config.MigrationPath, ftr.Config.FilenameMerged)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "EnableFilterable_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
				rt, err := task.NewReindexTracker(shard.PathLSM())
				require.NoError(t, err)
				ftr := rt.(*reindex.FileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.Config.MigrationPath, ftr.Config.FilenameTidied)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": false,
			},
		},
		{
			name: "EnableFilterable_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "EnableFilterable_IsTidied_via_full_trio",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
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
			className := "EnableFilterableCase_" + uuid.NewString()[:8]
			class := newEnableFilterableTestClass(className, propName)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newEnableFilterableTask(t, idx, className, propName)

			tc.driveToState(t, ctx, shard, task)

			// Verify driveToState actually landed at the intended on-disk
			// state. Without this guard a buggy driveToState would let
			// recovery from a different state appear to "converge".
			rt, err := task.NewReindexTracker(shard.PathLSM())
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
			// idx.initShard re-runs reindex.FinalizeCompletedMigrations →
			// OnBeforeLsmInit → LSM init → OnAfterLsmInit. Same restart
			// primitive PR #11415 uses for the searchable half. We keep
			// the class fixture at IndexFilterable=false across the
			// restart: in production the cluster-wide RAFT schema flip
			// only fires from OnTaskCompleted after every shard on every
			// node has tidied, so mid-restart the schema flag is still
			// false.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newEnableFilterableTask(t, idx, className, propName)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Drive the async loop. For semantic strategies (which
			// EnableFilterable is) the in-process OnAfterLsmInitAsync
			// path stops at IsReindexed when skipSwapOnFinish is set;
			// for non-set cases we still drain it in case any work is
			// pending.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err, "recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Semantic migrations require an explicit RunSwapOnShard to
			// move past IsReindexed (in production OnGroupCompleted does
			// this on re-ack). Mirror what FilterableRetokenize does at
			// `inverted_reindex_recovery_filterable_retokenize_test.go:412`.
			rt2, err := task2.NewReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

			bucket := shard2.Store().Bucket(helpers.BucketFromPropNameLSM(propName))
			require.NotNilf(t, bucket, "post-recovery filterable bucket must exist (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyRoaringSet, bucket.Strategy(),
				"post-recovery filterable bucket must be StrategyRoaringSet (case %q)", tc.name)

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
