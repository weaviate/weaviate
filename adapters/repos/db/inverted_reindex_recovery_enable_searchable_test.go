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
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for EnableSearchable — the from-scratch
// searchable-bucket migration. Pre-migration the bucket doesn't exist
// (property has IndexSearchable=false); PreReindexHook creates it as
// StrategyInverted and the backfill populates via the AnalyzerOverlay-
// forced tokenization. Matrix shape mirrors
// SearchableRetokenize_FromEachState.

// newEnableSearchableTestClass builds a class with IndexSearchable=false
// (so PreReindexHook actually creates the bucket).
// newTestClassWithProps can't be reused — it leaves IndexSearchable=nil
// which defaults to true, and the bucket gets created at shard init.
func newEnableSearchableTestClass(className string, propNames []string) *models.Class {
	vFalse := false
	props := make([]*models.Property, len(propNames))
	for i, name := range propNames {
		props[i] = &models.Property{
			Name:            name,
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWord,
			IndexSearchable: &vFalse,
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
			UsingBlockMaxWAND:      false,
		},
		Properties: props,
	}
}

// newEnableSearchableTask wraps an reindex.EnableSearchableStrategy in the
// shared test infrastructure. Mirrors newFilterableRetokenizeTask
// (`inverted_reindex_recovery_filterable_retokenize_test.go:106`) and
// newSearchableRetokenizeTask (`convergence_test.go:125`); only the
// strategy struct's field set differs (EnableSearchable takes a slice
// of prop names + tokenization, not a single prop + targetTokenization).
//
// The reindex.ReindexTaskConfig mirrors production's blockmaxSearchableTaskConfig
// (`inverted_reindex_blockmax_searchable_task.go:20`) — same
// concurrency, memtable factors, processing/pause durations, and
// selectionEnabled with the prop list. Drift from production here
// would let the test pass while production fails the same convergence
// invariant.
func newEnableSearchableTask(
	t *testing.T, idx *Index, className, propName, tokenization string,
) (*reindex.ShardReindexTaskGeneric, *testEnableSearchableStrategyWrapper) {
	t.Helper()
	wrapped := &testEnableSearchableStrategyWrapper{
		EnableSearchableStrategy: reindex.EnableSearchableStrategy{
			PropNames:    []string{propName},
			Tokenization: tokenization,
			Generation:   1,
		},
	}
	selected := map[string]struct{}{propName: {}}
	task := reindex.NewShardReindexTaskGeneric(
		"EnableSearchable", idx.logger, wrapped,
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
				className: selected,
			},
			SelectedShardsByCollection: map[string]map[string]struct{}{
				className: nil,
			},
		},
		&reindex.UuidKeyParser{}, reindex.UuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testEnableSearchableStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert the callback fires. The
// production strategy's OnMigrationComplete is a documented no-op (the
// schema flip lives in reindex.ReindexProvider.OnTaskCompleted cluster-wide),
// so this wrapper is purely observational — same shape as
// testFilterableRetokenizeStrategyWrapper.
type testEnableSearchableStrategyWrapper struct {
	reindex.EnableSearchableStrategy
	migrationCompleted bool
}

func (s *testEnableSearchableStrategyWrapper) OnMigrationComplete(_ context.Context, _ reindex.ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// computeEnableSearchableBaseline runs a clean EnableSearchable
// migration on a throw-away shard and returns the post-migration
// fingerprint of the (newly created) searchable bucket. Every
// recovery-from-state case asserts bit-equal convergence against this
// baseline. Sibling of computeFilterableRetokenizeBaseline and
// computeSearchableRetokenizeBaseline.
func computeEnableSearchableBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "EnableSearchableBaselineRef_" + uuid.NewString()[:8]
	class := newEnableSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newEnableSearchableTask(t, idx, className, propName,
		models.PropertyTokenizationWord)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return fingerprintInvertedBucket(t,
		shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_EnableSearchable_Baseline establishes that
// the production EnableSearchable migration code path drives a class
// from "no searchable bucket" → "blockmax searchable bucket populated
// from objects" on the same scaffolding PR #11415 used for the
// retokenize halves. Sanity check before the matrix: if this fails,
// every cell in the matrix would fail for the same root cause.
func TestRecoveryConvergence_EnableSearchable_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "EnableSearchableBaseline_" + uuid.NewString()[:8]
	class := newEnableSearchableTestClass(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	// Precondition: searchable bucket does NOT exist (IndexSearchable=false
	// on the prop → shard init skipped the bucket creation in
	// shard_init_properties.go:493). EnableSearchable.PreReindexHook is
	// what creates it.
	require.Nil(t, shard.Store().Bucket(searchBucketName),
		"pre-migration searchable bucket must NOT exist (IndexSearchable=false)")

	task, wrapped := newEnableSearchableTask(t, idx, className, propName,
		models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.Store().Bucket(searchBucketName)
	require.NotNil(t, postBucket,
		"post-migration searchable bucket must exist (created by PreReindexHook)")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must be StrategyInverted (blockmax)")

	postFP := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration searchable fingerprint must be non-empty (word tokenization)")

	// Under word tokenization our 25-token cycling dictionary produces
	// 25 distinct terms (same shape as the MapToBlockmax baseline's
	// expectedTokens block at convergence_test.go:264-274).
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
			"baseline fingerprint missing token %q (post-migration bucket should contain every dictionary word)", tok)
		require.NotEmptyf(t, docIDs,
			"baseline fingerprint token %q has no docIDs (posting list is empty)", tok)
	}

	rt, err := task.NewReindexTracker(shard.PathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_EnableSearchable_FromEachState pins the
// #240 Symptom B invariant for the enable-searchable migration: from
// any on-disk state a replica could land in after a mid-migration
// restart, the recovery code path converges on bucket content
// bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either production code (the
// Run*OnShard trio) or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 used for SearchableRetokenize and the
// FilterableRetokenize follow-up, with the difference being that
// pre-migration the searchable bucket does NOT exist and is created
// by PreReindexHook.
func TestRecoveryConvergence_EnableSearchable_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeEnableSearchableBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "EnableSearchable_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "EnableSearchable_IsPrepended_synthetic_merged_removed",
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
			name: "EnableSearchable_IsSwapped_synthetic_tidied_removed",
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
			name: "EnableSearchable_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "EnableSearchable_IsTidied_via_full_trio",
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
			className := "EnableSearchableCase_" + uuid.NewString()[:8]
			class := newEnableSearchableTestClass(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newEnableSearchableTask(t, idx, className, propName,
				models.PropertyTokenizationWord)

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
			// primitive PR #11415 uses for the searchable retokenize and
			// the FilterableRetokenize follow-up.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newEnableSearchableTask(t, idx, className, propName,
				models.PropertyTokenizationWord)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Drive the async loop. For semantic strategies (which
			// EnableSearchable is, see reindex_provider.go:1850
			// reindex.IsSemanticMigration) the in-process OnAfterLsmInitAsync
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
			// this on re-ack). Mirror what the SearchableRetokenize and
			// FilterableRetokenize tests do at the equivalent point.
			rt2, err := task2.NewReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

			bucket := shard2.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			require.NotNilf(t, bucket,
				"post-recovery searchable bucket must exist (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
				"post-recovery searchable bucket must be StrategyInverted blockmax (case %q)", tc.name)

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
