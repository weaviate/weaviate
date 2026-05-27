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
	"sort"
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
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery-convergence matrix for FilterableRetokenize — the filterable
// half of change-tokenization. Production runs Searchable AND
// Filterable in sequence; the per-shard bucket pointer swap on
// filterable buckets is FilterableRetokenize's responsibility, so
// #240 Symptom B divergences can land here too. Source/target is
// StrategyRoaringSet; matrix shape mirrors
// SearchableRetokenize_FromEachState.

// fingerprintRoaringSetBucket returns a deterministic (term → sorted
// []docID) snapshot. RoaringSet-aware sibling of
// fingerprintInvertedBucket.
func fingerprintRoaringSetBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	c := b.CursorRoaringSet()
	defer c.Close()
	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		term := string(append([]byte(nil), k...))
		var ids []uint64
		if bm != nil {
			// sroar.Bitmap.ToArray returns docIDs in ascending order
			// already; we still sort defensively in case the API
			// contract changes (cheap on the sizes the tests use).
			ids = bm.ToArray()
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// newFilterableRetokenizeTask wraps a reindex.FilterableRetokenizeStrategy in
// the test infrastructure. Pattern mirrors newSearchableRetokenizeTask
// (`inverted_reindex_recovery_convergence_test.go:125`) but for the
// filterable half of a change-tokenization migration.
//
// `targetTokenization` is the post-migration tokenization (e.g.
// `models.PropertyTokenizationField` for word→field, the exact change
// the production e2e tests exercise).
func newFilterableRetokenizeTask(t *testing.T, logger logrus.FieldLogger, className, propName, targetTokenization string) (*reindex.ShardReindexTaskGeneric, *testFilterableRetokenizeStrategyWrapper) {
	t.Helper()
	wrapped := &testFilterableRetokenizeStrategyWrapper{
		FilterableRetokenizeStrategy: reindex.FilterableRetokenizeStrategy{
			PropName:           propName,
			TargetTokenization: targetTokenization,
			ClassName:          className,
			Generation:         1,
		},
	}
	task := reindex.NewShardReindexTaskGeneric(
		"FilterableRetokenize", logger, wrapped,
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

// testFilterableRetokenizeStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert completion. The real
// strategy's OnMigrationComplete is already a no-op (schema flip lives
// in OnTaskCompleted), so this wrapper is essentially an observer.
// Mirrors testSearchableRetokenizeStrategyWrapper for the searchable
// half.
type testFilterableRetokenizeStrategyWrapper struct {
	reindex.FilterableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testFilterableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ reindex.ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// computeFilterableRetokenizeBaseline runs a clean filterable
// retokenize migration on a throw-away shard and returns its
// post-migration fingerprint. Every recovery-from-state case asserts
// bit-equal convergence against this baseline. Sibling of
// computeSearchableRetokenizeBaseline.
func computeFilterableRetokenizeBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "FilterRetokenizeBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*db.Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableRetokenizeTask(t, f.Logger(), className, propName,
		models.PropertyTokenizationField)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return fingerprintRoaringSetBucket(t,
		shard.Store().Bucket(helpers.BucketFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_FilterableRetokenize_Baseline establishes
// that the production migration code path drives a class' filterable
// bucket from word → field tokenization on the same scaffolding PR
// #11415 used for the searchable half. Sanity check before the matrix:
// if this fails, every cell in the matrix would fail for the same root
// cause.
func TestRecoveryConvergence_FilterableRetokenize_Baseline(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	ctx := testCtx()
	className := "FilterRetokenizeBaseline_" + uuid.NewString()[:8]
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
	// Pre-migration: under word tokenization every word in our 25-word
	// dictionary appears as a term.
	preFP := fingerprintRoaringSetBucket(t, preBucket)
	require.NotEmpty(t, preFP,
		"pre-migration filterable fingerprint must be non-empty (word tokenization)")

	task, wrapped := newFilterableRetokenizeTask(t, f.Logger(), className, propName,
		models.PropertyTokenizationField)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	postBucket := shard.Store().Bucket(filtBucketName)
	require.NotNil(t, postBucket, "post-migration filterable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSet, postBucket.Strategy(),
		"post-migration filterable bucket must remain StrategyRoaringSet")
	postFP := fingerprintRoaringSetBucket(t, postBucket)
	require.NotEmpty(t, postFP,
		"post-migration filterable fingerprint must be non-empty (field tokenization)")
	// Under field tokenization the entire field value is one term per
	// document. With our 3-token cycling, every doc has a distinct
	// 3-word value so the post-migration bucket has numObjects distinct
	// terms.
	require.Lenf(t, postFP, numObjects,
		"post-migration field-tokenized bucket should have %d terms (one per object), got %d",
		numObjects, len(postFP))
	for term, ids := range postFP {
		require.Lenf(t, ids, 1,
			"post-migration field-tokenized term %q should have exactly 1 docID, got %d", term, len(ids))
	}

	rt, err := task.NewReindexTracker(shard.PathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestRecoveryConvergence_FilterableRetokenize_FromEachState pins the
// #240 Symptom B invariant for the filterable half of a
// change-tokenization migration: from any on-disk state a replica
// could land in after a mid-migration restart, the recovery code path
// converges on bucket content bit-equivalent to the clean baseline run.
//
// Five sentinel states, all reached via either production code (the
// Run*OnShard trio) or — for the two atomic-method-internal states
// (IsPrepended, IsSwapped) — synthetic removal of the later sentinel
// file. Same scheme PR #11415 used for SearchableRetokenize, with the
// only differences being the bucket strategy (RoaringSet) and the
// strategy struct.
func TestRecoveryConvergence_FilterableRetokenize_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeFilterableRetokenizeBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "FilterableRetokenize_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "FilterableRetokenize_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
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
			name: "FilterableRetokenize_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
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
			name: "FilterableRetokenize_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "FilterableRetokenize_IsTidied_via_full_trio",
			driveToState: func(t *testing.T, ctx context.Context, shard *db.Shard, task *reindex.ShardReindexTaskGeneric) {
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
			className := "FilterRetokenizeCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, _, f := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*db.Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableRetokenizeTask(t, f.Logger(), className, propName,
				models.PropertyTokenizationField)

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
			// idx.initShard re-runs FinalizeCompletedMigrations →
			// OnBeforeLsmInit → LSM init → OnAfterLsmInit. Same restart
			// primitive PR #11415 uses for the searchable half.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newFilterableRetokenizeTask(t, f.Logger(), className, propName,
				models.PropertyTokenizationField)
			f.SetShardReindexer(&testShardReindexer{task: task2})

			shd2, err := f.InitShard(ctx, shardName, class, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*db.Shard)
			defer shard2.Shutdown(ctx)
			f.StoreShard(shardName, shd2)

			// Drive the async loop. For semantic strategies (which
			// FilterableRetokenize is) the in-process OnAfterLsmInitAsync
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
			// this on re-ack). Mirror what the SearchableRetokenize
			// test does at convergence_test.go:790-795.
			rt2, err := task2.NewReindexTracker(shard2.PathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

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
