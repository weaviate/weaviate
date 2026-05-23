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
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Exhaustive single-process recovery-convergence tests for the v2
// inverted-index reindex pipeline. See weaviate/0-weaviate-issues#240.

// fingerprintInvertedBucket returns a (term → sorted []docID) snapshot
// of an inverted/MapCollection searchable bucket. Frequency is dropped;
// per-doc inclusion is enough to catch posting-list divergence.
func fingerprintInvertedBucket(t *testing.T, b *lsmkv.Bucket) map[string][]uint64 {
	t.Helper()
	out := map[string][]uint64{}
	if b == nil {
		return out
	}
	c, err := b.MapCursor()
	require.NoError(t, err)
	defer c.Close()
	for k, pairs := c.First(context.Background()); k != nil; k, pairs = c.Next(context.Background()) {
		term := string(append([]byte(nil), k...))
		ids := make([]uint64, 0, len(pairs))
		for _, p := range pairs {
			require.Lenf(t, p.Key, 8,
				"unexpected pair key length on term %q: want 8 bytes (big-endian docID), got %d",
				term, len(p.Key))
			id := uint64(p.Key[0])<<56 |
				uint64(p.Key[1])<<48 |
				uint64(p.Key[2])<<40 |
				uint64(p.Key[3])<<32 |
				uint64(p.Key[4])<<24 |
				uint64(p.Key[5])<<16 |
				uint64(p.Key[6])<<8 |
				uint64(p.Key[7])
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[term] = ids
	}
	return out
}

// newSearchableRetokenizeTask wraps the production
// SearchableRetokenizeStrategy in test scaffolding. Semantic
// migration: swap is driven via RunReindexOnly/RunPrepare/RunSwap on
// each shard, not the inline runtimeSwap used by MapToBlockmax.
func newSearchableRetokenizeTask(t *testing.T, idx *Index, className, propName, targetTokenization, bucketStrategy string) (*ShardReindexTaskGeneric, *testSearchableRetokenizeStrategyWrapper) {
	t.Helper()
	wrapped := &testSearchableRetokenizeStrategyWrapper{
		SearchableRetokenizeStrategy: SearchableRetokenizeStrategy{
			propName:           propName,
			targetTokenization: targetTokenization,
			className:          className,
			bucketStrategy:     bucketStrategy,
			generation:         1,
		},
	}
	task := NewShardReindexTaskGeneric(
		"SearchableRetokenize", idx.logger, wrapped,
		reindexTaskConfig{
			swapBuckets:                   true,
			tidyBuckets:                   true,
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testSearchableRetokenizeStrategyWrapper stubs OnMigrationComplete
// (the real strategy is a no-op for searchable — the schema flip is
// done by FilterableRetokenize when it runs second; we don't run that
// here). Same pattern as testMigrationStrategy for MapToBlockmax.
type testSearchableRetokenizeStrategyWrapper struct {
	SearchableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testSearchableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// makeConvergenceTestObjects builds n objects whose `title` cycles
// through a 25-token dictionary so each token appears in multiple docs.
func makeConvergenceTestObjects(t *testing.T, n int, className string) []*storobj.Object {
	t.Helper()
	tokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		text := tokens[i%len(tokens)] + " " + tokens[(i+1)%len(tokens)] + " " + tokens[(i+2)%len(tokens)]
		out[i] = createTestObjectWithText(className, text)
	}
	return out
}

// TestRecoveryConvergence_Baseline drives a clean MapToBlockmax
// migration to completion and fingerprints the post-state. The
// recovery-from-each-state cases below compare against this baseline.
func TestRecoveryConvergence_Baseline(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25

	className := "ConvergenceBaseline_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := makeConvergenceTestObjects(t, numObjects, className)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyMapCollection, preBucket.Strategy())

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, strategy.migrationCompleted)

	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy())

	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())

	fp := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, fp, "baseline fingerprint must have at least one term")

	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := fp[tok]
		require.Truef(t, ok, "baseline fingerprint missing token %q", tok)
		require.NotEmptyf(t, docIDs, "baseline fingerprint token %q has empty posting list", tok)
	}
}

// computeBaselineFingerprint runs a clean migration on a throw-away
// shard and returns its post-migration fingerprint. Recovery-from-state
// cases compare against this as ground truth.
func computeBaselineFingerprint(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "ConvergenceBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, strategy.migrationCompleted)

	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	return fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
}

// recoveryConvergenceCase: drive the shard to a specific on-disk state,
// then restart with a fresh task and assert post-recovery fingerprint
// matches the baseline.
type recoveryConvergenceCase struct {
	name                       string
	driveToState               func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric)
	expectedPostStateSentinels map[string]bool // sanity-check the drive-to actually halted there
}

// TestRecoveryConvergence_FromEachState pins recovery convergence
// from every on-disk state a crashed replica can land in (#240 Symptom B).
func TestRecoveryConvergence_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeBaselineFingerprint(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "MidIteration_after_first_batch_resume_completes",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				// Break the iteration loop after the first batch by
				// setting processingDuration to a value the
				// per-batch check immediately considers elapsed.
				task.config.checkProcessingEveryNoObjects = 5
				task.config.processingDuration = time.Nanosecond
				task.config.pauseDuration = time.Millisecond

				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				require.False(t, rerunAt.IsZero(), "iteration must pause mid-way")

				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				require.False(t, rt.IsReindexed())
				lastKey, _, err := rt.GetProgress()
				require.NoError(t, err)
				require.NotEmpty(t, lastKey.Bytes(), "GetProgress must return a partial lastProcessedKey")
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": false,
				"prepended": false,
				"merged":    false,
				"swapped":   false,
				"tidied":    false,
			},
		},
		{
			name: "IsReindexed_via_skipSwapOnFinish",
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
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true,
				"prepended": false,
				"merged":    false,
				"swapped":   false,
				"tidied":    false,
			},
		},
		{
			name: "IsPrepended_synthetic_merged_sentinel_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				// runtimePrepare writes markPrepended + markMerged in one
				// atomic method, so we synthesize the IsPrepended-only
				// state by removing merged.mig post-hoc.
				task.skipSwapOnFinish.Store(true)
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				props, err := task.readPropsToReindex(rt)
				require.NoError(t, err)
				require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))
				ftr := rt.(*fileReindexTracker)
				mergedPath := filepath.Join(ftr.config.migrationPath, ftr.config.filenameMerged)
				require.NoError(t, os.Remove(mergedPath))
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
			name: "IsMerged_via_runtimePrepare_no_runtimeSwap",
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
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				props, err := task.readPropsToReindex(rt)
				require.NoError(t, err)
				require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))
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
			name: "IsSwapped_synthetic_tidied_sentinel_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				// Synthesize IsSwapped-but-not-IsTidied by removing
				// tidied.mig after the full migration.
				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				for {
					rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
					require.NoError(t, err)
					if rerunAt.IsZero() {
						break
					}
				}
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)))
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
			name: "IsTidied_full_migration",
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
			className := "ConvergenceCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
			tc.driveToState(t, ctx, shard, task)

			rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
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

			strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task2 := newTestTask(idx.logger, strategy2)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err, "recovery loop (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
			bucket := shard2.store.Bucket(bucketName)
			require.NotNilf(t, bucket, "post-recovery searchable bucket missing (case %q)", tc.name)
			require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
				"post-recovery bucket strategy (case %q)", tc.name)

			got := fingerprintInvertedBucket(t, bucket)

			assert.Equalf(t, len(baseline), len(got), "term count (case %q)", tc.name)
			for term, expectedIDs := range baseline {
				gotIDs, ok := got[term]
				if !ok {
					assert.Failf(t, "missing term", "term %q missing post-recovery (case %q)", term, tc.name)
					continue
				}
				assert.Equalf(t, expectedIDs, gotIDs,
					"term %q diverges (case %q)\n  baseline (%d): %v\n  got      (%d): %v",
					term, tc.name, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
			}
		})
	}
}

// TestRecoveryConvergence_SearchableRetokenize_FromEachState — same
// cross-product as TestRecoveryConvergence_FromEachState but for the
// SearchableRetokenize semantic migration (#11383's change-tokenization
// path). Swap is driven via the RunReindexOnly/RunPrepare/RunSwap trio,
// not inline runtimeSwap.
func TestRecoveryConvergence_SearchableRetokenize_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeSearchableRetokenizeBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "Retokenize_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "Retokenize_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(filepath.Join(ftr.config.migrationPath, ftr.config.filenameMerged)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "Retokenize_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": false,
			},
		},
		{
			name: "Retokenize_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "Retokenize_IsTidied_via_full_trio",
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
			className := "RetokenizeCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{propName})

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
			preStrategy := shard.store.Bucket(searchBucketName).Strategy()

			task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)

			tc.driveToState(t, ctx, shard, task)

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

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, _ := newSearchableRetokenizeTask(t, idx, className, propName,
				models.PropertyTokenizationField, preStrategy)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoErrorf(t, err, "recovery loop (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}
			// Semantic migrations need explicit RunSwapOnShard to finish;
			// in-process OnAfterLsmInitAsync skips swap when IsReindexed.
			rt2, err := task2.newReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

			bucket := shard2.store.Bucket(searchBucketName)
			require.NotNilf(t, bucket, "post-recovery bucket missing (case %q)", tc.name)

			got := fingerprintInvertedBucket(t, bucket)

			assert.Equalf(t, len(baseline), len(got), "term count (case %q)", tc.name)
			for term, expectedIDs := range baseline {
				gotIDs, ok := got[term]
				if !ok {
					assert.Failf(t, "missing term", "term %q missing post-recovery (case %q)", term, tc.name)
					continue
				}
				assert.Equalf(t, expectedIDs, gotIDs,
					"term %q diverges (case %q)\n  baseline (%d): %v\n  got      (%d): %v",
					term, tc.name, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
			}
		})
	}
}

func computeSearchableRetokenizeBaseline(t *testing.T, propName string, numObjects int) map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "RetokenizeBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preStrategy := shard.store.Bucket(searchBucketName).Strategy()

	task, _ := newSearchableRetokenizeTask(t, idx, className, propName,
		models.PropertyTokenizationField, preStrategy)

	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return fingerprintInvertedBucket(t, shard.store.Bucket(searchBucketName))
}
