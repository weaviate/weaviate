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

// Deterministic restart-recovery convergence at integration level — what
// the e2e tests hit only probabilistically against weaviate/0-weaviate-
// issues#240 Symptom B (rolling-restart mid-migration leaves replicas
// diverged despite task=FINISHED). This file's baseline is the fingerprint
// every staged recovery-from-state case compares against.

// fingerprintInvertedBucket returns a deterministic (term → sorted
// []docID) snapshot. Per-doc inclusion only — frequency is not part of
// the comparison because the #11383 divergence shape is "term has no
// posting list on that node".
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
			// A pair-key shorter than 8 bytes is corruption or a
			// write-path bug; fail loudly rather than let a broken
			// bucket pass as "matches baseline".
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

// newSearchableRetokenizeTask wraps SearchableRetokenizeStrategy — the
// semantic-migration strategy driven by the Run*OnShard trio (not
// inline runtimeSwap), used by the #11383 change-tokenization migration.
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

// testSearchableRetokenizeStrategyWrapper stubs OnMigrationComplete so
// the test doesn't need the paired FilterableRetokenize run.
type testSearchableRetokenizeStrategyWrapper struct {
	SearchableRetokenizeStrategy
	migrationCompleted bool
}

func (s *testSearchableRetokenizeStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// makeConvergenceTestObjects builds a deterministic list of test
// objects. Text values cycle through a dictionary so the same word
// appears in multiple docs (replicates the BM25 "alpha appears in N
// docs" fingerprint that the #11383 acceptance test asserts on).
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

// TestRecoveryConvergence_Baseline pins the clean post-migration
// fingerprint that every recovery-from-state case compares against.
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

	// Pre-migration: bucket is MapCollection (source strategy).
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	preBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, preBucket, "pre-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyMapCollection, preBucket.Strategy(),
		"pre-migration searchable bucket must be StrategyMapCollection")

	// Drive the migration to completion using production code.
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
	require.True(t, strategy.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	// Post-migration: bucket strategy must have flipped to Inverted.
	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration searchable bucket must exist")
	require.Equal(t, lsmkv.StrategyInverted, postBucket.Strategy(),
		"post-migration searchable bucket must be StrategyInverted")

	// Tracker must show all sentinels in the terminal state.
	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())

	// Fingerprint: should be non-empty (every test object contributes
	// 3 tokens). Every token in our dictionary should appear at least
	// once because the cycling pattern ensures each token is hit.
	fp := fingerprintInvertedBucket(t, postBucket)
	require.NotEmpty(t, fp, "baseline fingerprint must have at least one term")

	// Every dictionary token should be present given numObjects=25
	// and the cycle pattern (each token starts a 3-token window for
	// some doc index, and our dictionary has 25 entries).
	expectedTokens := []string{
		"alpha", "bravo", "charlie", "delta", "echo",
		"foxtrot", "golf", "hotel", "india", "juliett",
		"kilo", "lima", "mike", "november", "oscar",
		"papa", "quebec", "romeo", "sierra", "tango",
		"uniform", "victor", "whiskey", "xray", "yankee",
	}
	for _, tok := range expectedTokens {
		docIDs, ok := fp[tok]
		require.Truef(t, ok, "baseline fingerprint missing token %q (post-migration bucket should contain every dictionary word)", tok)
		require.NotEmptyf(t, docIDs, "baseline fingerprint token %q has no docIDs (posting list is empty)", tok)
	}
}

// computeBaselineFingerprint runs a clean migration on a throw-away
// shard and returns its post-migration fingerprint. Used by every
// recovery-from-state case as ground truth.
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

// recoveryConvergenceCase describes one row in the cross-product test.
// driveToState moves the shard to the on-disk state a crashed replica
// could land in; the test then constructs a fresh task instance and
// asserts that recovery converges to the baseline fingerprint.
type recoveryConvergenceCase struct {
	name         string
	driveToState func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric)
	// expectedPostState describes which sentinels MUST be set on disk
	// after driveToState (and only those). Catches a driveToState
	// that doesn't actually halt at the intended state.
	expectedPostStateSentinels map[string]bool
}

// TestRecoveryConvergence_FromEachState pins the #240 Symptom B
// invariant: from any on-disk state a replica could land in after a
// mid-migration restart, the recovery code path converges on bucket
// content bit-equivalent to the clean baseline run.
//
// Each row stages a specific on-disk state via driveToState, then a
// fresh task instance simulates restart and drives recovery to
// completion. The final searchable bucket's (term -> sorted []docID)
// fingerprint must equal the baseline.
//
// Coverage in this commit:
//   - IsReindexed via skipSwapOnFinish (the barrier-path mode where
//     OnAfterLsmInitAsync halts after markReindexed without proceeding
//     to runtimePrepare/runtimeSwap). The default branch in
//     RunSwapOnShard must converge.
//   - IsTidied via full clean migration (no-op recovery on a fully
//     terminal state). Catches regressions where RunSwapOnShard on
//     IsTidied does the wrong thing.
//
// Coverage NOT in this commit (deferred to follow-up because they
// require either runtimePrepare/runtimeSwap to be split into
// separately-callable phases, or production-side test hooks):
//   - IsPrepended (markPrepended set, markMerged not)
//   - IsMerged (markMerged set, markSwapped not) - reachable via
//     calling task.runtimePrepare directly but markPrepended +
//     markMerged are set together by that call
//   - IsSwapped (markSwapped set, markTidied not) - inside the
//     atomic runtimeSwap call
func TestRecoveryConvergence_FromEachState(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	baseline := computeBaselineFingerprint(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "MidIteration_after_first_batch_resume_completes",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				// Force the iteration loop to break after the first
				// checkProcessingEveryNoObjects batch by setting an
				// already-elapsed processingDuration. The check at
				// inverted_reindex_task_generic.go:1420 only fires at
				// batch boundaries, so we set checkProcessingEveryNoObjects=5
				// to land a break around 5/25 objects processed.
				task.config.checkProcessingEveryNoObjects = 5
				task.config.processingDuration = time.Nanosecond
				// pauseDuration short so the rerun-pause doesn't dominate.
				task.config.pauseDuration = time.Millisecond
				// Also keep skipSwapOnFinish=false (default for non-
				// semantic) — we just want iteration to pause mid-way,
				// not stop forever.

				require.NoError(t, task.OnAfterLsmInit(ctx, shard))
				// First call: iteration starts, breaks after batch,
				// writes progress.mig, returns rerunAt=now+pauseDuration.
				rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
				require.NoError(t, err)
				require.False(t, rerunAt.IsZero(),
					"iteration must pause mid-way (rerunAt must be non-zero)")

				// At this point a partial progress.mig should exist.
				// Verify the iteration HASN'T finished — IsReindexed
				// must be false because we paused before completion.
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				require.False(t, rt.IsReindexed(),
					"iteration must NOT be complete yet — IsReindexed should be false")
				lastKey, _, err := rt.GetProgress()
				require.NoError(t, err)
				require.NotEmpty(t, lastKey.Bytes(),
					"GetProgress must return a non-empty lastProcessedKey after partial iteration")
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
				// runtimePrepare writes markPrepended + cleanup +
				// markMerged in one atomic method, so we can't reach
				// IsPrepended-without-IsMerged via production code
				// alone. Drive to IsMerged via runtimePrepare, then
				// remove the merged.mig sentinel by hand to simulate
				// a crash between markPrepended() and markMerged().
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
				// Synthetic step: remove the merged.mig file.
				ftr := rt.(*fileReindexTracker)
				mergedPath := filepath.Join(ftr.config.migrationPath, ftr.config.filenameMerged)
				require.NoError(t, os.Remove(mergedPath),
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
			name: "IsMerged_via_runtimePrepare_no_runtimeSwap",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				// Step 1: drive iteration to markReindexed via the
				// production barrier path.
				task.skipSwapOnFinish.Store(true)
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
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				props, err := task.readPropsToReindex(rt)
				require.NoError(t, err)
				logger := task.logger
				require.NoError(t, task.runtimePrepare(ctx, logger, shard, rt, props))
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
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				tidiedPath := filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)
				require.NoError(t, os.Remove(tidiedPath),
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

			// Phase 1: drive the migration to the case-specific state
			// using the production code path.
			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
			tc.driveToState(t, ctx, shard, task)

			// Verify the driveToState actually halted at the intended
			// sentinel state.
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

			// Drive the async loop to completion in case recovery is
			// only partially handled by OnBeforeLsmInit + OnAfterLsmInit.
			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoError(t, err,
					"recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Phase 3: convergence check against baseline fingerprint.
			bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
			bucket := shard2.store.Bucket(bucketName)
			require.NotNil(t, bucket, "post-recovery searchable bucket must exist (case %q)", tc.name)
			require.Equal(t, lsmkv.StrategyInverted, bucket.Strategy(),
				"post-recovery searchable bucket must be StrategyInverted (case %q)", tc.name)

			got := fingerprintInvertedBucket(t, bucket)

			// Catch divergence at term granularity for actionable
			// failure output (which token has the wrong posting list).
			assert.Equalf(t, len(baseline), len(got),
				"post-recovery term count diverges from baseline (case %q)", tc.name)
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

// TestRecoveryConvergence_SearchableRetokenize_FromEachState runs the
// same recovery cross-product as TestRecoveryConvergence_FromEachState
// but for the SearchableRetokenize strategy — the semantic migration
// that #11383's change-tokenization (word → field) actually uses.
//
// Key difference from MapToBlockmax: SearchableRetokenize is a
// SEMANTIC migration. The swap is driven via the explicit trio
// task.RunReindexOnlyOnShard → RunPrepareOnShard → RunSwapOnShard
// (production caller is reindex_provider.OnGroupCompleted), not the
// inline runtimeSwap inside OnAfterLsmInitAsync.
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
				require.NoError(t, err, "recovery OnAfterLsmInitAsync must not error (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}
			// Semantic migrations require explicit RunSwapOnShard to
			// complete (OnGroupCompleted would do this on re-ack); the
			// in-process OnAfterLsmInitAsync skips swap when
			// IsReindexed is set.
			rt2, err := task2.newReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			if !rt2.IsTidied() {
				if err := task2.RunSwapOnShard(ctx, shard2); err != nil {
					t.Logf("explicit RunSwapOnShard (case %q): %v", tc.name, err)
				}
			}

			bucket := shard2.store.Bucket(searchBucketName)
			require.NotNil(t, bucket, "post-recovery bucket must exist (case %q)", tc.name)

			got := fingerprintInvertedBucket(t, bucket)

			assert.Equalf(t, len(baseline), len(got),
				"post-recovery term count diverges from baseline (case %q)", tc.name)
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
