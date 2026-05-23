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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// simulateProcessRestartBucketCleanup releases every bucket-dir path
// under shardLSMPath from GlobalBucketRegistry — what the OS does at a
// real process restart. Needed after a runtimeSwap panic leaves orphan
// Bucket objects unreachable from any Store.
func simulateProcessRestartBucketCleanup(t *testing.T, shardLSMPath string) {
	t.Helper()
	err := filepath.WalkDir(shardLSMPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			lsmkv.GlobalBucketRegistry.Remove(path)
		}
		return nil
	})
	require.NoError(t, err, "walk shard LSM path for registry cleanup")
}

// TestRecoveryConvergence_MultiProp_FromEachState — same shape as
// TestRecoveryConvergence_FromEachState but with multiple properties
// migrating in lock-step, so per-prop loops inside
// runtimePrepare/runtimeSwap also have to converge.
func TestRecoveryConvergence_MultiProp_FromEachState(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description"}

	baseline := computeMultiPropBaseline(t, propNames, numObjects)
	for _, propName := range propNames {
		require.NotEmptyf(t, baseline[propName],
			"multi-prop baseline must have non-empty fingerprint for prop %q", propName)
	}

	cases := []recoveryConvergenceCase{
		{
			name: "MultiProp_IsReindexed_via_skipSwapOnFinish",
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
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "MultiProp_IsMerged_via_runtimePrepare",
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
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "MultiProp_IsTidied_full_migration",
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
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "MultiPropCase_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, propNames)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)
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
				assert.Equalf(t, want, got, "multi-prop sentinel %q (case %q)", name, tc.name)
			}

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task2 := newTestTask(idx.logger, strategy2)
			task2.skipSwapOnFinish.Store(false)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoError(t, err, "multi-prop shard re-init (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			for {
				rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
				require.NoError(t, err, "multi-prop recovery OnAfterLsmInitAsync (case %q)", tc.name)
				if rerunAt.IsZero() {
					break
				}
			}

			// Per-prop convergence check — every prop must converge.
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard2.store.Bucket(bucketName)
				require.NotNilf(t, bucket, "multi-prop bucket %q must exist (case %q)", propName, tc.name)
				require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
					"multi-prop bucket %q must be StrategyInverted (case %q)", propName, tc.name)

				got := fingerprintInvertedBucket(t, bucket)
				expected := baseline[propName]

				assert.Equalf(t, len(expected), len(got),
					"multi-prop term count for %q diverges (case %q)", propName, tc.name)
				for term, expectedIDs := range expected {
					gotIDs, ok := got[term]
					if !ok {
						assert.Failf(t, "multi-prop missing term",
							"term %q on prop %q present in baseline but missing post-recovery (case %q)",
							term, propName, tc.name)
						continue
					}
					assert.Equalf(t, expectedIDs, gotIDs,
						"multi-prop term %q on prop %q diverges (case %q)\n  baseline (%d): %v\n  got      (%d): %v",
						term, propName, tc.name, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
				}
			}
		})
	}
}

// computeMultiPropBaseline returns per-property fingerprints after a
// clean multi-prop migration. Used by the multi-prop convergence cases.
func computeMultiPropBaseline(t *testing.T, propNames []string, numObjects int) map[string]map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "MultiPropBaselineRef_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
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

	out := make(map[string]map[string][]uint64, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		out[propName] = fingerprintInvertedBucket(t, shard.store.Bucket(bucketName))
	}
	return out
}

// TestRecoveryConvergence_MidPropSwap_Loop interrupts runtimeSwap's
// Phase 2a per-prop loop after K of N props via the testHookPostPropSwap
// hook panicking, then asserts recovery converges every prop's bucket
// to baseline.
func TestRecoveryConvergence_MidPropSwap_Loop(t *testing.T) {
	const numObjects = 25
	propNames := []string{"title", "subtitle", "description", "keywords"}
	const haltAfter = 2 // after props 0 and 1 swapped, panic before prop 2

	baseline := computeMultiPropBaseline(t, propNames, numObjects)

	ctx := testCtx()
	className := "MidPropSwap_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, numObjects, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Drive iteration + runtimePrepare so runtimeSwap's Phase 2a is next.
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

	// Panic prefix lets the recover() handler distinguish THE expected
	// fault panic from any unrelated panic inside runtimeSwap.
	const haltPanicPrefix = "mid-loop halt: simulated crash"
	prodSwap := task.processOneSwapProp
	task.processOneSwapProp = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		bucket, err := prodSwap(ctx, store, rt, propIdx, propName)
		if err != nil {
			return nil, err
		}
		if propIdx == haltAfter-1 {
			panic(haltPanicPrefix + " after prop " + uuid.NewString()[:4])
		}
		return bucket, nil
	}

	var (
		panicked     bool
		panicValue   interface{}
		swapReturned bool
		swapErr      error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicValue = r
			}
		}()
		swapErr = task.runtimeSwap(ctx, task.logger, shard, rt, props)
		swapReturned = true
	}()

	// runtimeSwap returning without panic = hook didn't fire = harness
	// broken (would silently pass the convergence check otherwise).
	require.Falsef(t, swapReturned, "runtimeSwap returned without panicking (err=%v)", swapErr)
	require.Truef(t, panicked, "expected panic from hook; swapErr=%v", swapErr)
	panicStr, ok := panicValue.(string)
	require.Truef(t, ok && strings.HasPrefix(panicStr, haltPanicPrefix),
		"recovered panic not from hook (want prefix %q; got %T %v)",
		haltPanicPrefix, panicValue, panicValue)

	swappedCount := 0
	for _, p := range props {
		if rt.IsSwappedProp(p) {
			swappedCount++
		}
	}
	assert.GreaterOrEqualf(t, swappedCount, haltAfter, "≥%d markSwappedProp (got %d)", haltAfter, swappedCount)
	assert.Lessf(t, swappedCount, len(propNames), "halt didn't fire (got %d of %d)", swappedCount, len(propNames))

	shardName := shard.Name()
	shardLSMPath := shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))

	// K already-swapped old buckets were orphaned in runtimeSwap's
	// stack-local map when the panic unwound; their registry entries
	// weren't released. Simulate the OS-reclaim that a real process
	// restart would do.
	simulateProcessRestartBucketCleanup(t, shardLSMPath)

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err, "recovery loop")
		if rerunAt.IsZero() {
			break
		}
	}

	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard2.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "bucket %q missing post-recovery", propName)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(), "prop %q strategy", propName)

		got := fingerprintInvertedBucket(t, bucket)
		expected := baseline[propName]

		assert.Equalf(t, len(expected), len(got), "term count diverges for %q", propName)
		for term, expectedIDs := range expected {
			gotIDs, ok := got[term]
			if !ok {
				assert.Failf(t, "missing term", "term %q on prop %q missing post-recovery", term, propName)
				continue
			}
			assert.Equalf(t, expectedIDs, gotIDs,
				"mid-prop-swap term %q on prop %q diverges\n  baseline (%d): %v\n  got      (%d): %v",
				term, propName, len(expectedIDs), expectedIDs, len(gotIDs), gotIDs)
		}
	}
}

// TestRecoveryConvergence_CrossReplica_DivergentFlushTiming pins the
// per-replica determinism invariant for the inverted-index migration.
//
// Two replicas with the SAME logical object set but DIFFERENT segment
// layouts (forward + 1 segment/prop vs. reverse + 10 segments/prop)
// must produce identical inverted index buckets post-migration.
func TestRecoveryConvergence_CrossReplica_DivergentFlushTiming(t *testing.T) {
	const numObjects = 30
	propNames := []string{"title", "subtitle", "description"}

	className := "CrossReplicaSame_" + uuid.NewString()[:8]
	canonical := makeMultiPropConvergenceObjects(t, numObjects, className, propNames)

	fpA := runCrossReplicaMigration(t, propNames, className, canonical, false, 0)
	fpB := runCrossReplicaMigration(t, propNames, className, canonical, true, 3)

	for _, propName := range propNames {
		a := fpA[propName]
		b := fpB[propName]
		assert.Equalf(t, len(a), len(b), "term count diverges for %q (A:%d B:%d)", propName, len(a), len(b))
		for term, uuidsA := range a {
			uuidsB, ok := b[term]
			if !ok {
				assert.Failf(t, "missing term", "term %q on %q in A but not B", term, propName)
				continue
			}
			assert.Equalf(t, uuidsA, uuidsB,
				"term %q on %q diverges\n  A (forward, 1 seg/prop): %v\n  B (reverse, 10 seg/prop): %v",
				term, propName, uuidsA, uuidsB)
		}
		for term := range b {
			if _, ok := a[term]; !ok {
				assert.Failf(t, "extra term", "term %q on %q in B but not A", term, propName)
			}
		}
	}
}

// TestRecoveryConvergence_CrossReplica_DivergentLayoutAndCrash — the
// compound case: replicas with different layouts AND different
// crash-point sentinels must both converge to identical post-recovery
// UUID sets. Failure log == #240 reproducer.
func TestRecoveryConvergence_CrossReplica_DivergentLayoutAndCrash(t *testing.T) {
	const numObjects = 30
	propNames := []string{"title", "subtitle", "description"}

	className := "CrossRepCrash_" + uuid.NewString()[:8]
	canonical := makeMultiPropConvergenceObjects(t, numObjects, className, propNames)

	// A: forward layout, crash at IsReindexed → recovery re-runs prepare + swap
	fpA := runCrossReplicaMigrationWithCrash(t, propNames, className, canonical,
		false, 0, crashAtReindexed)
	// B: reverse + flushed layout, crash at IsSwapped → recovery runs tidy + markTidied
	fpB := runCrossReplicaMigrationWithCrash(t, propNames, className, canonical,
		true, 3, crashAtSwapped)

	// Invariant: both replicas have converged to a fully-completed
	for _, propName := range propNames {
		a := fpA[propName]
		b := fpB[propName]
		assert.Equalf(t, len(a), len(b), "term count diverges for %q", propName)
		for term, uuidsA := range a {
			uuidsB, ok := b[term]
			if !ok {
				assert.Failf(t, "missing term", "term %q on %q in A but not B", term, propName)
				continue
			}
			assert.Equalf(t, uuidsA, uuidsB,
				"term %q on %q diverges\n  A (IsReindexed crash): %v\n  B (IsSwapped crash): %v",
				term, propName, uuidsA, uuidsB)
		}
		for term := range b {
			if _, ok := a[term]; !ok {
				assert.Failf(t, "extra term", "term %q on %q in B but not A", term, propName)
			}
		}
	}
}

type crashSentinel int

const (
	crashAtReindexed crashSentinel = iota
	crashAtSwapped
)

// runCrossReplicaMigrationWithCrash drives the migration to a crash
// sentinel, restarts the shard, runs recovery, and returns per-prop
// UUID fingerprints.
func runCrossReplicaMigrationWithCrash(t *testing.T, propNames []string, className string,
	canonical []*storobj.Object, reverse bool, flushEvery int, crash crashSentinel,
) map[string]map[string][]string {
	t.Helper()
	ctx := testCtx()
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer func() {
		if shard != nil {
			shard.Shutdown(ctx)
		}
	}()

	insertOrder := make([]*storobj.Object, len(canonical))
	copy(insertOrder, canonical)
	if reverse {
		for i, j := 0, len(insertOrder)-1; i < j; i, j = i+1, j-1 {
			insertOrder[i], insertOrder[j] = insertOrder[j], insertOrder[i]
		}
	}
	for i, obj := range insertOrder {
		require.NoError(t, shard.PutObject(ctx, obj))
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard.store.Bucket(bucketName)
				if bucket != nil {
					require.NoError(t, bucket.FlushAndSwitch())
				}
			}
		}
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	switch crash {
	case crashAtReindexed:
		task.skipSwapOnFinish.Store(true)
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			if rerunAt.IsZero() {
				break
			}
		}
		rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
		require.True(t, rt.IsReindexed())
		require.False(t, rt.IsTidied())
	case crashAtSwapped:
		// Synthesize IsSwapped-but-not-IsTidied by removing tidied.mig
		// after a full migration.
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
		require.True(t, rt.IsSwapped())
		require.False(t, rt.IsTidied())
	}

	shardName := shard.Name()
	shardLSMPath := shard.pathLSM()
	require.NoError(t, shard.Shutdown(ctx))
	shard = nil // disable the defer's Shutdown
	simulateProcessRestartBucketCleanup(t, shardLSMPath)

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	task2.skipSwapOnFinish.Store(false)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "layout+crash: shard re-init must succeed")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err, "layout+crash: recovery loop")
		if rerunAt.IsZero() {
			break
		}
	}

	// Fingerprint each prop's post-recovery bucket, resolving to UUIDs.
	objectsBucket := shard2.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objectsBucket, "layout+crash: objects bucket must exist post-recovery")

	out := make(map[string]map[string][]string, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard2.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "layout+crash: bucket %q must exist post-recovery", propName)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
			"layout+crash: bucket %q must be StrategyInverted post-recovery", propName)
		docIDFingerprint := fingerprintInvertedBucket(t, bucket)
		out[propName] = resolveDocIDFingerprintToUUIDs(t, idx.logger, objectsBucket, docIDFingerprint)
	}
	return out
}

// runCrossReplicaMigration builds a fresh shard, inserts canonical
// objects with the requested ordering and flush cadence, runs the
// full MapToBlockmax migration, and returns per-prop fingerprints
// keyed on the object UUID — NOT on the local docID, because docIDs
// are local-per-replica (assigned by insertion order on each shard).
// The cross-replica invariant is about object identity, not local handles.
func runCrossReplicaMigration(t *testing.T, propNames []string, className string,
	canonical []*storobj.Object, reverse bool, flushEvery int,
) map[string]map[string][]string {
	t.Helper()
	ctx := testCtx()
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)
	_ = idx

	// Insert objects, optionally reverse, flushing every flushEvery objects
	// to create multiple segments. Touch every prop's searchable bucket so
	// the flush has data to push to disk.
	insertOrder := make([]*storobj.Object, len(canonical))
	copy(insertOrder, canonical)
	if reverse {
		for i, j := 0, len(insertOrder)-1; i < j; i, j = i+1, j-1 {
			insertOrder[i], insertOrder[j] = insertOrder[j], insertOrder[i]
		}
	}
	for i, obj := range insertOrder {
		require.NoError(t, shard.PutObject(ctx, obj))
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			for _, propName := range propNames {
				bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
				bucket := shard.store.Bucket(bucketName)
				if bucket != nil {
					require.NoError(t, bucket.FlushAndSwitch(),
						"flush bucket %q (insertIdx %d, reverse=%v, flushEvery=%d)",
						propName, i, reverse, flushEvery)
				}
			}
		}
	}

	// Run the full migration pipeline.
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err, "cross-replica migration loop (reverse=%v)", reverse)
		if rerunAt.IsZero() {
			break
		}
	}

	// Fingerprint each prop's post-migration bucket, resolving local
	// docIDs to global object UUIDs so the comparison is replica-invariant.
	objectsBucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objectsBucket, "cross-replica: objects bucket must exist")

	out := make(map[string]map[string][]string, len(propNames))
	for _, propName := range propNames {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		bucket := shard.store.Bucket(bucketName)
		require.NotNilf(t, bucket, "cross-replica: bucket %q must exist post-migration (reverse=%v)", propName, reverse)
		require.Equalf(t, lsmkv.StrategyInverted, bucket.Strategy(),
			"cross-replica: bucket %q must be StrategyInverted post-migration (reverse=%v)", propName, reverse)
		docIDFingerprint := fingerprintInvertedBucket(t, bucket)
		out[propName] = resolveDocIDFingerprintToUUIDs(t, idx.logger, objectsBucket, docIDFingerprint)
	}
	return out
}

// resolveDocIDFingerprintToUUIDs converts `term → []docID` to `term →
// []uuid` so cross-replica comparison is identity-based. Fails loudly
// on unresolved docIDs — silently dropping nils would let both
// replicas miss the same docID and still compare equal.
func resolveDocIDFingerprintToUUIDs(t *testing.T, logger logrus.FieldLogger,
	objectsBucket *lsmkv.Bucket, docIDMap map[string][]uint64,
) map[string][]string {
	t.Helper()
	out := make(map[string][]string, len(docIDMap))
	for term, docIDs := range docIDMap {
		objs, err := storobj.ObjectsByDocIDWithEmpty(objectsBucket, docIDs, additional.Properties{}, nil, logger)
		require.NoErrorf(t, err, "resolve docIDs for term %q", term)
		require.Lenf(t, objs, len(docIDs),
			"ObjectsByDocIDWithEmpty must return one slot per input docID for term %q (input=%d, got=%d)",
			term, len(docIDs), len(objs))
		uuids := make([]string, 0, len(objs))
		for i, obj := range objs {
			require.NotNilf(t, obj,
				"docID %d on term %q has no payload — posting list references a missing object (corruption or write-path bug); see resolveDocIDFingerprintToUUIDs godoc",
				docIDs[i], term)
			uuids = append(uuids, string(obj.ID()))
		}
		sort.Strings(uuids)
		out[term] = uuids
	}
	return out
}

// makeMultiPropConvergenceObjects produces objects with values for
// every prop. Each prop gets a different 3-token slice of the
// dictionary so per-prop fingerprints are non-trivial and distinct —
// catches a bug where one prop's posting list bleeds into another.
func makeMultiPropConvergenceObjects(t *testing.T, n int, className string, propNames []string) []*storobj.Object {
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
		props := map[string]interface{}{}
		for j, propName := range propNames {
			a := tokens[(i+j*7)%len(tokens)]
			b := tokens[(i+j*7+1)%len(tokens)]
			c := tokens[(i+j*7+2)%len(tokens)]
			props[propName] = a + " " + b + " " + c
		}
		out[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: props,
			},
		}
	}
	return out
}
