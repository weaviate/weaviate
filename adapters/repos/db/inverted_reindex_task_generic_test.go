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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// testMigrationStrategy wraps MapToBlockmaxStrategy but replaces
// OnMigrationComplete with a no-op (we don't have a real schema manager
// in tests).
type testMigrationStrategy struct {
	MapToBlockmaxStrategy
	migrationCompleted bool
}

func (s *testMigrationStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

// testShardReindexer wraps a single task into a ShardReindexerV3 for use
// during shard initialization. It calls task methods synchronously.
type testShardReindexer struct {
	task ShardReindexTaskV3
}

func (r *testShardReindexer) RunBeforeLsmInit(ctx context.Context, shard *Shard) error {
	return r.task.OnBeforeLsmInit(ctx, shard)
}

func (r *testShardReindexer) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	return r.task.OnAfterLsmInit(ctx, shard)
}

func (r *testShardReindexer) RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error {
	_, _, err := r.task.OnAfterLsmInitAsync(ctx, shard)
	return err
}

func (r *testShardReindexer) Stop(_ *Shard, _ error) {}

func createTestObjectWithText(className, text string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				"title": text,
			},
		},
	}
}

func newTestClass(className string) *models.Class {
	return newTestClassWithProps(className, []string{"title"})
}

// newTestClassWithProps builds a test class with N searchable text
// properties. Used by regression tests that exercise the per-prop
// loop inside runtimeSwap (the atomic-phase contract per
// https://github.com/weaviate/0-weaviate-issues/issues/216 — see file-level godoc in
// inverted_reindex_task_generic.go).
func newTestClassWithProps(className string, propNames []string) *models.Class {
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
			UsingBlockMaxWAND:      false, // Force MapCollection strategy
		},
		Properties: props,
	}
}

func newTestTask(logger logrus.FieldLogger, strategy MigrationStrategy) *ShardReindexTaskGeneric {
	return NewShardReindexTaskGeneric("MapToBlockmax", logger, strategy,
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
}

// TestMapToBlockmaxMigration_RuntimeSwap tests the runtime swap path where
// merge, swap, and tidy all happen inline after the reindex iteration
// completes — no shard restart needed.
func TestMapToBlockmaxMigration_RuntimeSwap(t *testing.T) {
	ctx := testCtx()
	className := "TestMigrationRuntime"
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	searchBucketName := helpers.BucketSearchableFromPropNameLSM("title")
	require.Equal(t, lsmkv.StrategyMapCollection,
		shard.store.Bucket(searchBucketName).Strategy())

	// Insert initial objects
	initialObjects := make([]*storobj.Object, 10)
	for i := range initialObjects {
		initialObjects[i] = createTestObjectWithText(className, "hello world document number "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, initialObjects[i]))
	}

	// Start migration (reloadShards=false → runtime swap)
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	// Per-migration generation suffix (`_<N>`) is appended to every
	// sidecar bucket name. The test strategy uses gen=1.
	reindexBucketName := searchBucketName + "__blockmax_reindex_1"
	ingestBucketName := searchBucketName + "__blockmax_ingest_1"
	require.NotNil(t, shard.store.Bucket(reindexBucketName))
	require.NotNil(t, shard.store.Bucket(ingestBucketName))

	// Insert double-write objects BEFORE running the async reindex.
	// These go to both the main bucket (MapCollection) and the ingest bucket
	// (Inverted) via double-write callbacks.
	doubleWriteObjects := make([]*storobj.Object, 5)
	for i := range doubleWriteObjects {
		doubleWriteObjects[i] = createTestObjectWithText(className, "during migration "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, doubleWriteObjects[i]))
	}

	// Run async reindex — this will also perform the runtime swap when done.
	for {
		rerunAt, reloadShard, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		require.False(t, reloadShard, "runtime swap should not request reload")
		if rerunAt.IsZero() {
			break
		}
	}

	// Verify migration completed — no restart needed!
	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	assert.True(t, rt.IsPrepended(), "tracker should show prepended")
	assert.True(t, rt.IsMerged(), "tracker should show merged")
	assert.True(t, rt.IsSwapped(), "tracker should show swapped")
	assert.True(t, rt.IsTidied(), "tracker should show tidied")
	assert.True(t, strategy.migrationCompleted, "OnMigrationComplete should have been called")

	// Searchable bucket should now be StrategyInverted
	assert.Equal(t, lsmkv.StrategyInverted,
		shard.store.Bucket(searchBucketName).Strategy(),
		"searchable bucket should be StrategyInverted after migration")

	// All objects should be readable from the same shard (no restart!)
	for _, obj := range initialObjects {
		result, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "initial object %s should be readable", obj.ID())
		require.NotNil(t, result, "initial object %s should exist", obj.ID())
	}
	for _, obj := range doubleWriteObjects {
		result, err := shard.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "double-write object %s should be readable", obj.ID())
		require.NotNil(t, result, "double-write object %s should exist", obj.ID())
	}

	// Temporary buckets should be cleaned up
	backupBucketName := searchBucketName + "__blockmax_map_1"
	assert.Nil(t, shard.store.Bucket(backupBucketName), "backup bucket should not exist")
	assert.Nil(t, shard.store.Bucket(reindexBucketName), "reindex bucket should not exist")
	assert.Nil(t, shard.store.Bucket(ingestBucketName), "ingest bucket should not exist")

	// Verify reindex dir is gone from disk (segments were prepended into ingest).
	assert.False(t, dirExists(filepath.Join(shard.pathLSM(), reindexBucketName)),
		"reindex dir should not exist on disk")
	// Backup dir is removed at end of runtimeSwap by the per-migration
	// trim (`trimOlderGenerationsLocked`), which deletes the current
	// gen's backup along with any older generations. This is part of
	// the bounded-depth invariant — at most one tidied gen + one
	// in-flight gen on disk at any time. See `docs/runtime-reindex.md`.
	assert.False(t, dirExists(filepath.Join(shard.pathLSM(), backupBucketName)),
		"backup dir should be removed by end-of-swap trim")

	// New writes should still work after migration
	postMigrationObj := createTestObjectWithText(className, "post migration "+uuid.NewString())
	require.NoError(t, shard.PutObject(ctx, postMigrationObj))
	result, err := shard.ObjectByID(ctx, postMigrationObj.ID(), nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, result, "post-migration object should exist")

	require.NoError(t, shard.Shutdown(ctx))
}

// TestMapToBlockmaxMigration_RuntimeSwap_ThenRestart tests that a shard
// correctly loads after a runtime swap completed and the process restarts.
// OnMigrationComplete should fire again from the IsTidied check.
func TestMapToBlockmaxMigration_RuntimeSwap_ThenRestart(t *testing.T) {
	ctx := testCtx()
	className := "TestMigrationRuntimeRestart"
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	// Insert objects and run full runtime swap
	objects := make([]*storobj.Object, 10)
	for i := range objects {
		objects[i] = createTestObjectWithText(className, "hello world "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, objects[i]))
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

	// Restart — shard should load cleanly, OnMigrationComplete called again
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	strategy2 := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task2 := newTestTask(idx.logger, strategy2)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	// After per-migration-generation refactor, FinalizeCompletedMigrations
	// runs at shard init, finalizes the tidied gen (renames ingest dir →
	// canonical, removes backup, removes the tracker dir). With the
	// tracker dir gone, the new task's OnAfterLsmInit sees IsStarted=
	// false and returns early — so OnMigrationComplete does NOT fire on
	// restart. This is correct: the migration was tidied in the original
	// swap and its OnMigrationComplete fired then; firing it again
	// on restart is unnecessary (and now impossible). Asserting that
	// canonical bucket exists post-restart is the load-bearing check.
	searchBucketName := helpers.BucketSearchableFromPropNameLSM("title")
	require.NotNil(t, shard2.store.Bucket(searchBucketName),
		"canonical main bucket should be loaded after restart-finalize")
	_ = strategy2.migrationCompleted // intentionally unused: OnMigrationComplete no longer fires on restart

	// All objects should be readable
	for _, obj := range objects {
		result, err := shard2.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "object %s should be readable", obj.ID())
		require.NotNil(t, result, "object %s should exist", obj.ID())
	}

	require.NoError(t, shard2.Shutdown(ctx))
}

// TestRunSwapOnShard_SentinelAwareDispatch pins the recovery branches in
// [ShardReindexTaskGeneric.RunSwapOnShard] that took over after
// https://github.com/weaviate/0-weaviate-issues/issues/214 Phase 7c.
//
// Before the dispatch fix, RunSwapOnShard unconditionally called
// runtimeSwap which required the reindex bucket to be in the in-memory
// store. After a rolling restart that landed inside the FINALIZING
// window past markPrepended(), the reindex bucket dirs were removed
// from disk in [runtimeSwap]'s Step 2.5 (removeReindexBucketsDirs) and
// the recovery rehydrate path would fail with "reindex bucket %q not
// found", emit a failure ack, and flip the cluster-wide task to FAILED
// while the OTHER replicas had already swapped their buckets — the
// schema↔bucket inversion bug.
//
// Each subtest sets up a tracker at a specific on-disk sentinel state
// (no in-process migration), then calls RunSwapOnShard and asserts the
// strategy's OnMigrationComplete fired (proves the dispatch reached
// finalizeMigrationAfterRecovery — the right tail of every recovery
// branch).
//
// Note: this test only exercises the SENTINEL FILE state, not the full
// on-disk bucket layout — the recovery functions tolerate missing
// bucket dirs when the sentinel says they were already moved. For the
// IsMerged/IsPrepended branches we still write a dummy ingest dir so
// recoverRuntimeSwapBuckets's "main exists / backup exists" guard
// can find something to rename. The acceptance tests in
// test/acceptance/reindex_multinode/issue_214_finalize_crash_test.go
// cover the end-to-end multi-node convergence assertion.
func TestRunSwapOnShard_SentinelAwareDispatch(t *testing.T) {
	tests := []struct {
		name      string
		setupRT   func(rt reindexTracker)
		setupDisk func(t *testing.T, shard *Shard, task *ShardReindexTaskGeneric)
		wantPath  string
	}{
		{
			name: "IsTidied/idempotent",
			setupRT: func(rt reindexTracker) {
				require.NoError(t, rt.markStarted(time.Now()))
				require.NoError(t, rt.markReindexed())
				require.NoError(t, rt.markPrepended())
				require.NoError(t, rt.markMerged())
				require.NoError(t, rt.markSwapped())
				require.NoError(t, rt.markTidied())
			},
			wantPath: "tidied",
		},
		{
			name: "IsSwapped/!IsTidied/tidy_only",
			setupRT: func(rt reindexTracker) {
				require.NoError(t, rt.markStarted(time.Now()))
				require.NoError(t, rt.markReindexed())
				require.NoError(t, rt.markPrepended())
				require.NoError(t, rt.markMerged())
				require.NoError(t, rt.markSwapped())
			},
			wantPath: "swapped",
		},
		{
			// Restart mid-FINALIZING: sentinel already set, disk renames
			// already converged → recovery's only remaining work is the
			// re-mark, which must tolerate the existing O_EXCL sentinel.
			// Regression for the CI failure in run 28709084881.
			name: "IsMerged/prop_sentinel_preset/recovery_mark_idempotent",
			setupRT: func(rt reindexTracker) {
				require.NoError(t, rt.markStarted(time.Now()))
				require.NoError(t, rt.markReindexed())
				require.NoError(t, rt.markPrepended())
				require.NoError(t, rt.markMerged())
				require.NoError(t, rt.markSwappedProp("title"))
			},
			setupDisk: func(t *testing.T, shard *Shard, task *ShardReindexTaskGeneric) {
				backupDir := filepath.Join(shard.pathLSM(), task.backupBucketName("title"))
				require.NoError(t, os.MkdirAll(backupDir, 0o777))
			},
			wantPath: "merged-recovery",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "TestSentinelDispatch_" + tc.name
			class := newTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy)

			// Set up the on-disk tracker at the target sentinel state.
			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			require.NoError(t, rt.saveProps([]string{"title"}))
			tc.setupRT(rt)
			if tc.setupDisk != nil {
				tc.setupDisk(t, shard, task)
			}

			// Call RunSwapOnShard — the new dispatch must route through
			// the recovery branch matching the sentinel state and
			// invoke OnMigrationComplete.
			err = task.RunSwapOnShard(ctx, shard)
			require.NoError(t, err,
				"RunSwapOnShard should succeed via the %s recovery branch", tc.wantPath)

			require.True(t, strategy.migrationCompleted,
				"OnMigrationComplete should have fired (finalizeMigrationAfterRecovery tail of every recovery branch)")
		})
	}
}

// TestRuntimeSwap_Phase2a_AtomicTightLoop pins the architectural
// contract from https://github.com/weaviate/0-weaviate-issues/issues/216 (per QA-Claude design
// consideration in PR https://github.com/weaviate/weaviate/pull/11322 comment 4470016252): between consecutive
// per-prop SwapBucketPointer calls inside runtimeSwap's Phase 2a, only
// the cheap sentinel fsync (markSwappedProp) is allowed — no Shutdown,
// no Rename, no RAFT, no compaction wait. The total Phase 2a wall-clock
// for an N-prop migration MUST stay inside the microseconds-to-low-ms
// budget at any scale; the per-shard tokenization overlay's
// "mixed-state" subwindow (some props swapped, others not — queries to
// not-yet-swapped props during the window would tokenize input with the
// new value against an old-tokenized bucket and return wrong results)
// is exactly this wall-clock.
//
// Regression scenarios this guards against:
//
//   - Bucket.Shutdown back inside the per-prop loop (pre-refactor
//     behavior; ~100s of ms at production scale because Shutdown waits
//     for in-flight compaction to drain).
//   - RAFT call inside the loop (cluster apply latency, ~100s of ms).
//   - os.Rename inside the loop (filesystem dependent, ms-to-tens-of-ms
//     per call).
//   - Any artificial slowdown (e.g. a sleep/Gosched accidentally added
//     during refactor).
//
// Threshold of 100ms across 4 props is generous enough that single-
// digit-ms per-prop markSwappedProp fsync on healthy CI disks comfortably
// fits, but tight enough that any of the regression scenarios above
// blow it. If a real reason emerges to relax the bound (e.g. CI disk
// performance regression), surface that as a separate signal — DO NOT
// just raise the threshold, that would silently swallow the architectural
// regression the test is meant to catch.
//
// Uses the test-only ShardReindexTaskGeneric.testHookPostPropSwap
// observation point so this test is deterministic (no race with a
// concurrent observer) and does not depend on probing the bucket map
// from another goroutine.
func TestRuntimeSwap_Phase2a_AtomicTightLoop(t *testing.T) {
	ctx := testCtx()
	className := "TestPhase2aAtomic"
	propNames := []string{"title", "description", "summary", "keywords"}
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	// Sanity: every prop's searchable bucket should start at
	// StrategyMapCollection so the MapToBlockmax migration picks them
	// all up.
	for _, p := range propNames {
		require.Equal(t, lsmkv.StrategyMapCollection,
			shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(p)).Strategy(),
			"prop %q must start at MapCollection for the migration to target it", p)
	}

	// Insert some objects so the reindex pipeline has data to iterate.
	objects := make([]*storobj.Object, 5)
	for i := range objects {
		objects[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					"title":       "doc " + strconv.Itoa(i),
					"description": "long description " + strconv.Itoa(i),
					"summary":     "short summary " + strconv.Itoa(i),
					"keywords":    "kw " + strconv.Itoa(i),
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, objects[i]))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Wire the Phase 2a observation hook. Production leaves this nil;
	// the test reads back the per-prop timestamps to assert the
	// atomic-phase budget.
	var (
		hookMu        sync.Mutex
		hookCallTimes []time.Time
		hookCallIdxs  []int
	)
	prodSwap := task.processOneSwapProp
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		bucket, err := prodSwap(ctx, store, rt, propIdx, propName)
		if err != nil {
			return nil, err
		}
		hookMu.Lock()
		hookCallTimes = append(hookCallTimes, time.Now())
		hookCallIdxs = append(hookCallIdxs, propIdx)
		hookMu.Unlock()
		return bucket, nil
	}

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	// Run the iteration → swap path inline. The hook will fire once per
	// prop inside runtimeSwap's Phase 2a tight loop.
	for {
		rerunAt, reloadShard, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		require.False(t, reloadShard, "runtime swap should not request reload")
		if rerunAt.IsZero() {
			break
		}
	}

	hookMu.Lock()
	defer hookMu.Unlock()

	require.Len(t, hookCallTimes, len(propNames),
		"testHookPostPropSwap should fire exactly once per prop (%d), got %d",
		len(propNames), len(hookCallTimes))

	// The per-prop loop iterates props in their (stored) order; the hook
	// receives the loop's 0-based index. The order is determined by
	// getPropsToReindex which sorts deterministically — so the hook
	// indices should be a strict increasing sequence starting at 0.
	for i, idx := range hookCallIdxs {
		require.Equal(t, i, idx,
			"hook fired at unexpected loop index — Phase 2a loop is out of order or has a yield point that re-orders props")
	}

	// Phase 2a wall-clock invariant. See test godoc for threshold
	// rationale.
	const atomicPhaseBudget = 100 * time.Millisecond
	totalDelta := hookCallTimes[len(hookCallTimes)-1].Sub(hookCallTimes[0])
	require.Lessf(t, totalDelta, atomicPhaseBudget,
		"Phase 2a wall-clock across %d props was %v — exceeded atomic-phase budget of %v. "+
			"Likely cause: a slow op (Shutdown, Rename, RAFT, sleep) was added to the per-prop "+
			"loop inside runtimeSwap. See phase-contract godoc at the top of "+
			"inverted_reindex_task_generic.go for the design invariant.",
		len(propNames), totalDelta, atomicPhaseBudget)

	// Sanity: assert markers landed as the contract specifies post-Phase
	// 2c (since this is the inline path, runtimeSwap also runs 2b + 2c
	// for the dead-bucket tidy and OnMigrationComplete + trim).
	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	for _, p := range propNames {
		assert.True(t, rt.IsSwappedProp(p),
			"prop %q should be IsSwappedProp post-runtimeSwap", p)
	}
	assert.True(t, rt.IsSwapped(), "aggregate swapped sentinel should be set post-runtimeSwap")
	assert.True(t, rt.IsTidied(), "aggregate tidied sentinel should be set post-runtimeSwap (inline path)")

	require.NoError(t, shard.Shutdown(ctx))
}
