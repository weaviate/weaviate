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
	"github.com/weaviate/weaviate/cluster/distributedtask"
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
	task *ShardReindexTaskGeneric
}

func (r *testShardReindexer) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	return r.task.OnAfterLsmInit(ctx, shard)
}

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

// testMigrationUnitFor derives the unit identity of shardName the same way the
// shard derives its own, so records a test task writes are the shard's own and
// survive the record store's foreign-unit filter on reload.
func testMigrationUnitFor(idx *Index, shardName string) string {
	return MigrationUnitID(shardName, idx.getSchema.NodeName())
}

func newTestTask(logger logrus.FieldLogger, strategy MigrationStrategy, unitID string) *ShardReindexTaskGeneric {
	return newTestTaskWithGuard(logger, strategy, defaultIndexClosingGuard, unitID)
}

func newTestTaskWithGuard(logger logrus.FieldLogger, strategy MigrationStrategy,
	closingGuard indexClosingGuard, unitID string,
) *ShardReindexTaskGeneric {
	task := newTestTaskWithoutIdentity(logger, strategy, closingGuard)
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-reindex-task", Version: 1},
		unitID,
		&ReindexTaskPayload{MigrationType: ReindexTypeChangeAlgorithm},
	)
	return task
}

func newTestTaskWithoutIdentity(logger logrus.FieldLogger, strategy MigrationStrategy,
	closingGuard indexClosingGuard,
) *ShardReindexTaskGeneric {
	return NewShardReindexTaskGeneric("MapToBlockmax", logger, strategy,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
		closingGuard,
	)
}

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
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())

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

	rec, ok := shard.migrationRecords.Get(task.migrationRecordKey())
	require.True(t, ok, "the migration should have left a record")
	require.Equal(t, MigrationStateSwapped, rec.State())
	assert.Contains(t, rec.(MigrationRecordSwapped).Flipped(), "title",
		"the migrated property should be in the recorded flip set")
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

	assert.Nil(t, shard.store.Bucket(reindexBucketName), "reindex bucket should not exist")
	assert.Nil(t, shard.store.Bucket(ingestBucketName), "ingest bucket should not exist")

	assert.False(t, dirExists(t, filepath.Join(shard.pathLSM(), reindexBucketName)),
		"reindex dir should not exist on disk (its segments were prepended into ingest)")

	displacedDir, hasDisplaced := rec.(MigrationRecordSwapped).DisplacedDir("title")
	require.True(t, hasDisplaced, "the flip should record the directory it displaced")
	assert.False(t, dirExists(t, filepath.Join(shard.pathLSM(), displacedDir)),
		"the displaced dir should be removed by the end of the swap")

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
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())
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
	task2 := newTestTask(idx.logger, strategy2, testMigrationUnitFor(idx, shardName))
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	searchBucketName := helpers.BucketSearchableFromPropNameLSM("title")
	require.NotNil(t, shard2.store.Bucket(searchBucketName),
		"canonical main bucket should be loaded after the promoting load")

	assert.False(t, strategy2.migrationCompleted,
		"OnMigrationComplete should not fire again on a shard whose flip is already recorded")

	// All objects should be readable
	for _, obj := range objects {
		result, err := shard2.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "object %s should be readable", obj.ID())
		require.NotNil(t, result, "object %s should exist", obj.ID())
	}

	require.NoError(t, shard2.Shutdown(ctx))
}

func TestRunSwapOnShard_RecordAwareDispatch(t *testing.T) {
	tests := []struct {
		name      string
		driveTo   func(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard)
		wantState MigrationState
	}{
		{
			name: "merged: the staged data is complete and only the flip is left",
			driveTo: func(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			wantState: MigrationStateMerged,
		},
		{
			name: "swapped: the flip is decided, so only in-process work is left",
			driveTo: func(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				require.NoError(t, task.RunSwapOnShard(ctx, shard))
			},
			wantState: MigrationStateSwapped,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RecordDispatch_" + uuid.NewString()[:8]
			class := newTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, 10, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			driver := newTestTask(idx.logger, &testMigrationStrategy{
				MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1},
			}, shard.migrationUnit())
			tc.driveTo(t, ctx, driver, shard)

			rec, ok := shard.migrationRecords.Get(driver.migrationRecordKey())
			require.True(t, ok, "the drive should have left a record")
			require.Equal(t, tc.wantState, rec.State(),
				"the drive landed somewhere other than the state this row dispatches from")

			strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
			task := newTestTask(idx.logger, strategy, shard.migrationUnit())
			require.NoErrorf(t, task.RunSwapOnShard(ctx, shard),
				"RunSwapOnShard should succeed from the %s state", tc.wantState)

			require.True(t, strategy.migrationCompleted,
				"OnMigrationComplete is the tail of every dispatch branch")

			post, ok := shard.migrationRecords.Get(task.migrationRecordKey())
			require.True(t, ok)
			require.True(t, post.FlipDecided(),
				"the flip decision must be durable once the dispatch returns")
		})
	}
}

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
	task := newTestTask(idx.logger, strategy, shard.migrationUnit())

	var (
		hookMu        sync.Mutex
		hookCallTimes []time.Time
		hookCallIdxs  []int
	)
	prodSwap := task.processOneSwapProp
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, propIdx int, propName string) (*lsmkv.Bucket, error) {
		bucket, err := prodSwap(ctx, store, propIdx, propName)
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
		"the swap hook should fire exactly once per prop (%d), got %d",
		len(propNames), len(hookCallTimes))

	for i, idx := range hookCallIdxs {
		require.Equal(t, i, idx,
			"hook fired at unexpected loop index — Phase 2a loop is out of order or has a yield point that re-orders props")
	}

	// Phase 2a wall-clock invariant. See test godoc for threshold
	// rationale.
	const atomicPhaseBudget = 20 * time.Millisecond
	totalDelta := hookCallTimes[len(hookCallTimes)-1].Sub(hookCallTimes[0])
	require.Lessf(t, totalDelta, atomicPhaseBudget,
		"Phase 2a wall-clock across %d props was %v — exceeded atomic-phase budget of %v. "+
			"Likely cause: a slow op (Shutdown, Rename, RAFT, sleep) was added to the per-prop "+
			"loop inside runtimeSwap. See phase-contract godoc at the top of "+
			"inverted_reindex_task_generic.go for the design invariant.",
		len(propNames), totalDelta, atomicPhaseBudget)

	rec, ok := shard.migrationRecords.Get(task.migrationRecordKey())
	require.True(t, ok, "the flip decision should be recorded post-runtimeSwap")
	require.Equal(t, MigrationStateSwapped, rec.State())
	require.True(t, rec.FlipDecided())
	for _, p := range propNames {
		assert.Contains(t, rec.(MigrationRecordSwapped).Flipped(), p,
			"prop %q should be in the recorded flip set", p)
		displaced, hasDisplaced := rec.(MigrationRecordSwapped).DisplacedDir(p)
		assert.True(t, hasDisplaced, "prop %q should record the directory its flip displaced", p)
		assert.NotEmpty(t, displaced)
	}
	require.NoError(t, shard.Shutdown(ctx))
}
