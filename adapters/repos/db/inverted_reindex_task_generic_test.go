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

func (s *testMigrationStrategy) OnMigrationComplete(_ context.Context, _ string) error {
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
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
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
	strategy := &testMigrationStrategy{}
	task := newTestTask(idx.logger, strategy)

	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	reindexBucketName := searchBucketName + "__blockmax_reindex"
	ingestBucketName := searchBucketName + "__blockmax_ingest"
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
	backupBucketName := searchBucketName + "__blockmax_map"
	assert.Nil(t, shard.store.Bucket(backupBucketName), "backup bucket should not exist")
	assert.Nil(t, shard.store.Bucket(reindexBucketName), "reindex bucket should not exist")
	assert.Nil(t, shard.store.Bucket(ingestBucketName), "ingest bucket should not exist")

	// Verify reindex dir is gone from disk (segments were prepended into ingest).
	assert.False(t, dirExists(filepath.Join(shard.pathLSM(), reindexBucketName)),
		"reindex dir should not exist on disk")
	// Backup dir persists on disk until next startup — runtimeSwap defers
	// filesystem cleanup (ingest→main rename, backup removal) to
	// FinalizeCompletedMigrations which runs in OnBeforeLsmInit.
	assert.True(t, dirExists(filepath.Join(shard.pathLSM(), backupBucketName)),
		"backup dir should still exist on disk (deferred finalize)")

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

	strategy := &testMigrationStrategy{}
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

	strategy2 := &testMigrationStrategy{}
	task2 := newTestTask(idx.logger, strategy2)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	assert.True(t, strategy2.migrationCompleted, "OnMigrationComplete should fire on restart")

	// All objects should be readable
	for _, obj := range objects {
		result, err := shard2.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "object %s should be readable", obj.ID())
		require.NotNil(t, result, "object %s should exist", obj.ID())
	}

	require.NoError(t, shard2.Shutdown(ctx))
}
