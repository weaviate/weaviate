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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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

// TestMapToBlockmaxMigration_FullJourney tests the complete migration lifecycle:
//
//	Boot 1: discover MapCollection buckets → create reindex+ingest buckets →
//	  reindex existing objects → accept double-writes for new objects
//	Boot 2: merge reindex into ingest → swap ingest with source →
//	  tidy backup → finalize migration
//
// After the full journey, the searchable bucket should use StrategyInverted
// and all objects (both pre-migration and double-written) should be readable.
func TestMapToBlockmaxMigration_FullJourney(t *testing.T) {
	ctx := testCtx()
	className := "TestMigration"

	class := &models.Class{
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

	// -------------------------------------------------------------------------
	// Phase 0: Create shard with MapCollection searchable buckets
	// -------------------------------------------------------------------------

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	searchBucketName := helpers.BucketSearchableFromPropNameLSM("title")
	require.Equal(t, lsmkv.StrategyMapCollection,
		shard.store.Bucket(searchBucketName).Strategy(),
		"searchable bucket should start as MapCollection")

	// -------------------------------------------------------------------------
	// Phase 1: Insert initial objects
	// -------------------------------------------------------------------------

	initialObjects := make([]*storobj.Object, 10)
	for i := range initialObjects {
		initialObjects[i] = createTestObjectWithText(className, "hello world document number "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, initialObjects[i]))
	}

	// -------------------------------------------------------------------------
	// Phase 2: Start migration (simulating first boot with reindexer)
	// -------------------------------------------------------------------------

	strategy := &testMigrationStrategy{}
	task := NewShardReindexTaskGeneric("MapToBlockmax", idx.logger, strategy,
		reindexTaskConfig{
			swapBuckets:                   true,
			tidyBuckets:                   true,
			reloadShards:                  false,
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)

	// OnAfterLsmInit: discover props, create reindex+ingest buckets, register callbacks
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))

	// Verify reindex and ingest buckets were created
	reindexBucketName := searchBucketName + "__blockmax_reindex"
	ingestBucketName := searchBucketName + "__blockmax_ingest"
	require.NotNil(t, shard.store.Bucket(reindexBucketName), "reindex bucket should exist")
	require.NotNil(t, shard.store.Bucket(ingestBucketName), "ingest bucket should exist")

	// Verify reindex+ingest buckets use StrategyInverted
	assert.Equal(t, lsmkv.StrategyInverted,
		shard.store.Bucket(reindexBucketName).Strategy())
	assert.Equal(t, lsmkv.StrategyInverted,
		shard.store.Bucket(ingestBucketName).Strategy())

	// -------------------------------------------------------------------------
	// Phase 3: Run async reindex until all objects are processed
	// -------------------------------------------------------------------------

	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	// -------------------------------------------------------------------------
	// Phase 4: Insert more objects during migration (double-write test)
	// -------------------------------------------------------------------------

	doubleWriteObjects := make([]*storobj.Object, 5)
	for i := range doubleWriteObjects {
		doubleWriteObjects[i] = createTestObjectWithText(className, "during migration "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, doubleWriteObjects[i]))
	}

	// Verify tracker shows reindexed
	rt := NewFileMapToBlockmaxReindexTracker(shard.pathLSM(), &UuidKeyParser{})
	require.True(t, rt.IsReindexed(), "tracker should show reindexed")
	require.False(t, rt.IsMerged(), "tracker should not show merged yet")

	// -------------------------------------------------------------------------
	// Phase 5: Simulate restart (shutdown + re-init with reindexer)
	// -------------------------------------------------------------------------

	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	// Create a new task for the second boot (same strategy, same config)
	strategy2 := &testMigrationStrategy{}
	task2 := NewShardReindexTaskGeneric("MapToBlockmax", idx.logger, strategy2,
		reindexTaskConfig{
			swapBuckets:                   true,
			tidyBuckets:                   true,
			reloadShards:                  false,
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            10 * time.Minute,
			pauseDuration:                 1 * time.Second,
			checkProcessingEveryNoObjects: 1000,
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)

	// Wire the reindexer into the index for the restart
	idx.shardReindexer = &testShardReindexer{task: task2}

	// Re-create shard from the same directory (simulating restart)
	// initShard runs: initLSMStore → RunBeforeLsmInit → initNonVector → RunAfterLsmInit → RunAfterLsmInitAsync
	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	// -------------------------------------------------------------------------
	// Phase 6: Verify migration completed
	// -------------------------------------------------------------------------

	// Tracker should show tidied (merge + swap + tidy all happened in OnBeforeLsmInit)
	rt2 := NewFileMapToBlockmaxReindexTracker(shard2.pathLSM(), &UuidKeyParser{})
	assert.True(t, rt2.IsMerged(), "tracker should show merged")
	assert.True(t, rt2.IsSwapped(), "tracker should show swapped")
	assert.True(t, rt2.IsTidied(), "tracker should show tidied")

	// OnMigrationComplete should have been called
	assert.True(t, strategy2.migrationCompleted, "OnMigrationComplete should have been called")

	// Searchable bucket should now be StrategyInverted
	assert.Equal(t, lsmkv.StrategyInverted,
		shard2.store.Bucket(searchBucketName).Strategy(),
		"searchable bucket should be StrategyInverted after migration")

	// All initial objects should be readable
	for _, obj := range initialObjects {
		result, err := shard2.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "initial object %s should be readable", obj.ID())
		require.NotNil(t, result, "initial object %s should exist", obj.ID())
	}

	// All double-write objects should be readable
	for _, obj := range doubleWriteObjects {
		result, err := shard2.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err, "double-write object %s should be readable", obj.ID())
		require.NotNil(t, result, "double-write object %s should exist", obj.ID())
	}

	// Backup bucket should not exist on disk (tidied)
	backupBucketName := searchBucketName + "__blockmax_map"
	assert.Nil(t, shard2.store.Bucket(backupBucketName),
		"backup bucket should not exist after tidy")

	// Reindex bucket should not exist on disk (merged)
	assert.Nil(t, shard2.store.Bucket(reindexBucketName),
		"reindex bucket should not exist after merge")

	// Cleanup
	require.NoError(t, shard2.Shutdown(ctx))
}
