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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestLazyLoadShardMetricsLifecycle tests the full lifecycle of shard metrics:
// 1. Creating a shard increments ShardsUnloaded
// 2. Loading a shard transitions from Unloaded -> Loading -> Loaded
// 3. Dropping a shard transitions from Loaded -> Unloading -> Unloaded
func TestLazyLoadShardMetricsLifecycle(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	className := "TestMetricsLifecycle"

	// Get metrics instance
	baseMetrics := monitoring.GetMetrics()
	metricsCopy := *baseMetrics
	metricsCopy.Registerer = monitoring.NoopRegisterer
	metrics := &metricsCopy

	// Reset shard metrics to known state
	metrics.ShardsLoaded.Set(0)
	metrics.ShardsLoading.Set(0)
	metrics.ShardsUnloaded.Set(0)
	metrics.ShardsUnloading.Set(0)

	// Create db with metrics
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		EnableLazyLoadShards:      true,
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	err = repo.WaitForStartup(testCtx())
	require.NoError(t, err)
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger, "node1")

	t.Run("create shard increments unloaded count", func(t *testing.T) {
		// Add class - this creates a shard in unloaded state
		class := &models.Class{
			Class:               className,
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		sch := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		err = migrator.AddClass(context.Background(), class)
		require.NoError(t, err)
		schemaGetter.schema = sch

		// After creating class, shard should be in unloaded state
		// (NewUnloadedshard() was called)
		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(1), unloadedCount, "shard should be counted as unloaded after creation")

		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "no shards should be loaded yet")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})

	t.Run("loading shard transitions metrics correctly", func(t *testing.T) {
		// Get shard name
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		// Get the lazy shard
		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		require.NotNil(t, shard)
		defer release()

		lazyShard, ok := shard.(*LazyLoadShard)
		require.True(t, ok, "shard should be a LazyLoadShard")

		// Load the shard - this should update metrics:
		// StartLoadingShard: unloaded--, loading++
		// FinishLoadingShard: loading--, loaded++
		err = lazyShard.Load(ctx)
		require.NoError(t, err)

		// After loading, shard should be counted as loaded
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(1), loadedCount, "shard should be counted as loaded after Load()")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "shard should no longer be counted as unloaded after Load()")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be in loading state after load completes")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})

	t.Run("add object to loaded shard", func(t *testing.T) {
		// Add an object so we have data for later tests
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", 1)).String())
		obj := &models.Object{Class: className, ID: id}
		err = repo.PutObject(ctx, obj, []float32{1, 2, 3, 4}, nil, nil, nil, 0)
		require.NoError(t, err)
	})

	t.Run("loading already loaded shard is idempotent", func(t *testing.T) {
		// Get shard
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		defer release()

		lazyShard := shard.(*LazyLoadShard)

		// Load again - should be a no-op since already loaded
		err = lazyShard.Load(ctx)
		require.NoError(t, err)

		// Metrics should remain unchanged
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(1), loadedCount, "loaded count should remain 1")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "unloaded count should remain 0")
	})

	t.Run("shutdown loaded shard transitions metrics correctly", func(t *testing.T) {
		// Get the loaded shard and shut it down
		index := repo.GetIndex(schema.ClassName(className))
		var shardName string
		index.shards.Range(func(name string, _ ShardLike) error {
			shardName = name
			return nil
		})

		shard, release, err := index.GetShard(ctx, shardName)
		require.NoError(t, err)
		release()

		lazyShard := shard.(*LazyLoadShard)

		// Shutdown the shard (unload from memory, but keep on disk)
		// This should call StartUnloadingShard and FinishUnloadingShard
		err = lazyShard.Shutdown(ctx)
		require.NoError(t, err)

		// After shutdown, the shard is unloaded (still exists on disk)
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "shard should no longer be counted as loaded after shutdown")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(1), unloadedCount, "shard should be counted as unloaded after shutdown")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading after shutdown completes")
	})

	t.Run("dropping shard decrements count correctly", func(t *testing.T) {
		// At this point, the shard is unloaded (from previous test)
		// Drop the class (which drops shards)
		// Since the shard was unloaded, LazyLoadShard.drop() should call DeleteUnloadedShard
		err = migrator.DropClass(ctx, className, false)
		require.NoError(t, err)

		// After dropping, all counts should be 0
		loadedCount := testutil.ToFloat64(metrics.ShardsLoaded)
		require.Equal(t, float64(0), loadedCount, "no shards should be loaded")

		unloadedCount := testutil.ToFloat64(metrics.ShardsUnloaded)
		require.Equal(t, float64(0), unloadedCount, "deleted shard should not be counted as unloaded")

		loadingCount := testutil.ToFloat64(metrics.ShardsLoading)
		require.Equal(t, float64(0), loadingCount, "no shards should be loading")

		unloadingCount := testutil.ToFloat64(metrics.ShardsUnloading)
		require.Equal(t, float64(0), unloadingCount, "no shards should be unloading")
	})
}
