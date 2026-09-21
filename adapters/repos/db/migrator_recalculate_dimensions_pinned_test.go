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

//go:build integrationTest

package db

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// These tests pin defects of REINDEX_VECTOR_DIMENSIONS_AT_STARTUP
// (Migrator.RecalculateVectorDimensions) and fail until it is fixed.

// The server calls RecalculateVectorDimensions without waiting for the db to
// load its indices. It then finds none, and reports the reindex as complete.
func TestRecalculateVectorDimensions_BeforeStartupCompleted(t *testing.T) {
	ctx := testCtx()
	logger, _ := test.NewNullLogger()
	dirName := t.TempDir()
	class := &models.Class{
		Class:               "Test",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  dirName,
		QueryMaximumResults:       1000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.False(t, repo.StartupComplete())

	err = NewMigrator(repo, logger, "node1").RecalculateVectorDimensions(ctx)
	require.Error(t, err, "no index was loaded yet, so nothing was reindexed, and that must not pass for success")
}

// resetDimensionsLSM names its temporary bucket after a counter that starts over
// with the process. A dir of that name left behind by an earlier run is loaded as
// the new, supposedly empty dimensions bucket.
func TestResetDimensionsLSM_StaleTemporaryBucket(t *testing.T) {
	ctx := testCtx()
	shd, idx := testShard(t, ctx, "TestClass", func(i *Index) { i.Config.TrackVectorDimensions = true })
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	staleName := fmt.Sprintf("%v", uniqueCounter.Load()+1)
	stale, err := lsmkv.NewBucketCreator().NewBucket(ctx, filepath.Join(shard.pathLSM(), staleName), "", idx.logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
	require.NoError(t, err)
	require.NoError(t, stale.RoaringSetAddList([]byte("\x03\x00\x00\x00"), []uint64{100, 101, 102}))
	require.NoError(t, stale.FlushAndSwitch())
	require.NoError(t, stale.Shutdown(ctx))

	require.NoError(t, shard.resetDimensionsLSM(ctx))

	dims, err := shard.Dimensions(ctx, "")
	require.NoError(t, err)
	require.Equal(t, 0, dims, "a dimensions bucket that was just reset must be empty")
}

// RecalculateVectorDimensions empties the dimensions bucket before it refills it
// from the objects. Interrupted in between, the shard is left with a bucket that
// looks complete and tracks too few dimensions, or none.
func TestResetDimensionsLSM_InterruptedBeforeRefill(t *testing.T) {
	ctx := testCtx()
	class := &models.Class{Class: "TestClass"}
	shd, idx := testShard(t, ctx, class.Class, func(i *Index) { i.Config.TrackVectorDimensions = true })
	shard := shd.(*Shard)

	for range 10 {
		obj := testObject(class.Class)
		obj.Vector = randVector(3)
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	dims, err := shard.Dimensions(ctx, "")
	require.NoError(t, err)
	require.Equal(t, 30, dims)

	require.NoError(t, shard.resetDimensionsLSM(ctx))
	require.NoError(t, shard.Shutdown(ctx))

	reloaded, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	defer reloaded.Shutdown(ctx)
	require.NotNil(t, reloaded.Store().Bucket(helpers.DimensionsBucketLSM))

	dims, err = reloaded.Dimensions(ctx, "")
	require.NoError(t, err)
	require.Equal(t, 30, dims, "the tracked dimensions must survive a reindex that did not finish")
}
