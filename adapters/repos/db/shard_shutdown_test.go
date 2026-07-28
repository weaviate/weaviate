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
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShardShutdownWhenIdle(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	// use shard
	shard, release1, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, shard)
	require.NotNil(t, release1)

	// use same shard
	sameShard, release2, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, sameShard)
	require.NotNil(t, release2)

	// sanity check, shard fully in service
	requireShardPhase(t, shard, shardLive)

	// release shard 2x
	release1()
	release2()

	// shutdown succeeds, shard idle
	err = shard.Shutdown(context.Background())
	require.NoError(t, err)
	requireShardPhase(t, shard, shardClosed)
}

func TestShardShutdownWhenIdleEventually(t *testing.T) {
	dirName := t.TempDir()
	index, cleanup := initIndexAndPopulate(t, dirName)
	defer cleanup()

	var shardName string
	index.shards.Range(func(name string, _ ShardLike) error {
		shardName = name
		return nil
	})

	// use shard
	shard, release1, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, shard)
	require.NotNil(t, release1)

	// use same shard
	sameShard, release2, err := index.GetShard(context.Background(), shardName)
	require.NoError(t, err)
	require.NotNil(t, sameShard)
	require.NotNil(t, release2)

	// sanity check, shard fully in service
	requireShardPhase(t, shard, shardLive)

	// shutdown fails, shard in use 2x
	err = shard.Shutdown(context.Background())
	require.ErrorContains(t, err, "still in use")
	requireShardPhase(t, shard, shardUnloading)

	// getting shard fails, shutdown in progress
	sameShardAgain, _, err := index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errShutdownInProgress)
	require.Nil(t, sameShardAgain)

	// release shard 1x
	release1()

	// shutdown still in progress, shard in use 1x
	requireShardPhase(t, shard, shardUnloading)

	// release shard 1x
	release2()

	// shutdown eventually completed, shard idle
	requireShardPhase(t, shard, shardClosed)

	// getting shard fails, shutdown completed
	sameShardYetAgain, _, err := index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errAlreadyShutdown)
	require.Nil(t, sameShardYetAgain)
}

func initIndexAndPopulate(t *testing.T, dirName string) (index *Index, cleanup func()) {
	logger, _ := test.NewNullLogger()
	className := "Test"

	// create db
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
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		EnableLazyLoadShards:      boolPtr(true),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	err = repo.WaitForStartup(testCtx())
	require.NoError(t, err)

	cleanup = func() { repo.Shutdown(context.Background()) }
	runCleanup := true // run cleanup if method fails
	defer func() {
		if runCleanup {
			cleanup()
		}
	}()

	// set schema
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

	migrator := NewMigrator(repo, logger, "node1")
	err = migrator.AddClass(context.Background(), class)
	require.NoError(t, err)
	schemaGetter.schema = sch

	// import objects
	for i := 0; i < 10; i++ {
		v := float32(i)
		vec := []float32{v, v + 1, v + 2, v + 3}

		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
		obj := &models.Object{Class: className, ID: id}
		err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
		require.NoError(t, err)
	}

	index = repo.GetIndex(schema.ClassName(className))
	runCleanup = false // all good, let caller cleanup
	return index, cleanup
}

// requireShardPhase replaces the former requireShardShutdownRequested /
// requireShardShut pair, two booleans that only ever held three combinations.
func requireShardPhase(t *testing.T, shard ShardLike, expected shardPhase) {
	t.Helper()
	require.Equal(t, expected, shard.(*LazyLoadShard).shard.lifecycle.phase())
}
