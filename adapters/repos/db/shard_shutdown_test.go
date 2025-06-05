//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
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

	// sanity check, no flags marked
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, false)

	// release shard 2x
	release1()
	release2()

	// shutdown succeeds, shard idle
	err = shard.Shutdown(context.Background())
	require.NoError(t, err)
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, true)
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

	// sanity check, no flags marked
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, false)

	// shutdown fails, shard in use 2x
	err = shard.Shutdown(context.Background())
	require.ErrorContains(t, err, "still in use")
	requireShardShutdownRequested(t, shard, true)
	requireShardShut(t, shard, false)

	// getting shard fails, shutdown in progress
	sameShardAgain, _, err := index.GetShard(context.Background(), shardName)
	require.ErrorIs(t, err, errShutdownInProgress)
	require.Nil(t, sameShardAgain)

	// release shard 1x
	release1()

	// shutdown still in progress, shard in use 1x
	requireShardShutdownRequested(t, shard, true)
	requireShardShut(t, shard, false)

	// release shard 1x
	release2()

	// shutdown eventually completed, shard idle
	requireShardShutdownRequested(t, shard, false)
	requireShardShut(t, shard, true)

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
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	},
		&fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{},
		&fakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
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

	migrator := NewMigrator(repo, logger)
	err = migrator.AddClass(context.Background(), class, schemaGetter.shardState)
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

func requireShardShutdownRequested(t *testing.T, shard ShardLike, expected bool) {
	if expected {
		require.True(t, shard.(*LazyLoadShard).shard.shutdownRequested.Load(), "shard should be marked for shut down")
	} else {
		require.False(t, shard.(*LazyLoadShard).shard.shutdownRequested.Load(), "shard should not be marked for shut down")
	}
}

func requireShardShut(t *testing.T, shard ShardLike, expected bool) {
	if expected {
		require.True(t, shard.(*LazyLoadShard).shard.shut.Load(), "shard should be marked as shut down")
	} else {
		require.False(t, shard.(*LazyLoadShard).shard.shut.Load(), "shard should not be marked as shut down")
	}
}
