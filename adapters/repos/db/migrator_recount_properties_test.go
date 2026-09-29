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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Called before the db has loaded its indices, RecountProperties would find none,
// recount nothing, and report the recount as complete.
func TestRecountProperties_BeforeStartupCompleted(t *testing.T) {
	repo := newUnstartedTestDB(t)
	logger, _ := test.NewNullLogger()

	err := NewMigrator(repo, logger, "node1").RecountProperties(testCtx())
	require.Error(t, err, "no index was loaded yet, so nothing was recounted, and that must not pass for success")
}

// newUnstartedTestDB returns a db that has not loaded its indices yet.
func newUnstartedTestDB(t *testing.T) *DB {
	t.Helper()
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
	return repo
}
