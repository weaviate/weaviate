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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestNodesAPI_Journey(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
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
		ServerVersion:             "server-version",
		GitHash:                   "git-hash",
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, &FakeNodeResolver{}, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer repo.Shutdown(context.Background())
	migrator := NewMigrator(repo, logger, "node1")

	// check nodes api response on empty DB
	nodeStatues, err := repo.GetNodeStatus(context.Background(), "", "", verbosity.OutputVerbose)
	require.Nil(t, err)
	require.NotNil(t, nodeStatues)

	require.Len(t, nodeStatues, 1)
	nodeStatus := nodeStatues[0]
	assert.NotNil(t, nodeStatus)
	assert.Equal(t, "node1", nodeStatus.Name)
	assert.Equal(t, "server-version", nodeStatus.Version)
	assert.Equal(t, "git-hash", nodeStatus.GitHash)
	assert.Len(t, nodeStatus.Shards, 0)
	assert.Equal(t, int64(0), nodeStatus.Stats.ObjectCount)
	assert.Equal(t, int64(0), nodeStatus.Stats.ShardCount)

	// import 2 objects
	class := &models.Class{
		Class:               "ClassNodesAPI",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	require.Nil(t,
		migrator.AddClass(context.Background(), class))

	schemaGetter.schema.Objects = &models.Schema{
		Classes: []*models.Class{class},
	}

	batch := objects.BatchObjects{
		objects.BatchObject{
			OriginalIndex: 0,
			Err:           nil,
			Object: &models.Object{
				Class: "ClassNodesAPI",
				Properties: map[string]interface{}{
					"stringProp": "first element",
				},
				ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
			},
			UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
		},
		objects.BatchObject{
			OriginalIndex: 1,
			Err:           nil,
			Object: &models.Object{
				Class: "ClassNodesAPI",
				Properties: map[string]interface{}{
					"stringProp": "second element",
				},
				ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
			},
			UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
		},
	}
	batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
	require.Nil(t, err)

	assert.Nil(t, batchRes[0].Err)
	assert.Nil(t, batchRes[1].Err)

	// check nodes api after importing 2 objects to DB
	nodeStatues, err = repo.GetNodeStatus(context.Background(), "", "", verbosity.OutputVerbose)
	require.Nil(t, err)
	require.NotNil(t, nodeStatues)

	require.Len(t, nodeStatues, 1)
	nodeStatus = nodeStatues[0]
	assert.NotNil(t, nodeStatus)
	assert.Equal(t, "node1", nodeStatus.Name)
	assert.Equal(t, "server-version", nodeStatus.Version)
	assert.Equal(t, "git-hash", nodeStatus.GitHash)
	assert.Len(t, nodeStatus.Shards, 1)
	assert.Equal(t, "ClassNodesAPI", nodeStatus.Shards[0].Class)
	assert.True(t, len(nodeStatus.Shards[0].Name) > 0)
	// a previous version of this test made assertions on object counts,
	// however with object count becoming async, we can no longer make exact
	// assertions here. See https://github.com/weaviate/weaviate/issues/4193
	// for details.
	assert.Equal(t, "READY", nodeStatus.Shards[0].VectorIndexingStatus)
	assert.Equal(t, int64(0), nodeStatus.Shards[0].VectorQueueLength)
	assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
}

func TestLazyLoadedShards(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	className := "TestClass"
	tenantNamePopulated := "test-tenant"

	// Create test class
	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	// Create fake schema
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	// Create sharding state
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenantNamePopulated: {
				Name:           tenantNamePopulated,
				BelongsToNodes: []string{"test-node"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}
	shardState.SetLocalName("test-node")

	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		return readerFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()

	// Create mock schema getter
	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
	mockSchema.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(class, shardState)
	}).Maybe()
	mockSchema.EXPECT().NodeName().Maybe().Return("test-node")
	mockSchema.EXPECT().TenantsShards(ctx, className, tenantNamePopulated).Maybe().
		Return(map[string]string{tenantNamePopulated: models.TenantActivityStatusHOT}, nil)

	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().GetWriteReplicasLocation(className, mock.Anything, tenantNamePopulated).
		Return(types.WriteReplicaSet{
			Replicas:           []types.Replica{{NodeName: "test-node", ShardName: tenantNamePopulated, HostAddr: "10.14.57.56"}},
			AdditionalReplicas: nil,
		}, nil).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(className, tenantNamePopulated, tenantNamePopulated).
		Return(types.ReadReplicaSet{
			Replicas: []types.Replica{{NodeName: "test-node", ShardName: tenantNamePopulated, HostAddr: "10.14.57.56"}},
		}, nil).Maybe()
	// Create index with lazy loading disabled to test active calculation methods
	schemaGetter := &fakeSchemaGetter{
		schema: fakeSchema, shardState: shardState,
	}
	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, schemaGetter)
	index, err := NewIndex(ctx, IndexConfig{
		RootPath:              dirName,
		ClassName:             schema.ClassName(className),
		ReplicationFactor:     1,
		ShardLoadLimiter:      loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		DisableLazyLoadShards: false, // we have to make sure lazyload shard disabled to load directly

	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{
			VectorCacheMaxObjects: 1000,
		}, nil, mockRouter, shardResolver, mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)

	// make sure that getting the node status does not trigger loading of lazy shards
	status := &[]*models.NodeShardStatus{}
	index.getShardsNodeStatus(ctx, status, "")
	status2 := &[]*models.NodeShardStatus{}
	index.getShardsNodeStatus(ctx, status2, "")
	require.Equal(t, status, status2)
	require.Len(t, *status, 1)
	require.False(t, (*status)[0].Loaded)
}
