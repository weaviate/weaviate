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

package usage

import (
	"context"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrus "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	backupusecase "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestService_Usage_SingleTenant(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"
	className := "TestClass"
	replication := 1
	uniqueShards := 1
	shardName := "abcd"
	ObjectStorageSize := int64(353)
	vectorName := "abcd"
	vectorType := "hnsw"
	compression := "standard"
	compressionRatio := 1.
	dimensionality := 3
	dimensionCount := 1
	memLimit := int64(1_000_000)

	debug.SetMemoryLimit(memLimit)

	class := &models.Class{
		Class: className,
		ReplicationConfig: &models.ReplicationConfig{
			Factor: int64(replication),
		},
		VectorConfig: map[string]models.VectorConfig{vectorName: {VectorIndexConfig: hnsw.UserConfig{}}},
	}

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			shardName: {
				Name:           shardName,
				BelongsToNodes: []string{nodeName},
				Status:         models.TenantActivityStatusHOT,
			},
		},
	}
	shardingState.SetLocalName(nodeName)

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(nil, shardingState)
		},
	)

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	})
	mockSchemaGetter.EXPECT().ReadOnlyClass(class.Class).Return(class)
	mockSchemaGetter.EXPECT().ShardFromUUID(class.Class, mock.Anything).Return(shardName)
	mockSchemaGetter.EXPECT().NodeName().Return(nodeName)

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{})
	repo := createTestDb(t, mockSchemaGetter, shardingState, class, nodeName)

	// adding objects with the schemareader triggers some replication code paths that we do not care about here and are
	// hard to setup with all these mocks and fake-things. So we add them with the schemaGetter and then switch on the
	// schemaReader for the actual usage call
	putObjectAndFlush(t, repo, className, "", map[string][]float32{vectorName: {0.1, 0.2, 0.3}})
	repo.Shutdown(ctx)
	repo.SetSchemaReader(mockSchemaReader)
	require.Nil(t, repo.WaitForStartup(context.Background()))

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchemaGetter, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 1)

	collection := result.Collections[0]
	assert.Equal(t, className, collection.Name)
	assert.Equal(t, replication, collection.ReplicationFactor)
	assert.Equal(t, uniqueShards, collection.UniqueShardCount)
	assert.Len(t, collection.Shards, 1)

	shard := collection.Shards[0]
	assert.Equal(t, shardName, shard.Name)
	assert.Equal(t, int64(1), shard.ObjectsCount)
	assert.Equal(t, uint64(ObjectStorageSize), shard.ObjectsStorageBytes)
	assert.Len(t, shard.NamedVectors, 1)

	vector := shard.NamedVectors[0]
	assert.Equal(t, vectorName, vector.Name)
	assert.Equal(t, vectorType, vector.VectorIndexType)
	assert.Equal(t, compression, vector.Compression)
	assert.Equal(t, compressionRatio, vector.VectorCompressionRatio)
	assert.Len(t, vector.Dimensionalities, 1)

	dim := vector.Dimensionalities[0]
	assert.Equal(t, dimensionality, dim.Dimensions)
	assert.Equal(t, dimensionCount, dim.Count)

	assert.Equal(t, memLimit, result.GoMemLimit)

	mockSchemaGetter.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
}

func TestService_Usage_MultiTenant_HotAndCold(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"
	className := "MultiTenantClass"
	uniqueShards := 2
	replication := 1
	hotTenant := "tenant1"
	coldTenant := "tenant2"
	vectorName := "abcd"
	vectorType := "hnsw"
	compression := "standard"
	compressionRatio := 1.0
	dimensionCount := 2
	memLimit := int64(100_000)

	debug.SetMemoryLimit(memLimit)

	class := &models.Class{
		Class:              className,
		ReplicationConfig:  &models.ReplicationConfig{Factor: int64(replication)},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		VectorConfig:       map[string]models.VectorConfig{vectorName: {VectorIndexConfig: hnsw.UserConfig{}}},
	}

	shardingState := &sharding.State{
		PartitioningEnabled: true,
		Physical: map[string]sharding.Physical{
			hotTenant: {
				Name:           hotTenant,
				BelongsToNodes: []string{nodeName},
				Status:         models.TenantActivityStatusHOT,
			},
			coldTenant: {
				Name:           coldTenant,
				BelongsToNodes: []string{nodeName},
				Status:         models.TenantActivityStatusCOLD,
			},
		},
	}
	shardingState.SetLocalName(nodeName)

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				class,
			},
		},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)
	mockSchema.EXPECT().ReadOnlyClass(class.Class).Return(class).Maybe()
	mockSchema.EXPECT().TenantsShards(mock.Anything, className, hotTenant).
		Return(map[string]string{hotTenant: models.TenantActivityStatusHOT}, nil).Maybe()
	mockSchema.EXPECT().OptimisticTenantStatus(mock.Anything, className, hotTenant).
		Return(map[string]string{hotTenant: models.TenantActivityStatusHOT}, nil).Maybe()
	mockSchema.EXPECT().ShardOwner(className, hotTenant).Return(nodeName, nil).Maybe()

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(nil, shardingState)
		},
	).Maybe()

	repo := createTestDb(t, mockSchema, shardingState, class, nodeName)
	putObjectAndFlush(t, repo, className, hotTenant, map[string][]float32{vectorName: {0.1, 0.2, 0.3}}, map[string][]float32{vectorName: {0.4, 0.5, 0.6}})
	repo.Shutdown(ctx)
	repo.SetSchemaReader(mockSchemaReader)
	require.Nil(t, repo.WaitForStartup(context.Background()))

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 1)

	collection := result.Collections[0]
	assert.Equal(t, className, collection.Name)
	assert.Equal(t, replication, collection.ReplicationFactor)
	assert.Equal(t, uniqueShards, collection.UniqueShardCount)
	assert.Len(t, collection.Shards, 2)

	var hotShard, coldShard *types.ShardUsage
	for _, shard := range collection.Shards {
		switch shard.Name {
		case hotTenant:
			hotShard = shard
		case coldTenant:
			coldShard = shard
		}
	}

	require.NotNil(t, hotShard)
	assert.Equal(t, int64(2), hotShard.ObjectsCount)
	assert.Equal(t, uint64(612), hotShard.ObjectsStorageBytes)
	assert.Equal(t, strings.ToLower(models.TenantActivityStatusACTIVE), hotShard.Status)
	assert.Len(t, hotShard.NamedVectors, 1)

	require.NotNil(t, coldShard)
	assert.Equal(t, int64(0), coldShard.ObjectsCount)
	assert.Equal(t, uint64(0), coldShard.ObjectsStorageBytes)
	assert.Equal(t, strings.ToLower(models.TenantActivityStatusINACTIVE), coldShard.Status)
	assert.Len(t, coldShard.NamedVectors, 1)

	vector := hotShard.NamedVectors[0]
	assert.Equal(t, vectorName, vector.Name)
	assert.Equal(t, vectorType, vector.VectorIndexType)
	assert.Equal(t, compression, vector.Compression)
	assert.Equal(t, compressionRatio, vector.VectorCompressionRatio)
	assert.Len(t, vector.Dimensionalities, 1)
	dim := vector.Dimensionalities[0]
	assert.Equal(t, 3, dim.Dimensions)
	assert.Equal(t, dimensionCount, dim.Count)

	assert.Equal(t, memLimit, result.GoMemLimit)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
}

func TestService_Usage_WithBackups(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"
	backupID := "backup-1"
	backupStatus := backup.Success
	completionTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	preCompressionSizeBytes := int64(1073741824) // 1 GiB
	sizeInGib := 1.0
	backupType := "SUCCESS"
	class1 := "Class1"
	class2 := "Class2"
	class3 := "Class3"

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{}},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{},
	}
	shardingState.SetLocalName(nodeName)

	repo := createTestDb(t, mockSchema, shardingState, nil, nodeName)

	mockBackupBackend := modulecapabilities.NewMockBackupBackend(t)
	backups := []*backup.DistributedBackupDescriptor{
		{
			ID:                      backupID,
			Status:                  backupStatus,
			CompletedAt:             completionTime,
			PreCompressionSizeBytes: preCompressionSizeBytes,
			Nodes: map[string]*backup.NodeDescriptor{
				"test-node": {
					Classes:                 []string{class1, class2},
					PreCompressionSizeBytes: preCompressionSizeBytes,
				},
			},
		},
		{
			ID:                      "backup-2",
			Status:                  backup.Failed,
			CompletedAt:             completionTime,
			PreCompressionSizeBytes: 2147483648,
			Nodes: map[string]*backup.NodeDescriptor{
				"node1": {Classes: []string{class3}},
			},
		},
	}
	mockBackupBackend.EXPECT().AllBackups(ctx).Return(backups, nil)

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{mockBackupBackend})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 0)
	assert.Len(t, result.Backups, 1)

	backup := result.Backups[0]
	assert.Equal(t, backupID, backup.ID)
	assert.Equal(t, completionTime.Format(time.RFC3339), backup.CompletionTime)
	assert.Equal(t, sizeInGib, backup.SizeInGib)
	assert.Equal(t, backupType, backup.Type)

	collections := backup.Collections
	sort.Strings(collections)
	expectedCollections := []string{class1, class2}
	sort.Strings(expectedCollections)
	assert.Equal(t, expectedCollections, collections)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
	mockBackupBackend.AssertExpectations(t)
}

func TestService_Usage_WithBackups_3node_cluster(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node-3"
	backupID := "backup-1"
	backupStatus := backup.Success
	completionTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	size1GB := int64(1073741824) // 1 GiB
	size2GB := 2 * size1GB       // 2 GiB
	size3GB := 3 * size1GB       // 3 GiB
	sizeInGib := float64(3)      // result: 3 GiB size for test-node-3
	backupType := "SUCCESS"
	class1 := "Class1"
	class2 := "Class2"

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{}},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{},
	}
	shardingState.SetLocalName(nodeName)

	repo := createTestDb(t, mockSchema, shardingState, nil, nodeName)

	mockBackupBackend := modulecapabilities.NewMockBackupBackend(t)
	backups := []*backup.DistributedBackupDescriptor{
		{
			ID:                      backupID,
			Status:                  backupStatus,
			CompletedAt:             completionTime,
			PreCompressionSizeBytes: size1GB + size2GB + size3GB,
			Nodes: map[string]*backup.NodeDescriptor{
				"test-node-1": {
					Classes:                 []string{class1, class2},
					PreCompressionSizeBytes: size1GB,
				},
				"test-node-2": {
					Classes:                 []string{class1, class2},
					PreCompressionSizeBytes: size2GB,
				},
				"test-node-3": {
					Classes:                 []string{class1, class2},
					PreCompressionSizeBytes: size3GB,
				},
			},
		},
	}
	mockBackupBackend.EXPECT().AllBackups(ctx).Return(backups, nil)

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{mockBackupBackend})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 0)
	assert.Len(t, result.Backups, 1)

	backup := result.Backups[0]
	assert.Equal(t, backupID, backup.ID)
	assert.Equal(t, completionTime.Format(time.RFC3339), backup.CompletionTime)
	assert.Equal(t, sizeInGib, backup.SizeInGib)
	assert.Equal(t, backupType, backup.Type)

	collections := backup.Collections
	sort.Strings(collections)
	expectedCollections := []string{class1, class2}
	sort.Strings(expectedCollections)
	assert.Equal(t, expectedCollections, collections)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
	mockBackupBackend.AssertExpectations(t)
}

func TestService_Usage_EmptyCollections(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{}},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{},
	}
	shardingState.SetLocalName(nodeName)

	repo := createTestDb(t, mockSchema, shardingState, nil, nodeName)

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 0)
	assert.Len(t, result.Backups, 0)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
}

func TestService_Usage_BackupError(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{}},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{},
	}
	shardingState.SetLocalName(nodeName)

	repo := createTestDb(t, mockSchema, shardingState, nil, nodeName)

	mockBackupBackend := modulecapabilities.NewMockBackupBackend(t)
	mockBackupBackend.EXPECT().AllBackups(ctx).Return(nil, assert.AnError)

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{mockBackupBackend})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	_, err := service.Usage(ctx, false)

	require.Error(t, err)
	require.ErrorIs(t, err, assert.AnError)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
	mockBackupBackend.AssertExpectations(t)
}

func TestService_Usage_NilVectorIndexConfig(t *testing.T) {
	ctx := context.Background()

	nodeName := "test-node"
	className := "NilConfigClass"
	replication := 1
	tenantName := "default"

	class := &models.Class{
		Class:             className,
		VectorIndexConfig: nil,
		ReplicationConfig: &models.ReplicationConfig{Factor: int64(replication)},
	}

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	})
	mockSchema.EXPECT().NodeName().Return(nodeName)

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenantName: {
				Name:           tenantName,
				BelongsToNodes: []string{nodeName},
				Status:         models.TenantActivityStatusHOT,
			},
		},
	}
	shardingState.SetLocalName(nodeName)
	mockSchema.EXPECT().ReadOnlyClass(class.Class).Return(class)

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(nil, shardingState)
		},
	)

	repo := createTestDb(t, mockSchema, shardingState, class, nodeName)
	repo.Shutdown(ctx)
	repo.SetSchemaReader(mockSchemaReader)
	require.Nil(t, repo.WaitForStartup(context.Background()))

	mockBackupProvider := backupusecase.NewMockBackupBackendProvider(t)
	mockBackupProvider.EXPECT().EnabledBackupBackends().Return([]modulecapabilities.BackupBackend{})

	logger, _ := logrus.NewNullLogger()
	service := NewService(mockSchema, repo, mockBackupProvider, logger)

	result, err := service.Usage(ctx, false)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, nodeName, result.Node)
	assert.Len(t, result.Collections, 1)

	collection := result.Collections[0]
	assert.Equal(t, className, collection.Name)
	assert.Len(t, collection.Shards, 1)

	shard := collection.Shards[0]
	assert.Equal(t, tenantName, shard.Name)
	assert.Equal(t, int64(0), shard.ObjectsCount)
	assert.Equal(t, uint64(0), shard.ObjectsStorageBytes)
	assert.Nil(t, shard.NamedVectors)

	mockSchema.AssertExpectations(t)
	mockBackupProvider.AssertExpectations(t)
}

func createTestDb(t *testing.T, sg schemaUC.SchemaGetter, shardingState *sharding.State, class *models.Class, nodeName string) *db.DB {
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return(nodeName).Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return(nodeName, true).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardingState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardingState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()
	logger, _ := logrus.NewNullLogger()
	repo, err := db.New(logger, nodeName, db.Config{
		MemtablesFlushDirtyAfter:  0,
		RootPath:                  t.TempDir(),
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		MaxReuseWalSize:           0, // disable to make count easier
		DisableLazyLoadShards:     true,
	}, &db.FakeRemoteClient{}, &db.FakeNodeResolver{}, &db.FakeRemoteNodeClient{}, &db.FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)

	repo.SetSchemaGetter(sg)

	require.Nil(t, repo.WaitForStartup(context.Background()))

	migrator := db.NewMigrator(repo, logger, nodeName)
	if class != nil && shardingState != nil {
		migrator.AddClass(context.Background(), class)
		for _, shard := range shardingState.Physical {
			require.NoError(t, migrator.LoadShard(context.Background(), class.Class, shard.Name))
			if shard.ActivityStatus() == models.TenantActivityStatusCOLD {
				require.NoError(t, migrator.ShutdownShard(context.Background(), class.Class, shard.Name))
			}
		}

	}

	return repo
}

func putObjectAndFlush(t *testing.T, repo *db.DB, className, tenant string, vectors ...map[string][]float32) {
	for _, vector := range vectors {
		uid := strfmt.UUID(uuid.New().String())
		obj := &models.Object{Class: className, ID: uid}
		if tenant != "" {
			obj.Tenant = tenant
		}
		require.NoError(t, repo.PutObject(context.Background(), obj, nil, vector, nil, nil, 0))
	}
	idx := repo.GetIndex(entschema.ClassName(className))
	idx.ForEachLoadedShard(func(name string, shard db.ShardLike) error {
		require.NoError(t, shard.Store().GetBucketsByName()["objects"].FlushMemtable())
		return nil
	})
}

type MockShardReader struct {
	lst models.ShardStatusList
	err error
}

func (m MockShardReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return m.lst, m.err
}
