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
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestShard_BatchDimensionValidation_GhostObjectPrevention(t *testing.T) {
	testCases := []struct {
		name  string
		uc    schemaConfig.VectorIndexConfig
		async bool
	}{
		{
			name:  "HNSW sync",
			uc:    hnsw.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "HNSW async",
			uc:    hnsw.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "Flat",
			uc:    flat.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "Dynamic",
			uc:    dynamic.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "HFresh",
			uc:    hfresh.NewDefaultUserConfig(),
			async: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BatchDimTest" + t.Name()
			class := &models.Class{Class: className}

			shd, idx := testShardWithSettings(t, ctx, class, tc.uc, false, tc.async)
			defer func() {
				require.Nil(t, idx.drop())
				require.Nil(t, os.RemoveAll(idx.Config.RootPath))
			}()

			testShard(t, context.Background(), class.Class)
			r := getRandomSeed()

			// Step 1: Establish dimension = 4 with an initial batch of 2 objects
			initialBatch := createRandomObjects(r, class.Class, 2, 4)
			errs := shd.PutObjectBatch(ctx, initialBatch)
			require.Len(t, errs, 2)
			for _, err := range errs {
				require.NoError(t, err)
			}
			require.Equal(t, 2, int(shd.Counter().Get()))

			if tc.async {
				waitForVectorQueue(t, shd, "")
			}

			// Step 2: Ingest a mixed batch containing:
			//   Obj 0: Valid (dim 4)
			//   Obj 1: Invalid (dim 5)
			//   Obj 2: Valid (dim 4)
			//   Obj 3: Invalid (dim 3)
			valid1 := createRandomObjects(r, class.Class, 1, 4)[0]
			invalid1 := createRandomObjects(r, class.Class, 1, 5)[0]
			valid2 := createRandomObjects(r, class.Class, 1, 4)[0]
			invalid2 := createRandomObjects(r, class.Class, 1, 3)[0]

			mixedBatch := []*storobj.Object{valid1, invalid1, valid2, invalid2}

			errs = shd.PutObjectBatch(ctx, mixedBatch)
			require.Len(t, errs, 4)

			// Step 3: Verify exact error messages on invalid objects and no error on valid objects
			require.NoError(t, errs[0])
			require.Error(t, errs[1])
			assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, errs[1].Error())
			require.NoError(t, errs[2])
			require.Error(t, errs[3])
			assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 3`, errs[3].Error())

			// Step 4: Verify Ghost Object Prevention:
			// Rejected objects must NEVER be written to LSM store (not retrievable by ID)
			obj1Retrieved, err := shd.ObjectByID(ctx, invalid1.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, obj1Retrieved, "rejected object 1 must not exist in LSM storage")

			obj3Retrieved, err := shd.ObjectByID(ctx, invalid2.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, obj3Retrieved, "rejected object 2 must not exist in LSM storage")

			// Valid objects MUST exist in LSM store
			valid1Retrieved, err := shd.ObjectByID(ctx, valid1.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, valid1Retrieved, "valid object 1 must exist in LSM storage")

			valid2Retrieved, err := shd.ObjectByID(ctx, valid2.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, valid2Retrieved, "valid object 2 must exist in LSM storage")

			// Step 5: Verify vector search operates without error
			searchRes, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{valid1.Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err, "vector search must succeed after rejected batch")
			require.NotEmpty(t, searchRes)

			// Step 6: Subsequent batch with valid dimensions (dim 4) succeeds
			subsequentBatch := createRandomObjects(r, class.Class, 3, 4)
			subsequentErrs := shd.PutObjectBatch(ctx, subsequentBatch)
			require.Len(t, subsequentErrs, 3)
			for _, err := range subsequentErrs {
				require.NoError(t, err, "subsequent valid batch must succeed")
			}
		})
	}
}

func TestShard_BatchDimensionValidation_CollectionPoisoningPrevention(t *testing.T) {
	testCases := []struct {
		name  string
		uc    schemaConfig.VectorIndexConfig
		async bool
	}{
		{
			name:  "HNSW sync",
			uc:    hnsw.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "HNSW async",
			uc:    hnsw.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "Flat",
			uc:    flat.NewDefaultUserConfig(),
			async: false,
		},
		{
			name:  "Dynamic",
			uc:    dynamic.NewDefaultUserConfig(),
			async: true,
		},
		{
			name:  "HFresh",
			uc:    hfresh.NewDefaultUserConfig(),
			async: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BatchDimPoisonTest" + t.Name()
			class := &models.Class{Class: className}

			shd, idx := testShardWithSettings(t, ctx, class, tc.uc, false, tc.async)
			defer func() {
				require.Nil(t, idx.drop())
				require.Nil(t, os.RemoveAll(idx.Config.RootPath))
			}()

			r := getRandomSeed()

			// Step 1: Verify index is uninitialized (dim == 0)
			shd.WithVectorIndex("", func(vidx VectorIndex) error {
				assert.Equal(t, 0, vidx.Dimension())
				return nil
			})

			// Step 2: Ingest a conflicting batch on uninitialized collection:
			//   Obj 0: 3D vector (conflicting)
			//   Obj 1: 4D vector (conflicting)
			//   Obj 2: 3D vector (conflicting)
			//   Obj 3: Non-vectorized object (len(Vector) == 0)
			conflicting1 := createRandomObjects(r, class.Class, 1, 3)[0]
			conflicting2 := createRandomObjects(r, class.Class, 1, 4)[0]
			conflicting3 := createRandomObjects(r, class.Class, 1, 3)[0]
			nonVectorized := createRandomObjects(r, class.Class, 1, 0)[0]

			batch := []*storobj.Object{conflicting1, conflicting2, conflicting3, nonVectorized}

			errs := shd.PutObjectBatch(ctx, batch)
			require.Len(t, errs, 4)

			// Step 3: Verify exact error message on all conflicting objects
			expectedErr := `target vector "": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
			require.Error(t, errs[0])
			assert.Equal(t, expectedErr, errs[0].Error())
			require.Error(t, errs[1])
			assert.Equal(t, expectedErr, errs[1].Error())
			require.Error(t, errs[2])
			assert.Equal(t, expectedErr, errs[2].Error())

			// Non-vectorized object must NOT be rejected
			require.NoError(t, errs[3])

			if tc.async {
				waitForVectorQueue(t, shd, "")
			}

			// Step 4: Verify LSM storage:
			// None of the conflicting objects are persisted to LSM store
			retrieved0, err := shd.ObjectByID(ctx, conflicting1.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, retrieved0, "conflicting object 1 must not exist in LSM storage")

			retrieved1, err := shd.ObjectByID(ctx, conflicting2.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, retrieved1, "conflicting object 2 must not exist in LSM storage")

			retrieved2, err := shd.ObjectByID(ctx, conflicting3.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.Nil(t, retrieved2, "conflicting object 3 must not exist in LSM storage")

			// Non-vectorized object MUST exist in LSM store
			retrievedNonVec, err := shd.ObjectByID(ctx, nonVectorized.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, retrievedNonVec, "non-vectorized object must exist in LSM storage")

			// Step 5: Verify index remains uninitialized (dim == 0)
			shd.WithVectorIndex("", func(vidx VectorIndex) error {
				assert.Equal(t, 0, vidx.Dimension(), "index must remain uninitialized (D = 0)")
				return nil
			})

			// Step 6: Subsequent batch with uniform vector dimensions (dim 4) succeeds
			subsequentBatch := createRandomObjects(r, class.Class, 3, 4)
			subsequentErrs := shd.PutObjectBatch(ctx, subsequentBatch)
			require.Len(t, subsequentErrs, 3)
			for _, err := range subsequentErrs {
				require.NoError(t, err, "subsequent valid batch must succeed")
			}

			if tc.async {
				waitForVectorQueue(t, shd, "")
			}

			// Subsequent objects exist in LSM store
			for _, obj := range subsequentBatch {
				retrieved, err := shd.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
				require.NoError(t, err)
				assert.NotNil(t, retrieved, "subsequent object must exist in LSM storage")
			}

			// Index dimension is now established to 4
			shd.WithVectorIndex("", func(vidx VectorIndex) error {
				assert.Equal(t, 4, vidx.Dimension(), "index dimension must now be established to 4")
				return nil
			})

			// Vector search operates successfully
			searchRes, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{subsequentBatch[0].Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err, "vector search must succeed after valid batch establishes dimension")
			require.NotEmpty(t, searchRes)
		})
	}
}

func TestIndex_BatchDimensionValidation_MultiTenantIsolation(t *testing.T) {
	ctx := testCtx()
	className := "BatchDimMultiTenantIsolation"
	class := &models.Class{Class: className}

	shards, idx := setupTestMultiTenantShards(t, ctx, class, hnsw.NewDefaultUserConfig(), "tenantA", "tenantB")
	defer func() {
		require.Nil(t, idx.drop())
		require.Nil(t, os.RemoveAll(idx.Config.RootPath))
	}()

	shardA := shards["tenantA"]
	shardB := shards["tenantB"]
	r := getRandomSeed()

	// Both tenant shards start uninitialized (dim == 0)
	shardA.WithVectorIndex("", func(vidx VectorIndex) error {
		assert.Equal(t, 0, vidx.Dimension())
		return nil
	})
	shardB.WithVectorIndex("", func(vidx VectorIndex) error {
		assert.Equal(t, 0, vidx.Dimension())
		return nil
	})

	// Tenant A receives a conflicting batch (3D, 4D) + non-vectorized object
	conflictingA1 := createRandomObjects(r, class.Class, 1, 3)[0]
	conflictingA2 := createRandomObjects(r, class.Class, 1, 4)[0]
	nonVecA := createRandomObjects(r, class.Class, 1, 0)[0]
	batchA := []*storobj.Object{conflictingA1, conflictingA2, nonVecA}

	errsA := shardA.PutObjectBatch(ctx, batchA)
	require.Len(t, errsA, 3)

	expectedErr := `target vector "": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
	require.Error(t, errsA[0])
	assert.Equal(t, expectedErr, errsA[0].Error())
	require.Error(t, errsA[1])
	assert.Equal(t, expectedErr, errsA[1].Error())
	require.NoError(t, errsA[2])

	// Tenant A: conflicting objects not in LSM, non-vec in LSM, index remains 0
	retA1, err := shardA.ObjectByID(ctx, conflictingA1.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	assert.Nil(t, retA1)

	retA2, err := shardA.ObjectByID(ctx, conflictingA2.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	assert.Nil(t, retA2)

	retNonVecA, err := shardA.ObjectByID(ctx, nonVecA.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	assert.NotNil(t, retNonVecA)

	shardA.WithVectorIndex("", func(vidx VectorIndex) error {
		assert.Equal(t, 0, vidx.Dimension(), "tenant A index must remain uninitialized")
		return nil
	})

	// Tenant B receives a uniform batch (5D)
	batchB := createRandomObjects(r, class.Class, 2, 5)
	errsB := shardB.PutObjectBatch(ctx, batchB)
	require.Len(t, errsB, 2)
	for _, err := range errsB {
		require.NoError(t, err)
	}

	// Tenant B: objects in LSM, index establishes dim 5
	for _, obj := range batchB {
		retB, err := shardB.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retB)
	}
	shardB.WithVectorIndex("", func(vidx VectorIndex) error {
		assert.Equal(t, 5, vidx.Dimension(), "tenant B index must establish dim 5")
		return nil
	})

	// Tenant B search succeeds
	searchResB, _, err := shardB.ObjectVectorSearch(ctx, []models.Vector{batchB[0].Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, searchResB)

	// Tenant A now receives a uniform batch (4D)
	subsequentA := createRandomObjects(r, class.Class, 2, 4)
	errsSubA := shardA.PutObjectBatch(ctx, subsequentA)
	require.Len(t, errsSubA, 2)
	for _, err := range errsSubA {
		require.NoError(t, err)
	}

	for _, obj := range subsequentA {
		retA, err := shardA.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retA)
	}
	shardA.WithVectorIndex("", func(vidx VectorIndex) error {
		assert.Equal(t, 4, vidx.Dimension(), "tenant A index must now establish dim 4")
		return nil
	})

	// Tenant A search succeeds
	searchResA, _, err := shardA.ObjectVectorSearch(ctx, []models.Vector{subsequentA[0].Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, searchResA)
}

func setupTestMultiTenantShards(t testing.TB, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig, tenants ...string,
) (map[string]ShardLike, *Index) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	maxResults := int64(10_000)

	builder := NewMultiTenantShardingStateBuilder().
		WithIndexName("multi-tenant-index").
		WithNodePrefix("node").
		WithReplicationFactor(1)
	for _, tenant := range tenants {
		builder = builder.WithTenant(tenant, "HOT")
	}
	shardState := builder.Build()

	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlyClass(mock.Anything).RunAndReturn(func(name string) *models.Class {
		return &models.Class{Class: name}
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  tmpDir,
		QueryMaximumResults:       maxResults,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(true),
		AsyncIndexingEnabled:      false,
		HaltForTransferTimeout:    config.DefaultHaltForTransferTimeout,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.Nil(t, err)
	installNoLiveReindexLookup(repo)
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}

	iic := schema.InvertedIndexConfig{}
	if class.InvertedIndexConfig != nil {
		iic = inverted.ConfigFromModel(class.InvertedIndexConfig)
	}

	metrics, err := NewMetrics(logger, nil, class.Class, "")
	require.NoError(t, err)

	localNodeName := "node1"

	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().GetWriteReplicasLocation(class.Class, mock.Anything, mock.Anything).Return(
		types.WriteReplicaSet{
			Replicas: []types.Replica{{NodeName: localNodeName, ShardName: "shard1", HostAddr: "127.0.0.1"}},
		}, nil,
	).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(class.Class, mock.Anything, mock.Anything).Return(
		types.ReadReplicaSet{
			Replicas: []types.Replica{{NodeName: localNodeName, ShardName: "shard1", HostAddr: "127.0.0.1"}},
		}, nil,
	).Maybe()
	mockRouter.EXPECT().
		BuildRoutingPlanOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(tenant, shard string, cl types.ConsistencyLevel, direct string) types.RoutingPlanBuildOptions {
			return types.RoutingPlanBuildOptions{Shard: shard, Tenant: tenant, ConsistencyLevel: cl}
		}).
		Maybe()
	mockRouter.EXPECT().
		BuildReadRoutingPlan(mock.Anything).
		Return(types.ReadRoutingPlan{
			LocalHostname: "127.0.0.1",
			ReplicaSet: types.ReadReplicaSet{
				Replicas: []types.Replica{{NodeName: localNodeName, ShardName: "shard1", HostAddr: "127.0.0.1"}},
			},
		}, nil).
		Maybe()

	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname(mock.Anything).Return("127.0.0.1", true).Maybe()

	getDeletionStrategy := func() string {
		return models.ReplicationConfigDeletionStrategyNoAutomatedResolution
	}
	repClient := &FakeReplicationClient{}
	replicator, err := replica.NewReplicator(
		class.Class,
		mockRouter,
		nodeResolver,
		localNodeName,
		getDeletionStrategy,
		repClient,
		monitoring.GetMetrics(),
		logger,
	)
	require.NoError(t, err)

	idx := &Index{
		Config: IndexConfig{
			RootPath:               tmpDir,
			ClassName:              schema.ClassName(class.Class),
			QueryMaximumResults:    maxResults,
			ReplicationFactor:      1,
			NodeName:               "node1",
			EnableLazyLoadShards:   true,
			HaltForTransferTimeout: config.DefaultHaltForTransferTimeout,
		},
		metrics:                metrics,
		partitioningEnabled:    shardState.PartitioningEnabled,
		invertedIndexConfig:    iic,
		vectorIndexUserConfig:  vic,
		vectorIndexUserConfigs: map[string]schemaConfig.VectorIndexConfig{},
		logger:                 logger,
		getSchema:              schemaGetter,
		schemaReader:           mockSchemaReader,
		centralJobQueue:        repo.jobQueueCh,
		stopwords:              nil,
		allocChecker:           memwatch.NewDummyMonitor(),
		shardCreateLocks:       esync.NewKeyRWLocker(),
		backupLock:             esync.NewKeyRWLocker(),
		scheduler:              repo.scheduler,
		shardLoadLimiter:       loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		shardReindexer:         NewShardReindexerV3Noop(),
		bitmapBufPool:          roaringset.NewBitmapBufPoolNoop(),
		replicator:             replicator,
		router:                 mockRouter,
		db:                     repo,
	}
	idx.stopwordProvider.Store(stopwords.NewProvider(nil, nil))
	idx.closingCtx, idx.closingCancel = context.WithCancel(context.Background())
	idx.initCycleCallbacksNoop()

	shards := make(map[string]ShardLike)
	for _, tenant := range tenants {
		shard, err := idx.initShard(ctx, tenant, class, nil, idx.Config.EnableLazyLoadShards, true)
		require.NoError(t, err)
		idx.shards.Store(tenant, shard)
		shards[tenant] = idx.shards.Load(tenant)
	}

	return shards, idx
}

func createNamedVectorObject(className string, vectors map[string][]float32) *storobj.Object {
	obj := testObject(className)
	obj.Vectors = vectors
	return obj
}

func createMultiVectorObject(className string, multiVectors map[string][][]float32) *storobj.Object {
	obj := testObject(className)
	obj.MultiVectors = multiVectors
	return obj
}

func TestShard_BatchDimensionValidation_NamedVectors(t *testing.T) {
	ctx := testCtx()

	t.Run("established collection with named vectors", func(t *testing.T) {
		className := "NamedVectorsEstablished"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"target1": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
				"target2": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"target1": hnsw.NewDefaultUserConfig(),
				"target2": hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Step 1: Establish dimensions = 4 for target1 and target2 with an initial batch
		initialObj := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})
		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initialObj})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		shardLike.WithVectorIndex("target1", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})
		shardLike.WithVectorIndex("target2", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Step 2: Batch containing:
		// - Obj 0: Valid on both target1 (4D) and target2 (4D)
		// - Obj 1: Valid on target1 (4D), INVALID on target2 (3D)
		// - Obj 2: INVALID on target1 (5D), valid on target2 (4D)
		objValid := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})
		objBadTarget2 := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7}, // 3D != 4D
		})
		objBadTarget1 := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4, 0.5}, // 5D != 4D
			"target2": {0.5, 0.6, 0.7, 0.8},
		})

		batch := []*storobj.Object{objValid, objBadTarget2, objBadTarget1}
		batchErrs := shardLike.PutObjectBatch(ctx, batch)
		require.Len(t, batchErrs, 3)

		// Obj 0 must succeed
		require.NoError(t, batchErrs[0])

		// Obj 1 must fail with target2 dimension mismatch
		require.Error(t, batchErrs[1])
		assert.Equal(t, `target vector "target2": dimension mismatch: expected 4, got 3`, batchErrs[1].Error())

		// Obj 2 must fail with target1 dimension mismatch
		require.Error(t, batchErrs[2])
		assert.Equal(t, `target vector "target1": dimension mismatch: expected 4, got 5`, batchErrs[2].Error())

		// Step 3: Verify LSM store
		// Valid object MUST exist in LSM
		retValid, err := shardLike.ObjectByID(ctx, objValid.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retValid, "valid object must exist in LSM")

		// Rejected objects MUST NOT exist in LSM
		retBad2, err := shardLike.ObjectByID(ctx, objBadTarget2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBad2, "rejected object 1 must not exist in LSM")

		retBad1, err := shardLike.ObjectByID(ctx, objBadTarget1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBad1, "rejected object 2 must not exist in LSM")

		// Step 4: Vector search on both targets succeeds for valid object
		res1, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{objValid.Vectors["target1"]}, []string{"target1"}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res1)

		res2, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{objValid.Vectors["target2"]}, []string{"target2"}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res2)
	})

	t.Run("uninitialized collection with conflicting named vector dimensions", func(t *testing.T) {
		className := "NamedVectorsUninitialized"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"targetA": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
				"targetB": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"targetA": hnsw.NewDefaultUserConfig(),
				"targetB": hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Both start uninitialized
		shardLike.WithVectorIndex("targetA", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})

		// Batch has conflicting dimensions for targetA: 3D and 4D
		obj1 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3},
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})
		obj2 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4},
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})

		batchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj1, obj2})
		require.Len(t, batchErrs, 2)

		expectedErr := `target vector "targetA": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
		require.Error(t, batchErrs[0])
		assert.Equal(t, expectedErr, batchErrs[0].Error())
		require.Error(t, batchErrs[1])
		assert.Equal(t, expectedErr, batchErrs[1].Error())

		// Neither object in LSM
		ret1, err := shardLike.ObjectByID(ctx, obj1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret1)

		ret2, err := shardLike.ObjectByID(ctx, obj2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret2)

		// Indexes remain uninitialized
		shardLike.WithVectorIndex("targetA", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})
	})
}

func TestShard_BatchDimensionValidation_MultiVectors(t *testing.T) {
	ctx := testCtx()

	t.Run("inconsistent token lengths in single object rejected before LSM", func(t *testing.T) {
		className := "MultiVectorInconsistentTokens"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"colbert": hnsw.NewDefaultMultiVectorUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Token 0 is 3D, Token 1 is 2D
		badObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3},
				{0.4, 0.5},
			},
		})

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{badObj})
		require.Len(t, errs, 1)
		require.Error(t, errs[0])
		assert.Equal(t, `target vector "colbert": multi vector array consists of vectors with varying dimensions`, errs[0].Error())

		// Object must NOT exist in LSM
		ret, err := shardLike.ObjectByID(ctx, badObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret, "object with inconsistent tokens must not exist in LSM")

		// Index dimension remains 0
		shardLike.WithVectorIndex("colbert", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})
	})

	t.Run("conflicting token dimensions on uninitialized multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorConflictingUninitialized"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"colbert": hnsw.NewDefaultMultiVectorUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Obj 0 has 3D tokens, Obj 1 has 4D tokens
		obj3D := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3},
				{0.4, 0.5, 0.6},
			},
		})
		obj4D := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
			},
		})

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj3D, obj4D})
		require.Len(t, errs, 2)

		expectedErr := `target vector "colbert": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
		require.Error(t, errs[0])
		assert.Equal(t, expectedErr, errs[0].Error())
		require.Error(t, errs[1])
		assert.Equal(t, expectedErr, errs[1].Error())

		// Neither in LSM
		ret0, err := shardLike.ObjectByID(ctx, obj3D.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret0)

		ret1, err := shardLike.ObjectByID(ctx, obj4D.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret1)

		// Index dimension remains 0
		shardLike.WithVectorIndex("colbert", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})
	})

	t.Run("wrong token dimension on established multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorEstablishedMismatch"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"colbert": hnsw.NewDefaultMultiVectorUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Step 1: Establish dimension = 4
		initialObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
				{0.5, 0.6, 0.7, 0.8},
			},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initialObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		shardLike.WithVectorIndex("colbert", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Step 2: Batch with valid 4D object and invalid 3D object
		validObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.2, 0.3, 0.4, 0.5},
			},
		})
		invalidObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3},
			},
		})

		batchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{validObj, invalidObj})
		require.Len(t, batchErrs, 2)

		require.NoError(t, batchErrs[0])
		require.Error(t, batchErrs[1])
		assert.Equal(t, `target vector "colbert": dimension mismatch: expected 4, got 3`, batchErrs[1].Error())

		// Valid object in LSM, invalid object NOT in LSM
		retValid, err := shardLike.ObjectByID(ctx, validObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retValid)

		retInvalid, err := shardLike.ObjectByID(ctx, invalidObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retInvalid)
	})

	t.Run("inconsistent token lengths on established multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorInconsistentTokensEstablished"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"colbert": hnsw.NewDefaultMultiVectorUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Step 1: Establish dimension = 4
		initialObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
			},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initialObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		// Step 2: Batch with object having varying tokens [4D, 3D]
		badObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
				{0.1, 0.2, 0.3},
			},
		})

		batchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{badObj})
		require.Len(t, batchErrs, 1)
		require.Error(t, batchErrs[0])
		assert.Equal(t, `target vector "colbert": multi vector array consists of vectors with varying dimensions`, batchErrs[0].Error())

		ret, err := shardLike.ObjectByID(ctx, badObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret, "object with inconsistent tokens must not exist in LSM")
	})
}

func TestShard_BatchDimensionValidation_AsyncParity(t *testing.T) {
	ctx := testCtx()

	t.Run("ghost object prevention and queue exclusion on established collection", func(t *testing.T) {
		className := "AsyncParityEstablishedGhost"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		r := getRandomSeed()

		// Step 1: Establish dimension = 4
		initialBatch := createRandomObjects(r, className, 2, 4)
		errs := shardLike.PutObjectBatch(ctx, initialBatch)
		require.Len(t, errs, 2)
		for _, err := range errs {
			require.NoError(t, err)
		}
		waitForVectorQueue(t, shardLike, "")

		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Step 2: Mixed batch: 2 valid (4D), 2 invalid (5D, 3D)
		valid1 := createRandomObjects(r, className, 1, 4)[0]
		invalid1 := createRandomObjects(r, className, 1, 5)[0]
		valid2 := createRandomObjects(r, className, 1, 4)[0]
		invalid2 := createRandomObjects(r, className, 1, 3)[0]

		mixedBatch := []*storobj.Object{valid1, invalid1, valid2, invalid2}
		errs = shardLike.PutObjectBatch(ctx, mixedBatch)
		require.Len(t, errs, 4)

		require.NoError(t, errs[0])
		require.Error(t, errs[1])
		assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, errs[1].Error())
		require.NoError(t, errs[2])
		require.Error(t, errs[3])
		assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 3`, errs[3].Error())

		// Step 3: Rejected objects must NEVER exist in LSM store
		retInv1, err := shardLike.ObjectByID(ctx, invalid1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retInv1, "invalid object 1 must not exist in LSM")

		retInv2, err := shardLike.ObjectByID(ctx, invalid2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retInv2, "invalid object 2 must not exist in LSM")

		// Valid objects exist in LSM
		retVal1, err := shardLike.ObjectByID(ctx, valid1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retVal1, "valid object 1 must exist in LSM")

		retVal2, err := shardLike.ObjectByID(ctx, valid2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retVal2, "valid object 2 must exist in LSM")

		// Step 4: Wait for queue draining - workers process only valid vectors without errors
		waitForVectorQueue(t, shardLike, "")

		// Total counter is 4 (2 initial + 2 valid)
		assert.Equal(t, 4, int(shardLike.Counter().Get()))

		// Step 5: Vector search succeeds
		res, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{valid1.Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("uninitialized collection conflict rejection and queue clean in async mode", func(t *testing.T) {
		className := "AsyncParityUninitializedConflict"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		r := getRandomSeed()

		// Verify uninitialized
		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})

		// Conflicting batch: 3D, 4D, 3D, non-vectorized
		conflicting1 := createRandomObjects(r, className, 1, 3)[0]
		conflicting2 := createRandomObjects(r, className, 1, 4)[0]
		conflicting3 := createRandomObjects(r, className, 1, 3)[0]
		nonVec := createRandomObjects(r, className, 1, 0)[0]

		batch := []*storobj.Object{conflicting1, conflicting2, conflicting3, nonVec}
		errs := shardLike.PutObjectBatch(ctx, batch)
		require.Len(t, errs, 4)

		expectedErr := `target vector "": conflicting dimensions in batch for uninitialized index (found [3, 4]); cannot establish dimension`
		require.Error(t, errs[0])
		assert.Equal(t, expectedErr, errs[0].Error())
		require.Error(t, errs[1])
		assert.Equal(t, expectedErr, errs[1].Error())
		require.Error(t, errs[2])
		assert.Equal(t, expectedErr, errs[2].Error())
		require.NoError(t, errs[3])

		waitForVectorQueue(t, shardLike, "")

		// Conflicting objects not in LSM
		ret1, err := shardLike.ObjectByID(ctx, conflicting1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret1)
		ret2, err := shardLike.ObjectByID(ctx, conflicting2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret2)
		ret3, err := shardLike.ObjectByID(ctx, conflicting3.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret3)

		// Non-vectorized object is in LSM
		retNonVec, err := shardLike.ObjectByID(ctx, nonVec.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retNonVec)

		// Index dimension remains 0
		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})

		// Subsequent uniform batch establishes dimension 4
		subsequentBatch := createRandomObjects(r, className, 2, 4)
		subErrs := shardLike.PutObjectBatch(ctx, subsequentBatch)
		require.Len(t, subErrs, 2)
		for _, err := range subErrs {
			require.NoError(t, err)
		}

		waitForVectorQueue(t, shardLike, "")

		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Search succeeds
		res, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{subsequentBatch[0].Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("named vectors async parity", func(t *testing.T) {
		className := "AsyncParityNamedVectors"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"target1": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
				"target2": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"target1": hnsw.NewDefaultUserConfig(),
				"target2": hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Establish 4D for both targets
		initObj := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})
		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		waitForVectorQueue(t, shardLike, "target1")
		waitForVectorQueue(t, shardLike, "target2")

		shardLike.WithVectorIndex("target1", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})
		shardLike.WithVectorIndex("target2", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Mixed batch:
		// objValid: target1 (4D), target2 (4D)
		// objBad2:  target1 (4D), target2 (3D)
		// objBad1:  target1 (5D), target2 (4D)
		objValid := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})
		objBad2 := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7},
		})
		objBad1 := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4, 0.5},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})

		batch := []*storobj.Object{objValid, objBad2, objBad1}
		batchErrs := shardLike.PutObjectBatch(ctx, batch)
		require.Len(t, batchErrs, 3)

		require.NoError(t, batchErrs[0])
		require.Error(t, batchErrs[1])
		assert.Equal(t, `target vector "target2": dimension mismatch: expected 4, got 3`, batchErrs[1].Error())
		require.Error(t, batchErrs[2])
		assert.Equal(t, `target vector "target1": dimension mismatch: expected 4, got 5`, batchErrs[2].Error())

		// Rejected objects not in LSM
		retBad2, err := shardLike.ObjectByID(ctx, objBad2.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBad2)

		retBad1, err := shardLike.ObjectByID(ctx, objBad1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBad1)

		// Valid object in LSM
		retVal, err := shardLike.ObjectByID(ctx, objValid.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retVal)

		waitForVectorQueue(t, shardLike, "target1")
		waitForVectorQueue(t, shardLike, "target2")

		// Search succeeds on both targets
		res1, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{objValid.Vectors["target1"]}, []string{"target1"}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res1)

		res2, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{objValid.Vectors["target2"]}, []string{"target2"}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res2)
	})

	t.Run("multi-vectors async parity", func(t *testing.T) {
		className := "AsyncParityMultiVectors"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"colbert": {VectorIndexType: hnsw.NewDefaultMultiVectorUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"colbert": hnsw.NewDefaultMultiVectorUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Establish 4D token dimension
		initObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
				{0.5, 0.6, 0.7, 0.8},
			},
		})
		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		waitForVectorQueue(t, shardLike, "colbert")

		shardLike.WithVectorIndex("colbert", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Batch: valid (4D), invalid token dim (3D), inconsistent tokens ([4D, 3D])
		validObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.2, 0.3, 0.4, 0.5},
			},
		})
		badDimObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3},
			},
		})
		badTokensObj := createMultiVectorObject(className, map[string][][]float32{
			"colbert": {
				{0.1, 0.2, 0.3, 0.4},
				{0.1, 0.2, 0.3},
			},
		})

		batch := []*storobj.Object{validObj, badDimObj, badTokensObj}
		batchErrs := shardLike.PutObjectBatch(ctx, batch)
		require.Len(t, batchErrs, 3)

		require.NoError(t, batchErrs[0])
		require.Error(t, batchErrs[1])
		assert.Equal(t, `target vector "colbert": dimension mismatch: expected 4, got 3`, batchErrs[1].Error())
		require.Error(t, batchErrs[2])
		assert.Equal(t, `target vector "colbert": multi vector array consists of vectors with varying dimensions`, batchErrs[2].Error())

		// Invalid objects not in LSM
		retBadDim, err := shardLike.ObjectByID(ctx, badDimObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBadDim)

		retBadTokens, err := shardLike.ObjectByID(ctx, badTokensObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBadTokens)

		// Valid object in LSM
		retVal, err := shardLike.ObjectByID(ctx, validObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retVal)

		waitForVectorQueue(t, shardLike, "colbert")
	})

	t.Run("single-object write parity with batch write in async mode", func(t *testing.T) {
		className := "AsyncParitySingleObject"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		r := getRandomSeed()

		// PutObject establishes dimension = 4
		initObj := createRandomObjects(r, className, 1, 4)[0]
		err := shardLike.PutObject(ctx, initObj)
		require.NoError(t, err)

		waitForVectorQueue(t, shardLike, "")

		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Single-object write with wrong dimension (5D) is rejected upfront
		badSingleObj := createRandomObjects(r, className, 1, 5)[0]
		err = shardLike.PutObject(ctx, badSingleObj)
		require.Error(t, err)
		require.ErrorIs(t, err, common.ErrWrongDimensions)

		// Verify rejected single-object is NOT in LSM store
		retBad, err := shardLike.ObjectByID(ctx, badSingleObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBad, "rejected single object must not exist in LSM")

		// Batch write with wrong dimension (5D) is also rejected upfront
		badBatchObj := createRandomObjects(r, className, 1, 5)[0]
		batchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{badBatchObj})
		require.Len(t, batchErrs, 1)
		require.Error(t, batchErrs[0])
		assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, batchErrs[0].Error())

		// Verify rejected batch object is NOT in LSM store
		retBatchBad, err := shardLike.ObjectByID(ctx, badBatchObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBatchBad, "rejected batch object must not exist in LSM")

		// Valid batch object succeeds
		validBatchObj := createRandomObjects(r, className, 1, 4)[0]
		validBatchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{validBatchObj})
		require.Len(t, validBatchErrs, 1)
		require.NoError(t, validBatchErrs[0])

		waitForVectorQueue(t, shardLike, "")

		retValid, err := shardLike.ObjectByID(ctx, validBatchObj.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retValid, "valid batch object must exist in LSM")

		// Search succeeds
		res, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{validBatchObj.Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("ghost object prevention when queue has established dimension but index worker has not processed it yet", func(t *testing.T) {
		className := "AsyncQueueEstablishedBeforeWorker"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		r := getRandomSeed()

		// Pause the vector index queue before any writes so workers do not drain the batch
		shardLike.WithVectorIndexQueue("", func(q *VectorIndexQueue) error {
			require.NotNil(t, q)
			require.NoError(t, q.Pause(ctx))
			return nil
		})

		// Batch 1: establishes dimension 4 in the async queue
		batch1 := createRandomObjects(r, className, 2, 4)
		errs1 := shardLike.PutObjectBatch(ctx, batch1)
		require.Len(t, errs1, 2)
		for _, err := range errs1 {
			require.NoError(t, err)
		}

		// Verify:
		// 1. Vector index dimension is STILL 0 because queue is paused and worker hasn't processed batch 1!
		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension())
			return nil
		})
		// 2. Vector index queue tracks dimension 4
		shardLike.WithVectorIndexQueue("", func(q *VectorIndexQueue) error {
			assert.Equal(t, 4, q.Dimension())
			return nil
		})

		// Batch 2: arrives with dimension 5 while workers haven't processed batch 1!
		batch2 := createRandomObjects(r, className, 1, 5)
		errs2 := shardLike.PutObjectBatch(ctx, batch2)
		require.Len(t, errs2, 1)
		require.Error(t, errs2[0])
		assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, errs2[0].Error())

		// Batch 2 object must NOT exist in the LSM store!
		retBatch2, err := shardLike.ObjectByID(ctx, batch2[0].ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, retBatch2, "rejected batch 2 object must not exist in LSM")

		// Resume queue and drain
		shardLike.WithVectorIndexQueue("", func(q *VectorIndexQueue) error {
			q.Resume()
			return nil
		})
		waitForVectorQueue(t, shardLike, "")

		// Index dimension is now 4
		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Batch 1 objects exist and are searchable
		retBatch1, err := shardLike.ObjectByID(ctx, batch1[0].ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, retBatch1)
	})
}

func TestShard_BatchDimensionValidation_UnmatchedTargetVector(t *testing.T) {
	ctx := testCtx()
	className := "UnmatchedTargetVectorTest"
	class := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			"validTarget": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
		},
	}
	vic := hnsw.UserConfig{Distance: "cosine"}
	shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			"validTarget": hnsw.NewDefaultUserConfig(),
		}
	})
	defer func() {
		require.Nil(t, idx.drop())
		require.Nil(t, os.RemoveAll(idx.Config.RootPath))
	}()

	// Object providing an unknown target vector "unknownTarget"
	badObj := createNamedVectorObject(className, map[string][]float32{
		"unknownTarget": {0.1, 0.2, 0.3},
	})
	goodObj := createNamedVectorObject(className, map[string][]float32{
		"validTarget": {0.1, 0.2, 0.3, 0.4},
	})

	batch := []*storobj.Object{badObj, goodObj}
	errs := shardLike.PutObjectBatch(ctx, batch)
	require.Len(t, errs, 2)

	require.Error(t, errs[0])
	assert.Equal(t, `target vector "unknownTarget": vector index not found`, errs[0].Error())
	require.NoError(t, errs[1])

	// badObj must NOT exist in the LSM store!
	retBad, err := shardLike.ObjectByID(ctx, badObj.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	assert.Nil(t, retBad, "object with unmatched target vector must not exist in LSM")

	// goodObj must exist in the LSM store
	retGood, err := shardLike.ObjectByID(ctx, goodObj.ID(), search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	assert.NotNil(t, retGood, "valid object must exist in LSM")
}

func TestShard_BatchDimensionValidation_OrderIndependenceAndFiltering(t *testing.T) {
	ctx := testCtx()

	t.Run("order invariance: uninitialized target A and established target B", func(t *testing.T) {
		className := "OrderInvarianceUninitAEstB"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"targetA": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
				"targetB": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"targetA": hnsw.NewDefaultUserConfig(),
				"targetB": hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Establish targetB at 4D with initial object
		initObj := createNamedVectorObject(className, map[string][]float32{
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		shardLike.WithVectorIndex("targetA", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension(), "targetA should be uninitialized")
			return nil
		})
		shardLike.WithVectorIndex("targetB", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension(), "targetB should be established at 4D")
			return nil
		})

		// Obj 0: targetA=3D, targetB=4D (valid on established B)
		// Obj 1: targetA=4D, targetB=5D (invalid on established B: 5D != 4D)
		obj0 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3},
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})
		obj1 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4},
			"targetB": {0.1, 0.2, 0.3, 0.4, 0.5},
		})

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)

		// Obj 0 must succeed and establish targetA at 3D
		require.NoError(t, errs[0])
		// Obj 1 must fail on targetB dimension mismatch
		require.Error(t, errs[1])
		assert.Equal(t, `target vector "targetB": dimension mismatch: expected 4, got 5`, errs[1].Error())

		// Obj 0 in LSM, Obj 1 NOT in LSM
		ret0, err := shardLike.ObjectByID(ctx, obj0.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, ret0, "obj0 must exist in LSM")

		ret1, err := shardLike.ObjectByID(ctx, obj1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret1, "obj1 must not exist in LSM")

		// targetA is now established at 3D
		shardLike.WithVectorIndex("targetA", func(vidx VectorIndex) error {
			assert.Equal(t, 3, vidx.Dimension(), "targetA dimension should be established to 3")
			return nil
		})
	})

	t.Run("order invariance: established target A and uninitialized target B", func(t *testing.T) {
		className := "OrderInvarianceEstAUninitB"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"targetA": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
				"targetB": {VectorIndexType: hnsw.NewDefaultUserConfig().IndexType(), VectorIndexConfig: hnsw.NewDefaultUserConfig()},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"targetA": hnsw.NewDefaultUserConfig(),
				"targetB": hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Establish targetA at 4D with initial object
		initObj := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		shardLike.WithVectorIndex("targetA", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension(), "targetA should be established at 4D")
			return nil
		})
		shardLike.WithVectorIndex("targetB", func(vidx VectorIndex) error {
			assert.Equal(t, 0, vidx.Dimension(), "targetB should be uninitialized")
			return nil
		})

		// Obj 0: targetA=4D, targetB=3D (valid on established A)
		// Obj 1: targetA=5D, targetB=4D (invalid on established A: 5D != 4D)
		obj0 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4},
			"targetB": {0.1, 0.2, 0.3},
		})
		obj1 := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4, 0.5},
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)

		// Obj 0 must succeed and establish targetB at 3D
		require.NoError(t, errs[0])
		// Obj 1 must fail on targetA dimension mismatch
		require.Error(t, errs[1])
		assert.Equal(t, `target vector "targetA": dimension mismatch: expected 4, got 5`, errs[1].Error())

		// Obj 0 in LSM, Obj 1 NOT in LSM
		ret0, err := shardLike.ObjectByID(ctx, obj0.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, ret0, "obj0 must exist in LSM")

		ret1, err := shardLike.ObjectByID(ctx, obj1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.Nil(t, ret1, "obj1 must not exist in LSM")

		// targetB is now established at 3D
		shardLike.WithVectorIndex("targetB", func(vidx VectorIndex) error {
			assert.Equal(t, 3, vidx.Dimension(), "targetB dimension should be established to 3")
			return nil
		})
	})

	t.Run("superseded duplicate UUIDs in batch with different dimensions on uninitialized collection", func(t *testing.T) {
		className := "DuplicatesUninitCollection"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Duplicate UUID with 3D (superseded) and 4D (retained)
		dupID := strfmt.UUID("11111111-1111-1111-1111-111111111111")
		obj0 := testObject(className)
		obj0.SetID(dupID)
		obj0.Vector = []float32{0.1, 0.2, 0.3} // 3D

		obj1 := testObject(className)
		obj1.SetID(dupID)
		obj1.Vector = []float32{0.1, 0.2, 0.3, 0.4} // 4D

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)
		require.NoError(t, errs[0], "superseded duplicate should not have error")
		require.NoError(t, errs[1], "retained duplicate should succeed")

		// Retained object in LSM with 4D vector
		ret, err := shardLike.ObjectByID(ctx, dupID, search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, ret)
		assert.Equal(t, 4, len(ret.Vector))

		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension(), "dimension should be established to 4")
			return nil
		})
	})

	t.Run("superseded duplicate UUIDs in batch with different dimensions on established collection", func(t *testing.T) {
		className := "DuplicatesEstablishedCollection"
		class := &models.Class{Class: className}
		vic := hnsw.NewDefaultUserConfig()

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Establish dimension = 4
		r := getRandomSeed()
		initBatch := createRandomObjects(r, className, 1, 4)
		errs := shardLike.PutObjectBatch(ctx, initBatch)
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		shardLike.WithVectorIndex("", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})

		// Batch has superseded duplicate with wrong dim (3D) and retained entry with valid dim (4D)
		dupID := strfmt.UUID("22222222-2222-2222-2222-222222222222")
		obj0 := testObject(className)
		obj0.SetID(dupID)
		obj0.Vector = []float32{0.1, 0.2, 0.3} // 3D (superseded)

		obj1 := testObject(className)
		obj1.SetID(dupID)
		obj1.Vector = []float32{0.1, 0.2, 0.3, 0.4} // 4D (retained)

		errs = shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)
		require.NoError(t, errs[0], "superseded duplicate should not fail batch")
		require.NoError(t, errs[1], "retained duplicate should succeed")

		ret, err := shardLike.ObjectByID(ctx, dupID, search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, ret)
		assert.Equal(t, 4, len(ret.Vector))
	})

	t.Run("skipped vector index allows mixed dimensions without error", func(t *testing.T) {
		className := "SkippedVectorIndexMixedDims"
		class := &models.Class{
			Class: className,
			VectorIndexConfig: hnsw.UserConfig{
				Skip: true,
			},
		}
		vic := hnsw.UserConfig{Skip: true}

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Mixed dimensions: 3D, 5D, 4D
		obj0 := testObject(className)
		obj0.Vector = []float32{0.1, 0.2, 0.3}

		obj1 := testObject(className)
		obj1.Vector = []float32{0.1, 0.2, 0.3, 0.4, 0.5}

		obj2 := testObject(className)
		obj2.Vector = []float32{0.1, 0.2, 0.3, 0.4}

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1, obj2})
		require.Len(t, errs, 3)
		for i, err := range errs {
			require.NoError(t, err, "object %d should succeed on skipped index", i)
		}

		// All 3 objects exist in LSM
		for _, obj := range []*storobj.Object{obj0, obj1, obj2} {
			ret, err := shardLike.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, ret, "object %s must exist in LSM", obj.ID())
		}
	})

	t.Run("mixed skipped target and active uninitialized target in same batch", func(t *testing.T) {
		className := "MixedSkippedAndActiveTarget"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				"skippedTarget": {
					VectorIndexType:   hnsw.NewDefaultUserConfig().IndexType(),
					VectorIndexConfig: hnsw.UserConfig{Skip: true},
				},
				"activeTarget": {
					VectorIndexType:   hnsw.NewDefaultUserConfig().IndexType(),
					VectorIndexConfig: hnsw.NewDefaultUserConfig(),
				},
			},
		}
		vic := hnsw.UserConfig{Distance: "cosine"}
		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"skippedTarget": hnsw.UserConfig{Skip: true},
				"activeTarget":  hnsw.NewDefaultUserConfig(),
			}
		})
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// obj0: skippedTarget=3D, activeTarget=4D
		// obj1: skippedTarget=5D, activeTarget=4D
		obj0 := createNamedVectorObject(className, map[string][]float32{
			"skippedTarget": {0.1, 0.2, 0.3},
			"activeTarget":  {0.1, 0.2, 0.3, 0.4},
		})
		obj1 := createNamedVectorObject(className, map[string][]float32{
			"skippedTarget": {0.1, 0.2, 0.3, 0.4, 0.5},
			"activeTarget":  {0.5, 0.6, 0.7, 0.8},
		})

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		// Both exist in LSM
		ret0, err := shardLike.ObjectByID(ctx, obj0.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, ret0)

		ret1, err := shardLike.ObjectByID(ctx, obj1.ID(), search.SelectProperties{}, additional.Properties{})
		require.NoError(t, err)
		assert.NotNil(t, ret1)

		// activeTarget dimension established to 4
		shardLike.WithVectorIndex("activeTarget", func(vidx VectorIndex) error {
			assert.Equal(t, 4, vidx.Dimension())
			return nil
		})
	})

	t.Run("skipped vector index in async mode allows mixed dimensions", func(t *testing.T) {
		className := "SkippedVectorIndexAsync"
		class := &models.Class{
			Class: className,
			VectorIndexConfig: hnsw.UserConfig{
				Skip: true,
			},
		}
		vic := hnsw.UserConfig{Skip: true}

		shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, true)
		defer func() {
			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		}()

		// Mixed dimensions: 3D, 5D
		obj0 := testObject(className)
		obj0.Vector = []float32{0.1, 0.2, 0.3}

		obj1 := testObject(className)
		obj1.Vector = []float32{0.1, 0.2, 0.3, 0.4, 0.5}

		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{obj0, obj1})
		require.Len(t, errs, 2)
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])

		waitForVectorQueue(t, shardLike, "")

		for _, obj := range []*storobj.Object{obj0, obj1} {
			ret, err := shardLike.ObjectByID(ctx, obj.ID(), search.SelectProperties{}, additional.Properties{})
			require.NoError(t, err)
			assert.NotNil(t, ret, "object %s must exist in LSM", obj.ID())
		}
	})
}

