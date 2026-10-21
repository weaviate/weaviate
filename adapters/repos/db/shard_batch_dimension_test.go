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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
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
