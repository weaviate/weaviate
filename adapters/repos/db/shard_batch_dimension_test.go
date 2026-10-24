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
	"os"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShard_BatchDimensionValidation_GhostObjectPrevention(t *testing.T) {
	testCases := standardVectorIndexTestCases()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BatchDimTest" + t.Name()
			class := &models.Class{Class: className}

			shd, _ := setupTestDimensionShard(t, ctx, class, tc.uc, tc.async)

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
			assertObjectExistsInLSM(t, ctx, shd, invalid1.ID(), false, "rejected object 1 must not exist in LSM storage")
			assertObjectExistsInLSM(t, ctx, shd, invalid2.ID(), false, "rejected object 2 must not exist in LSM storage")

			// Valid objects MUST exist in LSM store
			assertObjectExistsInLSM(t, ctx, shd, valid1.ID(), true, "valid object 1 must exist in LSM storage")
			assertObjectExistsInLSM(t, ctx, shd, valid2.ID(), true, "valid object 2 must exist in LSM storage")

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
	testCases := standardVectorIndexTestCases()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "BatchDimPoisonTest" + t.Name()
			class := &models.Class{Class: className}

			shd, _ := setupTestDimensionShard(t, ctx, class, tc.uc, tc.async)

			r := getRandomSeed()

			// Step 1: Verify index is uninitialized (dim == 0)
			assertShardIndexDimension(t, shd, "", 0)

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
			assertObjectExistsInLSM(t, ctx, shd, conflicting1.ID(), false, "conflicting object 1 must not exist in LSM storage")
			assertObjectExistsInLSM(t, ctx, shd, conflicting2.ID(), false, "conflicting object 2 must not exist in LSM storage")
			assertObjectExistsInLSM(t, ctx, shd, conflicting3.ID(), false, "conflicting object 3 must not exist in LSM storage")

			// Non-vectorized object MUST exist in LSM store
			assertObjectExistsInLSM(t, ctx, shd, nonVectorized.ID(), true, "non-vectorized object must exist in LSM storage")

			// Step 5: Verify index remains uninitialized (dim == 0)
			assertShardIndexDimension(t, shd, "", 0, "index must remain uninitialized (D = 0)")

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
				assertObjectExistsInLSM(t, ctx, shd, obj.ID(), true, "subsequent object must exist in LSM storage")
			}

			// Index dimension is now established to 4
			assertShardIndexDimension(t, shd, "", 4, "index dimension must now be established to 4")

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
	idx, _ := setupTestIndexWithTenants(t, ctx, class, vic, false, false, tenants)

	shards := make(map[string]ShardLike)
	for _, tenant := range tenants {
		shard, err := idx.initShard(ctx, tenant, class, nil, idx.Config.EnableLazyLoadShards, true)
		require.NoError(t, err)
		idx.shards.Store(tenant, shard)
		shards[tenant] = idx.shards.Load(tenant)
	}

	return shards, idx
}

func setupTestDimensionShard(t testing.TB, ctx context.Context, class *models.Class, vic schemaConfig.VectorIndexConfig, async bool, opts ...func(*Index)) (ShardLike, *Index) {
	shardLike, idx := testShardWithSettings(t, ctx, class, vic, false, async, opts...)
	t.Cleanup(func() {
		require.Nil(t, idx.drop())
		require.Nil(t, os.RemoveAll(idx.Config.RootPath))
	})
	return shardLike, idx
}

func setupSingleVectorShard(t testing.TB, ctx context.Context, className string, async bool) (ShardLike, *Index) {
	class := &models.Class{Class: className}
	return setupTestDimensionShard(t, ctx, class, hnsw.NewDefaultUserConfig(), async)
}

func setupNamedVectorShard(t testing.TB, ctx context.Context, className string, targetVectors []string, async bool) (ShardLike, *Index) {
	vectorConfig := make(map[string]models.VectorConfig)
	userConfigs := make(map[string]schemaConfig.VectorIndexConfig)
	for _, target := range targetVectors {
		vectorConfig[target] = models.VectorConfig{
			VectorIndexType:   hnsw.NewDefaultUserConfig().IndexType(),
			VectorIndexConfig: hnsw.NewDefaultUserConfig(),
		}
		userConfigs[target] = hnsw.NewDefaultUserConfig()
	}
	class := &models.Class{
		Class:        className,
		VectorConfig: vectorConfig,
	}
	return setupTestDimensionShard(t, ctx, class, hnsw.UserConfig{Distance: "cosine"}, async, func(i *Index) {
		i.vectorIndexUserConfigs = userConfigs
	})
}

func setupMultiVectorShard(t testing.TB, ctx context.Context, className string, targetVector string, async bool) (ShardLike, *Index) {
	class := &models.Class{
		Class: className,
		VectorConfig: map[string]models.VectorConfig{
			targetVector: {
				VectorIndexType:   hnsw.NewDefaultMultiVectorUserConfig().IndexType(),
				VectorIndexConfig: hnsw.NewDefaultMultiVectorUserConfig(),
			},
		},
	}
	return setupTestDimensionShard(t, ctx, class, hnsw.UserConfig{Distance: "cosine"}, async, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
			targetVector: hnsw.NewDefaultMultiVectorUserConfig(),
		}
	})
}

type vectorIndexTestCase struct {
	name  string
	uc    schemaConfig.VectorIndexConfig
	async bool
}

func standardVectorIndexTestCases() []vectorIndexTestCase {
	return []vectorIndexTestCase{
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
}

func assertObjectExistsInLSM(t *testing.T, ctx context.Context, shard ShardLike, id strfmt.UUID, shouldExist bool, msgAndArgs ...interface{}) *storobj.Object {
	t.Helper()
	ret, err := shard.ObjectByID(ctx, id, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	if shouldExist {
		assert.NotNil(t, ret, msgAndArgs...)
	} else {
		assert.Nil(t, ret, msgAndArgs...)
	}
	return ret
}

func assertShardIndexDimension(t *testing.T, shard ShardLike, targetVector string, expectedDim int, msgAndArgs ...interface{}) {
	t.Helper()
	shard.WithVectorIndex(targetVector, func(vidx VectorIndex) error {
		assert.Equal(t, expectedDim, vidx.Dimension(), msgAndArgs...)
		return nil
	})
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
		shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"target1", "target2"}, false)

		// Step 1: Establish dimensions = 4 for target1 and target2 with an initial batch
		initialObj := createNamedVectorObject(className, map[string][]float32{
			"target1": {0.1, 0.2, 0.3, 0.4},
			"target2": {0.5, 0.6, 0.7, 0.8},
		})
		errs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initialObj})
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		assertShardIndexDimension(t, shardLike, "target1", 4)
		assertShardIndexDimension(t, shardLike, "target2", 4)

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
		assertObjectExistsInLSM(t, ctx, shardLike, objValid.ID(), true, "valid object must exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, objBadTarget2.ID(), false, "rejected object 1 must not exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, objBadTarget1.ID(), false, "rejected object 2 must not exist in LSM")

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
		shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"targetA", "targetB"}, false)

		// Both start uninitialized
		assertShardIndexDimension(t, shardLike, "targetA", 0)

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
		assertObjectExistsInLSM(t, ctx, shardLike, obj1.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, obj2.ID(), false)

		// Indexes remain uninitialized
		assertShardIndexDimension(t, shardLike, "targetA", 0)
	})
}

func TestShard_BatchDimensionValidation_MultiVectors(t *testing.T) {
	ctx := testCtx()

	t.Run("inconsistent token lengths in single object rejected before LSM", func(t *testing.T) {
		className := "MultiVectorInconsistentTokens"
		shardLike, _ := setupMultiVectorShard(t, ctx, className, "colbert", false)

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
		assertObjectExistsInLSM(t, ctx, shardLike, badObj.ID(), false, "object with inconsistent tokens must not exist in LSM")

		// Index dimension remains 0
		assertShardIndexDimension(t, shardLike, "colbert", 0)
	})

	t.Run("conflicting token dimensions on uninitialized multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorConflictingUninitialized"
		shardLike, _ := setupMultiVectorShard(t, ctx, className, "colbert", false)

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
		assertObjectExistsInLSM(t, ctx, shardLike, obj3D.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, obj4D.ID(), false)

		// Index dimension remains 0
		assertShardIndexDimension(t, shardLike, "colbert", 0)
	})

	t.Run("wrong token dimension on established multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorEstablishedMismatch"
		shardLike, _ := setupMultiVectorShard(t, ctx, className, "colbert", false)

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

		assertShardIndexDimension(t, shardLike, "colbert", 4)

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
		assertObjectExistsInLSM(t, ctx, shardLike, validObj.ID(), true)
		assertObjectExistsInLSM(t, ctx, shardLike, invalidObj.ID(), false)
	})

	t.Run("inconsistent token lengths on established multi-vector collection rejected before LSM", func(t *testing.T) {
		className := "MultiVectorInconsistentTokensEstablished"
		shardLike, _ := setupMultiVectorShard(t, ctx, className, "colbert", false)

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

		assertObjectExistsInLSM(t, ctx, shardLike, badObj.ID(), false, "object with inconsistent tokens must not exist in LSM")
	})
}

func TestShard_BatchDimensionValidation_AsyncParity(t *testing.T) {
	ctx := testCtx()

	t.Run("ghost object prevention and queue exclusion on established collection", func(t *testing.T) {
		className := "AsyncParityEstablishedGhost"
		shardLike, _ := setupSingleVectorShard(t, ctx, className, true)

		r := getRandomSeed()

		// Step 1: Establish dimension = 4
		initialBatch := createRandomObjects(r, className, 2, 4)
		errs := shardLike.PutObjectBatch(ctx, initialBatch)
		require.Len(t, errs, 2)
		for _, err := range errs {
			require.NoError(t, err)
		}
		waitForVectorQueue(t, shardLike, "")

		assertShardIndexDimension(t, shardLike, "", 4)

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
		assertObjectExistsInLSM(t, ctx, shardLike, invalid1.ID(), false, "invalid object 1 must not exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, invalid2.ID(), false, "invalid object 2 must not exist in LSM")

		// Valid objects exist in LSM
		assertObjectExistsInLSM(t, ctx, shardLike, valid1.ID(), true, "valid object 1 must exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, valid2.ID(), true, "valid object 2 must exist in LSM")

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
		shardLike, _ := setupSingleVectorShard(t, ctx, className, true)

		r := getRandomSeed()

		// Verify uninitialized
		assertShardIndexDimension(t, shardLike, "", 0)

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
		assertObjectExistsInLSM(t, ctx, shardLike, conflicting1.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, conflicting2.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, conflicting3.ID(), false)

		// Non-vectorized object is in LSM
		assertObjectExistsInLSM(t, ctx, shardLike, nonVec.ID(), true)

		// Index dimension remains 0
		assertShardIndexDimension(t, shardLike, "", 0)

		// Subsequent uniform batch establishes dimension 4
		subsequentBatch := createRandomObjects(r, className, 2, 4)
		subErrs := shardLike.PutObjectBatch(ctx, subsequentBatch)
		require.Len(t, subErrs, 2)
		for _, err := range subErrs {
			require.NoError(t, err)
		}

		waitForVectorQueue(t, shardLike, "")

		assertShardIndexDimension(t, shardLike, "", 4)

		// Search succeeds
		res, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{subsequentBatch[0].Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("named vectors async parity", func(t *testing.T) {
		className := "AsyncParityNamedVectors"
		shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"target1", "target2"}, true)

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

		assertShardIndexDimension(t, shardLike, "target1", 4)
		assertShardIndexDimension(t, shardLike, "target2", 4)

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
		assertObjectExistsInLSM(t, ctx, shardLike, objBad2.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, objBad1.ID(), false)

		// Valid object in LSM
		assertObjectExistsInLSM(t, ctx, shardLike, objValid.ID(), true)

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
		shardLike, _ := setupMultiVectorShard(t, ctx, className, "colbert", true)

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

		assertShardIndexDimension(t, shardLike, "colbert", 4)

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
		assertObjectExistsInLSM(t, ctx, shardLike, badDimObj.ID(), false)
		assertObjectExistsInLSM(t, ctx, shardLike, badTokensObj.ID(), false)

		// Valid object in LSM
		assertObjectExistsInLSM(t, ctx, shardLike, validObj.ID(), true)

		waitForVectorQueue(t, shardLike, "colbert")
	})

	t.Run("single-object write parity with batch write in async mode", func(t *testing.T) {
		className := "AsyncParitySingleObject"
		shardLike, _ := setupSingleVectorShard(t, ctx, className, true)

		r := getRandomSeed()

		// PutObject establishes dimension = 4
		initObj := createRandomObjects(r, className, 1, 4)[0]
		err := shardLike.PutObject(ctx, initObj)
		require.NoError(t, err)

		waitForVectorQueue(t, shardLike, "")

		assertShardIndexDimension(t, shardLike, "", 4)

		// Single-object write with wrong dimension (5D) is rejected upfront
		badSingleObj := createRandomObjects(r, className, 1, 5)[0]
		err = shardLike.PutObject(ctx, badSingleObj)
		require.Error(t, err)
		require.ErrorIs(t, err, common.ErrWrongDimensions)

		// Verify rejected single-object is NOT in LSM store
		assertObjectExistsInLSM(t, ctx, shardLike, badSingleObj.ID(), false, "rejected single object must not exist in LSM")

		// Batch write with wrong dimension (5D) is also rejected upfront
		badBatchObj := createRandomObjects(r, className, 1, 5)[0]
		batchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{badBatchObj})
		require.Len(t, batchErrs, 1)
		require.Error(t, batchErrs[0])
		assert.Equal(t, `target vector "": dimension mismatch: expected 4, got 5`, batchErrs[0].Error())

		// Verify rejected batch object is NOT in LSM store
		assertObjectExistsInLSM(t, ctx, shardLike, badBatchObj.ID(), false, "rejected batch object must not exist in LSM")

		// Valid batch object succeeds
		validBatchObj := createRandomObjects(r, className, 1, 4)[0]
		validBatchErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{validBatchObj})
		require.Len(t, validBatchErrs, 1)
		require.NoError(t, validBatchErrs[0])

		waitForVectorQueue(t, shardLike, "")

		assertObjectExistsInLSM(t, ctx, shardLike, validBatchObj.ID(), true, "valid batch object must exist in LSM")

		// Search succeeds
		res, _, err := shardLike.ObjectVectorSearch(ctx, []models.Vector{validBatchObj.Vector}, []string{""}, 0, 10, nil, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("ghost object prevention when queue has established dimension but index worker has not processed it yet", func(t *testing.T) {
		className := "AsyncQueueEstablishedBeforeWorker"
		shardLike, _ := setupSingleVectorShard(t, ctx, className, true)

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
		assertShardIndexDimension(t, shardLike, "", 0)
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
		assertObjectExistsInLSM(t, ctx, shardLike, batch2[0].ID(), false, "rejected batch 2 object must not exist in LSM")

		// Resume queue and drain
		shardLike.WithVectorIndexQueue("", func(q *VectorIndexQueue) error {
			q.Resume()
			return nil
		})
		waitForVectorQueue(t, shardLike, "")

		// Index dimension is now 4
		assertShardIndexDimension(t, shardLike, "", 4)

		// Batch 1 objects exist and are searchable
		assertObjectExistsInLSM(t, ctx, shardLike, batch1[0].ID(), true)
	})
}

func TestShard_BatchDimensionValidation_UnmatchedTargetVector(t *testing.T) {
	ctx := testCtx()
	className := "UnmatchedTargetVectorTest"
	shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"validTarget"}, false)

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
	assertObjectExistsInLSM(t, ctx, shardLike, badObj.ID(), false, "object with unmatched target vector must not exist in LSM")

	// goodObj must exist in the LSM store
	assertObjectExistsInLSM(t, ctx, shardLike, goodObj.ID(), true, "valid object must exist in LSM")
}

func TestShard_BatchDimensionValidation_OrderIndependenceAndFiltering(t *testing.T) {
	ctx := testCtx()

	t.Run("order invariance: uninitialized target A and established target B", func(t *testing.T) {
		className := "OrderInvarianceUninitAEstB"
		shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"targetA", "targetB"}, false)

		// Establish targetB at 4D with initial object
		initObj := createNamedVectorObject(className, map[string][]float32{
			"targetB": {0.1, 0.2, 0.3, 0.4},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		assertShardIndexDimension(t, shardLike, "targetA", 0, "targetA should be uninitialized")
		assertShardIndexDimension(t, shardLike, "targetB", 4, "targetB should be established at 4D")

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
		assertObjectExistsInLSM(t, ctx, shardLike, obj0.ID(), true, "obj0 must exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, obj1.ID(), false, "obj1 must not exist in LSM")
		assertShardIndexDimension(t, shardLike, "targetA", 3, "targetA dimension should be established to 3")
	})

	t.Run("order invariance: established target A and uninitialized target B", func(t *testing.T) {
		className := "OrderInvarianceEstAUninitB"
		shardLike, _ := setupNamedVectorShard(t, ctx, className, []string{"targetA", "targetB"}, false)

		// Establish targetA at 4D with initial object
		initObj := createNamedVectorObject(className, map[string][]float32{
			"targetA": {0.1, 0.2, 0.3, 0.4},
		})
		initErrs := shardLike.PutObjectBatch(ctx, []*storobj.Object{initObj})
		require.Len(t, initErrs, 1)
		require.NoError(t, initErrs[0])

		assertShardIndexDimension(t, shardLike, "targetA", 4, "targetA should be established at 4D")
		assertShardIndexDimension(t, shardLike, "targetB", 0, "targetB should be uninitialized")

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
		assertObjectExistsInLSM(t, ctx, shardLike, obj0.ID(), true, "obj0 must exist in LSM")
		assertObjectExistsInLSM(t, ctx, shardLike, obj1.ID(), false, "obj1 must not exist in LSM")
		assertShardIndexDimension(t, shardLike, "targetB", 3, "targetB dimension should be established to 3")
	})

	t.Run("superseded duplicate UUIDs in batch with different dimensions on uninitialized collection", func(t *testing.T) {
		className := "DuplicatesUninitCollection"
		shardLike, _ := setupSingleVectorShard(t, ctx, className, false)

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
		assertObjectExistsInLSM(t, ctx, shardLike, dupID, true)
		assertShardIndexDimension(t, shardLike, "", 4, "dimension should be established to 4")
	})

	t.Run("superseded duplicate UUIDs in batch with different dimensions on established collection", func(t *testing.T) {
		className := "DuplicatesEstablishedCollection"
		shardLike, _ := setupSingleVectorShard(t, ctx, className, false)

		// Establish dimension = 4
		r := getRandomSeed()
		initBatch := createRandomObjects(r, className, 1, 4)
		errs := shardLike.PutObjectBatch(ctx, initBatch)
		require.Len(t, errs, 1)
		require.NoError(t, errs[0])

		assertShardIndexDimension(t, shardLike, "", 4)

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

		ret := assertObjectExistsInLSM(t, ctx, shardLike, dupID, true)
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
		shardLike, _ := setupTestDimensionShard(t, ctx, class, vic, false)

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
			assertObjectExistsInLSM(t, ctx, shardLike, obj.ID(), true, "object %s must exist in LSM", obj.ID())
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
		shardLike, _ := setupTestDimensionShard(t, ctx, class, vic, false, func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"skippedTarget": hnsw.UserConfig{Skip: true},
				"activeTarget":  hnsw.NewDefaultUserConfig(),
			}
		})

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
		assertObjectExistsInLSM(t, ctx, shardLike, obj0.ID(), true)
		assertObjectExistsInLSM(t, ctx, shardLike, obj1.ID(), true)

		// activeTarget dimension established to 4
		assertShardIndexDimension(t, shardLike, "activeTarget", 4)
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
		shardLike, _ := setupTestDimensionShard(t, ctx, class, vic, true)

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
			assertObjectExistsInLSM(t, ctx, shardLike, obj.ID(), true, "object %s must exist in LSM", obj.ID())
		}
	})
}

func TestShard_AtomicDimensionReservation_ConcurrentBatches(t *testing.T) {
	ctx := testCtx()

	for iter := 0; iter < 10; iter++ {
		className := fmt.Sprintf("AtomicReservationConcurrent_%d", iter)
		shardLike, _ := setupSingleVectorShard(t, ctx, className, false)

		obj3D := testObject(className)
		obj3D.Vector = []float32{0.1, 0.2, 0.3}

		obj4D := testObject(className)
		obj4D.Vector = []float32{0.1, 0.2, 0.3, 0.4}

		barrier := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		var errs3D, errs4D []error

		go func() {
			defer wg.Done()
			<-barrier
			errs3D = shardLike.PutObjectBatch(ctx, []*storobj.Object{obj3D})
		}()

		go func() {
			defer wg.Done()
			<-barrier
			errs4D = shardLike.PutObjectBatch(ctx, []*storobj.Object{obj4D})
		}()

		close(barrier)
		wg.Wait()

		require.Len(t, errs3D, 1)
		require.Len(t, errs4D, 1)

		// Exactly one batch must succeed and the other must fail with dimension mismatch
		if errs3D[0] == nil {
			// 3D won
			require.Error(t, errs4D[0])
			assert.Contains(t, errs4D[0].Error(), "dimension mismatch")

			// 3D in LSM, 4D NOT in LSM (no ghost object)
			assertObjectExistsInLSM(t, ctx, shardLike, obj3D.ID(), true, "winning 3D object must exist in LSM")
			assertObjectExistsInLSM(t, ctx, shardLike, obj4D.ID(), false, "losing 4D object must NEVER exist in LSM")
			assertShardIndexDimension(t, shardLike, "", 3)
		} else {
			// 4D won
			require.NoError(t, errs4D[0])
			require.Error(t, errs3D[0])
			assert.Contains(t, errs3D[0].Error(), "dimension mismatch")

			// 4D in LSM, 3D NOT in LSM (no ghost object)
			assertObjectExistsInLSM(t, ctx, shardLike, obj4D.ID(), true, "winning 4D object must exist in LSM")
			assertObjectExistsInLSM(t, ctx, shardLike, obj3D.ID(), false, "losing 3D object must NEVER exist in LSM")
			assertShardIndexDimension(t, shardLike, "", 4)
		}
	}
}
