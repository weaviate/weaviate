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
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	dynamicindex "github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	hnswindex "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestShard_UpdateStatus(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	t.Run("insert data into shard", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("mark shard readonly and fail to insert", func(t *testing.T) {
		err := shd.SetStatusReadonly("testing")
		require.Nil(t, err)

		err = shd.PutObject(ctx, testObject(className))
		require.Contains(t, err.Error(), storagestate.ErrStatusReadOnly.Error())
		require.Contains(t, err.Error(), "testing")
	})

	t.Run("mark shard ready and insert successfully", func(t *testing.T) {
		err := shd.UpdateStatus(storagestate.StatusReady.String(), "test ready")
		require.Nil(t, err)

		err = shd.PutObject(ctx, testObject(className))
		require.Nil(t, err)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

// tests adding multiple larger batches in parallel using different settings of the goroutine factor.
// In all cases all objects should be added
func TestShard_ParallelBatches(t *testing.T) {
	r := getRandomSeed()
	batches := make([][]*storobj.Object, 4)
	for i := range batches {
		batches[i] = createRandomObjects(r, "TestClass", 1000, 4)
	}
	totalObjects := 1000 * len(batches)
	ctx := testCtx()
	shd, idx := testShard(t, context.Background(), "TestClass")

	// add batches in parallel
	wg := sync.WaitGroup{}
	wg.Add(len(batches))
	for _, batch := range batches {
		go func(localBatch []*storobj.Object) {
			shd.PutObjectBatch(ctx, localBatch)
			wg.Done()
		}(batch)
	}
	wg.Wait()

	require.Equal(t, totalObjects, int(shd.Counter().Get()))
	require.Nil(t, idx.drop())
}

func TestShard_InvalidVectorBatches(t *testing.T) {
	ctx := testCtx()

	class := &models.Class{Class: "TestClass"}

	shd, idx := testShardWithSettings(t, ctx, class, hnsw.NewDefaultUserConfig(), false, false)

	testShard(t, context.Background(), class.Class)

	r := getRandomSeed()

	batchSize := 1000

	validBatch := createRandomObjects(r, class.Class, batchSize, 4)

	shd.PutObjectBatch(ctx, validBatch)
	require.Equal(t, batchSize, int(shd.Counter().Get()))

	invalidBatch := createRandomObjects(r, class.Class, batchSize, 5)

	errs := shd.PutObjectBatch(ctx, invalidBatch)
	require.Len(t, errs, batchSize)
	for _, err := range errs {
		require.ErrorContains(t, err, "new node has a vector with length 5. Existing nodes have vectors with length 4")
	}
	require.Equal(t, batchSize, int(shd.Counter().Get()))

	require.Nil(t, idx.drop())
}

func TestShard_InvalidHFreshBatches(t *testing.T) {
	ctx := testCtx()

	class := &models.Class{Class: "TestClass"}

	shd, idx := testShardWithSettings(t, ctx, class, hfresh.NewDefaultUserConfig(), false, false)

	testShard(t, context.Background(), class.Class)

	r := getRandomSeed()

	batchSize := 1000

	validBatch := createRandomObjects(r, class.Class, batchSize, 4)

	shd.PutObjectBatch(ctx, validBatch)
	require.Equal(t, batchSize, int(shd.Counter().Get()))

	invalidBatch := createRandomObjects(r, class.Class, batchSize, 5)

	errs := shd.PutObjectBatch(ctx, invalidBatch)
	require.Len(t, errs, batchSize)
	for _, err := range errs {
		require.ErrorContains(t, err, "new node has a vector with length 5. Existing nodes have vectors with length 4")
	}
	require.Equal(t, batchSize, int(shd.Counter().Get()))

	require.Nil(t, idx.drop())
}

func TestShard_InvalidMultiVectorBatches(t *testing.T) {
	t.Run("regular multivector", func(t *testing.T) {
		ctx := testCtx()
		class := &models.Class{Class: "TestClass"}
		vectorIndexConfig := hnsw.NewDefaultMultiVectorUserConfig()
		shd, idx := testShardWithSettings(t, ctx, class, vectorIndexConfig, false, false)
		testShard(t, context.Background(), class.Class)
		r := getRandomSeed()
		batchSize := 100
		validBatch := createRandomMultiVectorObjects(r, class.Class, batchSize, 4, 4)
		shd.PutObjectBatch(ctx, validBatch)
		require.Equal(t, batchSize, int(shd.Counter().Get()))
		invalidBatch := createRandomMultiVectorObjects(r, class.Class, batchSize, 2, 5)
		errs := shd.PutObjectBatch(ctx, invalidBatch)
		require.Len(t, errs, batchSize)
		for _, err := range errs {
			require.ErrorContains(t, err, "new node has a multi vector with length 5 at position 0. Existing nodes have vectors with length 4")
		}
		require.Equal(t, batchSize, int(shd.Counter().Get()))
		require.Nil(t, idx.drop())
	})

	t.Run("muvera multivector", func(t *testing.T) {
		ctx := testCtx()
		class := &models.Class{Class: "TestClass"}
		vectorIndexConfig := hnsw.NewDefaultMultiVectorUserConfig()
		vectorIndexConfig.Multivector = hnsw.MultivectorConfig{
			Enabled:      true,
			MuveraConfig: hnsw.MuveraConfig{Enabled: true, KSim: 1, Repetitions: 2, DProjections: 5},
		}
		shd, idx := testShardWithSettings(t, ctx, class, vectorIndexConfig, false, false)
		testShard(t, context.Background(), class.Class)
		r := getRandomSeed()
		batchSize := 100
		validBatch := createRandomMultiVectorObjects(r, class.Class, batchSize, 4, 4)
		shd.PutObjectBatch(ctx, validBatch)
		require.Equal(t, batchSize, int(shd.Counter().Get()))
		invalidBatch := createRandomMultiVectorObjects(r, class.Class, batchSize, 2, 5)
		errs := shd.PutObjectBatch(ctx, invalidBatch)
		require.Len(t, errs, batchSize)
		for _, err := range errs {
			require.ErrorContains(t, err, "new node has a multi vector with length 5 at position 0. Existing nodes have vectors with length 4")
		}
		require.Equal(t, batchSize, int(shd.Counter().Get()))
		require.Nil(t, idx.drop())
	})
}

func TestShard_DebugResetVectorIndex(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, hnsw.UserConfig{}, false, true)

	amount := 1500

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject(className)
		obj.Vector = randVector(3)
		objs = append(objs, obj)
	}

	errs := shd.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	// wait for the first batch to be indexed
	oldIdx, q := getVectorIndexAndQueue(t, shd, "")
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() <= 500 {
			break
		}
	}

	err := shd.DebugResetVectorIndex(ctx, "")
	require.Nil(t, err)

	newIdx, _ := getVectorIndexAndQueue(t, shd, "")

	// the new index should be different from the old one.
	// pointer comparison is enough here
	require.NotEqual(t, oldIdx, newIdx)

	// the reset refills the new index from the object store in the background
	require.Eventually(t, func() bool {
		for _, obj := range objs {
			if !newIdx.ContainsDoc(obj.DocID) {
				return false
			}
		}
		return true
	}, time.Minute, 100*time.Millisecond, "not every object made it into the rebuilt index")
	waitForVectorQueue(t, shd, "")

	// the reset rebuilt at the recorded ID: record ready, storage present
	s := underlyingShard(t, shd)
	rec, ok, err := s.mapping.Get("")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, vectorIndexRecord{PhysicalID: "main", IndexType: "hnsw", State: "ready"}, rec)
	assert.True(t, storageExistsFor(t, s, rec))

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

// A vector added on a running shard is recorded, so the reset finds its
// record and rebuilds at its ID; the record is ready again afterwards.
func TestShard_DebugResetVectorIndex_AddedVector(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, hnsw.UserConfig{}, false, true)
	defer func(path string) {
		require.NoError(t, os.RemoveAll(path))
	}(idx.Config.RootPath)

	require.NoError(t, idx.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"added": hnsw.UserConfig{}}))
	s := underlyingShard(t, shd)
	before, ok, err := s.mapping.Get("added")
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, shd.DebugResetVectorIndex(ctx, "added"))

	after, ok, err := s.mapping.Get("added")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, before, after)
	assert.True(t, storageExistsFor(t, s, after))

	// a vector without a record cannot be reset
	require.NoError(t, idx.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"skipped": hnsw.UserConfig{Skip: true}}))
	require.ErrorContains(t, shd.DebugResetVectorIndex(ctx, "skipped"), "no mapping record")

	require.Nil(t, idx.drop())
}

// TestShard_DebugResetVectorIndex_Dynamic pins a bug where resetting a
// dynamic index broke the shard: dynamic.Drop closed and deleted the
// shard-SHARED index.db, and the re-init reused the shard's stale closed
// handle, so the reset errored with bolt's "database not open" and every
// sibling dynamic vector lost its state DB.
func TestShard_DebugResetVectorIndex_Dynamic(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	dist := distancer.NewL2SquaredProvider()
	fuc := flat.UserConfig{}
	fuc.SetDefaults()
	uc := dynamic.UserConfig{
		Threshold: 1_000_000,
		Distance:  dist.Type(),
		HnswUC:    hnsw.UserConfig{MaxConnections: 8, EFConstruction: 16, EF: 8, VectorCacheMaxObjects: 1000},
		FlatUC:    fuc,
	}
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, uc,
		false, true /* async indexing on: required by the dynamic index */)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	require.NoError(t, shd.DebugResetVectorIndex(ctx, ""))

	// a second reset proves the first left the shard metadata DB usable
	require.NoError(t, shd.DebugResetVectorIndex(ctx, ""))

	require.Nil(t, idx.drop())
}

func TestShard_DebugResetVectorIndex_WithTargetVectors(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: className},
		hnsw.UserConfig{},
		false,
		true,
		func(i *Index) {
			i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
			i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{}
		},
	)

	amount := 1500

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject(className)
		obj.Vectors = map[string][]float32{
			"foo": {1, 2, 3},
		}
		objs = append(objs, obj)
	}

	errs := shd.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	oldIdx, q := getVectorIndexAndQueue(t, shd, "foo")

	// wait for the first batch to be indexed
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() <= 500 {
			break
		}
	}

	err := shd.DebugResetVectorIndex(ctx, "foo")
	require.Nil(t, err)

	newIdx, _ := getVectorIndexAndQueue(t, shd, "foo")

	// the new index should be different from the old one.
	// pointer comparison is enough here
	require.NotEqual(t, oldIdx, newIdx)

	// the reset refills the new index from the object store in the background
	require.Eventually(t, func() bool {
		for _, obj := range objs {
			if !newIdx.ContainsDoc(obj.DocID) {
				return false
			}
		}
		return true
	}, time.Minute, 100*time.Millisecond, "not every object made it into the rebuilt index")
	waitForVectorQueue(t, shd, "foo")

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_resetDimensionsLSM(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10
	shd.Index().Config.TrackVectorDimensions = true
	shd.resetDimensionsLSM(ctx)

	t.Run("count dimensions before insert", func(t *testing.T) {
		dims, err := shd.Dimensions(ctx, "")
		require.NoError(t, err)
		require.Equal(t, 0, dims)
	})

	t.Run("insert data into shard", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)
			obj.Vector = randVector(3)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("count dimensions", func(t *testing.T) {
		dims, err := shd.Dimensions(ctx, "")
		require.NoError(t, err)
		require.Equal(t, 3*amount, dims)
	})

	t.Run("reset dimensions lsm", func(t *testing.T) {
		err := shd.resetDimensionsLSM(ctx)
		require.Nil(t, err)
	})

	t.Run("count dimensions after reset", func(t *testing.T) {
		dims, err := shd.Dimensions(ctx, "")
		require.NoError(t, err)
		require.Equal(t, 0, dims)
	})

	t.Run("insert data into shard after reset", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)
			obj.Vector = randVector(3)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("count dimensions after reset and insert", func(t *testing.T) {
		dims, err := shd.Dimensions(ctx, "")
		require.NoError(t, err)
		require.Equal(t, 3*amount, dims)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_UpgradeIndex(t *testing.T) {
	t.Setenv("QUEUE_SCHEDULER_INTERVAL", "1ms")

	cfg := dynamic.NewDefaultUserConfig()
	cfg.Threshold = 400

	ctx := context.Background()
	className := "SomeClass"
	var opts []func(*Index)
	opts = append(opts, func(i *Index) {
		i.vectorIndexUserConfig = cfg
	})

	shd, _ := testShardWithSettings(t, ctx, &models.Class{Class: className}, cfg, false, true, opts...)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	amount := 400
	for i := 0; i < 3; i++ {
		objs := make([]*storobj.Object, 0, amount)
		for j := 0; j < amount; j++ {
			objs = append(objs, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    strfmt.UUID(uuid.NewString()),
					Class: className,
				},
				Vector: make([]float32, 1536),
			})
		}

		errs := shd.PutObjectBatch(ctx, objs)
		for _, err := range errs {
			require.Nil(t, err)
		}
	}

	q, release, ok := shd.AcquireVectorIndexQueue("")
	require.True(t, ok)
	defer release()
	require.True(t, ok)

	// wait for the queue to be empty
	require.EventuallyWithT(t, func(t *assert.CollectT) {
		assert.Zero(t, q.Size())
	}, 300*time.Second, 1*time.Second)
}

func TestShard_DynamicIndexStartsTombstoneCleanupCycle(t *testing.T) {
	ctx := context.Background()
	className := "TombstoneClass"

	tests := []struct {
		name          string
		config        schemaConfig.VectorIndexConfig
		expectRunning bool
	}{
		{
			name:          "dynamic starts tombstone cleanup cycle",
			config:        dynamic.NewDefaultUserConfig(),
			expectRunning: true,
		},
		{
			name:          "hnsw starts tombstone cleanup cycle",
			config:        hnsw.NewDefaultUserConfig(),
			expectRunning: true,
		},
		{
			name:          "flat does not start tombstone cleanup cycle",
			config:        flat.NewDefaultUserConfig(),
			expectRunning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tombstoneCycle cyclemanager.CycleManager

			asyncIndexingEnabled := true
			opts := []func(*Index){
				func(i *Index) {
					i.vectorIndexUserConfig = tt.config
					tombstoneCycle = i.cycleCallbacks.vectorTombstoneCleanupCycle
				},
			}
			shd, _ := testShardWithSettings(t, ctx, &models.Class{Class: className}, tt.config, false, asyncIndexingEnabled, opts...)

			defer func(path string) {
				err := os.RemoveAll(path)
				if err != nil {
					fmt.Println(err)
				}
			}(shd.Index().Config.RootPath)

			require.Equal(t, tt.expectRunning, tombstoneCycle.Running(),
				"vectorTombstoneCleanupCycle.Running()")
		})
	}
}

func TestShard_RequantizeIndex(t *testing.T) {
	hnswConfig := hnsw.NewDefaultUserConfig()
	hnswConfig.BQ = hnsw.BQConfig{Enabled: true}

	flatConfig := flat.NewDefaultUserConfig()
	flatConfig.BQ = flat.CompressionUserConfig{Enabled: true, Cache: true}

	tests := []struct {
		name                   string
		targetVector           string
		multiVector            bool
		cfg                    schemaConfig.VectorIndexConfig
		idxOpt                 func(*Index)
		getVectorIndexAndQueue func(ShardLike) (VectorIndex, *VectorIndexQueue)
	}{
		{
			name:         "hnsw",
			cfg:          hnswConfig,
			targetVector: "",
			getVectorIndexAndQueue: func(shd ShardLike) (VectorIndex, *VectorIndexQueue) {
				return getVectorIndexAndQueue(t, shd, "")
			},
		},
		{
			name:         "hnsw with target vectors",
			targetVector: "foo",
			cfg:          hnswConfig,
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = hnswConfig
			},
			getVectorIndexAndQueue: func(shd ShardLike) (VectorIndex, *VectorIndexQueue) {
				return getVectorIndexAndQueue(t, shd, "foo")
			},
		},
		{
			name:         "flat",
			cfg:          flatConfig,
			targetVector: "",
			getVectorIndexAndQueue: func(shd ShardLike) (VectorIndex, *VectorIndexQueue) {
				return getVectorIndexAndQueue(t, shd, "")
			},
		},
		{
			name:         "flat with target vectors",
			targetVector: "foo",
			cfg:          flatConfig,
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = flatConfig
			},
			getVectorIndexAndQueue: func(shd ShardLike) (VectorIndex, *VectorIndexQueue) {
				return getVectorIndexAndQueue(t, shd, "foo")
			},
		},
	}

	for testIndex, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			className := fmt.Sprintf("TestClass%d", testIndex)
			var opts []func(*Index)
			if test.idxOpt != nil {
				opts = append(opts, test.idxOpt)
			}
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, test.cfg, false, false, opts...)

			amount := 50

			defer func(path string) {
				err := os.RemoveAll(path)
				if err != nil {
					fmt.Println(err)
				}
			}(shd.Index().Config.RootPath)

			var objs []*storobj.Object
			for i := 0; i < amount; i++ {
				obj := testObject(className)
				if test.targetVector != "" {
					obj.Vectors = map[string][]float32{
						test.targetVector: randVector(3),
					}
				} else {
					obj.Vector = randVector(3)
				}
				objs = append(objs, obj)
			}

			errs := shd.PutObjectBatch(ctx, objs)
			for _, err := range errs {
				require.Nil(t, err)
			}

			vidx, _ := test.getVectorIndexAndQueue(shd)

			// For HNSW, the compressed bucket name uses the index name, not the target vector
			compressedBucketName := helpers.GetCompressedBucketName(test.targetVector)
			compressedBucket := shd.Store().Bucket(compressedBucketName)
			require.NotNil(t, compressedBucket)

			// store original vectors for comparison
			originalVectors := make(map[uint64][]byte)
			for i := 10; i < amount; i++ {
				idBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(idBytes, uint64(i))
				compressedVector, err := compressedBucket.Get(idBytes)
				require.NoError(t, err)
				require.NotEmpty(t, compressedVector)
				originalVectors[uint64(i)] = compressedVector
			}

			// delete those IDs from the vector index
			for i := 10; i < amount; i++ {
				if test.multiVector {
					err := vidx.Delete(uint64(i))
					require.NoError(t, err)
				} else {
					err := vidx.Delete(uint64(i))
					require.NoError(t, err)
				}
			}

			// run requantize index
			err := shd.RequantizeIndex(ctx, test.targetVector)
			require.NoError(t, err)

			// check that vectors are identical after requantization
			for i := 10; i < amount; i++ {
				idBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(idBytes, uint64(i))
				compressedVector, err := compressedBucket.Get(idBytes)
				require.NoError(t, err)
				require.NotEmpty(t, compressedVector)

				originalVector, exists := originalVectors[uint64(i)]
				require.True(t, exists, "original vector should exist for ID %d", i)
				require.Equal(t, originalVector, compressedVector, "vectors should be identical for ID %d", i)
			}

			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		})
	}
}

func getVectorIndexAndQueue(t *testing.T, shard ShardLike, targetVector string) (VectorIndex, *VectorIndexQueue) {
	idx, releaseIdx, vok := shard.AcquireVectorIndex(targetVector)
	if vok {
		defer releaseIdx()
	}
	q, releaseQ, qok := shard.AcquireVectorIndexQueue(targetVector)
	if qok {
		defer releaseQ()
	}
	require.True(t, vok && qok)
	return idx, q
}

// Every shard opens index.db at load, and its lock makes an offline read fail.
func TestShard_OpensMetadataDBForEveryShard(t *testing.T) {
	ctx := testCtx()
	shd, _ := testShard(t, ctx, "MetaDBEveryShard")
	s := shd.(*Shard)

	require.NotNil(t, s.metadataDB)
	_, err := os.Stat(path.Join(s.path(), shardmeta.FileName))
	require.NoError(t, err)

	// the loaded shard holds the lock
	_, _, err = shardmeta.GetOffline(s.path(), dynamicindex.StateNamespace, []byte("upgraded"))
	require.Error(t, err)
}

func TestShard_TombstoneCleanupInterval_NamedVector(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className},
		hnsw.UserConfig{}, false, false,
		func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"foo": hnsw.UserConfig{
					CleanupIntervalSeconds: 1,
					MaxConnections:         16,
					EFConstruction:         64,
					VectorCacheMaxObjects:  100000,
				},
			}
			// the helper installs noop cycle callbacks; the real ticker is under test
			i.initCycleCallbacks()
		},
	)
	defer func() {
		require.NoError(t, idx.drop())
		require.NoError(t, os.RemoveAll(idx.Config.RootPath))
	}()

	amount := 100
	objs := make([]*storobj.Object, 0, amount)
	for i := 0; i < amount; i++ {
		obj := testObject(className)
		obj.Vectors = map[string][]float32{
			"foo": {float32(i), float32(i % 7), float32(i % 3)},
		}
		objs = append(objs, obj)
	}
	for _, err := range shd.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}

	deleted := 20
	for _, obj := range objs[:deleted] {
		require.NoError(t, shd.DeleteObject(ctx, obj.ID(), time.Now()))
	}

	vi, release, ok := shd.AcquireVectorIndex("foo")
	require.True(t, ok)
	defer release()
	hnswIdx, ok := vi.(*hnswindex.HNSW)
	require.True(t, ok)

	numTombstones := func() int {
		stats, err := hnswIdx.Stats()
		require.NoError(t, err)
		return stats.NumTombstones
	}

	// Cleanup runs every second, so the tombstones must drain well before the
	// 300s default would have fired.
	require.Eventually(t, func() bool { return numTombstones() == 0 },
		20*time.Second, 200*time.Millisecond,
		"tombstones did not drain: %d remaining", numTombstones())
}

// A vector payload whose shape does not match the target's index must be
// refused before anything is written, never panic or fail later in indexing.
// Flat, dynamic and hfresh never take multi-vectors; hnsw only takes the kind
// its multivector setting selects.
func TestShard_MultiVectorOnIndexWithoutMultiSupport(t *testing.T) {
	singleVector := []float32{0.1, 0.2, 0.3, 0.4}
	multiVector := [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}}

	// The dynamic index can only be created with async indexing enabled.
	indexes := []struct {
		name    string
		cfg     schemaConfig.VectorIndexConfig
		async   []bool
		payload models.Vector
	}{
		{name: "flat", cfg: flat.NewDefaultUserConfig(), async: []bool{false, true}, payload: multiVector},
		{name: "dynamic", cfg: dynamic.NewDefaultUserConfig(), async: []bool{true}, payload: multiVector},
		{name: "hfresh", cfg: hfresh.NewDefaultUserConfig(), async: []bool{false, true}, payload: multiVector},
		{name: "hnsw", cfg: hnsw.NewDefaultUserConfig(), async: []bool{false, true}, payload: multiVector},
		{name: "hnsw multivector", cfg: hnsw.NewDefaultMultiVectorUserConfig(), async: []bool{false, true}, payload: singleVector},
	}

	objWith := func(payload models.Vector) *storobj.Object {
		obj := testObject("TestClass")
		switch v := payload.(type) {
		case []float32:
			obj.Vectors = map[string][]float32{"foo": v}
		case [][]float32:
			obj.MultiVectors = map[string][][]float32{"foo": v}
		}
		return obj
	}

	ops := map[string]func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error{
		"put": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			return shd.PutObject(ctx, objWith(payload))
		},
		"merge": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			return shd.MergeObject(ctx, objects.MergeDocument{
				Class:   "TestClass",
				ID:      strfmt.UUID(uuid.NewString()),
				Vectors: models.Vectors{"foo": payload},
			})
		},
		"batch": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			errs := shd.PutObjectBatch(ctx, []*storobj.Object{objWith(payload)})
			require.Len(t, errs, 1)
			return errs[0]
		},
		"search": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			_, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{payload}, []string{"foo"}, 0, 10,
				nil, nil, nil, additional.Properties{}, nil, nil)
			return err
		},
		"search by distance": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			_, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{payload}, []string{"foo"}, 0.5, -1,
				nil, nil, nil, additional.Properties{}, nil, nil)
			return err
		},
		"distance for query": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			_, err := shd.VectorDistanceForQuery(ctx, 0, []models.Vector{payload}, []string{"foo"})
			return err
		},
		"aggregate": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			limit := 10
			_, err := shd.Aggregate(ctx, aggregation.Params{
				ClassName: "TestClass", IncludeMetaCount: true,
				SearchVector: payload, TargetVector: "foo", ObjectLimit: &limit,
			}, nil)
			return err
		},
		"aggregate by distance": func(t *testing.T, ctx context.Context, shd ShardLike, payload models.Vector) error {
			_, err := shd.Aggregate(ctx, aggregation.Params{
				ClassName: "TestClass", IncludeMetaCount: true,
				SearchVector: payload, TargetVector: "foo", Certainty: 0.5,
			}, nil)
			return err
		},
	}

	for _, index := range indexes {
		for _, async := range index.async {
			for opName, op := range ops {
				t.Run(fmt.Sprintf("%s/async=%t/%s", index.name, async, opName), func(t *testing.T) {
					ctx := testCtx()
					class := &models.Class{Class: "TestClass"}
					shd, idx := testShardWithSettings(t, ctx, class, hnsw.NewDefaultUserConfig(), false, async,
						func(i *Index) {
							i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"foo": index.cfg}
						})
					defer func() { require.NoError(t, idx.drop()) }()

					err := op(t, ctx, shd, index.payload)
					require.ErrorContains(t, err, "multi-vector")
					require.Equal(t, 0, int(shd.Counter().Get()))
				})
			}
		}
	}
}
