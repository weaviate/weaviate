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
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	hnswindex "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
		err := shd.UpdateStatus(storagestate.StatusReady.String())
		require.Nil(t, err)

		err = shd.PutObject(ctx, testObject(className))
		require.Nil(t, err)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_ReadOnly_HaltCompaction(t *testing.T) {
	amount := 10000
	sizePerValue := 8
	bucketName := "testbucket"

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	shd, idx := testShard(t, context.Background(), "TestClass")

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	err := shd.Store().CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithMemtableThreshold(1024))
	require.Nil(t, err)

	bucket := shd.Store().Bucket(bucketName)
	require.NotNil(t, bucket)
	dirName := path.Join(shd.Index().path(), shd.Name(), "lsm", bucketName)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			n, err := json.Marshal(i)
			require.Nil(t, err)

			keys[i] = n
			values[i] = make([]byte, sizePerValue)
			rand.Read(values[i])
		}
	})

	t.Run("insert data into bucket", func(t *testing.T) {
		for i := range keys {
			err := bucket.Put(keys[i], values[i])
			assert.Nil(t, err)
			time.Sleep(time.Microsecond)
		}

		t.Logf("insertion complete!")
	})

	t.Run("halt compaction with readonly status", func(t *testing.T) {
		err := shd.UpdateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)

		// give the status time to propagate
		// before grabbing the baseline below
		time.Sleep(time.Second)

		// once shard status is set to readonly,
		// the number of segment files should
		// not change
		entries, err := os.ReadDir(dirName)
		require.Nil(t, err)
		numSegments := len(entries)

		// if the number of segments remain the
		// same for 30 seconds, we can be
		// reasonably sure that the compaction
		// process was halted
		for i := 0; i < 30; i++ {
			entries, err := os.ReadDir(dirName)
			require.Nil(t, err)

			require.Equal(t, numSegments, len(entries))
			t.Logf("iteration %d, sleeping", i)
			time.Sleep(time.Second)
		}
	})

	t.Run("update shard status to ready", func(t *testing.T) {
		err := shd.UpdateStatus(storagestate.StatusReady.String())
		require.Nil(t, err)

		time.Sleep(time.Second)
	})

	require.Nil(t, idx.drop())
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

func TestShard_DebugResetVectorIndex(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, hnsw.UserConfig{}, false, true /* withCheckpoints */)

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
		objs = append(objs, obj)
	}

	errs := shd.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	oldIdx := shd.VectorIndex()

	// wait for the first batch to be indexed
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if shd.Queue().Size() <= 500 {
			break
		}
	}

	err := shd.DebugResetVectorIndex(ctx, "")
	require.Nil(t, err)

	newIdx := shd.VectorIndex()

	// the new index should be different from the old one.
	// pointer comparison is enough here
	require.NotEqual(t, oldIdx, newIdx)

	// queue should be empty after reset
	require.EqualValues(t, 0, shd.Queue().Size())

	// make sure the new index does not contain any of the objects
	for _, obj := range objs {
		if newIdx.ContainsDoc(obj.DocID) {
			t.Fatalf("node %d should not be in the vector index", obj.DocID)
		}
	}

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_DebugResetVectorIndex_WithTargetVectors(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
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

	oldIdx := shd.VectorIndexes()["foo"]
	q := shd.Queues()["foo"]

	// wait for the first batch to be indexed
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() <= 500 {
			break
		}
	}

	err := shd.DebugResetVectorIndex(ctx, "foo")
	require.Nil(t, err)

	newIdx := shd.VectorIndexes()["foo"]

	// the new index should be different from the old one.
	// pointer comparison is enough here
	require.NotEqual(t, oldIdx, newIdx)

	// queue should be empty after reset
	require.EqualValues(t, 0, q.Size())

	// make sure the new index does not contain any of the objects
	for _, obj := range objs {
		if newIdx.ContainsDoc(obj.DocID) {
			t.Fatalf("node %d should not be in the vector index", obj.DocID)
		}
	}

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_RepairIndex(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	tests := []struct {
		name           string
		targetVector   string
		multiVector    bool
		cfg            schemaConfig.VectorIndexConfig
		idxOpt         func(*Index)
		getQueue       func(ShardLike) *VectorIndexQueue
		getVectorIndex func(ShardLike) VectorIndex
	}{
		{
			name: "hnsw",
			cfg:  hnsw.UserConfig{},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queue()
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndex()
			},
		},
		{
			name:         "hnsw with target vectors",
			targetVector: "foo",
			cfg:          hnsw.UserConfig{},
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{}
			},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queues()["foo"]
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndexes()["foo"]
			},
		},
		{
			name:         "hnsw with multi vectors",
			targetVector: "foo",
			multiVector:  true,
			cfg:          hnsw.UserConfig{},
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{
					Multivector: hnsw.MultivectorConfig{
						Enabled: true,
					},
				}
			},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queues()["foo"]
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndexes()["foo"]
			},
		},
		{
			name: "flat",
			cfg:  flat.NewDefaultUserConfig(),
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queue()
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndex()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			className := "TestClass"
			var opts []func(*Index)
			if test.idxOpt != nil {
				opts = append(opts, test.idxOpt)
			}
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, test.cfg, false, true /* withCheckpoints */, opts...)

			amount := 1000

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
					if test.multiVector {
						obj.MultiVectors = map[string][][]float32{
							test.targetVector: {{1, 2, 3}, {4, 5, 6}},
						}
					} else {
						obj.Vectors = map[string][]float32{
							test.targetVector: {1, 2, 3},
						}
					}
				}
				objs = append(objs, obj)
			}

			errs := shd.PutObjectBatch(ctx, objs)
			for _, err := range errs {
				require.Nil(t, err)
			}

			vidx := test.getVectorIndex(shd)
			q := test.getQueue(shd)

			// wait for the queue to be empty
			for i := 0; i < 20; i++ {
				time.Sleep(500 * time.Millisecond)
				if q.Size() == 0 {
					break
				}
			}

			// remove some objects from the vector index
			for i := 400; i < 600; i++ {
				if test.multiVector {
					err := vidx.DeleteMulti(uint64(i))
					require.NoError(t, err)
				} else {
					err := vidx.Delete(uint64(i))
					require.NoError(t, err)
				}
			}

			// remove some objects from the store
			bucket := shd.Store().Bucket(helpers.ObjectsBucketLSM)
			buf := make([]byte, 8)
			for i := 100; i < 300; i++ {
				binary.LittleEndian.PutUint64(buf, uint64(i))
				v, err := bucket.GetBySecondary(0, buf)
				require.NoError(t, err)
				obj, err := storobj.FromBinary(v)
				require.NoError(t, err)
				idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
				require.NoError(t, err)
				err = bucket.Delete(idBytes)
				require.NoError(t, err)
			}

			err := shd.RepairIndex(ctx, test.targetVector)
			require.NoError(t, err)

			// wait for the queue to be empty
			for i := 0; i < 20; i++ {
				time.Sleep(500 * time.Millisecond)
				if q.Size() == 0 {
					break
				}
			}

			// wait for the worker to start the indexing
			time.Sleep(500 * time.Millisecond)

			// make sure all objects except >= 100 < 300 are back in the vector index
			for i := 0; i < amount; i++ {
				if i >= 100 && i < 300 {
					if vidx.ContainsDoc(uint64(i)) {
						t.Fatalf("doc %d should not be in the vector index", i)
					}
					continue
				}

				if !vidx.ContainsDoc(uint64(i)) {
					t.Fatalf("doc %d should be in the vector index", i)
				}
			}

			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		})
	}
}

func TestShard_FillQueue(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")

	tests := []struct {
		name           string
		targetVector   string
		multiVector    bool
		cfg            schemaConfig.VectorIndexConfig
		idxOpt         func(*Index)
		getQueue       func(ShardLike) *VectorIndexQueue
		getVectorIndex func(ShardLike) VectorIndex
	}{
		{
			name: "hnsw",
			cfg:  hnsw.UserConfig{},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queue()
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndex()
			},
		},
		{
			name:         "hnsw with target vectors",
			targetVector: "foo",
			cfg:          hnsw.UserConfig{},
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{}
			},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queues()["foo"]
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndexes()["foo"]
			},
		},
		{
			name:         "hnsw with multi vectors",
			targetVector: "foo",
			multiVector:  true,
			cfg:          hnsw.UserConfig{},
			idxOpt: func(i *Index) {
				i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{
					Multivector: hnsw.MultivectorConfig{
						Enabled: true,
					},
				}
			},
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queues()["foo"]
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndexes()["foo"]
			},
		},
		{
			name: "flat",
			cfg:  flat.NewDefaultUserConfig(),
			getQueue: func(s ShardLike) *VectorIndexQueue {
				return s.Queue()
			},
			getVectorIndex: func(s ShardLike) VectorIndex {
				return s.VectorIndex()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			className := "TestClass"
			var opts []func(*Index)
			if test.idxOpt != nil {
				opts = append(opts, test.idxOpt)
			}
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, test.cfg, false, true /* withCheckpoints */, opts...)

			amount := 1000

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
					if test.multiVector {
						obj.MultiVectors = map[string][][]float32{
							test.targetVector: {{1, 2, 3}, {4, 5, 6}},
						}
					} else {
						obj.Vectors = map[string][]float32{
							test.targetVector: {1, 2, 3},
						}
					}
				}
				objs = append(objs, obj)
			}

			errs := shd.PutObjectBatch(ctx, objs)
			for _, err := range errs {
				require.Nil(t, err)
			}

			vidx := test.getVectorIndex(shd)
			q := test.getQueue(shd)

			// wait for the queue to be empty
			require.EventuallyWithT(t, func(t *assert.CollectT) {
				assert.Zero(t, q.Size())
			}, 5*time.Second, 100*time.Millisecond)

			// remove most of the objects from the vector index
			for i := 100; i < amount; i++ {
				if test.multiVector {
					err := vidx.DeleteMulti(uint64(i))
					require.NoError(t, err)
				} else {
					err := vidx.Delete(uint64(i))
					require.NoError(t, err)
				}
			}

			// we need to delete tombstones so the vectors with the same doc ids could be inserted
			if hnswindex.IsHNSWIndex(vidx) {
				err := hnswindex.AsHNSWIndex(vidx).CleanUpTombstonedNodes(func() bool { return false })
				require.NoError(t, err)
			}

			// refill only subset of the objects
			err := shd.FillQueue(test.targetVector, 150)
			require.NoError(t, err)

			require.EventuallyWithT(t, func(t *assert.CollectT) {
				assert.Zero(t, q.Size())
			}, 5*time.Second, 100*time.Millisecond)

			// wait for the worker to index
			time.Sleep(500 * time.Millisecond)

			// make sure all objects except >= 100 < 300 are back in the vector index
			for i := 0; i < amount; i++ {
				if 100 <= i && i < 150 {
					require.Falsef(t, vidx.ContainsDoc(uint64(i)), "doc %d should not be in the vector index", i)
					continue
				}
				require.Truef(t, vidx.ContainsDoc(uint64(i)), "doc %d should be in the vector index", i)
			}

			require.Nil(t, idx.drop())
			require.Nil(t, os.RemoveAll(idx.Config.RootPath))
		})
	}
}

func TestShard_resetDimensionsLSM(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10
	shd.Index().Config.TrackVectorDimensions = true
	shd.resetDimensionsLSM()

	t.Run("count dimensions before insert", func(t *testing.T) {
		dims := shd.Dimensions(ctx)
		require.Equal(t, 0, dims)
	})

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

	t.Run("count dimensions", func(t *testing.T) {
		dims := shd.Dimensions(ctx)
		require.Equal(t, 3*amount, dims)
	})

	t.Run("reset dimensions lsm", func(t *testing.T) {
		err := shd.resetDimensionsLSM()
		require.Nil(t, err)
	})

	t.Run("count dimensions after reset", func(t *testing.T) {
		dims := shd.Dimensions(ctx)
		require.Equal(t, 0, dims)
	})

	t.Run("insert data into shard after reset", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("count dimensions after reset and insert", func(t *testing.T) {
		dims := shd.Dimensions(ctx)
		require.Equal(t, 3*amount, dims)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}
