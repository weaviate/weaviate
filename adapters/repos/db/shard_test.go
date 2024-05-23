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
// +build integrationTest

package db

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
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
		err := shd.UpdateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)

		err = shd.PutObject(ctx, testObject(className))
		require.EqualError(t, err, storagestate.ErrStatusReadOnly.Error())
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

func TestShard_UuidToIdLockPoolId_unmerged(t *testing.T) {
	lsmkv.FeatureUseMergedBuckets = false
	shd, _ := testShard(t, context.Background(), "TestClass")

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	res := shd.UuidToIdLockPoolId_unmerged([]byte("testtesttesttesttest"))
	require.Equal(t, uint8(0x74), res)

	fakeVectorConfig := fakeVectorConfig{}
	fakeVectorConfig.raw = "db.fakeVectorConfig"
	err := shd.UpdateVectorIndexConfig_unmerged(context.TODO(), fakeVectorConfig)
	require.EqualError(t, err, "unrecognized vector index config: db.fakeVectorConfig")

	shd.NotifyReady_unmerged()

	res2 := shd.ObjectCount_unmerged()
	require.Equal(t, 0, res2)

	res3 := shd.IsFallbackToSearchable_unmerged()
	require.Equal(t, false, res3)

	res5 := shd.Tenant_unmerged()
	require.Equal(t, "", res5)

	res4 := shd.Drop_unmerged()
	require.Nil(t, res4)

	err = shd.Shutdown_unmerged(context.TODO())
	require.Nil(t, err)
}

type fakeVectorConfig struct {
	raw interface{}
}

func (f fakeVectorConfig) IndexType() string {
	return "fake"
}

func (f fakeVectorConfig) Raw() interface{} {
	return f.raw
}

func (f fakeVectorConfig) Validate() error {
	return nil
}

func (f fakeVectorConfig) String() string {
	return "fake"
}

func (f fakeVectorConfig) IsDefault() bool {
	return false
}

func (f fakeVectorConfig) IsEmpty() bool {
	return false
}

func (f fakeVectorConfig) DistanceName() string {
	return ""
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
