//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	}(shd.index.Config.RootPath)

	t.Run("insert data into shard", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject(className)

			err := shd.putObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.objectList(ctx, amount, nil, additional.Properties{}, shd.index.Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("mark shard readonly and fail to insert", func(t *testing.T) {
		err := shd.updateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)

		err = shd.putObject(ctx, testObject(className))
		require.EqualError(t, err, storagestate.ErrStatusReadOnly.Error())
	})

	t.Run("mark shard ready and insert successfully", func(t *testing.T) {
		err := shd.updateStatus(storagestate.StatusReady.String())
		require.Nil(t, err)

		err = shd.putObject(ctx, testObject(className))
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
	}(shd.index.Config.RootPath)

	err := shd.store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithMemtableThreshold(1024))
	require.Nil(t, err)

	bucket := shd.store.Bucket(bucketName)
	require.NotNil(t, bucket)
	dirName := path.Join(shd.DBPathLSM(), bucketName)

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
		err := shd.updateStatus(storagestate.StatusReadOnly.String())
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
		err := shd.updateStatus(storagestate.StatusReady.String())
		require.Nil(t, err)

		time.Sleep(time.Second)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

// tests adding multiple larger batches in parallel using different settings of the goroutine factor.
// In all cases all objects should be added
func TestShard_ParallelBatches(t *testing.T) {
	var parallelTests = []struct {
		name            string
		goroutineFactor []string
	}{
		{"Valid factor", []string{"1"}},
		{"Low factor", []string{"0.5"}},
		{"High factor", []string{"5"}},
		{"invalid factor", []string{"-1"}},
		{"not given", []string{}},
		{"not parsable", []string{"I'm not a number"}},
	}

	batches := make([][]*storobj.Object, 4)
	wgAdd := sync.WaitGroup{}
	wgAdd.Add(len(batches))
	for i := range batches {
		go func() {
			batches[i] = createRandomObjects("TestClass", 1000)
			wgAdd.Done()
		}()
	}
	wgAdd.Wait()
	totalObjects := 1000 * len(batches)
	for _, tt := range parallelTests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.goroutineFactor) == 1 {
				os.Setenv("MaxImportGoroutinesFactor", tt.goroutineFactor[0])
			}

			ctx := testCtx()
			shd, _ := testShard(context.Background(), "TestClass")

			// add batches in parallel
			wg := sync.WaitGroup{}
			wg.Add(len(batches))
			for _, batch := range batches {
				go func() {
					shd.putObjectBatch(ctx, batch)
					wg.Done()
				}()
			}
			wg.Wait()

			require.Equal(t, totalObjects, int(shd.counter.Get()))
		})
	}
}
