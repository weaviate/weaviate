//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/storagestate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShard_UpdateStatus(t *testing.T) {
	ctx := testCtx()
	shd := testShard(ctx)

	amount := 10

	t.Run("insert data into shard", func(t *testing.T) {
		for i := 0; i < amount; i++ {
			obj := testObject()

			err := shd.putObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.objectList(ctx, amount, additional.Properties{})
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("mark shard readonly and fail to insert", func(t *testing.T) {
		err := shd.updateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)

		err = shd.putObject(ctx, testObject())
		require.EqualError(t, err, storagestate.ErrStatusReadOnly.Error())
	})

	t.Run("mark shard ready and insert successfully", func(t *testing.T) {
		err := shd.updateStatus(storagestate.StatusReady.String())
		require.Nil(t, err)

		err = shd.putObject(ctx, testObject())
		require.Nil(t, err)
	})
}

func TestReadOnlyShard_HaltCompaction(t *testing.T) {
	amount := 100000
	sizePerValue := 8
	bucketName := "testbucket"

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	shd := testShard(context.Background())
	shd.store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithMemtableThreshold(1024))

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
	})

	t.Run("halt compaction with readonly status", func(t *testing.T) {
		err := shd.updateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)

		// once shard status is set to readonly,
		// the number of segment files should
		// not change
		entries, err := os.ReadDir(dirName)
		require.Nil(t, err)
		numSegments := len(entries)

		for i := 0; i < 30; i++ {
			entries, err := os.ReadDir(dirName)
			require.Nil(t, err)

			require.Equal(t, numSegments, len(entries))
			time.Sleep(time.Second)
		}
	})
}
