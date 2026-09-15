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

package integrationslowtest

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShard_ReadOnly_HaltCompaction(t *testing.T) {
	amount := 10000
	sizePerValue := 8
	bucketName := "testbucket"
	className := "TestClass"

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	repo, _ := newRepo(t, repoParams{}, &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.UserConfig{Skip: true},
		InvertedIndexConfig: invertedConfig(),
	})
	shd := singleShard(t, repo, className)

	err := shd.Store().CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithMemtableThreshold(1024), lsmkv.WithStrategy(lsmkv.StrategyReplace))
	require.Nil(t, err)

	bucket := shd.Store().Bucket(bucketName)
	require.NotNil(t, bucket)
	dirName := path.Join(repo.GetConfig().RootPath, shd.Index().ID(), shd.Name(), "lsm", bucketName)

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
		err := shd.UpdateStatus(storagestate.StatusReadOnly.String(), "test readonly")
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
		err := shd.UpdateStatus(storagestate.StatusReady.String(), "test ready")
		require.Nil(t, err)

		time.Sleep(time.Second)
	})

	require.Nil(t, repo.DeleteIndex(schema.ClassName(className)))
}
