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

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
)

func TestVectorDropIndexHelper_RemoveVectorIndexFiles(t *testing.T) {
	h := newVectorDropIndexHelper()

	setup := func(t *testing.T) (string, string) {
		t.Helper()
		indexPath := t.TempDir()
		shardName := "shard1"
		return indexPath, shardName
	}

	pathExists := func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	}

	t.Run("removes all flat vector artifacts", func(t *testing.T) {
		indexPath, shardName := setup(t)

		vectorsBucket := filepath.Join(indexPath, shardName, "lsm", fmt.Sprintf("%s_flat_bq", helpers.VectorsBucketLSM))
		compressedBucket := filepath.Join(indexPath, shardName, "lsm", helpers.GetCompressedBucketName("flat_bq"))

		require.NoError(t, os.MkdirAll(vectorsBucket, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(vectorsBucket, "data.db"), []byte("data"), 0o644))
		require.NoError(t, os.MkdirAll(compressedBucket, 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "flat_bq")
		require.NoError(t, err)

		assert.False(t, pathExists(vectorsBucket))
		assert.False(t, pathExists(compressedBucket))
	})

	t.Run("removes all hnsw vector artifacts", func(t *testing.T) {
		indexPath, shardName := setup(t)

		vectorsBucket := filepath.Join(indexPath, shardName, "lsm", fmt.Sprintf("%s_hnsw_rq8", helpers.VectorsBucketLSM))
		compressedBucket := filepath.Join(indexPath, shardName, "lsm", helpers.GetCompressedBucketName("hnsw_rq8"))
		commitLog := filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")
		snapshot := filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")

		require.NoError(t, os.MkdirAll(vectorsBucket, 0o755))
		require.NoError(t, os.MkdirAll(compressedBucket, 0o755))
		require.NoError(t, os.MkdirAll(commitLog, 0o755))
		require.NoError(t, os.MkdirAll(snapshot, 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "hnsw_rq8")
		require.NoError(t, err)

		assert.False(t, pathExists(vectorsBucket))
		assert.False(t, pathExists(compressedBucket))
		assert.False(t, pathExists(commitLog))
		assert.False(t, pathExists(snapshot))
	})

	t.Run("succeeds when files do not exist", func(t *testing.T) {
		indexPath, shardName := setup(t)
		require.NoError(t, os.MkdirAll(filepath.Join(indexPath, shardName, "lsm"), 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "nonexistent")
		require.NoError(t, err)
	})
}

func TestVectorDropIndexHelper_EnsureFilesAreRemovedForDroppedVectorIndexes(t *testing.T) {
	h := newVectorDropIndexHelper()

	setup := func(t *testing.T) (string, string) {
		t.Helper()
		indexPath := t.TempDir()
		shardName := "shard1"
		return indexPath, shardName
	}

	createLSMBucket := func(t *testing.T, indexPath, shardName, bucketName string) {
		t.Helper()
		bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
		require.NoError(t, os.MkdirAll(bucketPath, 0o755))
	}

	createShardDir := func(t *testing.T, indexPath, shardName, dirName string) {
		t.Helper()
		dirPath := filepath.Join(indexPath, shardName, dirName)
		require.NoError(t, os.MkdirAll(dirPath, 0o755))
	}

	pathExists := func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	}

	t.Run("no vector config - no error", func(t *testing.T) {
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class:        "TestClass",
			VectorConfig: nil,
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("all vectors active - files kept", func(t *testing.T) {
		indexPath, shardName := setup(t)
		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {VectorIndexType: "flat"},
				"hnsw_rq8": {VectorIndexType: "hnsw"},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
	})

	t.Run("dropped vector - files removed", func(t *testing.T) {
		indexPath, shardName := setup(t)

		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {},
				"hnsw_rq8": {VectorIndexType: "hnsw"},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))

		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
	})

	t.Run("all vectors dropped - all files removed", func(t *testing.T) {
		indexPath, shardName := setup(t)

		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {},
				"hnsw_rq8": {},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")))
	})

	t.Run("dropped vector but no files on disk - no error", func(t *testing.T) {
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"dropped": {},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})
}
