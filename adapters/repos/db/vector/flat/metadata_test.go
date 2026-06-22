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

package flat

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_FlatDimensions(t *testing.T) {
	ctx := context.TODO()
	store := testinghelpers.NewDummyStore(t)
	rootPath := t.TempDir()
	defer store.Shutdown(context.Background())
	indexID := "init-dimensions-zero"
	distancer := distancer.NewCosineDistanceProvider()

	config := flatent.UserConfig{}
	config.SetDefaults()

	index, err := New(Config{
		ID:                indexID,
		RootPath:          rootPath,
		DistanceProvider:  distancer,
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, config, store)

	t.Run("initial dimensions zero", func(t *testing.T) {
		require.Nil(t, err)
		require.Equal(t, int32(0), index.dims)
	})

	t.Run("metadata is closed after index creation", func(t *testing.T) {
		require.Nil(t, index.metadata, "metadata file should be closed")
	})

	t.Run("dimensions updated", func(t *testing.T) {
		err = index.Add(ctx, 1, []float32{1, 2, 3})
		require.Nil(t, err)
		require.Equal(t, int32(3), index.dims)
	})

	t.Run("metadata is closed after insert", func(t *testing.T) {
		require.Nil(t, index.metadata, "metadata file should be closed")
	})

	t.Run("error when adding vector with wrong dimensions", func(t *testing.T) {
		err = index.Add(ctx, 2, []float32{1, 2, 3, 4})
		require.NotNil(t, err)
		require.ErrorContains(t, err, "insert called with a vector of the wrong size")
	})

	t.Run("backup metadata file exists", func(t *testing.T) {
		files, err := index.ListFiles(context.Background(), rootPath)
		require.Nil(t, err)
		require.Len(t, files, 1)
		require.Equal(t, "meta.db", files[0])
	})

	t.Run("can restore dimensions", func(t *testing.T) {
		index.Shutdown(context.Background())
		index = nil

		index, err = New(Config{
			ID:                indexID,
			RootPath:          rootPath,
			DistanceProvider:  distancer,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, config, store)

		require.Nil(t, err)
		require.Equal(t, index.dims, int32(3))

		err = index.Add(ctx, 2, []float32{1, 2, 3, 4})
		require.NotNil(t, err)
		require.ErrorContains(t, err, "insert called with a vector of the wrong size")
	})

	t.Run("can restore dimensions without root path", func(t *testing.T) {
		emptyRoot := t.TempDir()
		index.Shutdown(context.Background())
		index = nil

		index, err = New(Config{
			ID:                indexID,
			RootPath:          emptyRoot,
			DistanceProvider:  distancer,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, config, store)

		require.Nil(t, err)
		require.Equal(t, index.dims, int32(3))

		err = index.Add(ctx, 2, []float32{1, 2, 3, 4})
		require.NotNil(t, err)
		require.ErrorContains(t, err, "insert called with a vector of the wrong size")
	})
}

func Test_FlatDimensionsTargetVector(t *testing.T) {
	ctx := context.TODO()
	store := testinghelpers.NewDummyStore(t)
	rootPath := t.TempDir()
	defer store.Shutdown(context.Background())
	indexID := "test"
	distancer := distancer.NewCosineDistanceProvider()

	config := flatent.UserConfig{}
	config.SetDefaults()

	index, err := New(Config{
		ID:                indexID,
		RootPath:          rootPath,
		TargetVector:      "target",
		DistanceProvider:  distancer,
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, config, store)

	t.Run("initial dimensions zero", func(t *testing.T) {
		require.Nil(t, err)
		require.Equal(t, int32(0), index.dims)
	})

	t.Run("dimensions updated", func(t *testing.T) {
		err = index.Add(ctx, 1, []float32{1, 2})
		require.Nil(t, err)
		require.Equal(t, int32(2), index.dims)
	})

	t.Run("can restore dimensions", func(t *testing.T) {
		index.Shutdown(context.Background())
		index = nil

		index, err = New(Config{
			ID:                indexID,
			RootPath:          rootPath,
			TargetVector:      "target",
			DistanceProvider:  distancer,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, config, store)

		require.Nil(t, err)
		require.Equal(t, index.dims, int32(2))

		err = index.Add(ctx, 2, []float32{1, 2, 3, 4})
		require.NotNil(t, err)
		require.ErrorContains(t, err, "insert called with a vector of the wrong size")
	})

	t.Run("target vector file validation", func(t *testing.T) {
		index.targetVector = "./../foo"
		require.Equal(t, "meta_foo.db", index.getMetadataFile())
	})
}

func Test_RQDataSerialization(t *testing.T) {
	ctx := context.TODO()
	store := testinghelpers.NewDummyStore(t)
	rootPath := t.TempDir()
	defer store.Shutdown(context.Background())
	indexID := "rq-data-serialization"
	distancer := distancer.NewCosineDistanceProvider()

	config := flatent.UserConfig{}
	config.SetDefaults()
	rq := flatent.RQUserConfig{
		Enabled:      true,
		Cache:        false,
		RescoreLimit: 10,
	}
	config.RQ = rq

	index, err := New(Config{
		ID:                indexID,
		RootPath:          rootPath,
		DistanceProvider:  distancer,
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, config, store)
	require.Nil(t, err)

	t.Run("serialize and deserialize RQ1 data", func(t *testing.T) {
		// Add some vectors to create quantizer data
		err = index.Add(ctx, 1, []float32{1.0, 2.0, 3.0, 4.0})
		require.Nil(t, err)
		err = index.Add(ctx, 2, []float32{5.0, 6.0, 7.0, 8.0})
		require.Nil(t, err)

		// Test serialization
		originalRQ1Data, err := index.serializeRQ1Data()
		require.Nil(t, err)
		require.NotNil(t, originalRQ1Data)
		require.Equal(t, uint32(256), originalRQ1Data.InputDim)
		require.Greater(t, originalRQ1Data.OutputDim, uint32(0))
		require.Greater(t, originalRQ1Data.Rounds, uint32(0))

		// Test container creation and serialization
		container := &RQDataContainer{
			Version:         RQDataVersion,
			CompressionType: CompressionTypeRQ1,
			Data:            originalRQ1Data,
		}

		// Serialize to msgpack
		data, err := msgpack.Marshal(container)
		require.Nil(t, err)
		require.NotEmpty(t, data)

		// Deserialize from msgpack
		var restoredContainer RQDataContainer
		err = msgpack.Unmarshal(data, &restoredContainer)
		require.Nil(t, err)
		require.Equal(t, container.Version, restoredContainer.Version)
		require.Equal(t, container.CompressionType, restoredContainer.CompressionType)

		// Test manual deserialization of Data field
		err = index.handleDeserializedData(&restoredContainer)
		require.Nil(t, err)

		restoredRQ1Data, err := index.serializeRQ1Data()
		require.Nil(t, err)
		require.NotNil(t, restoredRQ1Data)

		require.Equal(t, originalRQ1Data.InputDim, restoredRQ1Data.InputDim)
		require.Equal(t, originalRQ1Data.OutputDim, restoredRQ1Data.OutputDim)
		require.Equal(t, originalRQ1Data.Rounds, restoredRQ1Data.Rounds)
		require.Equal(t, originalRQ1Data.Swaps, restoredRQ1Data.Swaps)
		require.Equal(t, originalRQ1Data.Signs, restoredRQ1Data.Signs)
		require.Equal(t, originalRQ1Data.Rounding, restoredRQ1Data.Rounding)
	})

	t.Run("compression type validation", func(t *testing.T) {
		// Test with wrong compression type
		wrongContainer := &RQDataContainer{
			Version:         RQDataVersion,
			CompressionType: CompressionTypeRQ8, // Wrong type
			Data:            &RQ1Data{},
		}

		err = index.handleDeserializedData(wrongContainer)
		require.NotNil(t, err)
		require.ErrorContains(t, err, "compression type mismatch")
	})

	t.Run("version validation", func(t *testing.T) {
		// Test with unsupported version
		wrongVersionContainer := &RQDataContainer{
			Version:         999, // Unsupported version
			CompressionType: CompressionTypeRQ1,
			Data:            &RQ1Data{},
		}

		err = index.handleDeserializedData(wrongVersionContainer)
		require.NotNil(t, err)
		require.ErrorContains(t, err, "unsupported RQ data version")
	})
}

func Test_RQ8DataSerialization(t *testing.T) {
	ctx := context.TODO()
	store := testinghelpers.NewDummyStore(t)
	rootPath := t.TempDir()
	defer store.Shutdown(context.Background())
	indexID := "rq8-data-serialization"
	distancer := distancer.NewCosineDistanceProvider()

	config := flatent.UserConfig{}
	config.SetDefaults()
	rq := flatent.RQUserConfig{
		Enabled:      true,
		Cache:        false,
		RescoreLimit: 10,
		Bits:         8,
	}
	config.RQ = rq

	index, err := New(Config{
		ID:                indexID,
		RootPath:          rootPath,
		DistanceProvider:  distancer,
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, config, store)
	require.Nil(t, err)

	t.Run("serialize and deserialize RQ8 data", func(t *testing.T) {
		// Add some vectors to create quantizer data
		err = index.Add(ctx, 1, []float32{1.0, 2.0, 3.0, 4.0})
		require.Nil(t, err)
		err = index.Add(ctx, 2, []float32{5.0, 6.0, 7.0, 8.0})
		require.Nil(t, err)

		// Test serialization
		originalRQ8Data, err := index.serializeRQ8Data()
		require.Nil(t, err)
		require.NotNil(t, originalRQ8Data)
		require.Equal(t, uint32(4), originalRQ8Data.InputDim)
		require.Equal(t, uint32(8), originalRQ8Data.Bits)
		require.Greater(t, originalRQ8Data.OutputDim, uint32(0))
		require.Greater(t, originalRQ8Data.Rounds, uint32(0))

		// Test container creation and serialization
		container := &RQDataContainer{
			Version:         RQDataVersion,
			CompressionType: CompressionTypeRQ8,
			Data:            originalRQ8Data,
		}

		// Serialize to msgpack
		data, err := msgpack.Marshal(container)
		require.Nil(t, err)
		require.NotEmpty(t, data)

		// Deserialize from msgpack
		var restoredContainer RQDataContainer
		err = msgpack.Unmarshal(data, &restoredContainer)
		require.Nil(t, err)
		require.Equal(t, container.Version, restoredContainer.Version)
		require.Equal(t, container.CompressionType, restoredContainer.CompressionType)

		// Test manual deserialization of Data field
		err = index.handleDeserializedData(&restoredContainer)
		require.Nil(t, err)

		restoredRQ8Data, err := index.serializeRQ8Data()
		require.Nil(t, err)
		require.NotNil(t, restoredRQ8Data)

		require.Equal(t, originalRQ8Data.InputDim, restoredRQ8Data.InputDim)
		require.Equal(t, originalRQ8Data.Bits, restoredRQ8Data.Bits)
		require.Equal(t, originalRQ8Data.OutputDim, restoredRQ8Data.OutputDim)
		require.Equal(t, originalRQ8Data.Rounds, restoredRQ8Data.Rounds)
		require.Equal(t, originalRQ8Data.Swaps, restoredRQ8Data.Swaps)
		require.Equal(t, originalRQ8Data.Signs, restoredRQ8Data.Signs)
	})
}

// ino returns the inode of path so a test can distinguish a hardlink (shared
// inode) from an independent copy (different inode).
func ino(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

// readStagedDimensions opens a staged meta.db copy as a bbolt DB and returns the
// persisted dimensions, proving the staged file is a valid, readable database.
func readStagedDimensions(t *testing.T, path string) (int32, bool) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	require.NoError(t, err)
	defer db.Close()

	var (
		dims  int32
		found bool
	)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte("dimensions"))
		if v == nil {
			return nil
		}
		dims = int32(binary.LittleEndian.Uint32(v))
		found = true
		return nil
	}))
	return dims, found
}

// TestSnapshotMutableFiles is the primary regression for the torn-bbolt backup
// bug: a write that mutates meta.db after the snapshot must not alter the staged
// copy, and the staged copy must be an independent file (different inode), never
// a hardlink of the live, still-mutated meta.db.
func TestSnapshotMutableFiles(t *testing.T) {
	ctx := context.Background()
	dp := distancer.NewCosineDistanceProvider()

	newIndex := func(t *testing.T, rootPath, target string, uc flatent.UserConfig) *flat {
		store := testinghelpers.NewDummyStore(t)
		t.Cleanup(func() { store.Shutdown(context.Background()) })
		index, err := New(Config{
			ID:                "snapshot-test",
			RootPath:          rootPath,
			TargetVector:      target,
			DistanceProvider:  dp,
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, uc, store)
		require.NoError(t, err)
		return index
	}

	t.Run("staged copy is independent of post-snapshot setDimensions write", func(t *testing.T) {
		rootPath := t.TempDir()
		uc := flatent.UserConfig{}
		uc.SetDefaults()
		index := newIndex(t, rootPath, "", uc)

		// First Add writes dimensions=3 into meta.db (setDimensions).
		require.NoError(t, index.Add(ctx, 1, []float32{1, 2, 3}))

		staging := t.TempDir()
		relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
		require.NoError(t, err)
		require.Equal(t, []string{"meta.db"}, relPaths)

		liveMeta := filepath.Join(rootPath, "meta.db")
		stagedMeta := filepath.Join(staging, "meta.db")

		// The staged copy must NOT be a hardlink of the live file; otherwise a
		// later in-place write would tear it.
		require.NotEqual(t, ino(t, liveMeta), ino(t, stagedMeta),
			"staged meta.db must be an independent copy, not a hardlink")

		stagedDims, found := readStagedDimensions(t, stagedMeta)
		require.True(t, found)
		require.Equal(t, int32(3), stagedDims)

		// Mutate the live meta.db after the snapshot. setDimensions overwrites the
		// same key; the staged copy must remain at the pre-snapshot value.
		require.NoError(t, index.setDimensions(99))

		stagedDimsAfter, found := readStagedDimensions(t, stagedMeta)
		require.True(t, found)
		require.Equal(t, int32(3), stagedDimsAfter,
			"staged copy must reflect pre-snapshot state, not the post-snapshot write")
	})

	t.Run("staged copy is independent of post-snapshot RQ8 persist write", func(t *testing.T) {
		rootPath := t.TempDir()
		uc := flatent.UserConfig{}
		uc.SetDefaults()
		uc.RQ = flatent.RQUserConfig{Enabled: true, Bits: 8, RescoreLimit: 10}
		index := newIndex(t, rootPath, "", uc)

		// First Add sets dimensions and persists RQ data into meta.db.
		require.NoError(t, index.Add(ctx, 1, []float32{1, 2, 3, 4}))

		staging := t.TempDir()
		relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
		require.NoError(t, err)
		require.Equal(t, []string{"meta.db"}, relPaths)

		stagedMeta := filepath.Join(staging, "meta.db")
		require.NotEqual(t, ino(t, filepath.Join(rootPath, "meta.db")), ino(t, stagedMeta),
			"staged meta.db must be an independent copy, not a hardlink")

		stagedBefore, err := os.ReadFile(stagedMeta)
		require.NoError(t, err)

		// Re-persisting RQ data mutates meta.db in place (persistRQData).
		require.NoError(t, index.persistRQData())

		stagedAfter, err := os.ReadFile(stagedMeta)
		require.NoError(t, err)
		require.Equal(t, stagedBefore, stagedAfter,
			"staged copy bytes must be unchanged by the post-snapshot RQ8 write")

		// The staged file must still open as a valid bbolt database.
		_, found := readStagedDimensions(t, stagedMeta)
		require.True(t, found)
	})

	t.Run("multi-target meta file snapshots to the correct relpath", func(t *testing.T) {
		rootPath := t.TempDir()
		uc := flatent.UserConfig{}
		uc.SetDefaults()
		index := newIndex(t, rootPath, "custom", uc)

		require.NoError(t, index.Add(ctx, 1, []float32{1, 2}))

		staging := t.TempDir()
		relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
		require.NoError(t, err)
		require.Equal(t, []string{"meta_custom.db"}, relPaths)

		stagedMeta := filepath.Join(staging, "meta_custom.db")
		require.NotEqual(t, ino(t, filepath.Join(rootPath, "meta_custom.db")), ino(t, stagedMeta))
		dims, found := readStagedDimensions(t, stagedMeta)
		require.True(t, found)
		require.Equal(t, int32(2), dims)
	})

	t.Run("snapshots from a closed (nil) metadata handle", func(t *testing.T) {
		rootPath := t.TempDir()
		uc := flatent.UserConfig{}
		uc.SetDefaults()
		index := newIndex(t, rootPath, "", uc)

		require.NoError(t, index.Add(ctx, 1, []float32{5, 6, 7}))
		// The open-close-per-operation pattern leaves the handle nil between ops.
		require.Nil(t, index.metadata, "precondition: metadata handle is closed")

		staging := t.TempDir()
		relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
		require.NoError(t, err)
		require.Equal(t, []string{"meta.db"}, relPaths)
		// The private read-only handle must not be published onto the index.
		require.Nil(t, index.metadata, "snapshot must not assign a handle to index.metadata")

		dims, found := readStagedDimensions(t, filepath.Join(staging, "meta.db"))
		require.True(t, found)
		require.Equal(t, int32(3), dims)
	})

	t.Run("no metadata file yet returns nil", func(t *testing.T) {
		rootPath := t.TempDir()
		// A bare flat index that has never persisted metadata: removing the file
		// that New created simulates the never-written state.
		uc := flatent.UserConfig{}
		uc.SetDefaults()
		index := newIndex(t, rootPath, "", uc)
		require.NoError(t, os.Remove(filepath.Join(rootPath, "meta.db")))

		staging := t.TempDir()
		relPaths, err := index.SnapshotMutableFiles(ctx, rootPath, staging)
		require.NoError(t, err)
		require.Nil(t, relPaths)
	})
}
