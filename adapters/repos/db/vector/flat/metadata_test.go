//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package flat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
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
		ID:               indexID,
		RootPath:         rootPath,
		DistanceProvider: distancer,
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
			ID:               indexID,
			RootPath:         rootPath,
			DistanceProvider: distancer,
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
			ID:               indexID,
			RootPath:         emptyRoot,
			DistanceProvider: distancer,
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
		ID:               indexID,
		RootPath:         rootPath,
		TargetVector:     "target",
		DistanceProvider: distancer,
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
			ID:               indexID,
			RootPath:         rootPath,
			TargetVector:     "target",
			DistanceProvider: distancer,
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
		ID:               indexID,
		RootPath:         rootPath,
		DistanceProvider: distancer,
	}, config, store)
	require.Nil(t, err)

	t.Run("serialize and deserialize RQ1 data", func(t *testing.T) {
		// Add some vectors to create quantizer data
		err = index.Add(ctx, 1, []float32{1.0, 2.0, 3.0, 4.0})
		require.Nil(t, err)
		err = index.Add(ctx, 2, []float32{5.0, 6.0, 7.0, 8.0})
		require.Nil(t, err)

		// Test serialization
		rq1Data, err := index.serializeRQ1Data()
		require.Nil(t, err)
		require.NotNil(t, rq1Data)
		require.Equal(t, uint32(256), rq1Data.InputDim)
		require.Greater(t, rq1Data.OutputDim, uint32(0))
		require.Greater(t, rq1Data.Rounds, uint32(0))

		// Test container creation and serialization
		container := &RQDataContainer{
			Version:         RQDataVersion,
			CompressionType: CompressionTypeRQ1,
			Data:            rq1Data,
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
		ID:               indexID,
		RootPath:         rootPath,
		DistanceProvider: distancer,
	}, config, store)
	require.Nil(t, err)

	t.Run("serialize and deserialize RQ8 data", func(t *testing.T) {
		// Add some vectors to create quantizer data
		err = index.Add(ctx, 1, []float32{1.0, 2.0, 3.0, 4.0})
		require.Nil(t, err)
		err = index.Add(ctx, 2, []float32{5.0, 6.0, 7.0, 8.0})
		require.Nil(t, err)

		// Test serialization
		rq8Data, err := index.serializeRQ8Data()
		require.Nil(t, err)
		require.NotNil(t, rq8Data)
		require.Equal(t, uint32(4), rq8Data.InputDim)
		require.Equal(t, uint32(8), rq8Data.Bits)
		require.Greater(t, rq8Data.OutputDim, uint32(0))
		require.Greater(t, rq8Data.Rounds, uint32(0))

		// Test container creation and serialization
		container := &RQDataContainer{
			Version:         RQDataVersion,
			CompressionType: CompressionTypeRQ8,
			Data:            rq8Data,
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
	})
}
