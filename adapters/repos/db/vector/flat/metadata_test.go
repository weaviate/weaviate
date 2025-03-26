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

package flat

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
