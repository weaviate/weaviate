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

//go:build !race

package compressionhelpers_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func Test_NoRaceQuantizedVectorCompressor(t *testing.T) {
	t.Run("loading and deleting data works", func(t *testing.T) {
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), nil)
		assert.Nil(t, err)
		compressor.Preload(1, []float32{-0.5, 0.5})
		vec, err := compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 1, 2)
		assert.Equal(t, float32(0), vec)
		assert.NotNil(t, err)

		compressor.Preload(2, []float32{0.25, 0.7})
		compressor.Preload(3, []float32{0.5, 0.5})
		compressor.Delete(context.Background(), 1)

		_, err = compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 1, 2)
		assert.NotNil(t, err)
	})

	t.Run("distance are right when using BQ", func(t *testing.T) {
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), nil)
		assert.Nil(t, err)
		compressor.Preload(1, []float32{-0.5, 0.5})
		compressor.Preload(2, []float32{0.25, 0.7})
		compressor.Preload(3, []float32{0.5, 0.5})

		d, err := compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 1, 2)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 1, 3)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 2, 3)
		assert.Nil(t, err)
		assert.Equal(t, float32(0), d)
	})

	t.Run("distance are right when using BQDistancer", func(t *testing.T) {
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), nil)
		assert.Nil(t, err)
		compressor.Preload(1, []float32{-0.5, 0.5})
		compressor.Preload(2, []float32{0.25, 0.7})
		compressor.Preload(3, []float32{0.5, 0.5})
		distancer, returnFn := compressor.NewDistancer([]float32{0.1, -0.2})
		defer returnFn()

		d, err := distancer.DistanceToNode(1)
		assert.Nil(t, err)
		assert.Equal(t, float32(2), d)

		d, err = distancer.DistanceToNode(2)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = distancer.DistanceToNode(3)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = distancer.DistanceToFloat([]float32{0.8, -0.2})
		assert.Nil(t, err)
		assert.Equal(t, float32(0.88), d)
	})

	t.Run("distance are right when using BQDistancer to compressed node", func(t *testing.T) {
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), nil)
		assert.Nil(t, err)
		compressor.Preload(1, []float32{-0.5, 0.5})
		compressor.Preload(2, []float32{0.25, 0.7})
		compressor.Preload(3, []float32{0.5, 0.5})
		distancer, err := compressor.NewDistancerFromID(1)

		assert.Nil(t, err)

		d, err := distancer.DistanceToNode(1)
		assert.Nil(t, err)
		assert.Equal(t, float32(0), d)

		d, err = distancer.DistanceToNode(2)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = distancer.DistanceToNode(3)
		assert.Nil(t, err)
		assert.Equal(t, float32(1), d)

		d, err = distancer.DistanceToFloat([]float32{0.8, -0.2})
		assert.Nil(t, err)
		assert.Equal(t, float32(2), d)
	})

	t.Run("don't panic when vector dimensions are mismatched", func(t *testing.T) {
		var (
			config = hnsw.PQConfig{
				Enabled:  true,
				Segments: 1,
				Encoder: hnsw.PQEncoder{
					Type:         hnsw.PQEncoderTypeKMeans,
					Distribution: hnsw.PQEncoderDistributionLogNormal,
				},
				Centroids: 1,
			}
			dist         = distancer.NewCosineDistanceProvider()
			dims         = 3
			cacheMaxObjs = 4
			trainingData = [][]float32{
				{0.0, 0.1, 0.2},
			}
		)

		var (
			storedVec     = []float32{0.0, 0.1, 0.2}
			mismatchedVec = []float32{0.0, 0.1}
		)

		compressor, err := compressionhelpers.NewHNSWPQCompressor(
			config, dist, dims, cacheMaxObjs, nil, trainingData,
			testinghelpers.NewDummyStore(t),
			memwatch.NewDummyMonitor(),
		)
		compressor.Preload(0, storedVec)
		require.Nil(t, err)
		_, err = compressor.DistanceBetweenCompressedAndUncompressedVectorsFromID(
			context.Background(),
			0, mismatchedVec)
		msg := "ProductQuantizer.DistanceBetweenCompressedAndUncompressedVectors: " +
			"mismatched dimensions: 2 (search vector), 3 (configured index dims)"
		assert.EqualError(t, err, msg)
	})
}
