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
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func Test_NoRaceQuantizedVectorCompressor(t *testing.T) {
	t.Run("loading and deleting data works", func(t *testing.T) {
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), 0, 0, nil, "name", nil)
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
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), 0, 0, nil, "name", nil)
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
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), 0, 0, nil, "name", nil)
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
		compressor, err := compressionhelpers.NewBQCompressor(distancer.NewCosineDistanceProvider(), 1e12, nil, testinghelpers.NewDummyStore(t), 0, 0, nil, "name", nil)
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

	t.Run("recover missing compressed vector from raw vectorForID", func(t *testing.T) {
		vectors := map[uint64][]float32{
			1: {-0.5, 0.5},
			2: {0.25, 0.7},
			3: {0.5, 0.5},
		}
		vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
			v, ok := vectors[id]
			if !ok {
				return nil, storobj.NewErrNotFoundf(id, "not found")
			}
			return v, nil
		}

		compressor, err := compressionhelpers.NewBQCompressor(
			distancer.NewCosineDistanceProvider(), 1e12, nil,
			testinghelpers.NewDummyStore(t), 0, 0, nil, "name", vectorForID)
		require.NoError(t, err)

		// Only preload vectors 1 and 2; leave 3 missing from the compressed bucket.
		compressor.Preload(1, vectors[1])
		compressor.Preload(2, vectors[2])

		// Accessing vector 3 should recover it from vectorForID.
		d, err := compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 2, 3)
		assert.NoError(t, err)
		assert.Equal(t, float32(0), d)

		// DistanceToNode should also recover.
		dist, returnFn := compressor.NewDistancer([]float32{0.1, -0.2})
		defer returnFn()
		_, err = dist.DistanceToNode(3)
		assert.NoError(t, err)
	})

	t.Run("recovery returns ErrNotFound for deleted vectors", func(t *testing.T) {
		vectorForID := func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, storobj.NewErrNotFoundf(id, "deleted")
		}

		compressor, err := compressionhelpers.NewBQCompressor(
			distancer.NewCosineDistanceProvider(), 1e12, nil,
			testinghelpers.NewDummyStore(t), 0, 0, nil, "name", vectorForID)
		require.NoError(t, err)

		// Vector 1 was never preloaded and vectorForID returns not-found.
		_, err = compressor.DistanceBetweenCompressedVectorsFromIDs(context.Background(), 1, 2)
		var e storobj.ErrNotFound
		assert.ErrorAs(t, err, &e)
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
			0,
			0,
			memwatch.NewDummyMonitor(),
			"name",
			nil,
		)
		require.Nil(t, err)
		d, _ := compressor.NewDistancer(storedVec)
		_, err = d.DistanceToFloat(mismatchedVec)
		assert.EqualError(t, err, "2 vs 3: vector lengths don't match")
	})
}
