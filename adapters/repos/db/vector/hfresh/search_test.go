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

package hfresh

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestSearchWithEmptyIndex(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 32)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if indexID == 0 {
			return vectors[0], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// search on empty index returns 0 results and no error
	ids, dists, err := index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)

	err = index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Len(t, dists, 1)
	require.Equal(t, uint64(0), ids[0])

	err = index.Delete(0)
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}

// TestSearchCosineDistanceRescore verifies that HFresh correctly computes and
// reports distances when using cosine distance. The VectorForIDThunk returns
// raw (unnormalized) vectors to simulate the real object store, where vectors
// are stored as provided by the user.
//
// The bug: HFresh normalizes the query vector internally for quantized search
// but the rescore step fetches raw vectors from vectorForId. The cosine-dot
// distance function computes 1-dot(a,b) and clamps negative values to 0.
// Since dot(normalized_query, unnormalized_stored) > 1 for vectors with
// magnitude > 1, all rescored distances get clamped to 0.
func TestSearchCosineDistanceRescore(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Use cosine distance for both the main index and centroids,
	// matching how shard_init_vector.go configures HFresh in production.
	cfg.DistanceProvider = distancer.NewCosineDistanceProvider()
	cfg.Centroids.HNSWConfig.DistanceProvider = distancer.NewCosineDistanceProvider()

	vectorsSize := 500
	dimensions := 32
	k := 10

	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectorsSize, 0, dimensions)

	// Verify that our test vectors have magnitude > 1, which is
	// required to trigger the bug.
	var norm float32
	for _, v := range vectors[0] {
		norm += v * v
	}
	norm = float32(math.Sqrt(float64(norm)))
	require.Greater(t, norm, float32(1.0),
		"test vectors should have magnitude > 1 to exercise the bug")

	// VectorForIDThunk returns RAW (unnormalized) vectors, simulating
	// the real object store. In production, the shard stores the user's
	// original vector, not the normalized version that HFresh uses internally.
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	for i := 0; i < vectorsSize; i++ {
		err := index.Add(t.Context(), uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Wait for background tasks (splits, merges) to complete.
	for index.taskQueue.Size() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	// Search for a vector that was indexed. The self-match must be the
	// top result with distance ≈ 0, and the remaining results must have
	// non-zero distances in ascending order.
	queryID := uint64(42)
	ids, dists, err := index.SearchByVector(t.Context(), vectors[queryID], k, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	// Self-match must be rank 1.
	assert.Equal(t, queryID, ids[0],
		"self-match should be the first result")

	// Self-match distance must be approximately 0.
	assert.InDelta(t, 0, dists[0], 0.01,
		"self-match distance should be near zero")

	// Not all distances should be zero — if they are, the rescore is broken.
	allZero := true
	for _, d := range dists {
		if d > 0 {
			allZero = false
			break
		}
	}
	assert.False(t, allZero,
		"all distances are zero: rescore is not producing correct distances")

	// Distances must be monotonically non-decreasing (correctly ordered).
	for i := 1; i < len(dists); i++ {
		assert.LessOrEqual(t, dists[i-1], dists[i],
			"distances should be non-decreasing: dists[%d]=%f > dists[%d]=%f",
			i-1, dists[i-1], i, dists[i])
	}

	// Verify returned distances match independently computed cosine distances.
	cosine := distancer.NewCosineDistanceProvider()
	for i, id := range ids {
		normalizedQuery := distancer.Normalize(vectors[queryID])
		normalizedStored := distancer.Normalize(vectors[id])
		expected, err := cosine.SingleDist(normalizedQuery, normalizedStored)
		require.NoError(t, err)
		assert.InDelta(t, expected, dists[i], 0.01,
			"result %d (id=%d): returned distance %f != expected cosine distance %f",
			i, id, dists[i], expected)
	}
}
