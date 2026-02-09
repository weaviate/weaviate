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

package diversity

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// stubView implements common.BucketView for testing.
type stubView struct{}

func (stubView) ReleaseView() {}

// makeVecForID builds a TempVectorForIDWithView backed by an in-memory map.
func makeVecForID(vecs map[uint64][]float32) common.TempVectorForIDWithView[float32] {
	return func(_ context.Context, id uint64, _ *common.VectorSlice, _ common.BucketView) ([]float32, error) {
		v, ok := vecs[id]
		if !ok {
			return nil, fmt.Errorf("vector not found for id %d", id)
		}
		return v, nil
	}
}

func TestMMR_EmptyInput(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(nil), nil, nil, 5, 0.5, stubView{})
	require.NoError(t, err)
	assert.Nil(t, ids)
	assert.Nil(t, dists)
}

func TestMMR_ZeroK(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{1: {1, 0}}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1}, []float32{0.1}, 0, 0.5, stubView{})
	require.NoError(t, err)
	assert.Nil(t, ids)
	assert.Nil(t, dists)
}

func TestMMR_SingleCandidate(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{10: {1, 2, 3}}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{10}, []float32{0.5}, 5, 0.5, stubView{})
	require.NoError(t, err)
	assert.Equal(t, []uint64{10}, ids)
	assert.Equal(t, []float32{0.5}, dists)
}

func TestMMR_KEqualsN(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{
		1: {0, 0},
		2: {1, 0},
		3: {0, 1},
	}
	queryDists := []float32{0.1, 0.2, 0.3}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 0.5, stubView{})
	require.NoError(t, err)
	assert.Len(t, ids, 3)
	assert.Len(t, dists, 3)
	// First selected must be most relevant (lowest query distance).
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, float32(0.1), dists[0])
}

func TestMMR_KGreaterThanN(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{
		1: {1, 0},
		2: {0, 1},
	}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2}, []float32{0.1, 0.2}, 10, 0.5, stubView{})
	require.NoError(t, err)
	// Should clamp to n=2.
	assert.Len(t, ids, 2)
	assert.Len(t, dists, 2)
}

func TestMMR_PureRelevance(t *testing.T) {
	// lambda=1: ignore diversity, return in order of query distance.
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{
		1: {0, 0},
		2: {10, 10},
		3: {5, 5},
	}
	queryDists := []float32{0.1, 0.9, 0.5}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 1.0, stubView{})
	require.NoError(t, err)
	// Pure relevance: sorted by ascending query distance.
	assert.Equal(t, []uint64{1, 3, 2}, ids)
	assert.Equal(t, []float32{0.1, 0.5, 0.9}, dists)
}

func TestMMR_PureDiversity(t *testing.T) {
	// lambda=0: ignore relevance, maximize diversity.
	provider := distancer.NewL2SquaredProvider()
	// Three vectors: two are identical (no diversity), one is far away.
	vecs := map[uint64][]float32{
		1: {0, 0},
		2: {0, 0.01},
		3: {10, 10},
	}
	// All have similar query distances so relevance doesn't matter.
	queryDists := []float32{0.5, 0.5, 0.5}

	ids, _, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 0.0, stubView{})
	require.NoError(t, err)
	// First pick: id 1 (or 2, tied by query dist, picks first encountered).
	assert.Equal(t, uint64(1), ids[0])
	// Second pick should be id 3 (farthest from id 1).
	assert.Equal(t, uint64(3), ids[1])
	// Third: id 2 (only one left).
	assert.Equal(t, uint64(2), ids[2])
}

func TestMMR_DiversityChangesOrder(t *testing.T) {
	// With two clusters near the query, MMR should pick one from each cluster
	// before picking the second from the same cluster.
	provider := distancer.NewL2SquaredProvider()

	// Cluster A: ids 1,2 near each other, slightly closer to query.
	// Cluster B: ids 3,4 near each other, slightly farther from query.
	vecs := map[uint64][]float32{
		1: {1, 0},
		2: {1.1, 0},
		3: {0, 10},
		4: {0, 10.1},
	}
	// Query distances: cluster A is closer.
	queryDists := []float32{0.1, 0.15, 0.5, 0.55}

	ids, _, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3, 4}, queryDists, 4, 0.5, stubView{})
	require.NoError(t, err)
	// First: id 1 (most relevant).
	assert.Equal(t, uint64(1), ids[0])
	// Second: should be from cluster B (diverse from id 1), i.e. id 3 or 4.
	assert.Contains(t, []uint64{3, 4}, ids[1],
		"second pick should be from the other cluster for diversity")
}

func TestMMR_VecLoadError(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	failVecForID := func(_ context.Context, id uint64, _ *common.VectorSlice, _ common.BucketView) ([]float32, error) {
		return nil, fmt.Errorf("disk read error for id %d", id)
	}

	_, _, err := mmr(context.Background(), provider,
		failVecForID, []uint64{1}, []float32{0.1}, 1, 0.5, stubView{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk read error")
}

func TestMMR_ReturnsCorrectDistances(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{
		1: {0, 0},
		2: {5, 5},
		3: {10, 10},
	}
	queryDists := []float32{0.2, 0.8, 1.5}

	ids, dists, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 0.7, stubView{})
	require.NoError(t, err)
	require.Len(t, ids, 3)
	// Verify each returned distance matches the query distance for that id.
	idToDist := map[uint64]float32{1: 0.2, 2: 0.8, 3: 1.5}
	for i, id := range ids {
		assert.Equal(t, idToDist[id], dists[i],
			"distance for id %d should match input query distance", id)
	}
}

func TestMMR_LambdaZeroVsOne(t *testing.T) {
	// Verify that lambda=0 and lambda=1 produce different orderings when
	// relevance and diversity disagree.
	provider := distancer.NewL2SquaredProvider()
	vecs := map[uint64][]float32{
		1: {0, 0},   // very close to query
		2: {0.1, 0}, // also close to query and close to id 1
		3: {10, 10}, // far from query but diverse
	}
	queryDists := []float32{0.01, 0.02, 5.0}

	idsRelevance, _, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 1.0, stubView{})
	require.NoError(t, err)

	idsDiversity, _, err := mmr(context.Background(), provider,
		makeVecForID(vecs), []uint64{1, 2, 3}, queryDists, 3, 0.0, stubView{})
	require.NoError(t, err)

	// With pure relevance, order should be 1,2,3 (by query distance).
	assert.Equal(t, []uint64{1, 2, 3}, idsRelevance)
	// With pure diversity, second pick should be 3 (farthest from 1), not 2.
	assert.Equal(t, uint64(1), idsDiversity[0])
	assert.Equal(t, uint64(3), idsDiversity[1])
}
