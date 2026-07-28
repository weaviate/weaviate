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

package hnsw

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// minimalMuveraHnsw returns a minimal hnsw instance configured for
// computeScoreWithView and computeLateInteraction testing, with the
// TempMultiVectorForIDWithViewThunk returning the provided docVecs for every
// docID.
func minimalMuveraHnsw(t *testing.T, docVecs [][]float32, limit *configRuntime.DynamicValue[int]) *hnsw {
	t.Helper()
	logger, _ := test.NewNullLogger()
	h := &hnsw{
		logger:                 logger,
		multiDistancerProvider: distancer.NewDotProductProvider(),
		muveraRescoreLimit:     limit,
		rescoreConcurrency:     1,
		GetViewThunk:           func() common.BucketView { return &noopBucketView{} },
		TempMultiVectorForIDWithViewThunk: func(_ context.Context, _ uint64, _ *common.VectorSlice, _ common.BucketView) ([][]float32, error) {
			return docVecs, nil
		},
	}
	h.muvera.Store(true)
	h.pools = newPools(2, 512)
	return h
}

// TestComputeScoreWithView_NoLimit verifies that maxDocVecs=0 uses all vectors.
func TestComputeScoreWithView_NoLimit(t *testing.T) {
	// 4 doc vectors, query has 1 search vector.
	docVecs := [][]float32{
		{1, 0},
		{0, 1},
		{-1, 0},
		{0, -1},
	}
	searchVecs := [][]float32{{1, 0}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	// With maxDocVecs=0: all 4 doc vectors are evaluated.
	score0, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 0)
	require.NoError(t, err)

	// Manually compute expected: dot product is a distance (lower = more similar).
	// DotProduct distance for (1,0)·(1,0) = -(1·1+0·0) = -1; for (0,1)·(1,0)=0; for (-1,0)·(1,0)=1; for (0,-1)·(1,0)=0.
	// min across all = -1, so similarity = -1.
	assert.Equal(t, float32(-1), score0)
}

// TestComputeScoreWithView_WithLimit verifies that maxDocVecs<len(docVecs) uses strided sampling
// and that both nDocVecsRead and nDistanceComputations are returned correctly.
func TestComputeScoreWithView_WithLimit(t *testing.T) {
	// 10 doc vectors; budget = 2 → step = 10/2 = 5, so indices 0 and 5 are used.
	docVecs := make([][]float32, 10)
	for i := range docVecs {
		docVecs[i] = []float32{float32(i), 0}
	}
	// 2 search vectors to verify nDistanceComputations = nDocVecsRead * len(searchVecs).
	searchVecs := [][]float32{{1, 0}, {0, 1}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	// maxDocVecs=2 with 10 doc vectors → step=5, indices 0 and 5 are sampled.
	score, nDocVecs, nDistComps, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
	require.NoError(t, err)

	// DotProduct distances for searchVec=(1,0):
	//   index 0: (0,0)·(1,0) = 0, dist = -0 = 0
	//   index 5: (5,0)·(1,0) = 5, dist = -5
	// min = -5
	// DotProduct distances for searchVec=(0,1):
	//   index 0: (0,0)·(0,1) = 0, dist = 0
	//   index 5: (5,0)·(0,1) = 0, dist = 0
	// min = 0
	// similarity = -5 + 0 = -5
	assert.Equal(t, float32(-5), score)
	// 2 doc-vector slots sampled (step=5, indices 0 and 5).
	assert.Equal(t, 2, nDocVecs)
	// 2 doc vecs × 2 query vecs = 4 distance computations.
	assert.Equal(t, 4, nDistComps)
}

// TestComputeScoreWithView_LimitGELength verifies that when maxDocVecs >= len(docVecs),
// all vectors are used (step=1, same as no limit).
func TestComputeScoreWithView_LimitGELength(t *testing.T) {
	docVecs := [][]float32{{1, 0}, {0, 1}, {-1, 0}}
	searchVecs := [][]float32{{1, 0}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	// Score with no limit.
	scoreNoLimit, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 0)
	require.NoError(t, err)

	// Score with maxDocVecs == len(docVecs): should behave identically.
	scoreLimitEqual, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 3)
	require.NoError(t, err)

	// Score with maxDocVecs > len(docVecs): should also behave identically.
	scoreLimitMore, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 100)
	require.NoError(t, err)

	assert.Equal(t, scoreNoLimit, scoreLimitEqual)
	assert.Equal(t, scoreNoLimit, scoreLimitMore)
}

// TestComputeScoreWithView_Determinism verifies that repeated calls with the
// same arguments produce the same result.
func TestComputeScoreWithView_Determinism(t *testing.T) {
	docVecs := [][]float32{{1, 0}, {0, 1}, {0.5, 0.5}, {-1, 0}, {0, -1}}
	searchVecs := [][]float32{{0.7, 0.3}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	first, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		again, _, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
		require.NoError(t, err)
		assert.Equal(t, first, again, "result must be deterministic (run %d)", i)
	}
}

// TestComputeLateInteraction_RespectsMuveraRescoreLimit pins that the configured
// rescore budget reaches the per-candidate sampling stride, including the
// division and the clamp that turns a budget thinner than the candidate count
// into one vector each.
//
// Every candidate returns the same ten doc vectors, whose distances against the
// query are -1..-10, so the returned score alone identifies which vectors were
// sampled: a full scan reaches the best (-10) vector, while a budget that forces
// a stride only ever reaches a worse one. A limit that binds therefore has to
// produce a different score from one that does not.
func TestComputeLateInteraction_RespectsMuveraRescoreLimit(t *testing.T) {
	const (
		nCandidates = 100
		nVecsPerDoc = 10
		k           = 10
	)

	// docVecs[i] = {i+1, 0} against query {1, 0} yields dot product i+1 and thus
	// distance -(i+1), so the sampled vector with the highest index wins.
	docVecs := make([][]float32, nVecsPerDoc)
	for i := range docVecs {
		docVecs[i] = []float32{float32(i + 1), 0}
	}
	queryVectors := [][]float32{{1, 0}}

	tests := []struct {
		name     string
		limit    *configRuntime.DynamicValue[int]
		wantDist float32
	}{
		{"unset limit scans every vector", nil, -10},
		{"zero limit scans every vector", configRuntime.NewDynamicValue(0), -10},
		{"budget of one vector per candidate samples index 0 only", configRuntime.NewDynamicValue(100), -1},
		{"budget below the candidate count is clamped to one vector each", configRuntime.NewDynamicValue(50), -1},
		{"budget of two vectors per candidate samples indices 0 and 5", configRuntime.NewDynamicValue(200), -6},
		{"budget above the vectors available does not bind", configRuntime.NewDynamicValue(2000), -10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := minimalMuveraHnsw(t, docVecs, tc.limit)
			var scored atomic.Int64
			h.TempMultiVectorForIDWithViewThunk = func(_ context.Context, _ uint64, _ *common.VectorSlice, _ common.BucketView) ([][]float32, error) {
				scored.Add(1)
				return docVecs, nil
			}

			candidateSet := make(map[uint64]struct{}, nCandidates)
			for i := 0; i < nCandidates; i++ {
				candidateSet[uint64(i)] = struct{}{}
			}

			ids, dists, err := h.computeLateInteraction(context.Background(), queryVectors, k, candidateSet)
			require.NoError(t, err)
			require.Len(t, ids, k)
			require.Len(t, dists, k)

			for i, got := range dists {
				assert.Equal(t, tc.wantDist, got, "result %d was scored on the wrong doc vectors", i)
			}

			// The budget caps vectors per candidate, never the candidate count, so
			// every candidate must still be rescored.
			assert.Equal(t, int64(nCandidates), scored.Load())
		})
	}
}
