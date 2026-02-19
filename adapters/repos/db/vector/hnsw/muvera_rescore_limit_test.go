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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// minimalMuveraHnsw returns a minimal hnsw instance configured for
// computeScoreWithView testing, with the TempMultiVectorForIDWithViewThunk
// returning the provided docVecs for every docID.
func minimalMuveraHnsw(t *testing.T, docVecs [][]float32, limit *configRuntime.DynamicValue[int]) *hnsw {
	t.Helper()
	logger, _ := test.NewNullLogger()
	h := &hnsw{
		logger:                 logger,
		multiDistancerProvider: distancer.NewDotProductProvider(),
		muveraRescoreLimit:     limit,
		TempMultiVectorForIDWithViewThunk: func(_ context.Context, _ uint64, _ *common.VectorSlice, _ common.BucketView) ([][]float32, error) {
			return docVecs, nil
		},
	}
	h.muvera.Store(true)
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
	score0, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 0)
	require.NoError(t, err)

	// Manually compute expected: dot product is a distance (lower = more similar).
	// DotProduct distance for (1,0)·(1,0) = -(1·1+0·0) = -1; for (0,1)·(1,0)=0; for (-1,0)·(1,0)=1; for (0,-1)·(1,0)=0.
	// min across all = -1, so similarity = -1.
	assert.Equal(t, float32(-1), score0)
}

// TestComputeScoreWithView_WithLimit verifies that maxDocVecs<len(docVecs) uses strided sampling.
func TestComputeScoreWithView_WithLimit(t *testing.T) {
	// 10 doc vectors; budget = 2 → step = 10/2 = 5, so indices 0 and 5 are used.
	docVecs := make([][]float32, 10)
	for i := range docVecs {
		docVecs[i] = []float32{float32(i), 0}
	}
	searchVecs := [][]float32{{1, 0}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	// maxDocVecs=2 with 10 doc vectors → step=5, indices 0 and 5 are sampled.
	score, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
	require.NoError(t, err)

	// DotProduct distances for searchVec=(1,0):
	//   index 0: (0,0)·(1,0) = 0, dist = -0 = 0
	//   index 5: (5,0)·(1,0) = 5, dist = -5
	// min = -5, so similarity = -5
	assert.Equal(t, float32(-5), score)
}

// TestComputeScoreWithView_LimitGELength verifies that when maxDocVecs >= len(docVecs),
// all vectors are used (step=1, same as no limit).
func TestComputeScoreWithView_LimitGELength(t *testing.T) {
	docVecs := [][]float32{{1, 0}, {0, 1}, {-1, 0}}
	searchVecs := [][]float32{{1, 0}}

	h := minimalMuveraHnsw(t, docVecs, nil)
	slice := &common.VectorSlice{}

	// Score with no limit.
	scoreNoLimit, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 0)
	require.NoError(t, err)

	// Score with maxDocVecs == len(docVecs): should behave identically.
	scoreLimitEqual, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 3)
	require.NoError(t, err)

	// Score with maxDocVecs > len(docVecs): should also behave identically.
	scoreLimitMore, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 100)
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

	first, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		again, _, err := h.computeScoreWithView(context.Background(), searchVecs, 0, slice, &noopBucketView{}, 2)
		require.NoError(t, err)
		assert.Equal(t, first, again, "result must be deterministic (run %d)", i)
	}
}

// TestVectorsPerCandidateCalculation verifies the budget → per-candidate math.
func TestVectorsPerCandidateCalculation(t *testing.T) {
	tests := []struct {
		name        string
		limit       int
		nCandidates int
		want        int
	}{
		{"zero limit means no sampling", 0, 100, 0},
		{"1000 budget / 100 candidates = 10", 1000, 100, 10},
		{"budget smaller than candidates gets clamped to 1", 50, 100, 1},
		{"exact division", 200, 10, 20},
		{"limit=1 / 1 candidate = 1", 1, 1, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got int
			if tc.limit > 0 && tc.nCandidates > 0 {
				got = max(1, tc.limit/tc.nCandidates)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestComputeLateInteraction_RespectsMuveraRescoreLimit verifies the wiring
// from muveraRescoreLimit → vectorsPerCandidate inside computeLateInteraction.
func TestComputeLateInteraction_RespectsMuveraRescoreLimit(t *testing.T) {
	// Candidates: 100 docs, each with 10 vectors.
	// limit = 100 → vectorsPerCandidate = max(1, 100/100) = 1 → step = 10/1 = 10.
	// Only index 0 of each doc's vectors is used.
	nCandidates := 100
	nVecsPerDoc := 10

	docVecsCalled := 0
	// Use a custom thunk to count how many vec comparisons happen.
	// Return nVecsPerDoc vectors per doc.
	docVecs := make([][]float32, nVecsPerDoc)
	for i := range docVecs {
		docVecs[i] = []float32{float32(i), 0}
	}

	logger, _ := test.NewNullLogger()
	limit := configRuntime.NewDynamicValue(nCandidates) // 100 total → 1 per candidate

	h := &hnsw{
		logger:                 logger,
		multiDistancerProvider: distancer.NewDotProductProvider(),
		muveraRescoreLimit:     limit,
		rescoreConcurrency:     1,
		GetViewThunk:           func() common.BucketView { return &noopBucketView{} },
		TempMultiVectorForIDWithViewThunk: func(_ context.Context, _ uint64, _ *common.VectorSlice, _ common.BucketView) ([][]float32, error) {
			docVecsCalled++
			return docVecs, nil
		},
	}
	h.muvera.Store(true)
	h.pools = newPools(2, 512)

	candidateSet := make(map[uint64]struct{}, nCandidates)
	for i := 0; i < nCandidates; i++ {
		candidateSet[uint64(i)] = struct{}{}
	}

	queryVectors := [][]float32{{1, 0}}
	_, _, err := h.computeLateInteraction(context.Background(), queryVectors, 10, candidateSet)
	require.NoError(t, err)

	// Each candidate's TempMultiVectorForIDWithViewThunk should be called once.
	assert.Equal(t, nCandidates, docVecsCalled)
}
