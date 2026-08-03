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
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// referenceLateInteractionScore is the pre-SIMD scalar MaxSim loop, kept as
// the semantic reference for the parity test.
func referenceLateInteractionScore(provider distancer.Provider, searchVecs, docVecs [][]float32) (float32, error) {
	similarity := float32(0.0)
	for _, searchVec := range searchVecs {
		maxSim := float32(math.MaxFloat32)
		dist := provider.New(searchVec)
		for _, docVec := range docVecs {
			d, err := dist.Distance(docVec)
			if err != nil {
				return 0.0, err
			}
			if d < maxSim {
				maxSim = d
			}
		}
		similarity += maxSim
	}
	return similarity, nil
}

func randomVecSet(rng *rand.Rand, num, dims int) [][]float32 {
	out := make([][]float32, num)
	for i := range out {
		out[i] = make([]float32, dims)
		for j := range out[i] {
			out[i][j] = float32(rng.NormFloat64())
		}
	}
	return out
}

func TestLateInteractionScoreMatchesReference(t *testing.T) {
	provider := distancer.NewDotProductProvider()

	cases := []struct {
		name              string
		queryTokens, dims int
		docTokens         int
	}{
		{"colbert-shape", 32, 128, 100},
		{"single-doc-token", 32, 128, 1},
		{"single-query-token", 1, 128, 50},
		{"small-dims", 8, 48, 20},
		{"unaligned-dims", 8, 125, 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(tc.dims), uint64(tc.docTokens)))
			searchVecs := randomVecSet(rng, tc.queryTokens, tc.dims)
			docVecs := randomVecSet(rng, tc.docTokens, tc.dims)

			want, err := referenceLateInteractionScore(provider, searchVecs, docVecs)
			require.NoError(t, err)
			got, err := lateInteractionScore(provider, searchVecs, docVecs)
			require.NoError(t, err)
			require.InDelta(t, want, got, 1e-3)
		})
	}
}

// Mismatched dimensions must keep returning an error, exactly like the
// pre-SIMD path (the batched kernel would silently truncate instead).
func TestLateInteractionScoreRaggedDims(t *testing.T) {
	provider := distancer.NewDotProductProvider()
	rng := rand.New(rand.NewPCG(3, 4))

	searchVecs := randomVecSet(rng, 4, 128)
	docVecs := randomVecSet(rng, 4, 64)
	_, err := lateInteractionScore(provider, searchVecs, docVecs)
	require.ErrorIs(t, err, distancer.ErrVectorLength)

	// one ragged doc token among equal ones
	docVecs = randomVecSet(rng, 4, 128)
	docVecs[2] = docVecs[2][:100]
	_, err = lateInteractionScore(provider, searchVecs, docVecs)
	require.ErrorIs(t, err, distancer.ErrVectorLength)

	// ragged query tokens
	searchVecs[1] = searchVecs[1][:100]
	docVecs = randomVecSet(rng, 4, 128)
	_, err = lateInteractionScore(provider, searchVecs, docVecs)
	require.ErrorIs(t, err, distancer.ErrVectorLength)
}

// ColBERT-style shapes: 32 query tokens x 100 doc tokens, 128 dims.
func BenchmarkLateInteractionScore(b *testing.B) {
	provider := distancer.NewDotProductProvider()
	rng := rand.New(rand.NewPCG(42, 43))
	searchVecs := randomVecSet(rng, 32, 128)
	docVecs := randomVecSet(rng, 100, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := lateInteractionScore(provider, searchVecs, docVecs); err != nil {
			b.Fatal(err)
		}
	}
}

// Empty doc-token sets keep the pre-SIMD behavior: each query token
// contributes MaxFloat32.
func TestLateInteractionScoreEmptyDocVecs(t *testing.T) {
	provider := distancer.NewDotProductProvider()
	rng := rand.New(rand.NewPCG(5, 6))
	searchVecs := randomVecSet(rng, 2, 128)

	want, err := referenceLateInteractionScore(provider, searchVecs, nil)
	require.NoError(t, err)
	got, err := lateInteractionScore(provider, searchVecs, nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
