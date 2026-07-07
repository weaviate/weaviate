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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Regression tests for issue #276: multi-vector search with cosine distance
// must normalize tokens on both the insert and the query/rescore paths. The
// cosine-dot provider computes 1-dot, which only equals the cosine distance
// on unit vectors, and callers are not required to send normalized tokens.

func randomMultiVector(rng *rand.Rand, tokens, dim int) [][]float32 {
	mv := make([][]float32, tokens)
	for i := range mv {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		mv[i] = v
	}
	return mv
}

// TestSearchByMultiVectorMaxSimOrderingCosine mirrors e2e TC-013[cosine]:
// with A=[e0,e0], B=[e0,e1], C=[e1,e1] and query [e0], A and B tie on exact
// MaxSim (both contain a token identical to the query token), so the
// deterministic (distance, id) tie-break must order A before B, and C, the
// only doc without an e0 token, must come last.
func TestSearchByMultiVectorMaxSimOrderingCosine(t *testing.T) {
	const dim = 32
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))

	onehot := func(idx int) []float32 {
		v := make([]float32, dim)
		v[idx] = 1.0
		return v
	}
	e0, e1 := onehot(0), onehot(1)

	addMultiVectorToIndex(t, &tf, 0, [][]float32{e0, e0}) // A
	addMultiVectorToIndex(t, &tf, 1, [][]float32{e0, e1}) // B
	addMultiVectorToIndex(t, &tf, 2, [][]float32{e1, e1}) // C

	ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{e0}, 3, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2}, ids, "expected order A, B, C")

	require.Len(t, dists, 3)
	assert.Equal(t, float32(0), dists[0], "A contains the query token, distance must be 0")
	assert.Equal(t, float32(0), dists[1], "B contains the query token, distance must be 0")
	assert.Equal(t, float32(1), dists[2], "C is orthogonal to the query token")
}

// TestSearchByMultiVectorSelfRecallCosine mirrors e2e TC-006: querying with a
// document's own (unnormalized) multi-vector must return that document as
// top-1. All docs fit within the default rescoreLimit, so the exact MaxSim
// rescore sees every document and self-recall@1 must be perfect.
func TestSearchByMultiVectorSelfRecallCosine(t *testing.T) {
	const (
		nDocs  = 200
		tokens = 8
		dim    = 96
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))

	rng := rand.New(rand.NewSource(7))
	docs := make([][][]float32, nDocs)
	for i := 0; i < nDocs; i++ {
		docs[i] = randomMultiVector(rng, tokens, dim)
		addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
	}

	sampleRng := rand.New(rand.NewSource(13))
	const samples = 50
	for q := 0; q < samples; q++ {
		id := uint64(sampleRng.Intn(nDocs))
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 1, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		require.Equal(t, id, ids[0],
			"self-query for doc %d returned doc %d: exact MaxSim rescore must rank the identical multi-vector first", id, ids[0])
	}
}

// TestResultSetTieBreak pins the deterministic (distance, id) ordering of
// ResultSet: equal distances must be returned in ascending id order
// regardless of insertion order, including evictions at the capacity
// boundary.
func TestResultSetTieBreak(t *testing.T) {
	type entry struct {
		id   uint64
		dist float32
	}

	tests := []struct {
		name    string
		k       int
		inserts []entry
		want    []uint64
	}{
		{
			name:    "all tied, inserted ascending",
			k:       3,
			inserts: []entry{{1, 0.5}, {2, 0.5}, {3, 0.5}},
			want:    []uint64{1, 2, 3},
		},
		{
			name:    "all tied, inserted descending",
			k:       3,
			inserts: []entry{{3, 0.5}, {2, 0.5}, {1, 0.5}},
			want:    []uint64{1, 2, 3},
		},
		{
			name:    "tie broken within mixed distances",
			k:       4,
			inserts: []entry{{7, 0.2}, {3, 0.1}, {5, 0.2}, {1, 0.3}},
			want:    []uint64{3, 5, 7, 1},
		},
		{
			name:    "eviction at boundary keeps lowest ids among ties",
			k:       2,
			inserts: []entry{{9, 0.5}, {4, 0.5}, {6, 0.5}, {2, 0.5}},
			want:    []uint64{2, 4},
		},
		{
			name:    "tied item does not evict smaller id",
			k:       1,
			inserts: []entry{{2, 0.5}, {5, 0.5}},
			want:    []uint64{2},
		},
		{
			name:    "smaller distance still wins over smaller id",
			k:       2,
			inserts: []entry{{1, 0.9}, {2, 0.9}, {8, 0.1}},
			want:    []uint64{8, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := NewResultSet(tt.k)
			for _, in := range tt.inserts {
				rs.Insert(in.id, in.dist)
			}

			got := make([]uint64, 0, rs.Len())
			for id := range rs.Iter() {
				got = append(got, id)
			}
			require.Equal(t, tt.want, got)

			// distances must remain sorted ascending
			var prev float32 = -1
			for _, dist := range rs.data {
				require.GreaterOrEqual(t, dist.Distance, prev)
				prev = dist.Distance
			}
		})
	}
}
