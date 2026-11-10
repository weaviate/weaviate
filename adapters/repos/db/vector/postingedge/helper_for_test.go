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

package postingedge

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storobj"
)

// clusteredVectors draws n vectors around a number of random centers, so
// that neighborhoods are meaningful and postings have boundaries to cross.
func clusteredVectors(n, dims, clusters int, seed uint64) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, 17))
	centers := make([][]float32, clusters)
	for c := range centers {
		centers[c] = make([]float32, dims)
		for j := range centers[c] {
			centers[c][j] = float32(rng.NormFloat64())
		}
	}
	vectors := make([][]float32, n)
	for i := range vectors {
		center := centers[rng.IntN(clusters)]
		vectors[i] = make([]float32, dims)
		for j := range vectors[i] {
			vectors[i][j] = center[j] + 0.35*float32(rng.NormFloat64())
		}
	}
	return vectors
}

// sequentialIDs returns the ids offset, offset+1, ... for n vectors.
func sequentialIDs(n int, offset uint64) []uint64 {
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = offset + uint64(i)
	}
	return ids
}

// testConfig wires an index config to read full-precision vectors from the
// given slice, where the vector of id offset+i is vectors[i].
func testConfig(t testing.TB, vectors [][]float32, offset uint64, provider distancer.Provider) Config {
	t.Helper()
	return Config{
		ID:               "postingedge-test",
		RootPath:         t.TempDir(),
		DistanceProvider: provider,
		Seed:             42,
		TempVectorForIDWithViewThunk: func(_ context.Context, id uint64, container *common.VectorSlice,
			_ common.BucketView,
		) ([]float32, error) {
			if id < offset || id-offset >= uint64(len(vectors)) {
				return nil, storobj.NewErrNotFoundf(id, "no such vector")
			}
			container.Slice = append(container.Slice[:0], vectors[id-offset]...)
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
	}
}

// buildTestIndex creates an index over the vectors with ids 0..n-1, adds
// them in a few batches and builds it.
func buildTestIndex(t testing.TB, vectors [][]float32, provider distancer.Provider, uc UserConfig) *Index {
	t.Helper()
	ctx := context.Background()

	index, err := New(testConfig(t, vectors, 0, provider), uc)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Shutdown(context.Background()))
	})

	ids := sequentialIDs(len(vectors), 0)
	const batch = 500
	for start := 0; start < len(vectors); start += batch {
		end := min(start+batch, len(vectors))
		require.NoError(t, index.AddBatch(ctx, ids[start:end], vectors[start:end]))
	}
	require.NoError(t, index.Build(ctx))
	return index
}

// exactNearest returns the positions of the k vectors nearest to the query
// by brute force, nearest first, ties by position. skip excludes one
// position, or none if negative.
func exactNearest(t testing.TB, vectors [][]float32, query []float32, k, skip int,
	provider distancer.Provider,
) []scored {
	t.Helper()
	all := make([]scored, 0, len(vectors))
	for i, vec := range vectors {
		if i == skip {
			continue
		}
		dist, err := provider.SingleDist(query, vec)
		require.NoError(t, err)
		all = append(all, scored{id: uint64(i), dist: dist})
	}
	slices.SortFunc(all, func(a, b scored) int {
		switch {
		case worse(b, a):
			return -1
		case worse(a, b):
			return 1
		}
		return 0
	})
	return all[:min(k, len(all))]
}

// normalizedCopy returns unit-length copies of the vectors.
func normalizedCopy(vectors [][]float32) [][]float32 {
	out := make([][]float32, len(vectors))
	for i, vec := range vectors {
		out[i] = distancer.Normalize(vec)
	}
	return out
}

// recallAt measures how many of the exact top k the index returns, averaged
// over the queries. truth vectors are the ones distances are measured on.
func recallAt(t testing.TB, index *Index, truth [][]float32, queries [][]float32, k int,
	provider distancer.Provider,
) float64 {
	t.Helper()
	hits, total := 0, 0
	for _, query := range queries {
		ids, _, err := index.SearchByVector(context.Background(), query, k, nil)
		require.NoError(t, err)
		want := exactNearest(t, truth, query, k, -1, provider)
		for _, w := range want {
			if slices.Contains(ids, w.id) {
				hits++
			}
		}
		total += len(want)
	}
	return float64(hits) / float64(total)
}

func isFinite(vec []float32) bool {
	for _, x := range vec {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return false
		}
	}
	return true
}
