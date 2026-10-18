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

package testinghelpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type DistanceSearcher interface {
	SearchByVectorDistance(ctx context.Context, vector []float32, targetDistance float32,
		maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error)
}

// RunSearchByVectorDistanceCutoffTests checks that distance cutoffs are
// applied exactly, however small (gh-13315), while cosine still matches exact
// duplicates at distance 0 despite float32 noise. build must return an index
// with the given provider that holds vectors[i] under id i.
func RunSearchByVectorDistanceCutoffTests(t *testing.T,
	build func(t *testing.T, provider distancer.Provider, vectors [][]float32) DistanceSearcher,
) {
	for _, tc := range []struct {
		name     string
		provider distancer.Provider
		vectors  [][]float32
		query    []float32
		cutoffs  map[float32][]uint64
	}{
		{
			// squared distances 1e-40, 9e-40, 1e-38 (subnormal), the issue's repro
			name:     "l2 subnormal",
			provider: distancer.NewL2SquaredProvider(),
			vectors:  [][]float32{{1e-20, 0}, {3e-20, 0}, {1e-19, 0}},
			query:    []float32{0, 0},
			cutoffs:  map[float32][]uint64{4e-40: {0}, 5e-40: {0}, 2e-39: {0, 1}, 1e-38: {0, 1, 2}},
		},
		{
			// squared distances 1e-8, 9e-8, 1e-6: normal floats below the old 1e-6 tolerance
			name:     "l2 normal below 1e-6",
			provider: distancer.NewL2SquaredProvider(),
			vectors:  [][]float32{{1e-4, 0}, {3e-4, 0}, {1e-3, 0}},
			query:    []float32{0, 0},
			cutoffs:  map[float32][]uint64{5e-9: {}, 5e-8: {0}, 5e-7: {0, 1}, 2e-6: {0, 1, 2}},
		},
		{
			// a duplicate of [0.123, 0.456] computes as ~6e-8, not 0
			name:     "cosine duplicate at zero",
			provider: distancer.NewCosineDistanceProvider(),
			vectors:  [][]float32{distancer.Normalize([]float32{0.123, 0.456}), distancer.Normalize([]float32{0.456, 0.123})},
			query:    []float32{0.123, 0.456},
			cutoffs:  map[float32][]uint64{0: {0}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index := build(t, tc.provider, tc.vectors)
			for cutoff, want := range tc.cutoffs {
				ids, _, err := index.SearchByVectorDistance(context.Background(), tc.query, cutoff, 100, nil)
				require.NoError(t, err)
				assert.ElementsMatch(t, want, ids, "cutoff %g", cutoff)
			}
		})
	}
}
