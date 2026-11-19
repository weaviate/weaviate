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
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A search by distance asks for a first page of results and, while the last
// result of the page is within the distance, for a larger one. It kept the
// pages it had and added what the larger search returned past them. A search
// that is asked for more does not always rank its first results as the
// smaller one did (it looks at more of the graph), and where the two
// disagreed an id came back twice and another was left out. The answer is now
// that of one search: the last one.
//
// The search here is a stub that disagrees with itself in one known place.
func TestSearchByVectorDistanceIsOneSearch(t *testing.T) {
	const (
		page = DefaultSearchByDistInitialLimit
		// step is the distance between an id and the next: the id i is at
		// distance i*step.
		step = float32(0.001)
		// stray is the id that a search of one page returns in the place of
		// the last result of the page, which it misses.
		stray = 150
	)
	distanceOf := func(id int) float32 { return float32(id) * step }
	// within is a distance that takes in the first n ids and no other.
	within := func(n int) float32 { return (float32(n) - 0.5) * step }
	firstIDs := func(n int) []uint64 {
		ids := make([]uint64, n)
		for i := range ids {
			ids[i] = uint64(i)
		}
		return ids
	}
	// searchOver is a search of an index that holds the first `found` ids.
	// Asked for more than a page, it is exact.
	searchOver := func(found int, asked *[]int) func(context.Context, []float32, int, helpers.AllowList) ([]uint64, []float32, error) {
		return func(_ context.Context, _ []float32, k int, _ helpers.AllowList) ([]uint64, []float32, error) {
			*asked = append(*asked, k)
			ids := firstIDs(min(k, found))
			dists := make([]float32, len(ids))
			for i := range ids {
				dists[i] = distanceOf(i)
			}
			if k == page && found > stray {
				ids[page-1], dists[page-1] = stray, distanceOf(stray)
			}
			return ids, dists, nil
		}
	}

	firstPageWithStray := firstIDs(page)
	firstPageWithStray[page-1] = stray

	tests := []struct {
		name      string
		found     int
		distance  float32
		maxLimit  int64
		want      []uint64
		wantAsked []int
	}{
		{
			name:  "the distance ends in the first page",
			found: 2000, distance: within(50), maxLimit: 10_000,
			want: firstIDs(50), wantAsked: []int{100},
		},
		{
			name:  "the distance ends in the second page",
			found: 2000, distance: within(180), maxLimit: 10_000,
			want: firstIDs(180), wantAsked: []int{100, 1100},
		},
		{
			name:  "every result, with a maximum",
			found: 2000, distance: 1e9, maxLimit: 10_000,
			want: firstIDs(1100), wantAsked: []int{100, 1100},
		},
		{
			// the search of 11100 returns the 2000 there are, and the one
			// after it nothing past them
			name:  "every result, without a maximum",
			found: 2000, distance: 1e9, maxLimit: -1,
			want: firstIDs(2000), wantAsked: []int{100, 1100, 11100, 111100},
		},
		{
			name:  "the distance ends in the second page, without a maximum",
			found: 2000, distance: within(180), maxLimit: -1,
			want: firstIDs(180), wantAsked: []int{100, 1100},
		},
		{
			// the pages already returned are the answer
			name:  "the larger search has nothing past the first page",
			found: page, distance: 1e9, maxLimit: 10_000,
			want: firstIDs(page), wantAsked: []int{100, 1100},
		},
		{
			// the maximum is checked before a larger page is asked for, so
			// the first page is the answer as the search returned it
			name:  "the next page is over the maximum",
			found: 2000, distance: 1e9, maxLimit: 150,
			want: firstPageWithStray, wantAsked: []int{100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			var asked []int
			ids, dists, err := searchByVectorDistance(context.Background(), []float32{0},
				tt.distance, tt.maxLimit, nil, searchOver(tt.found, &asked), logger)
			require.NoError(t, err)
			require.Equal(t, tt.wantAsked, asked, "the pages asked for")
			require.Equal(t, tt.want, ids)
			require.Len(t, dists, len(ids))
			for i, id := range ids {
				require.Equal(t, distanceOf(int(id)), dists[i], "the distance of result %d", i)
			}
		})
	}
}

// The same with a real index. An index with ef 64 searches a page of 100 with
// ef 100 and a page of 1100 with ef 1100. The graph here has few connections,
// so that the two searches do not agree on the first 100 results of a query
// over an index small enough for a unit test.
func TestHnswSearchByVectorDistanceIsOneSearch(t *testing.T) {
	const (
		vectors = 2000
		dim     = 32
		queries = 10
		page    = DefaultSearchByDistInitialLimit
		// the first page and the one after it, the last under the maximum
		secondPage = page + page*DefaultSearchByDistLimitMultiplier
		// the page after that one, which holds every vector
		thirdPage = secondPage + page*DefaultSearchByDistLimitMultiplier*DefaultSearchByDistLimitMultiplier
		// farther than any vector
		anyDistance = float32(1e9)
	)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))
	randomVector := func() []float32 {
		vector := make([]float32, dim)
		for i := range vector {
			vector[i] = rng.Float32()*2 - 1
		}
		return vector
	}
	stored := make([][]float32, vectors)
	for i := range stored {
		stored[i] = randomVector()
	}

	cfg := createVectorHnswIndexTestConfig()
	cfg.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
		return stored[id], nil
	}
	uc := ent.NewDefaultUserConfig()
	uc.EF, uc.MaxConnections, uc.EFConstruction = 64, 8, 32
	index, err := New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	for i, vector := range stored {
		require.NoError(t, index.Add(ctx, uint64(i), vector))
	}

	for range queries {
		query := randomVector()

		// with a maximum, the answer is that of the search of the second page
		want, wantDists, err := index.SearchByVector(ctx, query, secondPage, nil)
		require.NoError(t, err)
		require.Len(t, want, secondPage)
		ids, dists, err := index.SearchByVectorDistance(ctx, query, anyDistance, 10_000, nil)
		require.NoError(t, err)
		requireNoIDTwice(t, ids)
		require.Equal(t, want, ids)
		require.Equal(t, wantDists, dists)

		// without one, that of the search of the third page
		want, wantDists, err = index.SearchByVector(ctx, query, thirdPage, nil)
		require.NoError(t, err)
		require.Greater(t, len(want), secondPage)
		ids, dists, err = index.SearchByVectorDistance(ctx, query, anyDistance, -1, nil)
		require.NoError(t, err)
		requireNoIDTwice(t, ids)
		require.Equal(t, want, ids)
		require.Equal(t, wantDists, dists)
	}
}

func requireNoIDTwice(t *testing.T, ids []uint64) {
	t.Helper()
	seen := make(map[uint64]int, len(ids))
	for i, id := range ids {
		first, ok := seen[id]
		require.False(t, ok, "id %d is result %d and result %d", id, first, i)
		seen[id] = i
	}
}
