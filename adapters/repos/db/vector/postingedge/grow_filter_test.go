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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// testFilter is a filter of the tests, as the measure draws them
// (integration-plan.md, section 8): a share of the vectors at random, or the
// share nearest to a pivot, which lies near the queries or far from them.
type testFilter struct {
	name  string
	allow helpers.AllowList
}

// randomFilter allows a share of the doc ids [0, n), drawn at random.
func randomFilter(n int, share float64, seed uint64) helpers.AllowList {
	rng := rand.New(rand.NewPCG(seed, 3))
	ids := rng.Perm(n)[:max(1, int(share*float64(n)))]
	allowed := make([]uint64, len(ids))
	for i, id := range ids {
		allowed[i] = uint64(id)
	}
	return helpers.NewAllowList(allowed...)
}

// ballFilter allows the share of the given vectors, by doc id, nearest to a
// pivot.
func ballFilter(t *testing.T, provider distancer.Provider, vectors [][]float32, pivot []float32, share float64,
) helpers.AllowList {
	t.Helper()
	all := make([]scored, len(vectors))
	for id, vector := range vectors {
		dist, err := provider.SingleDist(pivot, vector)
		require.NoError(t, err)
		all[id] = scored{id: uint64(id), dist: dist}
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
	allowed := make([]uint64, 0, max(1, int(share*float64(len(vectors)))))
	for _, s := range all[:cap(allowed)] {
		allowed = append(allowed, s.id)
	}
	return helpers.NewAllowList(allowed...)
}

// testFilters draws the filters of a test over the first n vectors: at
// random, and the balls around a vector near the queries and one far from
// them, at the shares given.
func testFilters(t *testing.T, provider distancer.Provider, vectors [][]float32, near, far []float32,
	shares ...float64,
) []testFilter {
	t.Helper()
	var out []testFilter
	for i, share := range shares {
		out = append(out,
			testFilter{name: errors.Errorf("%g at random", share).Error(), allow: randomFilter(len(vectors), share, uint64(i)+1)},
			testFilter{name: errors.Errorf("%g near the queries", share).Error(), allow: ballFilter(t, provider, vectors, near, share)},
			testFilter{name: errors.Errorf("%g far from them", share).Error(), allow: ballFilter(t, provider, vectors, far, share)},
		)
	}
	return out
}

// filteredTruth is the k doc ids nearest to the query among the allowed, by
// exact distance.
func filteredTruth(t *testing.T, provider distancer.Provider, vectors [][]float32, query []float32, k int,
	allowed func(id uint64) bool,
) []uint64 {
	t.Helper()
	var best topK
	best.reset(k)
	for id, vector := range vectors {
		if allowed != nil && !allowed(uint64(id)) {
			continue
		}
		dist, err := provider.SingleDist(query, vector)
		require.NoError(t, err)
		best.push(uint64(id), dist)
	}
	ids, _, err := topResults(best.sorted(), k)
	require.NoError(t, err)
	return ids
}

// The search of the index that grows on disk takes a filter as its twin in
// memory does: the members the filter leaves out route and are never
// candidates. Grown by the same inserts, with filters at random and around
// vectors near and far from the queries, at the shares of the measure and
// lower, the two return the same doc ids at the same distances, by the same
// work, and none of them is one the filter leaves out. So with the codes of
// the endpoints in memory and in the halos of the postings.
func TestGrowFilteredSearchEqualsMemory(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		format growConfig
		bits   int
	}{
		{name: "the format of a shard", format: shardFormat(8), bits: 64},
		{name: "no codes in memory", format: haloFormat(8, false), bits: 64},
		{name: "the full code", format: shardFormat(DefaultConsolidate)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims, k = 2200, 1000, 64, 10
			f := newGrowFixtureFormat(t, total, base, dims, false, tt.bits, 60, tt.format)
			for f.inserted < total {
				difference, err := f.insert(ctx)
				require.NoError(t, err)
				require.Empty(t, difference)
			}
			require.NoError(t, f.g.cleanCentroidGraph())
			farthest, far := 0, float32(0)
			for id, vector := range f.all {
				dist, err := f.provider.SingleDist(f.queries[0], vector)
				require.NoError(t, err)
				if dist > far {
					farthest, far = id, dist
				}
			}
			filters := testFilters(t, f.provider, f.all, f.all[f.inserted/2], f.all[farthest], 0.5, 0.1, 0.01, 0.002)
			for _, filter := range filters {
				hits, want := 0, 0
				for i, query := range f.queries {
					var memory SearchStats
					wantIDs, wantDists, err := f.twin.ix.search(ctx, query, k, filter.allow, &memory)
					require.NoError(t, err)
					search := f.g.searcher()
					search.allow = filter.allow
					stats := &diskSearchStats{Work: &SearchStats{}}
					ids, dists, err := search.run(ctx, query, k, nil, stats)
					require.NoError(t, err)
					require.Equal(t, wantIDs, ids, "%s, query %d: the doc ids from disk and in memory", filter.name, i)
					require.Equal(t, wantDists, dists, "%s, query %d", filter.name, i)
					require.Equal(t, diskWorkOf(&memory), diskWorkOf(stats.Work), "%s, query %d: the work", filter.name, i)
					for _, id := range ids {
						require.True(t, filter.allow.Contains(id), "%s, query %d: doc %d is left out by the filter", filter.name, i, id)
					}
					truth := filteredTruth(t, f.provider, f.all, query, k, filter.allow.Contains)
					for _, id := range truth {
						if slices.Contains(ids, id) {
							hits++
						}
					}
					want += len(truth)
				}
				t.Logf("%s: recall %.3f", filter.name, float64(hits)/float64(want))
			}
		})
	}
}

// The flat search of a filter ranks the allowed vectors by their routing
// codes and rescores the best ones. With no more allowed than it rescores it
// is the exact answer among them, and with more it rescores as many as it is
// told. It never returns a doc id the filter leaves out, nor one marked
// deleted. An index with no codes in memory ranks them by the full codes of
// the postings that hold them, which it loads once each (integration-plan.md,
// section 8, amendment 3). How much of the answer it finds with real vectors
// is for the tests of the index a shard opens: the tight clusters of these
// vectors have near ties a code cannot tell apart.
func TestGrowFlatSearch(t *testing.T) {
	ctx := context.Background()
	const total, base, dims, k = 2200, 1000, 64, 10
	f := newCleanFixtureFormat(t, total, base, dims, false, 60, haloFormat(8, true))
	f.insertSome(t, total-base)
	g := f.g
	filters := testFilters(t, f.provider, f.all, f.all[100], f.all[total-1], 0.5, 0.1, 0.01)
	answers := map[string][][]uint64{}
	tests := []struct {
		name    string
		rescore int
		// exact tells that every allowed vector is rescored.
		exact bool
	}{
		{name: "every allowed vector rescored", rescore: total, exact: true},
		{name: "the best 40 rescored", rescore: 40},
	}
	for _, tt := range tests {
		for _, filter := range filters {
			hits, want := 0, 0
			for i, query := range f.queries {
				stats := &diskSearchStats{}
				ids, dists, err := g.flatSearch(ctx, query, k, tt.rescore, filter.allow, stats)
				require.NoError(t, err)
				require.True(t, stats.Flat)
				require.True(t, slices.IsSorted(dists), "%s, %s, query %d", tt.name, filter.name, i)
				require.Len(t, ids, min(k, filter.allow.Len()), "%s, %s, query %d", tt.name, filter.name, i)
				for _, id := range ids {
					require.True(t, filter.allow.Contains(id), "%s, %s, query %d: doc %d is left out", tt.name, filter.name, i, id)
				}
				truth := filteredTruth(t, f.provider, f.all, query, k, filter.allow.Contains)
				if tt.exact {
					require.Equal(t, truth, ids, "%s, %s, query %d: the exact answer", tt.name, filter.name, i)
					require.Zero(t, stats.Scored, "nothing is ranked by its code")
				} else {
					require.Equal(t, min(tt.rescore, filter.allow.Len()), stats.Rescored)
					if filter.allow.Len() > tt.rescore {
						require.Equal(t, filter.allow.Len(), stats.Scored, "every allowed vector is ranked by its code")
					}
				}
				for _, id := range truth {
					if slices.Contains(ids, id) {
						hits++
					}
				}
				want += len(truth)
				answers[tt.name+filter.name] = append(answers[tt.name+filter.name], ids)
			}
			t.Logf("%s, %s: recall %.3f", tt.name, filter.name, float64(hits)/float64(want))
		}
	}

	// A vector marked deleted is not returned, however near: the next one
	// takes its place.
	query := f.queries[0]
	everything := randomFilter(total, 1, 9)
	before, _, err := g.flatSearch(ctx, query, k, total, everything, nil)
	require.NoError(t, err)
	require.Len(t, before, k)
	f.mark(t, before[0])
	after, _, err := g.flatSearch(ctx, query, k, total, everything, nil)
	require.NoError(t, err)
	require.NotContains(t, after, before[0])
	require.Equal(t, before[1:], after[:k-1])

	_, _, err = g.flatSearch(ctx, query[:dims-1], k, 40, everything, nil)
	require.Error(t, err, "a query of another length")

	// With no codes in memory the flat search ranks the allowed vectors by the
	// full codes their postings hold (integration-plan.md, section 8,
	// amendment 3): the posting map gives the posting of each, and each of
	// those postings is loaded once, its row of members alone, and no other.
	// It answers exactly where it rescores every allowed vector, and returns
	// nothing the filter leaves out nor the vector marked deleted.
	f.how.noCodes = true
	f.reopen(t)
	g = f.g
	require.True(t, g.noCodes)
	marked := before[0]
	held := func(id uint64) bool { return id != marked }
	for _, tt := range tests {
		for _, filter := range filters {
			postings, allowed := map[uint32]bool{}, 0
			for id := range uint64(total) {
				if filter.allow.Contains(id) && held(id) {
					postings[g.st.side.postingOf[id]] = true
					allowed++
				}
			}
			hits, want := 0, 0
			for i, query := range f.queries {
				at := errors.Errorf("no codes in memory, %s, %s, query %d", tt.name, filter.name, i).Error()
				stats := &diskSearchStats{}
				ids, dists, err := g.flatSearch(ctx, query, k, tt.rescore, filter.allow, stats)
				require.NoError(t, err, at)
				require.True(t, stats.Flat, at)
				require.True(t, slices.IsSorted(dists), at)
				require.Len(t, ids, min(k, allowed), at)
				for _, id := range ids {
					require.True(t, filter.allow.Contains(id), "%s: doc %d is left out", at, id)
					require.NotEqual(t, marked, id, "%s: the vector marked deleted", at)
				}
				truth := filteredTruth(t, f.provider, f.all, query, k, func(id uint64) bool {
					return filter.allow.Contains(id) && held(id)
				})
				if tt.exact {
					require.Equal(t, truth, ids, "%s: the exact answer", at)
					require.Zero(t, stats.Scored, "%s: nothing is ranked by its code", at)
					require.Zero(t, stats.Postings, "%s: no posting is loaded", at)
				} else {
					require.Equal(t, min(tt.rescore, allowed), stats.Rescored, at)
					if allowed > tt.rescore {
						require.Equal(t, allowed, stats.Scored, "%s: every allowed vector is ranked by its code", at)
						require.Equal(t, len(postings), stats.Postings, "%s: each posting of an allowed vector loaded once", at)
					}
				}
				for _, id := range truth {
					if slices.Contains(ids, id) {
						hits++
					}
				}
				want += len(truth)
			}
			t.Logf("no codes in memory, %s, %s: recall %.3f, %d allowed in %d postings",
				tt.name, filter.name, float64(hits)/float64(want), allowed, len(postings))
		}
	}
}
