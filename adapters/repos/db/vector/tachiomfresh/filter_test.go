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

package tachiomfresh

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// bruteForceAllowed is the exact dot MaxSim ranking over the allowed
// documents, the top k: what a filtered search must return.
func bruteForceAllowed(t *testing.T, docs [][][]float32, allowed map[uint64]bool, query [][]float32, k int) ([]uint64, []float32) {
	t.Helper()
	dot := distancer.NewDotProductProvider()
	type result struct {
		id   uint64
		dist float32
	}
	var results []result
	for d, doc := range docs {
		if !allowed[uint64(d)] {
			continue
		}
		var score float32
		for _, q := range query {
			minDist := float32(math.MaxFloat32)
			for _, tok := range doc {
				dist, err := dot.SingleDist(q, tok)
				require.NoError(t, err)
				minDist = min(minDist, dist)
			}
			score += minDist
		}
		results = append(results, result{id: uint64(d), dist: score})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].dist != results[j].dist {
			return results[i].dist < results[j].dist
		}
		return results[i].id < results[j].id
	})
	n := min(k, len(results))
	ids := make([]uint64, n)
	dists := make([]float32, n)
	for i := range n {
		ids[i], dists[i] = results[i].id, results[i].dist
	}
	return ids, dists
}

// randomAllowList picks a seeded fraction of the documents.
func randomAllowList(rng *rand.Rand, docs, fraction float64) (helpers.AllowList, map[uint64]bool) {
	allowed := map[uint64]bool{}
	var ids []uint64
	for d := range uint64(docs) {
		if rng.Float64() < fraction {
			allowed[d] = true
			ids = append(ids, d)
		}
	}
	return helpers.NewAllowList(ids...), allowed
}

// TestFilteredSearchMatchesBruteForce: with every centroid probed and every
// document scored, a filtered search returns the brute force over the
// allowed documents, through the staged path (the flat path disabled) and
// the flat path (its factor at its value: kDocsToScore is the corpus, so
// every filter is below the cutoff) alike; an empty filter returns
// nothing. The pattern of TestSearchMatchesBruteForceWhenEverythingIsProbed.
func TestFilteredSearchMatchesBruteForce(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)
	require.Equal(t, flatSearchFactor, tf.Index.opts.flatFactor)

	paths := []struct {
		name   string
		factor int
	}{
		{name: "staged", factor: 0},
		{name: "flat", factor: flatSearchFactor},
	}
	fractions := []float64{1, 0.5, 0.05, 0.005, 0}
	for _, path := range paths {
		for _, fraction := range fractions {
			t.Run(fmt.Sprintf("%s filter %g", path.name, fraction), func(t *testing.T) {
				tf.Index.opts.flatFactor = path.factor
				rng := rand.New(rand.NewSource(int64(fraction * 1000)))
				allow, allowed := randomAllowList(rng, testDocs, fraction)
				if fraction == 0 {
					require.True(t, allow.IsEmpty())
				}
				for q := 0; q < 10; q++ {
					query := randomMultiVector(rng, 5, testDim)
					ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, allow)
					require.NoError(t, err)
					wantIDs, wantDists := bruteForceAllowed(t, docs, allowed, query, 10)
					if len(wantIDs) == 0 {
						require.Empty(t, ids, "query %d", q)
						continue
					}
					require.Equal(t, wantIDs, ids, "query %d", q)
					for i := range wantDists {
						require.InDelta(t, wantDists[i], dists[i], 1e-5, "query %d rank %d", q, i)
					}
				}
			})
		}
	}

	t.Run("the cutoff boundary", func(t *testing.T) {
		// the same filter with kDocsToScore at the cutoff (2 x kDocsToScore
		// allowed documents: the flat path, exact) and one below it (the
		// staged path: what the stages give at that kDocsToScore, which
		// scores fewer candidates than the filter allows and may differ
		// from the brute force)
		rng := rand.New(rand.NewSource(99))
		allow, allowed := randomAllowList(rng, testDocs, 0.3)
		query := randomMultiVector(rng, 5, testDim)
		require.Zero(t, allow.Len()%flatSearchFactor, "pick a filter of even size")
		at := allow.Len() / flatSearchFactor

		tf.Index.opts.flatFactor = flatSearchFactor
		tf.Index.setSearchBudgets(testKCentroids, uint32(at))
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, allow)
		require.NoError(t, err)
		wantIDs, _ := bruteForceAllowed(t, docs, allowed, query, 10)
		require.Equal(t, wantIDs, ids, "at the cutoff: the flat path")

		tf.Index.setSearchBudgets(testKCentroids, uint32(at-1))
		ids, _, err = tf.Index.SearchByMultiVector(t.Context(), query, 10, allow)
		require.NoError(t, err)
		tf.Index.opts.flatFactor = 0
		staged, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, allow)
		require.NoError(t, err)
		require.Equal(t, staged, ids, "above the cutoff: the staged path")
		tf.Index.setSearchBudgets(testKCentroids, testKDocsToScore)
	})

	t.Run("the distance search honours the filter", func(t *testing.T) {
		tf.Index.opts.flatFactor = 0
		rng := rand.New(rand.NewSource(5))
		allow, allowed := randomAllowList(rng, testDocs, 0.5)
		query := randomMultiVector(rng, 5, testDim)
		wantIDs, wantDists := bruteForceAllowed(t, docs, allowed, query, 10)
		ids, dists, err := tf.Index.SearchByMultiVectorDistance(t.Context(), query, wantDists[4], 100, allow)
		require.NoError(t, err)
		require.Equal(t, wantIDs[:5], ids)
		for i := range 5 {
			require.InDelta(t, wantDists[i], dists[i], 1e-5, "rank %d", i)
		}
	})
}

// TestFilteredSearchWidens: a filtered search through the staged path
// doubles the probe when stage 1 reached fewer allowed documents than
// kDocsToScore, and stops at maxWidenings or when every centroid was
// probed; a scan that reaches enough widens nothing.
func TestFilteredSearchWidens(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)
	tf.Index.opts.flatFactor = 0
	// the last cases depend on the centroid count: the merges the searches
	// would enqueue are kept from changing it
	tf.Index.minPostingSize = 0
	tf.Index.opts.disableReassign = true
	centroids := tf.centroidCount(t)
	t.Logf("%d centroids", centroids)
	rng := rand.New(rand.NewSource(11))
	query := randomMultiVector(rng, 5, testDim)
	deleted := uint64(testDocs - 1)
	require.NoError(t, tf.Index.DeleteMulti(deleted))

	tests := []struct {
		name         string
		kCentroids   uint32
		kDocsToScore uint32
		allow        []uint64
		minWidenings uint64
		maxWidenings uint64
		exact        bool // the result is the brute force over the allowed documents
	}{
		{
			name: "enough reached at once", kCentroids: 1, kDocsToScore: 1,
			allow: allRange(testDocs), minWidenings: 0, maxWidenings: 0,
		},
		{
			name: "no filter never widens", kCentroids: 1, kDocsToScore: testDocs,
			allow: nil, minWidenings: 0, maxWidenings: 0,
		},
		{
			// found at once or by widening: the first scan may reach it
			name: "one document", kCentroids: 1, kDocsToScore: testDocs,
			allow: []uint64{7}, minWidenings: 0, maxWidenings: maxWidenings, exact: true,
		},
		{
			name: "only a deleted document: the cap", kCentroids: 1, kDocsToScore: testDocs,
			allow: []uint64{deleted}, minWidenings: maxWidenings, maxWidenings: maxWidenings, exact: true,
		},
		{
			// a probe above the centroid count returns fewer than asked,
			// which is how the search knows every centroid was probed (a
			// probe equal to the count learns it one widening later)
			name: "every centroid probed stops it", kCentroids: uint32(centroids) + 1, kDocsToScore: testDocs,
			allow: []uint64{deleted}, minWidenings: 0, maxWidenings: 0, exact: true,
		},
		{
			name: "a probe equal to the centroid count widens once", kCentroids: uint32(centroids), kDocsToScore: testDocs,
			allow: []uint64{deleted}, minWidenings: 1, maxWidenings: 1, exact: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf.Index.setSearchBudgets(tt.kCentroids, tt.kDocsToScore)
			var allow helpers.AllowList
			allowed := map[uint64]bool{}
			if tt.allow != nil {
				allow = helpers.NewAllowList(tt.allow...)
				for _, id := range tt.allow {
					allowed[id] = true
				}
			}
			before := tf.widenings.Load()
			ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, allow)
			require.NoError(t, err)
			widened := tf.widenings.Load() - before
			require.GreaterOrEqual(t, widened, tt.minWidenings)
			require.LessOrEqual(t, widened, tt.maxWidenings)
			if tt.exact {
				delete(allowed, deleted)
				wantIDs, _ := bruteForceAllowed(t, docs, allowed, query, 10)
				// the one allowed document may be out of reach at the cap:
				// then nothing is returned, never another document
				if len(ids) > 0 || widened < maxWidenings {
					require.Equal(t, wantIDs, ids)
				}
			}
		})
	}
}

func allRange(n int) []uint64 {
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i)
	}
	return ids
}
