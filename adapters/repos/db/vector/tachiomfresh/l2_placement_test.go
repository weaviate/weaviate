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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// A token is placed in the posting of the centroid of its type nearest in
// L2, not the one of largest dot product (nearestCentroids).

// l2 is the squared L2 distance on float32 vectors, the reference of the
// tests below.
func l2(a, b []float32) float32 {
	var d float32
	for i := range a {
		d += (a[i] - b[i]) * (a[i] - b[i])
	}
	return d
}

// Two centroids of one type: near, of norm 1, and far, of norm 3 in about
// the same direction. The dot product ranks far first for every token of
// the table; the L2 distance ranks near first for the tokens close to it.
func TestL2PlacementPicksNearestCentroidNotLargestDot(t *testing.T) {
	near := []float32{1, 0, 0, 0}
	far := []float32{3, 0.5, 0, 0}
	cases := []struct {
		name     string
		token    []float32
		wantNear bool // the L2-nearest is near (else far); the dot-nearest is far in every row
	}{
		{"next to near", []float32{1.1, 0.1, 0, 0}, true},
		{"half way along near", []float32{0.5, 0, 0, 0}, true},
		{"between the two, nearer to near", []float32{1.8, 0.2, 0, 0}, true},
		{"between the two, nearer to far", []float32{2.4, 0.4, 0, 0}, false},
		{"next to far", []float32{3, 0.4, 0, 0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tf := newTypedTestIndex(t, 64)
			nearID, _ := typedPosting(t, &tf, 1, near, 6, 100)
			farID, _ := typedPosting(t, &tf, 1, far, 6, 200)

			// the dot product would place every token of the table in far
			dotNear, err := tf.Index.config.DistanceProvider.SingleDist(tc.token, near)
			require.NoError(t, err)
			dotFar, err := tf.Index.config.DistanceProvider.SingleDist(tc.token, far)
			require.NoError(t, err)
			require.Less(t, dotFar, dotNear, "the table row must be one the dot product places in far")

			wantID, otherID := nearID, farID
			if !tc.wantNear {
				wantID, otherID = farID, nearID
			}
			require.Equal(t, l2(tc.token, near) < l2(tc.token, far), tc.wantNear, "the table row's L2 choice")

			// the scan ranks by L2, ascending, with the squared distances
			res, err := tf.Index.nearestCentroids(tc.token, 1, 2)
			require.NoError(t, err)
			var ids []uint64
			var dists []float32
			for id, dist := range res.Iter() {
				ids = append(ids, id)
				dists = append(dists, dist)
			}
			require.Equal(t, []uint64{wantID, otherID}, ids)
			require.Less(t, dists[0], dists[1])
			require.InDelta(t, min(l2(tc.token, near), l2(tc.token, far)), dists[0], 0.05)

			// and the insert follows it
			tf.mvStore.storeMultiVector(1000, [][]float32{tc.token})
			require.NoError(t, tf.Index.AddMultiWithTypes(t.Context(), 1000, [][]float32{tc.token}, []uint32{1}))
			ref := tokenRef{DocID: 1000}
			require.Contains(t, tf.posting(wantID), ref)
			require.NotContains(t, tf.posting(otherID), ref)
		})
	}
}

// The L2 distance of the scan, computed on the RQ-8 codes from the dot
// estimate and the stored norm, matches the float32 L2 distance to the
// decoded centroid within the codes' precision (1 % of the distance: the
// dot estimate compares two codes, the query's and the centroid's, where the
// float reference has the query exact); and the centroid the scan ranks first is as near as the
// nearest decoded one, within that precision.
func TestL2PlacementDistanceMatchesFloatL2(t *testing.T) {
	const dims, centroids, queries = 32, 20, 50
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	rng := rand.New(rand.NewSource(1))
	random := func(scale float32) []float32 {
		v := make([]float32, dims)
		for i := range v {
			v[i] = scale * float32(rng.NormFloat64())
		}
		return v
	}
	require.NoError(t, tf.Index.initDimensions(make([]float32, dims)))
	ids := make([]uint64, centroids)
	for i := range ids {
		// norms from 0.5 to 2, so the dot and L2 orders differ
		ids[i], _ = typedPosting(t, &tf, 1, random(0.5+float32(i)/13), 1, uint64(100*(i+1)))
	}
	decoded := make(map[uint64][]float32, centroids)
	for _, id := range ids {
		c, err := tf.Index.centroid(id)
		require.NoError(t, err)
		decoded[id] = c
	}

	var worst float32
	for q := range queries {
		token := random(1)
		res, err := tf.Index.nearestCentroids(token, 1, centroids)
		require.NoError(t, err)
		var exactNearest uint64
		exactDist := float32(1e9)
		for _, id := range ids {
			if d := l2(token, decoded[id]); d < exactDist {
				exactNearest, exactDist = id, d
			}
		}
		first := true
		prev := float32(-1)
		for id, dist := range res.Iter() {
			require.GreaterOrEqual(t, dist, prev, "query %d: the scan is sorted by L2", q)
			prev = dist
			exact := l2(token, decoded[id])
			tol := 0.01*exact + 0.005
			worst = max(worst, abs32(dist-exact)/exact)
			require.InDelta(t, exact, dist, float64(tol), "query %d, centroid %d", q, id)
			if first {
				require.InDelta(t, exactDist, exact, float64(tol), "query %d: the scan's first (%d) is as near as the L2-nearest decoded centroid (%d)", q, id, exactNearest)
				first = false
			}
		}
	}
	t.Logf("worst |code L2 - float L2| / float L2 over %d queries x %d centroids: %.5f", queries, centroids, worst)
}

func abs32(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}

// The split, merge and reassign checks compare the placement's distance,
// the squared L2 distance.
func TestCentroidDistanceFollowsThePlacement(t *testing.T) {
	a := []float32{1, 0, 0, 0}
	b := []float32{3, 0.5, 0, 0}
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	require.NoError(t, tf.Index.initDimensions(a))
	got, err := tf.Index.centroidDistance(a, b)
	require.NoError(t, err)
	require.InDelta(t, 4.25, got, 1e-6)
}
