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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

// newClaimOrderIndex wires the fixture every claim-order test shares: L2 for
// BOTH the index and the centroid HNSW (the default fixture leaves the
// centroid HNSW on cosine, under which the axis-aligned test vectors used
// here are all near-parallel and indistinguishable). probe == 0 keeps the
// default searchProbe.
func newClaimOrderIndex(t *testing.T, probe uint32) TestHFresh {
	t.Helper()
	uc := ent.NewDefaultUserConfig()
	if probe > 0 {
		uc.SearchProbe = probe
	}
	return newTestIndex(t, uc, nil, withDistanceProvider(distancer.NewL2SquaredProvider()))
}

type postingMember struct {
	id  uint64
	vec []float32
}

// addClaimTestPosting creates one posting holding the given members (in
// order, ids need not be sequential) under the given centroid, and registers
// it with every structure selection consults (centroid HNSW, posting store,
// posting map / sizes).
func addClaimTestPosting(t *testing.T, tf *TestHFresh, centroid []float32, members ...postingMember) {
	t.Helper()

	postingID, posting := createPostingWithVectors(t, tf, [][]float32{members[0].vec}, members[0].id)
	for _, m := range members[1:] {
		tf.Vectors.put(m.id, m.vec)
		require.NoError(t, tf.Index.VersionMap.store.Set(t.Context(), m.id, VectorVersion(1)))
		compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(m.vec))
		posting = posting.AddVector(NewVector(m.id, VectorVersion(1), compressed))
	}

	centCompressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))
	require.NoError(t, tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   centCompressed,
	}))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
}

// paddedAllowList returns an allowlist passing the given ids, padded with
// nonexistent high ids so Len() clears the flat-search cutoff and the search
// takes the centroid path. The padding ids do not exist in the index, so
// they only affect Len().
func paddedAllowList(ids ...uint64) helpers.AllowList {
	const (
		padLo = uint64(1_000_000)
		padN  = 5_006
	)
	bm := sroar.NewBitmap()
	for _, id := range ids {
		bm.Set(id)
	}
	for i := uint64(0); i < padN; i++ {
		bm.Set(padLo + i)
	}
	return helpers.NewAllowListFromBitmap(bm)
}

// TestFilteredSearchClaimOrder pins the recall bug this change removes: the
// wrapped allowlist used to "claim" a passing vector for the FIRST posting
// probed during centroid search — posting-id order in the flat-centroid
// regime, traversal order under ACORN, neither of which is query-distance
// order. A posting whose passing members were all claimed by earlier-probed
// postings reported Contains == false and was excluded from selection, even
// when it was the nearest posting to the query: under probe-budget pressure
// the claiming posting could miss the selection cut while the excluded near
// posting would have made it, and the passing vector became unreachable
// depending only on posting-id order.
//
// Setup: vector v (the exact nearest neighbor of the query) is replicated in
// two postings — one under a FAR centroid, one under a centroid AT v. Four
// filler postings sit in between; the allowlist passes v and the fillers.
// Posting ids are allocated sequentially, so creation order was claim-probe
// order: with the far posting first it claimed v and, pre-fix, the search
// returned a filler instead of v under probe pressure — while reversing only
// the posting-id order returned v. With membership-only Contains the near
// posting is always reachable and every case returns v. The third case pins
// the no-pressure boundary.
func TestFilteredSearchClaimOrder(t *testing.T) {
	const (
		vID   = uint64(1000) // the query's exact nearest neighbor
		wBase = uint64(2000) // filler vector ids: wBase+1 ...
		nWs   = 4
	)

	vVec := []float32{0.1, 0.1, 0.1, 0.12}
	farCentroid := []float32{10, 10, 10, 10}
	query := []float32{0.1, 0.1, 0.1, 0.1}

	buildIndex := func(t *testing.T, farPostingFirst bool, probe uint32) TestHFresh {
		tf := newClaimOrderIndex(t, probe)

		vPosting := func() { addClaimTestPosting(t, &tf, farCentroid, postingMember{vID, vVec}) }
		nearPosting := func() { addClaimTestPosting(t, &tf, vVec, postingMember{vID, vVec}) }
		fillers := func() {
			for i := 1; i <= nWs; i++ {
				w := []float32{2, 2, 2, 2 + float32(i)*0.01}
				addClaimTestPosting(t, &tf, w, postingMember{wBase + uint64(i), w})
			}
		}

		if farPostingFirst {
			vPosting()
			fillers()
			nearPosting()
		} else {
			nearPosting()
			fillers()
			vPosting()
		}
		return tf
	}

	cases := []struct {
		name            string
		farPostingFirst bool
		probe           uint32 // 0 = default searchProbe
	}{
		// control: the near posting is created first
		{name: "near posting first", farPostingFirst: false, probe: 2},
		// the pinned bug: pre-fix, the far posting claimed v under probe
		// pressure and the search returned a filler; identical data,
		// flipped posting ids, flipped results
		{name: "far posting first", farPostingFirst: true, probe: 2},
		// boundary: no budget pressure, every allowed posting is scanned
		{name: "far posting first, no budget pressure", farPostingFirst: true, probe: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tf := buildIndex(t, tc.farPostingFirst, tc.probe)

			allowedIDs := []uint64{vID}
			for i := 1; i <= nWs; i++ {
				allowedIDs = append(allowedIDs, wBase+uint64(i))
			}
			allow := paddedAllowList(allowedIDs...)
			defer allow.Close()

			ids, _, err := tf.Index.SearchByVector(t.Context(), query, 1, allow)
			require.NoError(t, err)
			require.Equal(t, []uint64{vID}, ids,
				"the allowlisted exact nearest neighbor must be returned regardless of posting id order")
		})
	}
}

// TestFilteredSearchReplicaDedupe guards the invariant that replaces the
// claim machinery: a docID replicated across multiple selected postings is
// absorbed by the posting scan's visited set and returned exactly once.
func TestFilteredSearchReplicaDedupe(t *testing.T) {
	const (
		vID  = uint64(1000)
		w1ID = uint64(2001)
		w2ID = uint64(2002)
	)

	vVec := []float32{0.1, 0.1, 0.1, 0.12}
	w1 := []float32{2, 2, 2, 2}
	// w2 doubles as posting 2's centroid: it must stay within
	// MaxDistanceRatio of the best centroid (~vVec, near-zero distance) or
	// selectCentroids prunes the posting before it is ever scanned
	w2 := []float32{0.5, 0.5, 0.5, 0.5}
	query := []float32{0.1, 0.1, 0.1, 0.1}

	tf := newClaimOrderIndex(t, 0)

	// v is replicated in both postings
	addClaimTestPosting(t, &tf, vVec, postingMember{vID, vVec}, postingMember{w1ID, w1})
	addClaimTestPosting(t, &tf, w2, postingMember{vID, vVec}, postingMember{w2ID, w2})

	allow := paddedAllowList(vID, w1ID, w2ID)
	defer allow.Close()

	ids, _, err := tf.Index.SearchByVector(t.Context(), query, 3, allow)
	require.NoError(t, err)

	// no double-counting: every allowed vector exactly once, in exact
	// distance order (v ~0.0004, w2 0.64, w1 14.44)
	require.Equal(t, []uint64{vID, w2ID, w1ID}, ids)
}

// TestCentroidSearchTieOrdering pins the deterministic (distance, id)
// ordering of the centroid ranked list: two postings under bit-identical
// centroids tie exactly, and the HNSW heap would order them arbitrarily —
// selection (and therefore filtered results) must not depend on that.
func TestCentroidSearchTieOrdering(t *testing.T) {
	const (
		aID = uint64(1000)
		bID = uint64(2000)
	)

	vec := []float32{0.2, 0.2, 0.2, 0.2}
	centroid := []float32{0.1, 0.1, 0.1, 0.1} // shared: exact distance tie
	query := []float32{0.1, 0.1, 0.1, 0.1}

	tf := newClaimOrderIndex(t, 0)
	// creation order deliberately inserts the HIGHER posting id first via
	// the second call; posting ids are sequential, so posting(aID) gets the
	// lower posting id regardless
	addClaimTestPosting(t, &tf, centroid, postingMember{aID, vec})
	addClaimTestPosting(t, &tf, centroid, postingMember{bID, vec})

	results, err := tf.Index.Centroids.Search(query, 10, nil)
	require.NoError(t, err)
	require.Len(t, results.data, 2)
	require.Equal(t, results.data[0].Distance, results.data[1].Distance,
		"identical centroids must tie exactly")
	require.Less(t, results.data[0].ID, results.data[1].ID,
		"distance ties must be ordered by ascending posting id")
}
