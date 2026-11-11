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
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// bruteForceDot returns the exact dot MaxSim top k of query over the
// documents that are not deleted, ties by doc id, as the index orders them.
func bruteForceDot(t *testing.T, docs [][][]float32, deleted map[uint64]bool, query [][]float32, k int) ([]uint64, []float32) {
	t.Helper()
	dot := distancer.NewDotProductProvider()
	type result struct {
		id   uint64
		dist float32
	}
	var results []result
	for d, doc := range docs {
		if deleted[uint64(d)] {
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
	results = results[:min(k, len(results))]
	ids := make([]uint64, len(results))
	dists := make([]float32, len(results))
	for i, r := range results {
		ids[i], dists[i] = r.id, r.dist
	}
	return ids, dists
}

// deleteDocs deletes every third document of the synthetic corpus and drops
// its doc record, as the shard does.
func deleteDocs(t *testing.T, tf *testIndex) map[uint64]bool {
	t.Helper()
	deleted := make(map[uint64]bool)
	for id := uint64(0); id < testDocs; id += 3 {
		require.NoError(t, tf.Index.DeleteMulti(id))
		tf.mvStore.deleteMultiVector(id)
		deleted[id] = true
	}
	return deleted
}

// A deleted document is never returned, and search over the remaining
// documents matches brute force when everything is probed.
func TestSearchAfterDeletesMatchesBruteForce(t *testing.T) {
	tf, docs := newRandomCorpusIndex(t)
	deleted := deleteDocs(t, &tf)

	// a deleted document does not find itself
	for id := range deleted {
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 10, nil)
		require.NoError(t, err)
		require.NotContains(t, ids, id)
	}

	rng := rand.New(rand.NewSource(21))
	for q := 0; q < 20; q++ {
		query := randomMultiVector(rng, 5, testDim)
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
		require.NoError(t, err)

		wantIDs, wantDists := bruteForceDot(t, docs, deleted, query, 10)
		require.Equal(t, wantIDs, ids, "query %d", q)
		for i := range wantDists {
			require.InDelta(t, wantDists[i], dists[i], 1e-5, "query %d rank %d", q, i)
		}
	}
}

// No entry of a deleted document survives a cleaned posting: after every
// posting went through a merge, a split or a cleanup, none is left.
func TestDeletedEntriesAreCleaned(t *testing.T) {
	tests := []struct {
		name  string
		clean func(ctx context.Context, tf *testIndex, id uint64) error
	}{
		{name: "merge", clean: func(ctx context.Context, tf *testIndex, id uint64) error { return tf.Index.doMerge(ctx, id) }},
		{name: "split", clean: func(ctx context.Context, tf *testIndex, id uint64) error { return tf.Index.doSplit(ctx, id, true) }},
		{name: "cleanup", clean: func(ctx context.Context, tf *testIndex, id uint64) error { return tf.Index.doCleanup(ctx, id) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf, docs := newRandomCorpusIndex(t)
			live := make(map[uint64][][]float32, len(docs))
			for i, doc := range docs {
				live[uint64(i)] = doc
			}
			deleted := deleteDocs(t, &tf)

			before := 0
			for id := range uint64(len(tf.Index.postings.lists)) {
				for _, ref := range tf.posting(id) {
					if deleted[uint64(ref.DocID)] {
						before++
					}
				}
			}
			require.Equal(t, len(deleted)*testTokens, before, "entries of deleted documents stay until a posting is cleaned")

			for id := range uint64(len(tf.Index.postings.lists)) {
				require.NoError(t, tt.clean(t.Context(), &tf, id))
			}
			tf.Index.waitForMaintenance(t)

			checkInvariants(t, &tf, live, deleted, true)
		})
	}
}

// port of the delete half of hfresh's behaviour: deleting twice is fine and
// the tombstone is visible
func TestDelete(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	addMultiVectorToIndex(t, &tf, 7, [][]float32{{1, 0, 0, 0}})

	require.NoError(t, tf.Index.Delete(7))
	require.NoError(t, tf.Index.DeleteMulti(7))

	isDeleted, err := tf.Index.VersionMap.IsDeleted(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, isDeleted)
}

// TestDeleteDuringBuildStaysDeleted runs the first half of a Delete of a
// staged document (the version map marked, the doc bit cleared: what Delete
// does before it waits on stageMu), then an insert that reaches the threshold
// and builds the index. The document must stay deleted, out of the doc set,
// and out of the staging bucket.
func TestDeleteDuringBuildStaysDeleted(t *testing.T) {
	tests := []struct {
		name string
		// placed: an earlier attempt of the build placed the document's
		// tokens (a retried build)
		placed bool
	}{
		{name: "first build"},
		{name: "retried build", placed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction)
			defer tf.close(t)
			tf.Index.setCenterTokens(5)
			ctx := t.Context()

			require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 1, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}, []uint32{7, 7}))
			require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 2, [][]float32{{0, 0, 1, 0}, {0, 0, 0, 1}}, []uint32{7, 7}))
			require.False(t, tf.Index.built.Load())
			if tt.placed {
				tf.Index.placed.add(1)
			}

			_, err := tf.Index.VersionMap.MarkDeleted(ctx, 1)
			require.NoError(t, err)
			tf.Index.docs.remove(1)

			require.NoError(t, tf.Index.AddMultiWithTypes(ctx, 3, [][]float32{{1, 1, 0, 0}, {0, 1, 1, 0}}, []uint32{7, 7}))
			require.True(t, tf.Index.built.Load())

			v, err := tf.Index.VersionMap.Get(ctx, 1)
			require.NoError(t, err)
			require.True(t, v.Deleted(), "doc 1 is live again")
			require.False(t, tf.Index.ContainsDoc(1))
			require.True(t, tf.Index.ContainsDoc(2))
			require.True(t, tf.Index.ContainsDoc(3))
			var staged []uint64
			require.NoError(t, tf.Index.staging.iterate(func(docID uint64, _ [][]float32, _ []uint32) error {
				staged = append(staged, docID)
				return nil
			}))
			require.Empty(t, staged)
		})
	}
}
