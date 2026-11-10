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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestInvariantsUnderMixedOperations runs seeded mixes of inserts, deletes,
// searches and forced splits, merges, cleanups and reassigns, with the task queue
// working in the background, and checks after the queue has drained that:
// every token of every live document is in exactly one posting; a posting
// with entries has a centroid; and, once
// every posting has been cleaned by a merge, no entry of a deleted document
// is left. Run it with -race.
func TestInvariantsUnderMixedOperations(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			const (
				maxPostingSize = 16
				tokens         = 6
				dim            = 32
				steps          = 600
			)
			tf := newTestIndex(t, maxPostingSize, 64, 64, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			rng := rand.New(rand.NewSource(seed))

			docs := make(map[uint64][][]float32)
			deleted := make(map[uint64]bool)
			var liveIDs []uint64
			var nextID uint64

			randomPosting := func() uint64 {
				tf.Index.postings.mu.RLock()
				defer tf.Index.postings.mu.RUnlock()
				return uint64(rng.Intn(max(1, len(tf.Index.postings.lists))))
			}

			for range steps {
				switch op := rng.Intn(100); {
				case op < 60 || len(liveIDs) == 0: // insert
					doc := randomMultiVector(rng, 1+rng.Intn(tokens), dim)
					docs[nextID] = doc
					addMultiVectorToIndex(t, &tf, nextID, doc)
					liveIDs = append(liveIDs, nextID)
					nextID++
				case op < 75: // delete, doc record dropped as the shard does
					i := rng.Intn(len(liveIDs))
					id := liveIDs[i]
					liveIDs[i] = liveIDs[len(liveIDs)-1]
					liveIDs = liveIDs[:len(liveIDs)-1]
					require.NoError(t, tf.Index.DeleteMulti(id))
					tf.mvStore.deleteMultiVector(id)
					deleted[id] = true
				case op < 80: // forced split
					require.NoError(t, tf.Index.doSplit(t.Context(), randomPosting(), true))
				case op < 84: // forced merge
					require.NoError(t, tf.Index.doMerge(t.Context(), randomPosting()))
				case op < 88: // forced cleanup of dead entries
					require.NoError(t, tf.Index.doCleanup(t.Context(), randomPosting()))
				case op < 92: // reassign of a token, hinted at the posting it is in
					id := randomPosting()
					if refs := tf.posting(id); len(refs) > 0 {
						ref := refs[rng.Intn(len(refs))]
						if !deleted[uint64(ref.DocID)] {
							require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: id, Ref: ref}))
						}
					}
				default: // search, which also enqueues merges and cleanups
					id := liveIDs[rng.Intn(len(liveIDs))]
					ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 5, nil)
					require.NoError(t, err)
					for _, got := range ids {
						require.False(t, deleted[got], "deleted doc %d returned", got)
					}
				}
			}
			tf.Index.waitForMaintenance(t)
			checkInvariants(t, &tf, docs, deleted, false)

			// clean every posting, then nothing of a deleted document is left
			tf.Index.postings.mu.RLock()
			postings := uint64(len(tf.Index.postings.lists))
			tf.Index.postings.mu.RUnlock()
			for id := range postings {
				require.NoError(t, tf.Index.doMerge(t.Context(), id))
			}
			tf.Index.waitForMaintenance(t)
			checkInvariants(t, &tf, docs, deleted, true)
		})
	}
}

// TestRestart persists an index and reopens it (postings are persisted
// through the posting bucket): the posting table, the centroids
// and the tombstones must come back, and search must still be exact. The
// counterpart of the "restart and re-test recall" step of hfresh's
// TestHFresh.
func TestRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() testIndex {
		return newTestIndexAt(t, dir, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction,
			withDistanceProvider(distancer.NewDotProductProvider()))
	}

	tf := open()
	rng := rand.New(rand.NewSource(7))
	docs := make([][][]float32, testDocs)
	live := make(map[uint64][][]float32, testDocs)
	for i := range docs {
		docs[i] = randomMultiVector(rng, testTokens, testDim)
		live[uint64(i)] = docs[i]
		addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
	}
	tf.Index.waitForMaintenance(t)
	deleted := deleteDocs(t, &tf)
	checkInvariants(t, &tf, live, deleted, false)

	want := make(map[uint64][]tokenRef)
	for id := range uint64(len(tf.Index.postings.lists)) {
		if refs := tf.posting(id); len(refs) > 0 {
			want[id] = refs
		}
	}
	tf.close(t)

	reopened := open()
	// the doc records live in the test's own store: hand them to the new one
	for id, doc := range live {
		if !deleted[id] {
			reopened.mvStore.storeMultiVector(id, doc)
		}
	}

	got := make(map[uint64][]tokenRef)
	for id := range uint64(len(reopened.Index.postings.lists)) {
		if refs := reopened.posting(id); len(refs) > 0 {
			got[id] = refs
		}
	}
	require.Len(t, got, len(want))
	for id, refs := range want {
		require.ElementsMatch(t, refs, got[id], "posting %d", id)
	}
	checkInvariants(t, &reopened, live, deleted, false)

	query := randomMultiVector(rng, 5, testDim)
	ids, dists, err := reopened.Index.SearchByMultiVector(t.Context(), query, 10, nil)
	require.NoError(t, err)
	wantIDs, wantDists := bruteForceDot(t, docs, deleted, query, 10)
	require.Equal(t, wantIDs, ids)
	for i := range wantDists {
		require.InDelta(t, wantDists[i], dists[i], 1e-5)
	}

	// and it keeps working: a new document goes in and is found
	extra := randomMultiVector(rng, testTokens, testDim)
	addMultiVectorToIndex(t, &reopened, testDocs, extra)
	ids, _, err = reopened.Index.SearchByMultiVector(t.Context(), extra, 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{testDocs}, ids)
	reopened.close(t)
}
