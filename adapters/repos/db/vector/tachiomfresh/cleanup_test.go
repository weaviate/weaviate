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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Tests of the cleanup of dead entries (cleanup.go): the trigger in
// stage 1 of a search, the task itself, its re-enqueue when the posting is
// busy, and its races with a split and a reassign. One-token documents as
// in split_test.go, on the dot distance with the unit vectors of
// types_test.go. Hand-built postings take the type testTokenType gives
// a one-token document, so that checkInvariants holds on a typed index.

// cleanupPostings registers posting A at e1 holding n one-token documents
// at e1 (doc ids from 100), deletes the first deleted of them (the record
// dropped as the shard does), and posting B at e2 of type bType holding
// one document at e2 (doc id 200): with bType 0, A is not alone in its
// type (aloneInGroup), and B is where a token near e2 belongs. It returns
// both ids and A's entries.
func cleanupPostings(t *testing.T, tf *testIndex, n, deleted int, bType uint32) (aID, bID uint64, refs []tokenRef) {
	t.Helper()
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = e1
	}
	// A has the type of the test documents' tokens; bType 0 gives B the
	// same, another value makes A alone in its type
	aType := testTokenType(0)
	if bType == 0 {
		bType = aType
	}
	aID, refs = createPostingWithTokens(t, tf, vectors, 100)
	registerTypedPosting(t, tf, aID, refs, e1, aType)
	bID, bRefs := createPostingWithTokens(t, tf, [][]float32{e2}, 200)
	registerTypedPosting(t, tf, bID, bRefs, e2, bType)
	for i := range deleted {
		require.NoError(t, tf.Index.Delete(uint64(100+i)))
		tf.mvStore.deleteMultiVector(uint64(100 + i))
	}
	return aID, bID, refs
}

// Stage 1 enqueues a cleanup of a scanned posting when more than a third
// of its entries belong to deleted documents, the ratio of hfresh's merge
// threshold, and not at a third exactly; and not when it enqueued a merge
// for the posting, which cleans it on its own.
func TestScanEnqueuesCleanupOverOneThirdDead(t *testing.T) {
	tests := []struct {
		name     string
		tokens   int
		deleted  int
		min      uint32 // minPostingSize, the merge trigger; 0 keeps it off
		cleaned  bool   // no dead entry is left once the queue drained
		cleanups uint64 // by cleanup tasks
	}{
		{name: "nothing deleted", tokens: 9},
		{name: "30 % deleted", tokens: 10, deleted: 3},
		{name: "a third exactly", tokens: 9, deleted: 3},
		{name: "one over a third", tokens: 9, deleted: 4, cleaned: true, cleanups: 1},
		{name: "50 % deleted", tokens: 10, deleted: 5, cleaned: true, cleanups: 1},
		{name: "everything deleted", tokens: 9, deleted: 9, cleaned: true, cleanups: 1},
		// the live entries are under the minimum: the scan enqueues the
		// merge, which garbage-collects the posting, and no cleanup
		{name: "below the minimum: the merge cleans", tokens: 9, deleted: 6, min: 5, cleaned: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// kCentroids 1: a query at e1 scans posting A alone
			tf := newTestIndex(t, 64, 1, 1, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			tf.Index.minPostingSize = tt.min
			tf.Index.opts.disableReassign = true
			aID, _, refs := cleanupPostings(t, &tf, tt.tokens, tt.deleted, 0)

			_, _, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{e1}, 1, nil)
			require.NoError(t, err)
			tf.Index.waitForMaintenance(t)

			require.Equal(t, tt.cleanups, tf.cleanupCount(t))
			got := tf.posting(aID)
			if !tt.cleaned {
				require.Equal(t, refs, got, "the posting must be left as it was")
				return
			}
			for _, ref := range got {
				require.GreaterOrEqual(t, ref.DocID, uint32(100+tt.deleted), "entry %+v of a deleted document is still there", ref)
			}
			for _, ref := range refs[tt.deleted:] {
				require.Contains(t, got, ref, "a live entry is gone")
			}
		})
	}
}

// The cleanup removes exactly the entries of the deleted documents, keeps
// the live entries in their order, writes the same to the store, so that
// the posting comes back cleaned after a restart, and leaves a posting
// with nothing dead as it is.
func TestCleanupRemovesDeadEntries(t *testing.T) {
	tests := []struct {
		name     string
		deleted  int // of 10
		cleanups uint64
	}{
		{name: "nothing dead"},
		{name: "some dead", deleted: 4, cleanups: 1},
		{name: "everything dead", deleted: 10, cleanups: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			open := func() testIndex {
				return newTestIndexAt(t, dir, 64, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()))
			}
			tf := open()
			// the merge trigger off: a cleaned posting under the minimum
			// would get its merge (TestCleanupEnqueuesMergeBelowMinimum)
			tf.Index.minPostingSize = 0
			aID, _, refs := cleanupPostings(t, &tf, 10, tt.deleted, 0)
			// nil rather than an empty slice when nothing is left: what an
			// empty posting decodes to
			want := append([]tokenRef(nil), refs[tt.deleted:]...)

			require.NoError(t, tf.Index.doCleanup(t.Context(), aID))
			require.Equal(t, want, tf.posting(aID), "the live entries, in their order")
			stored, err := tf.Index.PostingStore.Get(aID)
			require.NoError(t, err)
			require.ElementsMatch(t, want, stored)
			require.Equal(t, tt.cleanups, tf.cleanupCount(t))
			require.True(t, tf.Index.Centroids.Exists(aID), "a cleanup never removes a centroid")

			// a second pass finds nothing to do
			require.NoError(t, tf.Index.doCleanup(t.Context(), aID))
			require.Equal(t, want, tf.posting(aID))
			require.Equal(t, tt.cleanups, tf.cleanupCount(t))
			tf.close(t)

			reopened := open()
			require.Equal(t, want, reopened.posting(aID))
			reopened.close(t)
		})
	}
}

// A cleanup of a posting whose centroid is gone, or that is empty, does
// nothing.
func TestCleanupSkipsMissingPosting(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, tf *testIndex) uint64
		log   string
	}{
		{
			name:  "no centroid",
			setup: func(t *testing.T, tf *testIndex) uint64 { return 42 },
			log:   "posting not found, skipping cleanup operation",
		},
		{
			name: "a centroid without entries",
			setup: func(t *testing.T, tf *testIndex) uint64 {
				registerPosting(t, tf, 43, nil, []float32{5, 5, 5, 5})
				return 43
			},
		},
		{
			// the posting of TestSplitSuccessfully, split before its
			// cleanup ran: its centroid is gone and its entries are in
			// the two new postings
			name: "split away meanwhile",
			setup: func(t *testing.T, tf *testIndex) uint64 {
				tf.Index.maxPostingSize = 10
				vectors := make([][]float32, 15)
				for i := range vectors {
					vectors[i] = []float32{1, 0, 0, 0}
				}
				id, refs := createPostingWithTokens(t, tf, vectors, 300)
				registerPosting(t, tf, id, refs, []float32{1, 0, 0, 0})
				require.NoError(t, tf.Index.Delete(300))
				require.NoError(t, tf.Index.doSplit(t.Context(), id, false))
				require.False(t, tf.Index.Centroids.Exists(id))
				return id
			},
			log: "posting not found, skipping cleanup operation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
			require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
			id := tt.setup(t, &tf)

			require.NoError(t, tf.Index.doCleanup(t.Context(), id))

			require.Equal(t, tt.log != "", tf.hasLog("posting not found, skipping cleanup operation"))
			require.Zero(t, tf.cleanupCount(t))
			require.False(t, tf.Index.taskQueue.cleanupList.Contains(id))
		})
	}
}

// A posting that a cleanup leaves below its minimum size gets the merge a
// scan would enqueue for it, unless it is alone in its type.
func TestCleanupEnqueuesMergeBelowMinimum(t *testing.T) {
	tests := []struct {
		name   string
		min    uint32 // against the 4 live entries of A
		bType  uint32 // B's type; another than A's makes A alone in its type
		merged bool
	}{
		{name: "below the minimum", min: 5, merged: true},
		{name: "at the minimum", min: 4},
		{name: "alone in its type", min: 5, bType: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			tf.Index.minPostingSize = tt.min
			tf.Index.opts.disableReassign = true
			aID, bID, refs := cleanupPostings(t, &tf, 10, 6, tt.bType)
			bRefs := tf.posting(bID)

			require.NoError(t, tf.Index.doCleanup(t.Context(), aID))
			require.Equal(t, uint64(1), tf.cleanupCount(t))
			tf.Index.waitForMaintenance(t)

			if !tt.merged {
				require.Equal(t, refs[6:], tf.posting(aID))
				require.True(t, tf.Index.Centroids.Exists(bID))
				return
			}
			// B, the smaller, folded into A (doMerge)
			require.False(t, tf.Index.Centroids.Exists(bID))
			require.ElementsMatch(t, append(refs[6:], bRefs...), tf.posting(aID))
		})
	}
}

// A cleanup that finds its posting locked by another operation is
// re-enqueued and runs once the posting is free, as a merge is
// (doMerge).
func TestCleanupRetriesWhenPostingBusy(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	tf.Index.minPostingSize = 0 // the merge trigger off, as above
	aID, _, refs := cleanupPostings(t, &tf, 10, 5, 0)

	// the test stands in for the split or reassign holding the posting
	tf.Index.postingLocks.Lock(aID)
	require.NoError(t, tf.Index.doCleanup(t.Context(), aID))
	require.Equal(t, refs, tf.Index.postings.get(aID).decode(), "a busy posting is left alone")
	require.Zero(t, tf.cleanupCount(t))
	tf.Index.postingLocks.Unlock(aID)

	// the re-enqueued task runs now, without another trigger
	tf.Index.waitForMaintenance(t)
	require.Equal(t, refs[5:], tf.posting(aID))
	require.Equal(t, uint64(1), tf.cleanupCount(t))
}

// A cleanup racing a split and a reassign on the same posting loses
// nothing: whichever holds the posting first, every live token ends in
// exactly one posting and no dead entry is left. The pattern of
// TestSplitRetriesWhenDocDeletedDuringSplit, with the operations
// concurrent; run it with -race.
func TestCleanupRacingSplitAndReassign(t *testing.T) {
	tests := []struct {
		name     string
		split    bool
		reassign bool
	}{
		{name: "a reassign", reassign: true},
		{name: "a split", split: true},
		{name: "a split and a reassign", split: true, reassign: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// a few rounds, so that the orders differ
			for range 5 {
				// maxPostingSize 10: A's 10 live entries are enough to split
				tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction,
					withDistanceProvider(distancer.NewDotProductProvider()))
				tf.Index.minPostingSize = 0 // the merge trigger off, as above
				aID, _, refs := cleanupPostings(t, &tf, 12, 6, 0)
				docs := make(map[uint64][][]float32)
				deleted := make(map[uint64]bool)
				for i, ref := range refs {
					docs[uint64(ref.DocID)] = [][]float32{e1}
					if i < 6 {
						deleted[uint64(ref.DocID)] = true
					}
				}
				docs[200] = [][]float32{e2}
				// four tokens near e2, placed in A, that a reassign moves to B
				var moving []tokenRef
				for i := range 4 {
					id := uint64(300 + i)
					moving = append(moving, placeToken(t, &tf, id, nearE2, aID))
					docs[id] = [][]float32{nearE2}
				}

				var wg sync.WaitGroup
				errs := make(chan error, 2+len(moving))
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs <- tf.Index.doCleanup(t.Context(), aID)
				}()
				if tt.split {
					wg.Add(1)
					go func() {
						defer wg.Done()
						errs <- tf.Index.doSplit(t.Context(), aID, true)
					}()
				}
				if tt.reassign {
					for _, ref := range moving {
						wg.Add(1)
						go func() {
							defer wg.Done()
							errs <- tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref})
						}()
					}
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					require.NoError(t, err)
				}
				tf.Index.waitForMaintenance(t)

				checkInvariants(t, &tf, docs, deleted, true)
				if tt.split && !tt.reassign {
					// with the reassigns in the race, A may have lost
					// their four tokens first and be too small to split
					require.False(t, tf.Index.Centroids.Exists(aID), "the split must have gone through")
				}
			}
		})
	}
}
