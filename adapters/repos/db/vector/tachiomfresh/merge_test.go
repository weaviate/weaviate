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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Ports of hfresh's merge_test.go, one-token documents where hfresh has
// vectors. Like hfresh's tests they set min/maxPostingSize on the index.

// Merge a centroid that does not exist
func TestMergeCentroidThatDoesNotExist(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))

	err := tf.Index.doMerge(t.Context(), 0)
	require.NoError(t, err)

	require.True(t, tf.hasLog("posting not found, skipping merge operation"))
}

// Merge a posting that has a centroid but no entries
func TestMergePostingThatDoesNotExist(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

	postingID := uint64(999)
	registerPosting(t, &tf, postingID, nil, []float32{5.0, 5.0, 5.0, 5.0})

	err := tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	// empty, so deleted, even alone in its type
	require.True(t, tf.hasLog("posting is empty, deleting its centroid"))
	require.False(t, tf.Index.Centroids.Exists(postingID))
}

// Merges that must leave a lone posting in place: big enough, garbage
// collected only, or without any candidate.
func TestMergeWithoutCandidate(t *testing.T) {
	tests := []struct {
		name    string
		tokens  int
		deleted int
		min     uint32
		want    int
		log     string
	}{
		// port of TestMergePostingAboveMinSize
		{name: "above min size", tokens: 20, min: 10, want: 20, log: "posting is big enough, skipping merge operation"},
		// port of TestMergeWithDeletedVectors
		{name: "with deleted documents", tokens: 8, deleted: 4, min: 5, want: 4},
		// port of TestMergeNoCandidatesFound
		{name: "no candidates", tokens: 3, min: 5, want: 3, log: "no candidates found for merge operation, skipping"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

			vectors := createTestVectors(4, tt.tokens)
			postingID, refs := createPostingWithTokens(t, &tf, vectors, 600)
			registerPosting(t, &tf, postingID, refs, meanVector(vectors))

			for i := 0; i < tt.deleted; i++ {
				require.NoError(t, tf.Index.Delete(uint64(600+i)))
			}
			tf.Index.minPostingSize = tt.min

			err := tf.Index.doMerge(t.Context(), postingID)
			require.NoError(t, err)

			if tt.log != "" {
				require.True(t, tf.hasLog(tt.log), "missing log %q", tt.log)
			}
			require.Len(t, tf.posting(postingID), tt.want)
			stored, err := tf.Index.PostingStore.Get(postingID)
			require.NoError(t, err)
			require.ElementsMatch(t, tf.posting(postingID), stored)
		})
	}
}

// Merge a candidate that would exceed maxPostingSize
func TestMergeCandidateTooLarge(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

	smallVectors := createTestVectors(4, 3)
	smallPostingID, smallRefs := createPostingWithTokens(t, &tf, smallVectors, 900)

	largeVectors := createTestVectors(4, 15)
	largePostingID, largeRefs := createPostingWithTokens(t, &tf, largeVectors, 920)

	centroid := []float32{5.0, 5.0, 5.0, 5.0}
	registerPosting(t, &tf, smallPostingID, smallRefs, centroid)
	registerPosting(t, &tf, largePostingID, largeRefs, centroid)

	tf.Index.minPostingSize = 5
	tf.Index.maxPostingSize = 10

	err := tf.Index.doMerge(t.Context(), smallPostingID)
	require.NoError(t, err)

	require.Len(t, tf.posting(smallPostingID), 3, "small posting should not have been merged")
	require.Len(t, tf.posting(largePostingID), 15, "large posting should be unchanged")
}

// Merge properly manages task queue
func TestMergeTaskQueueOperations(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))

	postingID := uint64(1000)

	err := tf.Index.taskQueue.EnqueueMerge(postingID)
	require.NoError(t, err)

	require.True(t, tf.Index.taskQueue.MergeContains(postingID))

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	require.False(t, tf.Index.taskQueue.MergeContains(postingID))
}

// Merge completes: the small posting folds into the large candidate, its
// centroid is removed, and the reassignment gate re-enqueues the merged
// tokens, which sit closer to their old centroid than to the surviving one:
// with a third, nearer posting around they end up there.
func TestMergeSuccessfully(t *testing.T) {
	tests := []struct {
		name            string
		disableReassign bool
	}{
		{name: "union in the large posting", disableReassign: true},
		{name: "merged tokens are reassigned"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			tf.Index.opts.disableReassign = tt.disableReassign

			// small posting: 3 tokens along e1; large candidate: 6 along e2
			smallVectors := make([][]float32, 3)
			for i := range smallVectors {
				smallVectors[i] = []float32{1.0, 0.0, 0.0, 0.0}
			}
			largeVectors := make([][]float32, 6)
			for i := range largeVectors {
				largeVectors[i] = []float32{0.0, 1.0, 0.0, 0.0}
			}
			// a third posting next to e1, kept out of the merge by marking
			// it as queued for a merge of its own
			thirdVectors := [][]float32{{0.9, 0.0, 0.4359, 0.0}}

			smallID, smallRefs := createPostingWithTokens(t, &tf, smallVectors, 800)
			largeID, largeRefs := createPostingWithTokens(t, &tf, largeVectors, 900)
			thirdID, thirdRefs := createPostingWithTokens(t, &tf, thirdVectors, 950)
			registerPosting(t, &tf, smallID, smallRefs, []float32{1.0, 0.0, 0.0, 0.0})
			registerPosting(t, &tf, largeID, largeRefs, []float32{0.0, 1.0, 0.0, 0.0})
			registerPosting(t, &tf, thirdID, thirdRefs, thirdVectors[0])
			require.True(t, tf.Index.taskQueue.mergeList.TryAdd(thirdID))

			// 3 < minPostingSize (21 at maxPostingSize 64), so the small posting qualifies
			err := tf.Index.doMerge(t.Context(), smallID)
			require.NoError(t, err)
			tf.Index.taskQueue.MergeDone(thirdID)
			tf.Index.waitForMaintenance(t)

			// the small posting is emptied and its centroid removed from the index
			require.Empty(t, tf.posting(smallID))
			require.False(t, tf.Index.Centroids.Exists(smallID))
			// and on disk it is dropped: gone at the bucket's next flush
			require.True(t, tf.Index.PostingStore.isDead(smallID))

			if tt.disableReassign {
				// the large posting holds the union
				require.ElementsMatch(t, append(largeRefs, smallRefs...), tf.posting(largeID))
				return
			}
			// the gate enqueued the merged tokens and only them: they moved
			// to the posting next to e1, the residents stayed
			require.ElementsMatch(t, largeRefs, tf.posting(largeID))
			require.ElementsMatch(t, append(thirdRefs, smallRefs...), tf.posting(thirdID))
		})
	}
}

// mergeRuns counts the merge tasks that ran.
func mergeRuns(tf *testIndex) int {
	n := 0
	for _, entry := range tf.Logs.AllEntries() {
		if entry.Message == "merging posting" {
			n++
		}
	}
	return n
}

// An empty posting's centroid is deleted by the merge the first search
// enqueues. The search then no longer reaches the posting, so a second search
// pushes no task. The empty posting is reached in two ways: its last token
// moves away, or all its entries are of deleted documents.
func TestEmptyPostingLosesItsCentroid(t *testing.T) {
	tests := []struct {
		name string
		// kept is the size of the other posting; above maxPostingSize it
		// is not a merge candidate
		kept  int
		empty func(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef)
	}{
		{
			name: "the last token moved away",
			kept: 8,
			empty: func(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef) {
				for _, ref := range refs {
					removed, err := tf.Index.removeToken(t.Context(), postingID, ref)
					require.NoError(t, err)
					require.True(t, removed)
				}
			},
		},
		{
			name: "every entry is of a deleted document",
			kept: 8,
			empty: func(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef) {
				for _, ref := range refs {
					require.NoError(t, tf.Index.Delete(uint64(ref.DocID)))
				}
			},
		},
		{
			name: "every entry is of a deleted document, no candidate fits",
			kept: 13,
			empty: func(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef) {
				for _, ref := range refs {
					require.NoError(t, tf.Index.Delete(uint64(ref.DocID)))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// minPostingSize is 4 at maxPostingSize 12
			tf := newTestIndex(t, 12, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			tf.Index.opts.disableReassign = true

			emptied := [][]float32{{1, 0, 0, 0}, {1, 0.1, 0, 0}}
			emptiedID, emptiedRefs := createPostingWithTokens(t, &tf, emptied, 100)
			registerPosting(t, &tf, emptiedID, emptiedRefs, emptied[0])
			kept := make([][]float32, tt.kept)
			for i := range kept {
				kept[i] = []float32{0.9, float32(i) / 100, 0, 0}
			}
			keptID, keptRefs := createPostingWithTokens(t, &tf, kept, 200)
			registerPosting(t, &tf, keptID, keptRefs, kept[0])

			tt.empty(t, &tf, emptiedID, emptiedRefs)

			query := [][]float32{{1, 0, 0, 0}}
			for range 2 {
				ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 5, nil)
				require.NoError(t, err)
				require.NotEmpty(t, ids)
				tf.Index.waitForMaintenance(t)
			}

			require.Equal(t, 1, mergeRuns(&tf), "one merge task, for the two searches")
			require.False(t, tf.Index.Centroids.Exists(emptiedID))
			require.True(t, tf.Index.PostingStore.isDead(emptiedID))
			require.Empty(t, tf.posting(emptiedID))
			require.ElementsMatch(t, keptRefs, tf.posting(keptID))
		})
	}
}

// An empty posting alone in its token type loses its centroid like any
// other: queries search the centroids of every type, and an empty one would
// take a candidate's place. The next token of the type creates the type's
// first posting again.
func TestEmptyPostingAloneInItsTypeLosesItsCentroid(t *testing.T) {
	tf := newTestIndex(t, 12, 8, 8, hfreshEFConstruction,
		withDistanceProvider(distancer.NewDotProductProvider()))
	tf.Index.opts.disableReassign = true

	vectors := [][]float32{{1, 0, 0, 0}}
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
	registerPosting(t, &tf, postingID, refs, vectors[0])
	removed, err := tf.Index.removeToken(t.Context(), postingID, refs[0])
	require.NoError(t, err)
	require.True(t, removed)

	// a search enqueues the merge, which deletes the centroid
	_, _, err = tf.Index.SearchByMultiVector(t.Context(), vectors, 1, nil)
	require.NoError(t, err)
	tf.Index.waitForMaintenance(t)
	require.False(t, tf.Index.Centroids.Exists(postingID))
	require.Zero(t, tf.Index.types.groupSize(testPostingType))

	// the next token of the type creates its first posting again
	doc := [][]float32{{0, 1, 0, 0}}
	addMultiVectorToIndex(t, &tf, 300, doc)
	require.Equal(t, 1, tf.Index.types.groupSize(testTokenType(0)))
	ids, _, err := tf.Index.SearchByMultiVector(t.Context(), doc, 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{300}, ids)
}
