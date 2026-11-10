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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The tests below are ports of hfresh's split_test.go: same cases, with
// one-token documents where hfresh has vectors. maxPostingSize is a
// constructor argument here, so each test builds its index with the
// threshold hfresh's test sets on the field.

// createTestVectors creates a set of test vectors with the specified dimensions
// copied from hfresh's helper_for_test.go
func createTestVectors(dims int, count int) [][]float32 {
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, dims)
		for j := 0; j < dims; j++ {
			vec[j] = float32(i*dims + j)
		}
		vectors[i] = vec
	}
	return vectors
}

func meanVector(vectors [][]float32) []float32 {
	mean := make([]float32, len(vectors[0]))
	for _, vec := range vectors {
		for i := range vec {
			mean[i] += vec[i]
		}
	}
	for i := range mean {
		mean[i] /= float32(len(vectors))
	}
	return mean
}

// Split a posting when the centroid doesn't exist
func TestSplitCentroidNotExists(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	err := tf.Index.doSplit(t.Context(), 42, false)
	require.NoError(t, err)
}

// Split a posting that has a centroid but no entries
func TestSplitPostingThatDoesNotExist(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

	postingID := uint64(999)
	registerPosting(t, &tf, postingID, nil, []float32{5.0, 5.0, 5.0, 5.0})

	err := tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	require.True(t, tf.hasLog("posting is empty, skipping split operation"))
	require.True(t, tf.Index.Centroids.Exists(postingID))
}

// Split a posting below maxPostingSize, with and without deleted documents
func TestSplitPostingBelowThreshold(t *testing.T) {
	tests := []struct {
		name    string
		tokens  int
		deleted int
	}{
		{name: "nothing deleted", tokens: 5},
		// port of TestSplitWithDeletedVectors: only garbage collection runs
		{name: "with deleted documents", tokens: 10, deleted: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)

			vectors := createTestVectors(4, tt.tokens)
			postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
			registerPosting(t, &tf, postingID, refs, meanVector(vectors))

			for i := 0; i < tt.deleted; i++ {
				require.NoError(t, tf.Index.Delete(uint64(100+i)))
			}

			err := tf.Index.doSplit(t.Context(), postingID, false)
			require.NoError(t, err)

			require.Len(t, tf.posting(postingID), tt.tokens-tt.deleted)
			require.True(t, tf.Index.Centroids.Exists(postingID))

			// the posting store holds the same entries
			stored, err := tf.Index.PostingStore.Get(postingID)
			require.NoError(t, err)
			require.ElementsMatch(t, tf.posting(postingID), stored)
		})
	}
}

// Split successfully
func TestSplitSuccessfully(t *testing.T) {
	tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction)

	vectors := make([][]float32, 15)
	for i := range vectors {
		vectors[i] = []float32{1.0, 0.0, 0.0, 0.0}
	}

	postingID, refs := createPostingWithTokens(t, &tf, vectors, 200)
	registerPosting(t, &tf, postingID, refs, []float32{1.0, 0.0, 0.0, 0.0})

	err := tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	require.False(t, tf.Index.Centroids.Exists(postingID))
	require.True(t, tf.Index.Centroids.Exists(postingID+1))
	require.True(t, tf.Index.Centroids.Exists(postingID+2))

	require.Empty(t, tf.posting(postingID))
	both := append(tf.posting(postingID+1), tf.posting(postingID+2)...)
	require.ElementsMatch(t, refs, both)

	// and on disk: the old posting is dropped (gone at the bucket's next
	// flush), the new ones hold its tokens
	require.True(t, tf.Index.PostingStore.isDead(postingID))
	var stored []tokenRef
	for _, id := range []uint64{postingID + 1, postingID + 2} {
		require.False(t, tf.Index.PostingStore.isDead(id))
		p, err := tf.Index.PostingStore.Get(id)
		require.NoError(t, err)
		stored = append(stored, p...)
	}
	require.ElementsMatch(t, refs, stored)
}

// Split properly manages task queue
func TestSplitTaskQueueOperations(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction)
	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))

	postingID := uint64(500)

	err := tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)
	require.True(t, tf.Index.taskQueue.splitList.Contains(postingID))

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)
	require.False(t, tf.Index.taskQueue.splitList.Contains(postingID))

	err = tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)
	require.True(t, tf.Index.taskQueue.splitList.Contains(postingID), "queued again")
}

// A document deleted between the split's garbage-collection pass and its
// fetch fails that attempt; doSplit must retry, filter the now-deleted
// entry, and complete the split without it.
func TestSplitRetriesWhenDocDeletedDuringSplit(t *testing.T) {
	// both read paths: the token thunk and the document thunk
	tests := []struct {
		name   string
		tokens bool
	}{
		{name: "token reads", tokens: true},
		{name: "document reads", tokens: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts []testIndexOption
			if !tt.tokens {
				opts = append(opts, withoutTokenThunk())
			}
			tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction, opts...)

			vectors := createTestVectors(4, 15)
			postingID, refs := createPostingWithTokens(t, &tf, vectors, 400)
			registerPosting(t, &tf, postingID, refs, []float32{1.0, 0.0, 0.0, 0.0})

			// doc 405 is deleted concurrently: its first fetch fails, and the
			// deletion becomes visible in the version map right after; the retry's
			// garbage collection must filter it
			const deletedID = uint64(405)
			var tripped atomic.Bool
			trip := func(id uint64) bool {
				if id == deletedID && tripped.CompareAndSwap(false, true) {
					require.NoError(t, tf.Index.Delete(deletedID))
					return true
				}
				return false
			}
			if tt.tokens {
				base := tf.Index.tokenForID
				tf.Index.tokenForID = func(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
					if trip(id) {
						return nil, errors.New("deleted concurrently")
					}
					return base(ctx, id, pos, dims)
				}
			} else {
				require.Nil(t, tf.Index.tokenForID)
				base := tf.Index.multiVectorForID
				tf.Index.multiVectorForID = func(ctx context.Context, id uint64) ([][]float32, error) {
					if trip(id) {
						return nil, errors.New("deleted concurrently")
					}
					return base(ctx, id)
				}
			}

			err := tf.Index.doSplit(t.Context(), postingID, false)
			require.NoError(t, err)
			require.True(t, tripped.Load(), "the failing fetch must have been exercised")

			require.False(t, tf.Index.Centroids.Exists(postingID))
			require.True(t, tf.Index.Centroids.Exists(postingID+1))
			require.True(t, tf.Index.Centroids.Exists(postingID+2))

			both := append(tf.posting(postingID+1), tf.posting(postingID+2)...)
			require.Len(t, both, 14)
			for _, ref := range both {
				require.NotEqual(t, uint32(deletedID), ref.DocID, "the deleted document must not survive the retry")
			}
		})
	}
}

// After a split, a token of a neighbouring posting that is nearer to one of
// the new centroids than to its own is reassigned there (second check of
// enqueueReassignAfterSplit), and one that is not stays. With reassign off
// nothing moves.
func TestSplitReassignsNeighbourTokens(t *testing.T) {
	tests := []struct {
		name            string
		disableReassign bool
	}{
		{name: "reassign"},
		{name: "reassign disabled", disableReassign: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// dot distance: the vectors below are unit vectors, used as they are
			tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			tf.Index.opts.disableReassign = tt.disableReassign

			// the posting to split: six tokens at a, six at b, centroid between
			a := []float32{1, 0, 0, 0}
			b := []float32{0, 1, 0, 0}
			var vectors [][]float32
			for range 6 {
				vectors = append(vectors, a, b)
			}
			postingID, refs := createPostingWithTokens(t, &tf, vectors, 100)
			registerPosting(t, &tf, postingID, refs, []float32{0.7071, 0.7071, 0, 0})

			// the neighbour: its centroid n, a token near a and a token near n
			nearA := []float32{0.95, 0, 0.3122, 0}
			nearN := []float32{0.5, 0, 0.866, 0}
			neighbourID, neighbourRefs := createPostingWithTokens(t, &tf, [][]float32{nearA, nearN}, 200)
			registerPosting(t, &tf, neighbourID, neighbourRefs, []float32{0.6, 0, 0.8, 0})

			require.NoError(t, tf.Index.doSplit(t.Context(), postingID, true))
			tf.Index.waitForMaintenance(t)

			require.False(t, tf.Index.Centroids.Exists(postingID))
			neighbour := tf.posting(neighbourID)
			require.Contains(t, neighbour, neighbourRefs[1], "the token near its own centroid must stay")
			if tt.disableReassign {
				require.Contains(t, neighbour, neighbourRefs[0])
				return
			}
			require.NotContains(t, neighbour, neighbourRefs[0], "the token near a new centroid must leave")
			// it sits with the tokens at a, in one of the two new postings
			// (the ids after the neighbour's)
			var home uint64
			for _, id := range []uint64{neighbourID + 1, neighbourID + 2} {
				for _, ref := range tf.posting(id) {
					if ref == neighbourRefs[0] {
						home = id
					}
				}
			}
			require.NotZero(t, home, "the reassigned token is in neither new posting")
			require.Contains(t, tf.posting(home), refs[0], "the reassigned token must join the tokens at a")
		})
	}
}

// TestFailedSplitOrMergeLeavesEveryTokenInOnePosting fails a split or a
// merge after it stored a new or merged posting. Every token must then be
// in exactly one posting with a centroid, and the type's entry count must be
// what its postings hold.
func TestFailedSplitOrMergeLeavesEveryTokenInOnePosting(t *testing.T) {
	injected := errors.New("injected failure")
	tests := []struct {
		name string
		// run builds the postings, sets the failure and runs the operation
		run func(t *testing.T, tf *testIndex) (map[uint64][][]float32, error)
	}{
		{
			name: "split, the second new posting fails",
			run: func(t *testing.T, tf *testIndex) (map[uint64][][]float32, error) {
				vectors := createTestVectors(4, 15)
				postingID, refs := createPostingWithTokens(t, tf, vectors, 200)
				registerPosting(t, tf, postingID, refs, meanVector(vectors))
				// the first new posting's put goes through
				var puts atomic.Int64
				tf.Index.PostingStore.fault = func(op string) error {
					if op == "put" && puts.Add(1) == 2 {
						return injected
					}
					return nil
				}
				return oneTokenDocs(vectors, 200), tf.Index.doSplit(t.Context(), postingID, false)
			},
		},
		{
			name: "split, the old centroid's delete fails",
			run: func(t *testing.T, tf *testIndex) (map[uint64][][]float32, error) {
				vectors := createTestVectors(4, 15)
				postingID, refs := createPostingWithTokens(t, tf, vectors, 200)
				registerPosting(t, tf, postingID, refs, meanVector(vectors))
				tf.Index.opts.beforeCentroidDelete = func(uint64) error { return injected }
				return oneTokenDocs(vectors, 200), tf.Index.doSplit(t.Context(), postingID, false)
			},
		},
		{
			name: "merge, the small centroid's delete fails",
			run: func(t *testing.T, tf *testIndex) (map[uint64][][]float32, error) {
				tf.Index.minPostingSize = 5
				tf.Index.maxPostingSize = 10
				small := createTestVectors(4, 3)
				smallID, smallRefs := createPostingWithTokens(t, tf, small, 300)
				large := createTestVectors(4, 4)
				largeID, largeRefs := createPostingWithTokens(t, tf, large, 400)
				registerPosting(t, tf, smallID, smallRefs, meanVector(small))
				registerPosting(t, tf, largeID, largeRefs, meanVector(large))
				docs := oneTokenDocs(small, 300)
				for id, doc := range oneTokenDocs(large, 400) {
					docs[id] = doc
				}
				tf.Index.opts.beforeCentroidDelete = func(uint64) error { return injected }
				return docs, tf.Index.doMerge(t.Context(), smallID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction,
				withDistanceProvider(distancer.NewDotProductProvider()))
			defer tf.close(t)
			tf.Index.opts.disableReassign = true

			docs, err := tt.run(t, &tf)
			require.ErrorIs(t, err, injected)
			tf.Index.PostingStore.fault = nil
			tf.Index.opts.beforeCentroidDelete = nil
			checkPlacement(t, &tf, docs)

			g := tf.Index.types.group(testPostingType)
			require.NotNil(t, g)
			g.mu.RLock()
			var entries int
			for _, id := range g.ids {
				entries += len(tf.posting(id))
			}
			g.mu.RUnlock()
			require.Equal(t, int64(entries), g.entries.Load(), "the type's entry count")
		})
	}
}
