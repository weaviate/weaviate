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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// bruteForceIDs is the exact dot MaxSim ranking of the allowed documents
// (nil allows all), best first, ties by doc id.
func bruteForceIDs(t *testing.T, docs [][][]float32, allowed map[uint64]bool, query [][]float32, k int) []uint64 {
	t.Helper()
	if allowed == nil {
		allowed = map[uint64]bool{}
		for d := range docs {
			allowed[uint64(d)] = true
		}
	}
	ids, _ := bruteForceAllowed(t, docs, allowed, query, k)
	return ids
}

func TestSearchEdgeCases(t *testing.T) {
	t.Run("an empty index", func(t *testing.T) {
		tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{{1, 0, 0, 0}}, 10, nil)
		require.NoError(t, err)
		require.Empty(t, ids)
		require.Empty(t, dists)
	})

	t.Run("k above the corpus size", func(t *testing.T) {
		tf, docs := newRandomCorpusIndex(t)
		ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), docs[3], 10*testDocs, nil)
		require.NoError(t, err)
		require.Len(t, ids, testDocs)
		require.Len(t, dists, testDocs)
		require.Equal(t, uint64(3), ids[0])
	})

	t.Run("a filtered search while the index stages", func(t *testing.T) {
		tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		tf.Index.setCenterTokens(1 << 20)
		rng := rand.New(rand.NewSource(3))
		docs := make([][][]float32, 40)
		for i := range docs {
			docs[i] = randomMultiVector(rng, 4, 8)
			addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
		}
		require.False(t, tf.Index.built.Load())
		allowed := map[uint64]bool{1: true, 7: true, 12: true, 30: true}
		allow := helpers.NewAllowList(1, 7, 12, 30)
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[7], 3, allow)
		require.NoError(t, err)
		require.Equal(t, bruteForceIDs(t, docs, allowed, docs[7], 3), ids)
	})
}

func TestAddMultiBatch(t *testing.T) {
	doc := func(x float32) [][]float32 { return [][]float32{{x, 1, 0, 0}} }
	t.Run("mismatched lengths", func(t *testing.T) {
		tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		require.ErrorContains(t, tf.Index.AddMultiBatch(t.Context(), []uint64{1, 2}, [][][]float32{doc(1)}), "sizes do not match")
		require.False(t, tf.Index.ContainsDoc(1))
	})
	t.Run("empty", func(t *testing.T) {
		tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		require.ErrorContains(t, tf.Index.AddMultiBatch(t.Context(), nil, nil), "empty lists")
	})
	t.Run("a context cancelled mid-batch", func(t *testing.T) {
		tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		ids := []uint64{1, 2, 3}
		vectors := [][][]float32{doc(1), doc(2), doc(3)}
		for i, id := range ids {
			tf.mvStore.storeMultiVector(id, vectors[i])
		}
		// the batch checks the context, then AddMulti checks it again: the
		// fourth check is the second document's
		stop := &stoppingContext{Context: t.Context()}
		stop.n.Store(4)
		require.ErrorIs(t, tf.Index.AddMultiBatch(stop, ids, vectors), context.Canceled)
		require.True(t, tf.Index.ContainsDoc(1))
		require.False(t, tf.Index.ContainsDoc(2))
		require.False(t, tf.Index.ContainsDoc(3))
	})
}

func TestValidateMultiBeforeInsert(t *testing.T) {
	tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	tests := []struct {
		name    string
		vectors [][]float32
		wantErr string
	}{
		{name: "no token", vectors: nil, wantErr: "cannot be empty"},
		{name: "an empty token", vectors: [][]float32{{}}, wantErr: "cannot be empty"},
		{name: "tokens of different lengths", vectors: [][]float32{{1, 0, 0, 0}, {1, 0}}, wantErr: "inconsistent dimensions"},
		{name: "fine before the first insert", vectors: [][]float32{{1, 0, 0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tf.Index.ValidateMultiBeforeInsert(tt.vectors)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}

	t.Run("other dimensions than the index's", func(t *testing.T) {
		require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
		require.ErrorContains(t, tf.Index.ValidateMultiBeforeInsert([][]float32{{1, 0, 0}}), "dimension 3")
		require.NoError(t, tf.Index.ValidateMultiBeforeInsert([][]float32{{1, 0, 0, 0}}))
	})
}

// A doc id above the u32 range and a document of more than 65,536 tokens are
// refused before anything is staged or placed, whether the index stages or is
// built, and the index takes the next document.
func TestInsertLimits(t *testing.T) {
	tooMany := make([][]float32, math.MaxUint16+2)
	for i := range tooMany {
		tooMany[i] = []float32{1, 0, 0, 0}
	}
	cases := []struct {
		name    string
		id      uint64
		vectors [][]float32
		wantErr string
	}{
		{name: "doc id 2^32", id: 1 << 32, vectors: [][]float32{{1, 0, 0, 0}}, wantErr: "does not fit the u32"},
		{name: "65,537 tokens", id: 1, vectors: tooMany, wantErr: "does not fit the u16"},
	}
	for _, staged := range []bool{true, false} {
		for _, tt := range cases {
			name := tt.name + ", built"
			if staged {
				name = tt.name + ", staged"
			}
			t.Run(name, func(t *testing.T) {
				tf := newTestIndex(t, 8, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
				if staged {
					tf.Index.setCenterTokens(1 << 20)
				}
				require.ErrorContains(t, tf.Index.AddMulti(t.Context(), tt.id, tt.vectors), tt.wantErr)
				require.False(t, tf.Index.ContainsDoc(tt.id))
				docs, _ := tf.stagedCount(t)
				require.Zero(t, docs)
				require.Zero(t, tf.entryCount(t))

				ok := [][]float32{{0, 1, 0, 0}}
				tf.mvStore.storeMultiVector(2, ok)
				require.NoError(t, tf.Index.AddMulti(t.Context(), 2, ok))
				require.True(t, tf.Index.ContainsDoc(2))
			})
		}
	}
}

// An insert whose target posting is split between its centroid search and
// its append routes the token again; it lands in one of the new postings.
func TestInsertRacingASplitOfItsTarget(t *testing.T) {
	tf := newTestIndex(t, 10, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	tf.Index.opts.disableReassign = true
	vectors := createTestVectors(4, 9)
	postingID, refs := createPostingWithTokens(t, &tf, vectors, 200)
	registerPosting(t, &tf, postingID, refs, meanVector(vectors))
	docs := oneTokenDocs(vectors, 200)

	var once sync.Once
	fired := false
	tf.Index.opts.beforeAppend = func(target uint64) {
		once.Do(func() {
			fired = true
			require.Equal(t, postingID, target)
			// the posting grows past its limit and is split, before the
			// insert takes its lock
			extra := createTestVectors(4, 12)[9:]
			for i, vec := range extra {
				id := uint64(300 + i)
				tf.mvStore.storeMultiVector(id, [][]float32{vec})
				docs[id] = [][]float32{vec}
				added, err := tf.Index.append(t.Context(), tokenRef{DocID: uint32(id)}, postingID)
				require.NoError(t, err)
				require.True(t, added)
			}
			require.NoError(t, tf.Index.doSplit(t.Context(), postingID, false))
			require.False(t, tf.Index.Centroids.Exists(postingID))
		})
	}
	doc := [][]float32{vectors[4]}
	tf.mvStore.storeMultiVector(500, doc)
	docs[500] = doc
	require.NoError(t, tf.Index.AddMulti(t.Context(), 500, doc))
	require.True(t, fired, "the insert never reached its append")
	require.False(t, tf.Index.Centroids.Exists(postingID), "the target was not split")
	require.True(t, tf.Index.ContainsDoc(500))
	checkInvariants(t, &tf, docs, nil, false)
}

// A reassign whose target is deleted before the move routes the token again
// and moves it to the next nearest posting.
func TestReassignRoutesAgainWhenTheTargetGoes(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	aID, bID := twoPostings(t, &tf)
	cID, cRefs := createPostingWithTokens(t, &tf, [][]float32{{0.5, 0.85, 0, 0}}, 30)
	registerPosting(t, &tf, cID, cRefs, []float32{0.5, 0.85, 0, 0})
	ref := placeToken(t, &tf, 1000, []float32{0.1, 0.995, 0, 0}, aID)

	var calls []uint64
	tf.Index.opts.beforeAppend = func(target uint64) {
		calls = append(calls, target)
		if len(calls) == 1 {
			// what a merge of the target does to it
			require.Equal(t, bID, target)
			tf.Index.postingLocks.Lock(bID)
			tf.Index.types.leave(bID, tf.Index.types.typeOf(bID))
			require.NoError(t, tf.Index.Centroids.MarkAsDeleted(bID))
			tf.Index.dropPosting(bID)
			tf.Index.postingLocks.Unlock(bID)
		}
	}
	require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))
	require.Equal(t, []uint64{bID, cID}, calls)
	inMemory, onDisk := whereIs(t, &tf, ref, aID, cID)
	require.Equal(t, []uint64{cID}, inMemory)
	require.Equal(t, []uint64{cID}, onDisk)
}

// A merge whose candidate shares the merged posting's lock stripe takes the
// lock once; a merge whose candidate is locked by another operation waits for
// it and then merges.
func TestMergeCandidateLocks(t *testing.T) {
	setup := func(t *testing.T, sameStripe bool) (testIndex, uint64, uint64, map[uint64][][]float32) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		tf.Index.opts.disableReassign = true
		tf.Index.minPostingSize, tf.Index.maxPostingSize = 5, 10
		small := createTestVectors(4, 3)
		smallID, smallRefs := createPostingWithTokens(t, &tf, small, 300)
		registerPosting(t, &tf, smallID, smallRefs, meanVector(small))
		large := createTestVectors(4, 4)
		largeID := smallID + 1
		if sameStripe {
			largeID = smallID + 512
		}
		require.NotEqual(t, sameStripe, tf.Index.postingLocks.Hash(smallID) != tf.Index.postingLocks.Hash(largeID))
		_, largeRefs := createPostingWithTokens(t, &tf, large, 400)
		registerPosting(t, &tf, largeID, largeRefs, meanVector(large))
		docs := oneTokenDocs(small, 300)
		for id, doc := range oneTokenDocs(large, 400) {
			docs[id] = doc
		}
		return tf, smallID, largeID, docs
	}

	t.Run("a candidate on the same lock stripe", func(t *testing.T) {
		tf, smallID, largeID, docs := setup(t, true)
		require.NoError(t, tf.Index.doMerge(t.Context(), smallID))
		require.False(t, tf.Index.Centroids.Exists(smallID))
		require.Len(t, tf.posting(largeID), 7)
		checkInvariants(t, &tf, docs, nil, false)
	})

	t.Run("a candidate locked by another operation", func(t *testing.T) {
		tf, smallID, largeID, docs := setup(t, false)
		// the hook runs in the merge's goroutine: it records the candidate,
		// and the test goroutine checks it
		waiting := make(chan struct{})
		var once sync.Once
		var candidate atomic.Uint64
		tf.Index.opts.beforeCandidateLock = func(candidateID uint64) {
			once.Do(func() {
				candidate.Store(candidateID)
				close(waiting)
			})
		}
		tf.Index.postingLocks.Lock(largeID)
		var wg sync.WaitGroup
		wg.Add(1)
		var mergeErr error
		enterrors.GoWrapper(func() {
			defer wg.Done()
			mergeErr = tf.Index.doMerge(t.Context(), smallID)
		}, tf.Index.logger)
		<-waiting
		require.Equal(t, largeID, candidate.Load())
		require.True(t, tf.Index.Centroids.Exists(smallID), "the merge waits for the candidate")
		tf.Index.postingLocks.Unlock(largeID)
		wg.Wait()
		require.NoError(t, mergeErr)
		require.False(t, tf.Index.Centroids.Exists(smallID))
		require.Len(t, tf.posting(largeID), 7)
		checkInvariants(t, &tf, docs, nil, false)
	})
}

// TestFirstInsertsWithOtherDimensionsAreRefused: the dimension check before
// an insert reads the dimensions while they are still unset, so two first
// inserts racing with different dimensions can both pass it. initDimensions,
// which publishes the dimensions, is called here as each of them would call
// it: again with the same dimensions it accepts, with others it refuses.
func TestFirstInsertsWithOtherDimensionsAreRefused(t *testing.T) {
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)

	require.NoError(t, tf.Index.initDimensions([]float32{1, 0, 0, 0}))
	require.NoError(t, tf.Index.initDimensions([]float32{0, 1, 0, 0}))
	require.Error(t, tf.Index.initDimensions([]float32{1, 0, 0, 0, 0, 0, 0, 0}))
}
