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
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// twoPostings registers posting A with centroid e1 and posting B with
// centroid e2 and returns their ids. Dot distance, unit vectors.
func twoPostings(t *testing.T, tf *testIndex) (uint64, uint64) {
	t.Helper()
	aID, aRefs := createPostingWithTokens(t, tf, [][]float32{{1, 0, 0, 0}}, 10)
	bID, bRefs := createPostingWithTokens(t, tf, [][]float32{{0, 1, 0, 0}}, 20)
	registerPosting(t, tf, aID, aRefs, []float32{1, 0, 0, 0})
	registerPosting(t, tf, bID, bRefs, []float32{0, 1, 0, 0})
	return aID, bID
}

// placeToken stores vec as one-token document docID and puts its entry in
// posting postingID, wherever its nearest centroid is.
func placeToken(t *testing.T, tf *testIndex, docID uint64, vec []float32, postingID uint64) tokenRef {
	t.Helper()
	tf.mvStore.storeMultiVector(docID, [][]float32{vec})
	ref := tokenRef{DocID: uint32(docID)}
	require.NoError(t, tf.Index.putPosting(t.Context(), postingID, append(tf.posting(postingID), ref)))
	return ref
}

func TestReassign(t *testing.T) {
	nearB := []float32{0.1, 0.995, 0, 0}

	t.Run("token moves to its nearest posting", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, bID := twoPostings(t, &tf)
		ref := placeToken(t, &tf, 1000, nearB, aID)

		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))

		require.NotContains(t, tf.posting(aID), ref)
		require.Contains(t, tf.posting(bID), ref)
		// and on disk
		for id, want := range map[uint64]bool{aID: false, bID: true} {
			stored, err := tf.Index.PostingStore.Get(id)
			require.NoError(t, err)
			if want {
				require.Contains(t, stored, ref)
			} else {
				require.NotContains(t, stored, ref)
			}
		}
	})

	t.Run("token already in its nearest posting stays", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, bID := twoPostings(t, &tf)
		ref := placeToken(t, &tf, 1000, nearB, bID)

		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: bID, Ref: ref}))

		require.Contains(t, tf.posting(bID), ref)
		require.NotContains(t, tf.posting(aID), ref)
	})

	// the token left the hinted posting since the task was enqueued (a
	// split or merge moved it): the task must not add a second copy
	t.Run("stale hint is a no-op", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, bID := twoPostings(t, &tf)
		cID, cRefs := createPostingWithTokens(t, &tf, [][]float32{{0, 0, 1, 0}}, 30)
		registerPosting(t, &tf, cID, cRefs, []float32{0, 0, 1, 0})
		ref := placeToken(t, &tf, 1000, nearB, cID)

		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))

		require.Contains(t, tf.posting(cID), ref)
		require.NotContains(t, tf.posting(bID), ref)
		require.NotContains(t, tf.posting(aID), ref)
	})

	// port of TestReassignDeletedVector
	t.Run("deleted document is skipped", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, bID := twoPostings(t, &tf)
		ref := placeToken(t, &tf, 1000, nearB, aID)
		require.NoError(t, tf.Index.Delete(1000))

		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))

		require.Contains(t, tf.posting(aID), ref, "the entry waits for the next split or merge")
		require.NotContains(t, tf.posting(bID), ref)
	})

	// port of TestReassignVectorNotFound, on both read paths: the token
	// thunk, and the document thunk when there is none
	for _, tokenThunk := range []bool{true, false} {
		t.Run(fmt.Sprintf("fetch failure is an error, token thunk %v", tokenThunk), func(t *testing.T) {
			opts := []testIndexOption{withDistanceProvider(distancer.NewDotProductProvider())}
			if !tokenThunk {
				opts = append(opts, withoutTokenThunk())
			}
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, opts...)
			aID, _ := twoPostings(t, &tf)
			failing := func(context.Context) error { return errors.New("vector not found") }
			if tokenThunk {
				tf.Index.tokenForID = func(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
					return nil, failing(ctx)
				}
			} else {
				tf.Index.multiVectorForID = func(ctx context.Context, id uint64) ([][]float32, error) {
					return nil, failing(ctx)
				}
			}

			err := tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: tokenRef{DocID: 9999}})
			require.ErrorContains(t, err, "failed to get vector by index ID")
			require.ErrorContains(t, err, "vector not found")
		})
	}
}

// port of TestReassignReenqueuesWhenSelectedPostingDisappears: append reports
// a posting that is gone instead of adding to it, which is what makes
// doReassign and addToken route the token again.
func TestAppendToMissingPosting(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	twoPostings(t, &tf)

	added, err := tf.Index.append(t.Context(), tokenRef{DocID: 1}, 4242)
	require.NoError(t, err)
	require.False(t, added)
	require.Empty(t, tf.posting(4242))
}
