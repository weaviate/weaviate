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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// whereIs returns the postings that hold ref, in memory and on disk.
func whereIs(t *testing.T, tf *testIndex, ref tokenRef, ids ...uint64) (inMemory, onDisk []uint64) {
	t.Helper()
	for _, id := range ids {
		if tf.Index.postings.contains(id, ref) {
			inMemory = append(inMemory, id)
		}
		stored, err := tf.Index.PostingStore.Get(id)
		require.NoError(t, err)
		for _, r := range stored {
			if r == ref {
				onDisk = append(onDisk, id)
			}
		}
	}
	return inMemory, onDisk
}

// failing makes the posting store's op fail its first n calls.
func failing(op string, n int64) func(string) error {
	var left atomic.Int64
	left.Store(n)
	return func(got string) error {
		if got == op && left.Add(-1) >= 0 {
			return errors.New("injected " + op + " failure")
		}
		return nil
	}
}

// A reassign whose store write fails leaves the token where it was, in
// memory and on disk, and the retry moves it. The token is never in no
// posting.
func TestReassignStoreFailure(t *testing.T) {
	nearB := []float32{0.1, 0.995, 0, 0}
	tests := []struct {
		name string
		op   string
		n    int64
		// wantDiskCopy: the undo failed too, so the target's disk keeps a
		// copy, which restore resolves
		wantDiskCopy bool
		wantErr      string
	}{
		{name: "the append fails", op: "append", n: 1, wantErr: "failed to add token"},
		{name: "the remove fails, the undo works", op: "remove", n: 1, wantErr: "failed to remove token"},
		{name: "the remove and its undo fail", op: "remove", n: 2, wantDiskCopy: true, wantErr: "failed to remove token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
			aID, bID := twoPostings(t, &tf)
			ref := placeToken(t, &tf, 1000, nearB, aID)
			op := reassignOperation{PostingID: aID, Ref: ref}

			tf.Index.PostingStore.fault = failing(tt.op, tt.n)
			require.ErrorContains(t, tf.Index.doReassign(t.Context(), op), tt.wantErr)
			inMemory, onDisk := whereIs(t, &tf, ref, aID, bID)
			require.Equal(t, []uint64{aID}, inMemory)
			if tt.wantDiskCopy {
				require.Equal(t, []uint64{aID, bID}, onDisk)
			} else {
				require.Equal(t, []uint64{aID}, onDisk)
			}

			tf.Index.PostingStore.fault = nil
			require.NoError(t, tf.Index.doReassign(t.Context(), op))
			inMemory, onDisk = whereIs(t, &tf, ref, aID, bID)
			require.Equal(t, []uint64{bID}, inMemory)
			require.Equal(t, []uint64{bID}, onDisk)
		})
	}
}

// A reassign whose source posting is busy goes back to the queue and moves
// nothing; it moves the token once the posting is free. Source and target
// may share a lock stripe, which is then taken once.
func TestReassignLocks(t *testing.T) {
	nearB := []float32{0.1, 0.995, 0, 0}

	t.Run("busy source", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, bID := twoPostings(t, &tf)
		require.NotEqual(t, tf.Index.postingLocks.Hash(aID), tf.Index.postingLocks.Hash(bID))
		ref := placeToken(t, &tf, 1000, nearB, aID)

		tf.Index.postingLocks.Lock(aID)
		require.NoError(t, tf.Index.taskQueue.EnqueueReassign(aID, ref))
		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))
		require.False(t, tf.Index.taskQueue.reassignList.TryAdd(aID, ref), "the task is queued again")
		inMemory, onDisk := whereIs(t, &tf, ref, aID, bID)
		require.Equal(t, []uint64{aID}, inMemory)
		require.Equal(t, []uint64{aID}, onDisk)
		tf.Index.postingLocks.Unlock(aID)

		tf.Index.waitForMaintenance(t)
		inMemory, onDisk = whereIs(t, &tf, ref, aID, bID)
		require.Equal(t, []uint64{bID}, inMemory)
		require.Equal(t, []uint64{bID}, onDisk)
	})

	t.Run("source and target share a lock stripe", func(t *testing.T) {
		tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
		aID, aRefs := createPostingWithTokens(t, &tf, [][]float32{{1, 0, 0, 0}}, 10)
		registerPosting(t, &tf, aID, aRefs, []float32{1, 0, 0, 0})
		bID := aID + 1
		for tf.Index.postingLocks.Hash(bID) != tf.Index.postingLocks.Hash(aID) {
			bID++
		}
		tf.mvStore.storeMultiVector(20, [][]float32{{0, 1, 0, 0}})
		registerPosting(t, &tf, bID, []tokenRef{{DocID: 20}}, []float32{0, 1, 0, 0})
		ref := placeToken(t, &tf, 1000, nearB, aID)

		require.NoError(t, tf.Index.doReassign(t.Context(), reassignOperation{PostingID: aID, Ref: ref}))
		inMemory, onDisk := whereIs(t, &tf, ref, aID, bID)
		require.Equal(t, []uint64{bID}, inMemory)
		require.Equal(t, []uint64{bID}, onDisk)
	})
}

// removeToken writes the tombstone before it changes memory: a store error
// leaves the entry where the disk has it.
func TestRemoveTokenStoreFailure(t *testing.T) {
	tf := newTestIndex(t, 64, 8, 8, hfreshEFConstruction, withDistanceProvider(distancer.NewDotProductProvider()))
	aID, _ := twoPostings(t, &tf)
	ref := placeToken(t, &tf, 1000, []float32{1, 0, 0, 0}, aID)

	tf.Index.PostingStore.fault = failing("remove", 1)
	removed, err := tf.Index.removeToken(t.Context(), aID, ref)
	require.ErrorContains(t, err, "failed to remove token")
	require.False(t, removed)
	inMemory, onDisk := whereIs(t, &tf, ref, aID)
	require.Equal(t, []uint64{aID}, inMemory)
	require.Equal(t, []uint64{aID}, onDisk)
}
