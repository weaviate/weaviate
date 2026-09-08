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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDumpUninitialized(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.DumpCentroids(t.Context(), func(uint64, []float32) error { return nil })
	require.ErrorContains(t, err, "not initialized")

	err = tf.Index.DumpPostings(t.Context(), func(uint64, []PostingEntry) error { return nil })
	require.ErrorContains(t, err, "not initialized")
}

// Both dumps run without a shard reference, so they must stop on the index's
// own lifecycle context even when the caller's context stays alive.
func TestDumpStopsOnIndexShutdown(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 3)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 300)
	err := tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)
	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	tf.Index.cancel()

	err = tf.Index.DumpCentroids(t.Context(), func(uint64, []float32) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
	err = tf.Index.DumpPostings(t.Context(), func(uint64, []PostingEntry) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestDumpCentroidsAndPostings(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorsA := createTestVectors(4, 5)
	postingA, pa := createPostingWithVectors(t, &tf, vectorsA, 100)
	vectorsB := createTestVectors(4, 3)
	postingB, pb := createPostingWithVectors(t, &tf, vectorsB, 200)

	centroids := map[uint64][]float32{
		postingA: {1.0, 0.0, 0.0, 0.0},
		postingB: {0.0, 1.0, 0.0, 0.0},
	}
	for _, p := range []struct {
		id      uint64
		posting Posting
	}{{postingA, pa}, {postingB, pb}} {
		centroid := centroids[p.id]
		compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))
		err := tf.Index.Centroids.Insert(p.id, &Centroid{
			Uncompressed: centroid,
			Compressed:   compressed,
		})
		require.NoError(t, err)

		err = tf.Index.PostingStore.Put(t.Context(), p.id, p.posting)
		require.NoError(t, err)

		err = tf.Index.setPostingVectorIDs(t.Context(), p.id, p.posting)
		require.NoError(t, err)
	}

	// vector 100 is deleted, vector 200's stored copy is stale (the live
	// version moved on): both must still be reported, flagged as not live
	_, err := tf.Index.VersionMap.MarkDeleted(t.Context(), 100)
	require.NoError(t, err)
	_, err = tf.Index.VersionMap.Increment(t.Context(), 200, VectorVersion(1))
	require.NoError(t, err)

	t.Run("centroids", func(t *testing.T) {
		got := map[uint64][]float32{}
		err := tf.Index.DumpCentroids(t.Context(), func(id uint64, c []float32) error {
			got[id] = c
			return nil
		})
		require.NoError(t, err)
		require.Len(t, got, 2)
		for id, want := range centroids {
			require.Len(t, got[id], len(want))
			// the centroid HNSW stores 8-bit codes, so the dumped vector is a
			// close reconstruction rather than the inserted float32 values
			for i := range want {
				require.InDelta(t, want[i], got[id][i], 0.1, "centroid %d dim %d", id, i)
			}
		}
	})

	t.Run("postings", func(t *testing.T) {
		got := map[uint64][]PostingEntry{}
		err := tf.Index.DumpPostings(t.Context(), func(id uint64, entries []PostingEntry) error {
			got[id] = entries
			return nil
		})
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Len(t, got[postingA], 5)
		require.Len(t, got[postingB], 3)

		live := map[uint64]bool{}
		for _, entries := range got {
			for _, e := range entries {
				live[e.ID] = e.Live
				require.Equal(t, uint8(1), e.Version)
			}
		}
		require.Len(t, live, 8, "every stored copy is reported")
		require.False(t, live[100], "deleted vector is reported as not live")
		require.False(t, live[200], "stale copy is reported as not live")
		for _, id := range []uint64{101, 102, 103, 104, 201, 202} {
			require.True(t, live[id], "vector %d is live", id)
		}
	})

	t.Run("callback error stops the dump", func(t *testing.T) {
		sentinel := errors.New("stop")
		calls := 0
		err := tf.Index.DumpPostings(t.Context(), func(uint64, []PostingEntry) error {
			calls++
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, calls)
	})
}
