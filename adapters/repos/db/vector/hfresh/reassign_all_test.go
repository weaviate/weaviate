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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnqueueReassignAllUninitialized(t *testing.T) {
	tf := createHFreshIndex(t)

	_, err := tf.Index.EnqueueReassignAll(t.Context())
	require.ErrorContains(t, err, "not initialized")
}

// The scan runs without a shard reference, so it must stop on the index's
// own lifecycle context even when the caller's context stays alive.
func TestEnqueueReassignAllStopsOnIndexShutdown(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 3)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 300)
	err := tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)
	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	tf.Index.cancel()

	_, err = tf.Index.EnqueueReassignAll(t.Context())
	require.ErrorIs(t, err, context.Canceled)
}

func TestEnqueueReassignAll(t *testing.T) {
	tf := createHFreshIndex(t)

	vectorsA := createTestVectors(4, 5)
	postingA, pa := createPostingWithVectors(t, &tf, vectorsA, 100)
	vectorsB := createTestVectors(4, 3)
	postingB, pb := createPostingWithVectors(t, &tf, vectorsB, 200)

	for _, p := range []struct {
		id       uint64
		posting  Posting
		centroid []float32
	}{
		{postingA, pa, []float32{1.0, 0.0, 0.0, 0.0}},
		{postingB, pb, []float32{0.0, 1.0, 0.0, 0.0}},
	} {
		compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(p.centroid))
		err := tf.Index.Centroids.Insert(p.id, &Centroid{
			Uncompressed: p.centroid,
			Compressed:   compressed,
			Deleted:      false,
		})
		require.NoError(t, err)

		err = tf.Index.PostingStore.Put(t.Context(), p.id, p.posting)
		require.NoError(t, err)

		err = tf.Index.setPostingVectorIDs(t.Context(), p.id, p.posting)
		require.NoError(t, err)
	}

	// vector 100 is deleted, vector 200's stored entry is stale (the live
	// version moved on) — neither may be enqueued
	_, err := tf.Index.VersionMap.MarkDeleted(t.Context(), 100)
	require.NoError(t, err)
	_, err = tf.Index.VersionMap.Increment(t.Context(), 200, VectorVersion(1))
	require.NoError(t, err)

	stats, err := tf.Index.EnqueueReassignAll(t.Context())
	require.NoError(t, err)

	require.Equal(t, 2, stats.Postings)
	require.Equal(t, 6, stats.Enqueued, "8 stored entries minus 1 deleted minus 1 stale")
	require.Equal(t, 1, stats.SkippedDeleted)
	require.Equal(t, 1, stats.SkippedStale)

	for _, id := range []uint64{101, 102, 103, 104, 201, 202} {
		require.True(t, tf.Index.taskQueue.reassignList.Contains(id),
			"live vector %d should be enqueued", id)
	}
	for _, id := range []uint64{100, 200} {
		require.False(t, tf.Index.taskQueue.reassignList.Contains(id),
			"vector %d should be skipped", id)
	}
}
