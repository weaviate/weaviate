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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// Split a posting that doesn't exist
func TestSplitPostingThatDoesNotExist(t *testing.T) {
	tf := createHFreshIndex(t)

	postingID := uint64(999)
	centroid := []float32{5.0, 5.0, 5.0, 5.0}
	initializeDimensions(t, &tf, centroid)
	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))

	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 2)
	entry := tf.Logs.Entries[1]
	require.Equal(t, logrus.DebugLevel, entry.Level)
	require.Equal(t, "posting is empty, skipping split operation", entry.Message)
	require.Equal(t, uint64(999), entry.Data["postingID"])
}

// Split a posting when the centroid doesn't exist
func TestSplitCentroidNotExists(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.doSplit(t.Context(), 42, false)
	require.NoError(t, err)
}

// Split a posting below maxPostingSize
func TestSplitPostingBelowThreshold(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 5)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 1)

	uncompressed := make([]float32, 4)
	for _, vec := range vectors {
		for i := range vec {
			uncompressed[i] += vec[i]
		}
	}
	for i := range uncompressed {
		uncompressed[i] /= float32(len(vectors))
	}

	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(uncompressed))
	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: uncompressed,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.PostingMap.SetVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	p, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, len(posting), len(p))

	require.True(t, tf.Index.Centroids.Exists(postingID))
}

// Split a posting with deleted vectors
func TestSplitWithDeletedVectors(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 10)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 100)

	for i := 0; i < 5; i++ {
		vectorID := uint64(100 + i)
		_, err := tf.Index.VersionMap.MarkDeleted(t.Context(), vectorID)
		require.NoError(t, err)
	}

	uncompressed := make([]float32, 4)
	for _, vec := range vectors {
		for i := range vec {
			uncompressed[i] += vec[i]
		}
	}
	for i := range uncompressed {
		uncompressed[i] /= float32(len(vectors))
	}

	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(uncompressed))
	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: uncompressed,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.PostingMap.SetVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	p, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, 5, len(p), "posting should have 5 vectors after GC")
}

// Split successfully
func TestSplitSuccessfully(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := make([][]float32, 15)
	for i := range vectors {
		vectors[i] = []float32{1.0, 0.0, 0.0, 0.0}
	}

	postingID, posting := createPostingWithVectors(t, &tf, vectors, 200)

	uncompressed := []float32{1.0, 0.0, 0.0, 0.0}
	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(uncompressed))
	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: uncompressed,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), postingID, posting)
	require.NoError(t, err)

	err = tf.Index.PostingMap.SetVectorIDs(t.Context(), postingID, posting)
	require.NoError(t, err)

	originalMax := tf.Index.maxPostingSize
	tf.Index.maxPostingSize = 10
	defer func() { tf.Index.maxPostingSize = originalMax }()

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	require.False(t, tf.Index.Centroids.Exists(postingID))
	require.True(t, tf.Index.Centroids.Exists(postingID+1))
	require.True(t, tf.Index.Centroids.Exists(postingID+2))
}

// Split properly manages task queue
func TestSplitTaskQueueOperations(t *testing.T) {
	tf := createHFreshIndex(t)

	postingID := uint64(500)

	err := tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)

	err = tf.Index.doSplit(t.Context(), postingID, false)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueSplit(postingID)
	require.NoError(t, err)
}
