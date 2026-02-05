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

// Analyze when centroid doesn't exist
func TestAnalyzeCentroidNotExists(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.doAnalyze(t.Context(), 42)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 1)
}

// Analyze when posting doesn't exist in store
func TestAnalyzePostingNotFound(t *testing.T) {
	tf := createHFreshIndex(t)

	postingID := uint64(100)
	centroid := []float32{1.0, 0.0, 0.0, 0.0}

	addVectorToIndex(t, &tf, 1, centroid)

	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))
	err := tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.doAnalyze(t.Context(), postingID)
	require.NoError(t, err)

	require.Greater(t, len(tf.Logs.Entries), 1)
	entry := tf.Logs.Entries[len(tf.Logs.Entries)-1]
	require.Equal(t, logrus.DebugLevel, entry.Level)
	require.Equal(t, "posting is empty, skipping analyze operation", entry.Message)
	require.Equal(t, postingID, entry.Data["postingID"])
}

// Analyze when metadata is already in memory
func TestAnalyzeWithInMemoryMetadata(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 5)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 200)

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

	err = tf.Index.doAnalyze(t.Context(), postingID)
	require.NoError(t, err)

	count, err := tf.Index.PostingMap.CountVectorIDs(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, uint32(5), count)
}

// Analyze enqueues split when posting exceeds maxPostingSize
func TestAnalyzeEnqueuesSplitWhenNeeded(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 20)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 400)

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

	originalMax := tf.Index.maxPostingSize
	tf.Index.maxPostingSize = 10
	defer func() { tf.Index.maxPostingSize = originalMax }()

	err = tf.Index.doAnalyze(t.Context(), postingID)
	require.NoError(t, err)

	require.Greater(t, tf.Index.taskQueue.Size(), int64(0))
}

// Analyze doesn't enqueue split when posting is below threshold
func TestAnalyzeDoesNotEnqueueSplitWhenNotNeeded(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 5)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 500)

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

	originalMax := tf.Index.maxPostingSize
	tf.Index.maxPostingSize = 100
	defer func() { tf.Index.maxPostingSize = originalMax }()

	initialSize := tf.Index.taskQueue.Size()

	err = tf.Index.doAnalyze(t.Context(), postingID)
	require.NoError(t, err)

	require.Equal(t, initialSize, tf.Index.taskQueue.Size())
}

// Analyze properly manages task queue
func TestAnalyzeTaskQueueOperations(t *testing.T) {
	tf := createHFreshIndex(t)

	postingID := uint64(600)

	err := tf.Index.taskQueue.EnqueueAnalyze(postingID)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueAnalyze(postingID)
	require.NoError(t, err)

	err = tf.Index.doAnalyze(t.Context(), postingID)
	require.NoError(t, err)

	err = tf.Index.taskQueue.EnqueueAnalyze(postingID)
	require.NoError(t, err)
}
