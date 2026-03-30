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

// Merge a centroid that does not exist
func TestMergeCentroidThatDoesNotExist(t *testing.T) {
	tf := createHFreshIndex(t)

	err := tf.Index.doMerge(t.Context(), 0)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 2)

	entry := tf.Logs.Entries[1]
	require.Equal(t, logrus.DebugLevel, entry.Level)
	require.Equal(
		t,
		"posting not found, skipping merge operation",
		entry.Message,
	)
	require.Equal(t, uint64(0), entry.Data["postingID"])
}

// Merge a posting that doesn't exist
func TestMergePostingThatDoesNotExist(t *testing.T) {
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

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	require.Len(t, tf.Logs.Entries, 3)
	entry := tf.Logs.Entries[2]
	require.Equal(t, logrus.DebugLevel, entry.Level)
	require.Equal(t, "posting is empty, skipping merge operation", entry.Message)
	require.Equal(t, uint64(999), entry.Data["postingID"])
}

// Merge a posting with length above minPostingSize is not merged
func TestMergePostingAboveMinSize(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 20)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 600)

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

	originalMin := tf.Index.minPostingSize
	tf.Index.minPostingSize = 10
	defer func() { tf.Index.minPostingSize = originalMin }()

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	var foundLog bool
	for _, entry := range tf.Logs.Entries {
		if entry.Message == "posting is big enough, skipping merge operation" {
			foundLog = true
			require.Equal(t, postingID, entry.Data["postingID"])
			require.Equal(t, 20, entry.Data["size"])
			require.Equal(t, uint32(10), entry.Data["min"])
			break
		}
	}
	require.True(t, foundLog, "should log that posting is big enough")

	p, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, len(posting), len(p))
}

// Merge a posting with deleted vectors
func TestMergeWithDeletedVectors(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 8)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 700)

	for i := 0; i < 4; i++ {
		vectorID := uint64(700 + i)
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

	originalMin := tf.Index.minPostingSize
	tf.Index.minPostingSize = 5
	defer func() { tf.Index.minPostingSize = originalMin }()

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	p, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, 4, len(p), "posting should have 4 vectors after GC")
}

// Merge a posting when no candidates exist
func TestMergeNoCandidatesFound(t *testing.T) {
	tf := createHFreshIndex(t)

	vectors := createTestVectors(4, 3)
	postingID, posting := createPostingWithVectors(t, &tf, vectors, 800)

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

	originalMin := tf.Index.minPostingSize
	tf.Index.minPostingSize = 5
	defer func() { tf.Index.minPostingSize = originalMin }()

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	var foundLog bool
	for _, entry := range tf.Logs.Entries {
		if entry.Message == "no candidates found for merge operation, skipping" {
			foundLog = true
			require.Equal(t, postingID, entry.Data["postingID"])
			break
		}
	}
	require.True(t, foundLog, "should log no candidates found")

	p, err := tf.Index.PostingStore.Get(t.Context(), postingID)
	require.NoError(t, err)
	require.Equal(t, 3, len(p))
}

// Merge a candidate that would exceed maxPostingSize
func TestMergeCandidateTooLarge(t *testing.T) {
	tf := createHFreshIndex(t)

	smallVectors := createTestVectors(4, 3)
	smallPostingID, smallPosting := createPostingWithVectors(t, &tf, smallVectors, 900)

	largeVectors := createTestVectors(4, 15)
	largePostingID, largePosting := createPostingWithVectors(t, &tf, largeVectors, 920)

	centroid := []float32{5.0, 5.0, 5.0, 5.0}
	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))

	err := tf.Index.Centroids.Insert(smallPostingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.Centroids.Insert(largePostingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   compressed,
		Deleted:      false,
	})
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), smallPostingID, smallPosting)
	require.NoError(t, err)
	err = tf.Index.PostingMap.SetVectorIDs(t.Context(), smallPostingID, smallPosting)
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), largePostingID, largePosting)
	require.NoError(t, err)
	err = tf.Index.PostingMap.SetVectorIDs(t.Context(), largePostingID, largePosting)
	require.NoError(t, err)

	originalMin := tf.Index.minPostingSize
	originalMax := tf.Index.maxPostingSize
	tf.Index.minPostingSize = 5
	tf.Index.maxPostingSize = 10
	defer func() {
		tf.Index.minPostingSize = originalMin
		tf.Index.maxPostingSize = originalMax
	}()

	err = tf.Index.doMerge(t.Context(), smallPostingID)
	require.NoError(t, err)

	p, err := tf.Index.PostingStore.Get(t.Context(), smallPostingID)
	require.NoError(t, err)
	require.Equal(t, 3, len(p), "small posting should not have been merged")

	p, err = tf.Index.PostingStore.Get(t.Context(), largePostingID)
	require.NoError(t, err)
	require.Equal(t, 15, len(p), "large posting should be unchanged")
}

// Merge properly manages task queue
func TestMergeTaskQueueOperations(t *testing.T) {
	tf := createHFreshIndex(t)

	postingID := uint64(1000)

	err := tf.Index.taskQueue.EnqueueMerge(postingID)
	require.NoError(t, err)

	require.True(t, tf.Index.taskQueue.MergeContains(postingID))

	err = tf.Index.doMerge(t.Context(), postingID)
	require.NoError(t, err)

	require.False(t, tf.Index.taskQueue.MergeContains(postingID))
}
