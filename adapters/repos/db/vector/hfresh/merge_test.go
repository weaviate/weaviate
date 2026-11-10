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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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
	err = tf.Index.setPostingVectorIDs(t.Context(), smallPostingID, smallPosting)
	require.NoError(t, err)

	err = tf.Index.PostingStore.Put(t.Context(), largePostingID, largePosting)
	require.NoError(t, err)
	err = tf.Index.setPostingVectorIDs(t.Context(), largePostingID, largePosting)
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

// Merge completes: the small posting folds into the large candidate and the
// reassignment gate re-enqueues the merged vectors that sit closer to their
// old centroid than to the surviving one.
func TestMergeSuccessfully(t *testing.T) {
	tf := createHFreshIndex(t)

	// small posting: 3 vectors along e1; large candidate: 6 vectors along e2
	smallVectors := make([][]float32, 3)
	for i := range smallVectors {
		smallVectors[i] = []float32{1.0, 0.0, 0.0, 0.0}
	}
	largeVectors := make([][]float32, 6)
	for i := range largeVectors {
		largeVectors[i] = []float32{0.0, 1.0, 0.0, 0.0}
	}

	smallID, smallPosting := createPostingWithVectors(t, &tf, smallVectors, 800)
	largeID, largePosting := createPostingWithVectors(t, &tf, largeVectors, 900)

	for _, p := range []struct {
		id       uint64
		posting  Posting
		centroid []float32
	}{
		{smallID, smallPosting, []float32{1.0, 0.0, 0.0, 0.0}},
		{largeID, largePosting, []float32{0.0, 1.0, 0.0, 0.0}},
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

	// 3 < minPostingSize (642 at dims=4), so the small posting qualifies
	err := tf.Index.doMerge(t.Context(), smallID)
	require.NoError(t, err)

	// the large posting holds the union
	merged, err := tf.Index.PostingStore.Get(t.Context(), largeID)
	require.NoError(t, err)
	require.Equal(t, 9, len(merged))

	// the small posting is emptied and its centroid removed from the index
	small, err := tf.Index.PostingStore.Get(t.Context(), smallID)
	require.NoError(t, err)
	require.Empty(t, small)
	require.False(t, tf.Index.Centroids.Exists(smallID))

	// the reassignment gate: every merged vector is closer to its old
	// centroid (e1) than to the surviving one (e2), so all of them must be
	// enqueued for reassignment; the large posting's own vectors must not be
	for i := range smallVectors {
		require.True(t, tf.Index.taskQueue.reassignList.Contains(800+uint64(i)),
			"merged vector %d should be enqueued for reassignment", 800+i)
	}
	for i := range largeVectors {
		require.False(t, tf.Index.taskQueue.reassignList.Contains(900+uint64(i)),
			"resident vector %d should not be enqueued for reassignment", 900+i)
	}
}

// mergeRuns counts the merges that ran on postingID.
func mergeRuns(tf *TestHFresh, postingID uint64) int {
	runs := 0
	for _, entry := range tf.Logs.AllEntries() {
		if entry.Message == "merging posting" && entry.Data["postingID"] == postingID {
			runs++
		}
	}
	return runs
}

// A posting with no live entries is merged at most once, however many
// searches reach its centroid: the merge leaves its size at zero, and a
// search does not select a centroid whose posting size is zero, so it
// enqueues no further merge.
func TestEmptyPostingIsMergedAtMostOnce(t *testing.T) {
	e1 := []float32{1, 0, 0, 0}
	e2 := []float32{0, 1, 0, 0}
	tests := []struct {
		name string
		// candidate adds a second centroid that a merge could use
		candidate bool
		// deleteEntries stores the posting with entries and deletes them;
		// otherwise the posting is stored empty
		deleteEntries bool
		merges        int
	}{
		{name: "empty posting, no candidate", merges: 0},
		{name: "empty posting, with candidate", candidate: true, merges: 0},
		{name: "every entry deleted, no candidate", deleteEntries: true, merges: 1},
		{name: "every entry deleted, with candidate", candidate: true, deleteEntries: true, merges: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tf := createHFreshIndex(t, withDistanceProvider(distancer.NewL2SquaredProvider()))
			vectors := [][]float32{e1, e1, e1}
			postingID, posting := createPostingWithVectors(t, &tf, vectors, 100)
			if !tt.deleteEntries {
				posting = Posting{}
			}
			registerCentroid(t, &tf, postingID, posting, e1)
			if tt.deleteEntries {
				for i := range vectors {
					require.NoError(t, tf.Index.Delete(100+uint64(i)))
				}
			}
			if tt.candidate {
				// more entries than a merge can take, so that the candidate
				// keeps its centroid whatever the merge does
				many := make([][]float32, int(tf.Index.minPostingSize)+1)
				for i := range many {
					many[i] = e2
				}
				candidateID, candidatePosting := createPostingWithVectors(t, &tf, many, 1000)
				registerCentroid(t, &tf, candidateID, candidatePosting, e2)
			}

			for range 3 {
				_, _, err := tf.Index.SearchByVector(t.Context(), e1, 10, nil)
				require.NoError(t, err)
				tf.Index.waitForMaintenance(t)
			}

			require.Equal(t, tt.merges, mergeRuns(&tf, postingID), "merges run on the empty posting")
			size, err := tf.Index.PostingSizes.Get(t.Context(), postingID)
			require.NoError(t, err)
			require.Zero(t, size)
		})
	}
}
