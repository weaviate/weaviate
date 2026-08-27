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
	"math"
	"math/rand"
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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

	err = tf.Index.setPostingVectorIDs(t.Context(), postingID, posting)
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

// TestCentroidDistanceQuantizerMismatch is a regression test for the
// centroid-distance path used by the split/merge reassignment checks
// (Centroid.Distance on a centroid fetched via Centroids.Get). The centroid
// HNSW stores 8-bit RQ codes; before the fix in HNSWIndex.Get, the raw 8-bit
// code was handed to Distancer.DistanceBetweenCompressedVectors, which
// decodes it as a 1-bit code — every reassignment comparison was NaN or a
// value unrelated to actual proximity. Get now leaves Compressed nil and
// Centroid.Distance lazily encodes a 1-bit code on first use, so those
// comparisons behave like the estimator.
//
// Setup: well-separated unit centroids inserted exactly like doSplit inserts
// them, and posting vectors sampled tightly around each centroid, encoded
// exactly like posting entries are. For every sample, the production distance
// to its own and to a far centroid must match the 1-bit estimator run on an
// explicitly re-encoded centroid, and must track the true float distances.
func TestCentroidDistanceQuantizerMismatch(t *testing.T) {
	const (
		dims       = 256
		nCentroids = 8
		nSamples   = 25
		noiseSigma = 0.01
	)

	tf := createHFreshIndex(t)
	rng := rand.New(rand.NewSource(1))

	normalize := func(v []float32) []float32 {
		var sum float64
		for _, x := range v {
			sum += float64(x) * float64(x)
		}
		norm := float32(math.Sqrt(sum))
		out := make([]float32, len(v))
		for i, x := range v {
			out[i] = x / norm
		}
		return out
	}

	randomUnitVector := func() []float32 {
		v := make([]float32, dims)
		for i := range v {
			v[i] = float32(rng.NormFloat64())
		}
		return normalize(v)
	}

	centers := make([][]float32, nCentroids)
	for i := range centers {
		centers[i] = randomUnitVector()
	}

	initializeDimensions(t, &tf, centers[0])
	quantizer := tf.Index.quantizer
	dist := tf.Index.distancer

	oneBitCode := func(v []float32) []byte {
		return quantizer.CompressedBytes(quantizer.Encode(v))
	}

	// Insert the centroids exactly the way doSplit does (split.go): the
	// Compressed field passed here is ignored by Insert, and later reads go
	// through Centroids.Get.
	centroidIDs := make([]uint64, nCentroids)
	for i, c := range centers {
		id, err := tf.Index.IDs.Next()
		require.NoError(t, err)
		centroidIDs[i] = id
		require.NoError(t, tf.Index.Centroids.Insert(id, &Centroid{
			Uncompressed: c,
			Compressed:   oneBitCode(c),
		}))
	}

	fetched := make([]*Centroid, nCentroids)
	for i, id := range centroidIDs {
		c, err := tf.Index.Centroids.Get(id)
		require.NoError(t, err)
		fetched[i] = c
	}

	// Get must not hand out the centroid HNSW's internal 8-bit code: it leaves
	// Compressed nil and Centroid.Distance lazily encodes a 1-bit code on
	// first use (at d=256: 40 bytes, not 272).
	oneBitLen := len(oneBitCode(centers[0]))
	require.Nil(t, fetched[0].Compressed,
		"Centroids.Get must not populate Compressed eagerly")

	var (
		trueOwnMax, trueFarMin float32 = 0, 4
		prodOK, prodOrder      int
		total                  int
	)

	for ci := range centers {
		far := (ci + nCentroids/2) % nCentroids
		reencodedOwn := oneBitCode(fetched[ci].Uncompressed)
		reencodedFar := oneBitCode(fetched[far].Uncompressed)

		for s := range nSamples {
			sample := make([]float32, dims)
			for i := range sample {
				sample[i] = centers[ci][i] + float32(rng.NormFloat64())*noiseSigma
			}
			sample = normalize(sample)
			vec := NewVector(uint64(ci*nSamples+s), VectorVersion(1), oneBitCode(sample))

			trueOwn, err := dist.DistanceBetweenVectors(sample, centers[ci])
			require.NoError(t, err)
			trueFar, err := dist.DistanceBetweenVectors(sample, centers[far])
			require.NoError(t, err)

			// The production path: identical to the newDist/oldDist/prevDist
			// computations in enqueueReassignAfterSplit and doMerge.
			prodOwn, err := fetched[ci].Distance(dist, vec)
			require.NoError(t, err)
			prodFar, err := fetched[far].Distance(dist, vec)
			require.NoError(t, err)

			// It must agree exactly with the estimator run on an explicitly
			// re-encoded centroid.
			expectedOwn, err := dist.DistanceBetweenCompressedVectors(vec.Data(), reencodedOwn)
			require.NoError(t, err)
			require.Equal(t, expectedOwn, prodOwn)
			expectedFar, err := dist.DistanceBetweenCompressedVectors(vec.Data(), reencodedFar)
			require.NoError(t, err)
			require.Equal(t, expectedFar, prodFar)

			total++
			trueOwnMax = max(trueOwnMax, trueOwn)
			trueFarMin = min(trueFarMin, trueFar)

			// "does the production path agree the vector sits near its centroid?"
			if prodOwn < 0.5 && prodFar > 0.5 {
				prodOK++
			}
			// the comparison the reassignment logic actually gates on
			if prodOwn < prodFar {
				prodOrder++
			}
		}
	}

	// Distance memoized a code in the 1-bit quantizer's format.
	require.Len(t, fetched[0].Compressed, oneBitLen,
		"Centroid.Distance must memoize a 1-bit code")

	t.Logf("samples: %d", total)
	t.Logf("true own-centroid distance max: %.4f, far-centroid min: %.4f", trueOwnMax, trueFarMin)
	t.Logf("near/far separated correctly: %d/%d, own ranked closer: %d/%d", prodOK, total, prodOrder, total)

	// Geometry sanity: samples sit on their centroid, far centroids are far.
	require.Less(t, trueOwnMax, float32(0.2))
	require.Greater(t, trueFarMin, float32(0.6))

	// The production distances must track the true geometry: a vector sitting
	// essentially ON its centroid is reported near, a far centroid far, and
	// the own centroid ranks closer. Before the fix these were 0/200, 50/200.
	require.GreaterOrEqual(t, float64(prodOK)/float64(total), 0.95)
	require.GreaterOrEqual(t, float64(prodOrder)/float64(total), 0.95)
}
