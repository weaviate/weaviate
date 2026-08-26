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

// TestCentroidDistanceQuantizerMismatch demonstrates that the production
// centroid-distance path used by the split/merge reassignment checks
// (Centroid.Distance on a centroid fetched via Centroids.Get) produces
// meaningless distances: the fetched Compressed bytes are an 8-bit RQ code
// from the centroid HNSW, but Distancer.DistanceBetweenCompressedVectors
// decodes them as a 1-bit RQ code.
//
// Setup: well-separated unit centroids inserted exactly like doSplit inserts
// them, and posting vectors sampled tightly around each centroid, encoded
// exactly like posting entries are. For every sample we compare three
// distances to its own centroid and to a far centroid:
//
//   - true:    float cosine distance (ground truth)
//   - correct: the same 1-bit symmetric estimator, with the centroid encoded
//     by the 1-bit quantizer (what the code intends to compute)
//   - buggy:   the production path, Centroid.Distance on the Get() result
//
// The correct estimator tracks the truth; the production path returns ~1.0
// for everything, so "is this vector closer to its own centroid?" degrades
// to a coin flip.
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
	// the test helper builds Distancer without its sync.Pool; construct it
	// the way production does (insert.go)
	dist := NewDistancer(quantizer, tf.Index.config.DistanceProvider)

	oneBitCode := func(v []float32) []byte {
		return quantizer.CompressedBytes(quantizer.Encode(v))
	}

	// Insert the centroids exactly the way doSplit does (split.go): the
	// Compressed field passed here is ignored by Insert, and later reads go
	// through Centroids.Get, which returns the centroid HNSW's 8-bit code.
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

	// The format mismatch itself: Get hands back a code that is not a 1-bit
	// code. At d=256 the 1-bit code is 40 bytes, the 8-bit code 272 bytes.
	oneBitLen := len(oneBitCode(centers[0]))
	fetchedLen := len(fetched[0].Compressed)
	t.Logf("1-bit code: %d bytes, code returned by Centroids.Get: %d bytes", oneBitLen, fetchedLen)
	require.NotEqual(t, oneBitLen, fetchedLen,
		"Centroids.Get returned a code with the 1-bit layout; the mismatch this test pins no longer exists")

	var (
		trueOwnMax, trueFarMin       float32 = 0, 4
		correctOK, buggyOK, total    int
		correctOrder, buggyOrder     int
		buggyNaN, buggySane          int
		sumTrueOwn                   float64
		sumCorrectOwn, sumCorrectFar float64
	)

	for ci := range centers {
		far := (ci + nCentroids/2) % nCentroids
		correctOwnCentroid := oneBitCode(fetched[ci].Uncompressed)
		correctFarCentroid := oneBitCode(fetched[far].Uncompressed)

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

			correctOwn, err := dist.DistanceBetweenCompressedVectors(vec.Data(), correctOwnCentroid)
			require.NoError(t, err)
			correctFar, err := dist.DistanceBetweenCompressedVectors(vec.Data(), correctFarCentroid)
			require.NoError(t, err)

			// The production path: identical to the newDist/oldDist/prevDist
			// computations in enqueueReassignAfterSplit and doMerge.
			buggyOwn, err := fetched[ci].Distance(dist, vec)
			require.NoError(t, err)
			buggyFar, err := fetched[far].Distance(dist, vec)
			require.NoError(t, err)

			total++
			trueOwnMax = max(trueOwnMax, trueOwn)
			trueFarMin = min(trueFarMin, trueFar)
			sumTrueOwn += float64(trueOwn)
			sumCorrectOwn += float64(correctOwn)
			sumCorrectFar += float64(correctFar)

			if math.IsNaN(float64(buggyOwn)) {
				buggyNaN++
			}
			// a production own-centroid distance that is small enough to be
			// plausible for a vector sitting on its centroid (NaN compares
			// false, so NaN counts as not-sane)
			if buggyOwn < 0.5 {
				buggySane++
			}

			// "does the estimator agree the vector sits near its centroid?"
			if correctOwn < 0.5 && correctFar > 0.5 {
				correctOK++
			}
			if buggyOwn < 0.5 && buggyFar > 0.5 {
				buggyOK++
			}
			// the comparison the reassignment logic actually gates on
			if correctOwn < correctFar {
				correctOrder++
			}
			if buggyOwn < buggyFar {
				buggyOrder++
			}
		}
	}

	t.Logf("samples: %d", total)
	t.Logf("true own-centroid distance:      mean %.4f, max %.4f", sumTrueOwn/float64(total), trueOwnMax)
	t.Logf("true far-centroid distance:      min  %.4f", trueFarMin)
	t.Logf("correct 1-bit estimate own/far:  mean %.4f / %.4f", sumCorrectOwn/float64(total), sumCorrectFar/float64(total))
	t.Logf("production estimate own:         NaN for %d/%d, plausibly small (<0.5) for %d/%d", buggyNaN, total, buggySane, total)
	t.Logf("near/far separated correctly:    correct-encoding %d/%d, production %d/%d", correctOK, total, buggyOK, total)
	t.Logf("own ranked closer than far:      correct-encoding %d/%d, production %d/%d", correctOrder, total, buggyOrder, total)

	// Geometry sanity: samples sit on their centroid, far centroids are far.
	require.Less(t, trueOwnMax, float32(0.2))
	require.Greater(t, trueFarMin, float32(0.6))

	// The estimator itself is fine when both operands are real 1-bit codes.
	require.GreaterOrEqual(t, float64(correctOrder)/float64(total), 0.95,
		"the 1-bit estimator with properly encoded centroids should rank the own centroid closer")
	require.GreaterOrEqual(t, float64(correctOK)/float64(total), 0.95)

	// The production path is garbage: a vector sitting essentially ON its
	// centroid (true distance < 0.2) is never reported at a plausibly small
	// distance — the result is NaN (sqrt of a misinterpreted header float) or
	// a large value unrelated to actual proximity.
	require.Equal(t, 0, buggySane,
		"production distance to the own centroid should never look plausibly small if the mismatch is present")
	require.LessOrEqual(t, float64(buggyOK)/float64(total), 0.5,
		"production path should be unable to separate near from far centroids")
	require.LessOrEqual(t, float64(buggyOrder)/float64(total), 0.8,
		"production own-vs-far ordering should be near a coin flip, not reliable")
}
