//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package kmeans

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type Distribution string

const (
	uniform Distribution = "uniform"
	normal  Distribution = "normal"
)

const (
	seed uint64 = 0x6a62976fa43149c9
)

func generateData(n int, d int, dist Distribution, seed uint64) [][]float32 {
	r := rand.New(rand.NewPCG(seed, 8523456))
	data := make([][]float32, n)
	for i := range n {
		data[i] = make([]float32, d)
		for j := range d {
			switch dist {
			case uniform:
				data[i][j] = r.Float32()
			case normal:
				data[i][j] = float32(r.NormFloat64())
			}
		}
	}
	return data
}

type KMeansVariant struct {
	Initialization InitializationStrategy
	Assignment     AssignmentStrategy
}

var kMeansVariants = [...]KMeansVariant{
	{Initialization: RandomInitialization, Assignment: BruteForce},
	{Initialization: RandomInitialization, Assignment: GraphPruning},
	{Initialization: PlusPlusInitialization, Assignment: BruteForce},
	{Initialization: PlusPlusInitialization, Assignment: GraphPruning},
}

// Convenient instantiation of KMeans for testing.
func newKMeans(k int, d int, variant KMeansVariant) *KMeans {
	kmeans := New(k, d, 0)
	kmeans.Initialization = variant.Initialization
	kmeans.Assignment = variant.Assignment
	return kmeans
}

// By default we seed KMeans with a call to the global random number generator.
// For some tests we want deterministic behavior for stability, so we explicitly
// set the seed.
func newDeterministicKMeans(k int, d int, variant KMeansVariant) *KMeans {
	kmeans := newKMeans(k, d, variant)
	kmeans.Seed = seed
	return kmeans
}

func TestIterationThreshold(t *testing.T) {
	n := 100
	d := 4
	k := 8
	data := generateData(n, d, normal, seed)
	maxIterations := 5
	for _, variant := range kMeansVariants {
		for i := range maxIterations {
			km := newKMeans(k, d, variant)
			km.DisableDeltaThreshold()
			km.IterationThreshold = i
			km.Fit(data)
			assert.Equal(t, km.Metrics.Iterations, i)
			assert.Equal(t, len(km.Metrics.WCSS), i,
				"The length of the per-iteration metric slices should match the number of iterations.")
			assert.Equal(t, km.Metrics.Termination, MaxIterations)
		}
	}
}

func TestDeltaThreshold(t *testing.T) {
	n := 100
	d := 4
	k := 8
	data := generateData(n, d, normal, seed)

	for _, variant := range kMeansVariants {
		km := newDeterministicKMeans(k, d, variant)
		km.DisableIterationThreshold()
		// With a threshold of zero we iterate until clusters are perfectly stable.
		km.DeltaThreshold = 0.0
		km.Fit(data)
		assert.Greater(t, km.Metrics.Iterations, 1)
		assert.Less(t, km.Metrics.Iterations, 100)
		assert.Equal(t, km.Metrics.Termination, ClusterStability)
		assert.Equal(t, km.Metrics.Changes[len(km.Metrics.Changes)-1], 0)
	}
}

// The k-means objective functions is the Within-Cluster Sum of Squares (WCSS).
// With LLoyd's algorithm it is guaranteed to decrease with each iteration.
func TestDecreasingWCSS(t *testing.T) {
	n := 1000
	d := 8
	k := 16
	data := generateData(n, d, normal, seed)

	for _, variant := range kMeansVariants {
		km := newDeterministicKMeans(k, d, variant)
		km.Fit(data)
		slices.Reverse(km.Metrics.WCSS)
		assert.True(t, slices.IsSorted(km.Metrics.WCSS))
	}
}

// The GraphPruning assignment strategy only prunes away unnecessary distance
// computations. We should be producing the same result up to floating point
// errors and randomness in tie breaking. With random data ties are highly
// unlikely so any differences are likely due to a bug. Note that this test is a
// bit too tied to the implementation in that it requires exact equality of the
// centers, but it does the job for now.
func TestGraphPruningAssignment(t *testing.T) {
	n := 1000
	d := 8
	k := 32
	data := generateData(n, d, normal, seed)

	for _, init := range [...]InitializationStrategy{RandomInitialization, PlusPlusInitialization} {
		bf := newDeterministicKMeans(k, d, KMeansVariant{init, BruteForce})
		bf.Fit(data)

		prune := newDeterministicKMeans(k, d, KMeansVariant{init, GraphPruning})
		prune.Fit(data)

		for i := range k {
			assert.True(t, slices.Equal(bf.Centers[i], prune.Centers[i]))
		}
	}
}

func contains(data [][]float32, q []float32) bool {
	const eps = 1e-12
	l2 := distancer.NewL2SquaredProvider()
	for _, x := range data {
		dist, _ := l2.SingleDist(x, q)
		if dist < eps {
			return true
		}
	}
	return false
}

func TestCorrectnessAcrossSegments(t *testing.T) {
	// Create a dataset with two segments of two dimensions each.
	// The first segment has clusters centered at (1,1) and (-1, -1).
	// The second segment has clusters centered at (-1, 1) and (1, -1).
	data := [][]float32{
		{0.99, 0.99, -0.99, 0.99},
		{1.01, 1.01, -1.01, 1.01},
		{-0.99, -0.99, 0.99, -0.99},
		{-1.01, -1.01, 1.01, -1.01},
	}

	k := 2
	d := 2
	for _, variant := range kMeansVariants {
		// Run k-means clustering on each segment and assert that we find the true
		km := newDeterministicKMeans(k, d, variant)
		km.segment = 0
		km.Fit(data)
		assert.True(t, contains(km.Centers, []float32{1, 1}))
		assert.True(t, contains(km.Centers, []float32{-1, -1}))

		km = newDeterministicKMeans(k, d, variant)
		km.segment = 1
		km.Fit(data)
		assert.True(t, contains(km.Centers, []float32{-1, 1}))
		assert.True(t, contains(km.Centers, []float32{1, -1}))
	}
}

// Create a new data set that duplicates each data point k times and shuffles
// the data. Verify that with k-means++ initialization we end up with the
// original data points as centers. With random initialization we would be
// extremely unlikely to capture the original k centers.
func TestPlusPlusInitialization(t *testing.T) {
	k := 100
	d := 64
	centers := generateData(k, d, uniform, seed)

	data := make([][]float32, k*k)
	for i := range data {
		data[i] = make([]float32, d)
		copy(data[i], centers[i/k])
	}

	r := rand.New(rand.NewPCG(seed, 8523456))
	r.Shuffle(len(data), func(i, j int) {
		data[i], data[j] = data[j], data[i]
	})

	km := newDeterministicKMeans(k, d, KMeansVariant{PlusPlusInitialization, BruteForce})
	km.IterationThreshold = 0 // Only perform initialization.
	km.Fit(data)
	for _, trueCenter := range centers {
		var found bool
		for _, kmCenter := range km.Centers {
			dist := km.l2squared(trueCenter, kmCenter)
			if dist < 1e-12 {
				found = true
				break
			}
		}
		assert.True(t, found, "k-means++ failed to sample all true centers.")
	}
}

func wcss(data [][]float32, centers [][]float32) float64 {
	l2s := distancer.NewL2SquaredProvider()
	var sum float64
	for _, v := range data {
		var minDist float32 = math.MaxFloat32
		for _, c := range centers {
			if dist, _ := l2s.SingleDist(v, c); dist < minDist {
				minDist = dist
			}
		}
		sum += float64(minDist)
	}
	return sum
}

func TestFewDataPoints(t *testing.T) {
	n := 10
	d := 8
	k := n
	data := generateData(n, d, uniform, seed)

	// With k = n we all variants should select the original data points as
	// cluster centers, resulting in a WCSS of zero.
	for _, variant := range kMeansVariants {
		km := newDeterministicKMeans(k, d, variant)
		km.Fit(data)
		assert.Equal(t, wcss(data, km.Centers), 0.0)
	}
}

func TestOneCenter(t *testing.T) {
	data := [][]float32{
		{1, 0},
		{0, 1},
		{1, 1},
		{0, 0},
	}
	for _, variant := range kMeansVariants {
		km := newDeterministicKMeans(1, 2, variant)
		km.Fit(data)
		assert.Equal(t, len(km.Centers), 1)
		assert.True(t, slices.Equal(km.Centers[0], []float32{0.5, 0.5}))
	}
}

func BenchmarkKMeansFit(b *testing.B) {
	distribution := []Distribution{normal, uniform}
	dimensions := []int{4, 8, 16, 32}
	k := 256
	n := 100_000
	var seed uint64
	for _, dist := range distribution {
		for _, d := range dimensions {
			data := generateData(n, d, dist, seed)
			seed++
			for _, variant := range kMeansVariants {
				b.Run(fmt.Sprintf("KMeansFit-%v-d%d-%v-%v", dist, d, variant.Initialization, variant.Assignment), func(b *testing.B) {
					var computations int
					var WCSS float64
					var iterations int
					for i := 0; i < b.N; i++ {
						km := newDeterministicKMeans(k, d, variant)
						km.IterationThreshold = 50
						b.StartTimer()
						km.Fit(data)
						b.StopTimer()
						iterations += km.Metrics.Iterations
						WCSS += wcss(data, km.Centers)
						computations += km.Metrics.TotalComputations()
					}
					b.ReportMetric(float64(b.Elapsed().Seconds())/float64(b.N), "sec/fit") // More readable than ns.
					b.ReportMetric(float64(iterations)/float64(b.N), "iter")
					b.ReportMetric(WCSS/float64(b.N), "wcss")
					b.ReportMetric(float64(computations), "distcomps")
				})
			}
		}
	}
}
