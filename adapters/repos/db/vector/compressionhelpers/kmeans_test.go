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

package compressionhelpers_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type ClusteredDataGenerator struct {
	r *rand.Rand
}

func NewClusteredDataGenerator() *ClusteredDataGenerator {
	gen := &ClusteredDataGenerator{
		r: rand.New(rand.NewPCG(56734524, 8523456)),
	}
	return gen
}

func (gen *ClusteredDataGenerator) randomNormalVector(d int, s float32) []float32 {
	x := make([]float32, d)
	for i := range d {
		x[i] = s * float32(gen.r.NormFloat64())
	}
	return x
}

func (gen *ClusteredDataGenerator) Generate(d int, k int, m int) (data [][]float32, centers [][]float32) {
	data = make([][]float32, 0, k*m)
	centers = make([][]float32, 0, k)
	for range k {
		c := gen.randomNormalVector(d, 1.0)
		centers = append(centers, c)
		for range m {
			x := gen.randomNormalVector(d, 0.1)
			for i := range d {
				x[i] += c[i]
			}
			data = append(data, x)
		}
	}
	return data, centers
}

func wcss(data [][]float32, centers [][]float32) float32 {
	l2s := distancer.NewL2SquaredProvider()
	var sum float32
	for _, v := range data {
		var minDist float32 = math.MaxFloat32
		for _, c := range centers {
			if dist, _ := l2s.SingleDist(v, c); dist < minDist {
				minDist = dist
			}
		}
		sum += minDist
	}
	return sum
}

// The within cluster sum of squares (WCSS) is the objective function that
// k-means clustering seeks to minimize. With every iteration of Lloyd's
// algorithm the WCSS should be decreasing.
func TestKMeansDecreasingWCSS(t *testing.T) {
	d := 16
	k := 16
	m := 10
	gen := NewClusteredDataGenerator()
	data, _ := gen.Generate(d, k, m)

	maxIterations := 5
	objective := make([]float32, 0, maxIterations)
	for i := 1; i < maxIterations+1; i++ {
		kmeans := compressionhelpers.NewKMeans(k, d, 0)
		kmeans.DeltaThreshold = 0.0
		kmeans.IterationThreshold = i
		kmeans.Fit(data)
		objective = append(objective, wcss(data, kmeans.Centers()))
	}

	for j := 1; j < maxIterations; j++ {
		assert.True(t, objective[j] <= objective[j-1],
			"WCSS %v should be non-increasing.", objective)
	}
}

// Return true if data contains q (or a point very close to it).
func contains(data [][]float32, q []float32) bool {
	const eps = 1e-9
	l2 := distancer.NewL2SquaredProvider()
	for _, x := range data {
		dist, _ := l2.SingleDist(x, q)
		if dist < eps {
			return true
		}
	}
	return false
}

func TestKMeansCorrectnessAcrossSegments(t *testing.T) {
	// Create a dataset with two segments of two dimensions each.
	// The first segment has clusters centered at (1,1) and (-1, -1).
	// The second segment has clusters centered at (-1, 1) and (1, -1).
	data := [][]float32{
		{0.99, 0.99, -0.99, 0.99},
		{1.01, 1.01, -1.01, 1.01},
		{-0.99, -0.99, 0.99, -0.99},
		{-1.01, -1.01, 1.01, -1.01},
	}

	// Run k-means clustering on each segment and assert that we find the true
	// cluster centers.
	kmeans := compressionhelpers.NewKMeans(2, 2, 0)
	kmeans.Fit(data)
	assert.True(t, contains(kmeans.Centers(), []float32{1, 1}))
	assert.True(t, contains(kmeans.Centers(), []float32{-1, -1}))

	kmeans = compressionhelpers.NewKMeans(2, 2, 1)
	kmeans.Fit(data)
	assert.True(t, contains(kmeans.Centers(), []float32{-1, 1}))
	assert.True(t, contains(kmeans.Centers(), []float32{1, -1}))
}

// We should be able to handle duplicate data gracefully.
func TestKMeansHandlesDuplicates(t *testing.T) {
	data := [][]float32{
		{0, 0},
		{1, 1},
	}
	for range 100 {
		data = append(data, []float32{2, 2})
	}

	kmeans := compressionhelpers.NewKMeans(3, 2, 0)
	kmeans.Fit(data)
	assert.True(t, wcss(data, kmeans.Centers()) == 0)
}

// If the number of data points <= k we should see a WCSS of 0 since we can
// pick the points as centers.
func TestKMeansHandlesSmallData(t *testing.T) {
	d := 8
	k := 16
	m := 1
	gen := NewClusteredDataGenerator()
	data, _ := gen.Generate(d, k, m)

	kmeans := compressionhelpers.NewKMeans(k, d, 0)
	kmeans.Fit(data)
	assert.True(t, wcss(data, kmeans.Centers()) == 0)

	kmeans = compressionhelpers.NewKMeans(2*k, d, 0)
	kmeans.Fit(data)
	assert.True(t, wcss(data, kmeans.Centers()) == 0)
}

// Verify that k-means clustering tends to outperform random sampling.
func TestKMeansBeatsRandomSampling(t *testing.T) {
	r := rand.New(rand.NewPCG(42, 42))
	gen := NewClusteredDataGenerator()
	var count int
	n := 100
	for range n {
		d := 1 + r.IntN(64)         // Dimensionality
		c := 1 + r.IntN(64)         // Number of true cluster centers
		m := 1 + r.IntN(64)         // Number of points in each cluster
		k := min(1+r.IntN(64), c*m) // Number of clusters to look for
		data, _ := gen.Generate(d, c, m)

		// Sample random centers and compute WCSS
		randomCenters := make([][]float32, 0, k)
		randomIndices := r.Perm(len(data))[:k]
		for _, v := range randomIndices {
			randomCenters = append(randomCenters, data[v])
		}
		randomWCSS := wcss(data, randomCenters)

		// Run k-means clustering with default settings and compute WCSS
		kmeans := compressionhelpers.NewKMeans(k, d, 0)
		kmeans.Fit(data)
		kmeansWCSS := wcss(data, kmeans.Centers())

		assert.True(t, len(randomCenters) == k)
		assert.True(t, len(kmeans.Centers()) == k)
		if randomWCSS < kmeansWCSS {
			count++
		}
	}
	// We should not lose to random centers more than 5% of the time.
	assert.LessOrEqual(t, count, 5)
}

func BenchmarkKMeansFit(b *testing.B) {
	// We should ensure that we get at least 10 runs for every setting.
	settings := []struct {
		d int // dimension
		c int // true number of clusters in generated data
		m int // points in each cluster
		k int // number of clusters for k-means to look for
		t int // iterationthreshold
	}{
		// The typical use case for PQ is k = 256 with segments of
		// length d = 8.
		{8, 256, 100, 256, 2},
		{8, 256, 100, 256, 5},
		{8, 256, 100, 256, 10},

		// Try some IVF use cases with different numbers of true clusters.
		{128, 32, 100, 64, 5},
		{128, 64, 100, 64, 5},
		{128, 128, 100, 64, 5},
	}

	gen := NewClusteredDataGenerator()
	for _, s := range settings {
		data, _ := gen.Generate(s.d, s.c, s.m)

		b.Run(fmt.Sprintf("KMeansFit-d%d-c%d-m%d-k%d-t%d", s.d, s.c, s.m, s.k, s.t), func(b *testing.B) {
			var wcssSum float32
			for i := 0; i < b.N; i++ {
				kmeans := compressionhelpers.NewKMeans(s.k, s.d, 0)
				kmeans.Seed = uint64(i)
				kmeans.DeltaThreshold = 0
				kmeans.IterationThreshold = s.t
				b.StartTimer()
				kmeans.Fit(data)
				b.StopTimer()
				wcssSum += wcss(data, kmeans.Centers())
			}
			b.ReportMetric(float64(wcssSum)/float64(b.N), "wcss/op")
		})
	}
}
