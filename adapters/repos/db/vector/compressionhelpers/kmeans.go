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

package compressionhelpers

import (
	"math"
	"math/rand/v2"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type KMeans struct {
	K                  int     // How many centroids
	IterationThreshold int     // Used to stop fitting after a certain amount of iterations
	DeltaThreshold     float32 // Used to stop fitting if the fraction of points that change clusters drops below threshold
	Distance           distancer.Provider
	Seed               uint64      // The seed for the RNG using during fitting
	centers            [][]float32 // The current centroids
	dimensions         int         // Dimensions of the data
	segment            int         // Segment where it operates
}

func NewKMeans(k int, dimensions int, segment int) *KMeans {
	kMeans := &KMeans{
		K:                  k,
		DeltaThreshold:     0.01,
		IterationThreshold: 10,
		Distance:           distancer.NewL2SquaredProvider(),
		Seed:               392121183,
		dimensions:         dimensions,
		segment:            segment,
	}
	return kMeans
}

func (m *KMeans) Centers() [][]float32 {
	return m.centers
}

func (m *KMeans) nearest(point []float32) uint32 {
	pointSegment := m.seg(point)
	var minDist float32 = math.MaxFloat32
	idx := 0
	for i := range m.centers {
		if dist, _ := m.Distance.SingleDist(pointSegment, m.centers[i]); dist < minDist {
			minDist = dist
			idx = i
		}
	}
	return uint32(idx)
}

func (m *KMeans) seg(x []float32) []float32 {
	return x[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
}

func weightedSample(weights []float32, r *rand.Rand) int {
	var s float32
	for _, w := range weights {
		s += w
	}

	var v float32
	target := s * r.Float32()
	for i, w := range weights {
		v += w
		if target < v {
			return i
		}
	}
	// Fallback, should not happen.
	return r.IntN(len(weights))
}

// k-means++ initialization.
func (m *KMeans) initialize(data [][]float32) {
	r := rand.New(rand.NewPCG(m.Seed, 735755762))
	m.centers = make([][]float32, m.K)
	for c := range m.K {
		m.centers[c] = make([]float32, m.dimensions)
	}
	copy(m.centers[0], m.seg(data[r.IntN(len(data))]))

	distances := make([]float32, len(data))
	for i := range distances {
		distances[i] = math.MaxFloat32
	}

	for c := range m.K - 1 {
		for i, x := range data {
			if dist, _ := m.Distance.SingleDist(m.seg(x), m.centers[c]); dist < distances[i] {
				distances[i] = dist
			}
		}
		idx := weightedSample(distances, r)
		copy(m.centers[c+1], m.seg(data[idx]))
	}
}

func (m *KMeans) Fit(data [][]float32) {
	m.initialize(data)

	// We perform intermediate computations of the centroids using float64
	// for improved precision. The overhead of doing this seems to be negligible
	// (< 1% of running time) as measured by BenchmarkKMeansFit().
	centroids := make([][]float64, 0, m.K)
	for range m.K {
		centroids = append(centroids, make([]float64, m.dimensions))
	}
	assignment := make([]uint32, len(data))
	sizes := make([]int, m.K)
	changes := len(data)
	for range m.IterationThreshold {
		// Assign data points to clusters and compute new centroids.
		for i, x := range data {
			c := m.nearest(x)
			sizes[c]++
			if c != assignment[i] {
				changes++
				assignment[i] = c
			}

			// Update centroid.
			for j, z := range m.seg(x) {
				centroids[c][j] += float64(z)
			}
		}

		// Compute new centers.
		for c := range m.K {
			if sizes[c] == 0 {
				// This is not supposed to happen under normal circumstances.
				// If it happens it is likely due to duplicate data, but
				// k-means++ initialization should never pick duplicates except
				// as a fallback measure. We could pick another random center,
				// but it is unlikely to improve the situtation.
				continue // Keep the current center.
			}
			for j := range m.dimensions {
				m.centers[c][j] = float32(centroids[c][j] / float64(sizes[c]))
			}
		}

		if float32(changes) < m.DeltaThreshold*float32(len(data)) {
			break
		}

		// Clean up for next pass
		changes = 0
		clear(sizes)
		for c := range m.K {
			clear(centroids[c])
		}
	}
}
