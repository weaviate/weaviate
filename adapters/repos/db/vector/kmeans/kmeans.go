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
	"cmp"
	"errors"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

type IndexAndDistance struct {
	index    uint32
	distance float32
}

type temporaryData struct {
	centroids       [][]float64          // Higher-precision intermediate storage for computing centroids.
	sizes           []uint32             // Number of points in each cluster.
	assignment      []uint32             // For each data point the index of the cluster that it is assigned to.
	centerNeighbors [][]IndexAndDistance // Lists of nearest centers to each center, ordered by distance.
	rng             *rand.Rand           // RNG for initialization and tie breaking.
}

func (tmp *temporaryData) init(n int, d int, k int, seed uint64, strategy AssignmentStrategy) {
	tmp.centroids = make([][]float64, 0, k)
	for range k {
		tmp.centroids = append(tmp.centroids, make([]float64, d))
	}
	tmp.sizes = make([]uint32, k)
	tmp.assignment = make([]uint32, n)
	if strategy == GraphPruning {
		tmp.centerNeighbors = make([][]IndexAndDistance, k)
		for c := range k {
			tmp.centerNeighbors[c] = make([]IndexAndDistance, 0, k-1)
		}
	}
	tmp.rng = rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
}

func (tmp *temporaryData) free() {
	tmp.centroids = nil
	tmp.sizes = nil
	tmp.assignment = nil
	tmp.centerNeighbors = nil
	tmp.rng = nil
}

type TerminationCondition string

const (
	MaxIterations    TerminationCondition = "MaxIterations"
	ClusterStability TerminationCondition = "ClusterStability"
)

// The first entry in the slices contain statistics for the initialization.
// By convention we set the first number of changes equal to the size of the dataset.
type Metrics struct {
	Iterations   int                  // Number of iterations before terminating.
	Termination  TerminationCondition // The condition that causes k-means to terminate.
	Changes      []int                // For each iteration the number of points that moved to a new cluster.
	Computations []int                // Total number of distance computations.
	WCSS         []float64            // Within-cluster sum of squares per iteration (k-means objective).
}

func (m *Metrics) TotalComputations() int {
	var sum int
	for _, c := range m.Computations {
		sum += c
	}
	return sum
}

func (m *Metrics) TotalChanges() int {
	var sum int
	for _, c := range m.Changes {
		sum += c
	}
	return sum
}

func (m *Metrics) update(metrics IterationMetrics) {
	m.Computations = append(m.Computations, metrics.computations)
	m.WCSS = append(m.WCSS, metrics.wcss)
	m.Changes = append(m.Changes, metrics.changes)
	m.Iterations++
}

type InitializationStrategy string

const (
	PlusPlusInitialization InitializationStrategy = "PlusPlus"
	RandomInitialization   InitializationStrategy = "Random"
)

type AssignmentStrategy string

const (
	GraphPruning AssignmentStrategy = "GraphPruning"
	BruteForce   AssignmentStrategy = "BruteForce"
)

type KMeans struct {
	K                  int                    // How many centroids.
	IterationThreshold int                    // Used to stop fitting after a certain amount of iterations.
	DeltaThreshold     float32                // Used to stop fitting if the fraction of points that change clusters is at or below threshold.
	Initialization     InitializationStrategy // Algorithm used to initialize cluster centers.
	Assignment         AssignmentStrategy     // Whether to use inter-cluster distances to prune away distance computations.
	Seed               uint64                 // The seed for the RNG using during fitting.
	Metrics            Metrics                // Metrics for observability of the clustering algorithm.
	Centers            [][]float32            // The centers computed by Fit()
	distance           distancer.Provider     // The clustering algorithm is intended to work with L2 squared.
	dimensions         int                    // Dimensions of the data.
	segment            int                    // Segment where it operates.
	tmp                temporaryData          // Temporary heap-allocated data used during fitting.
}

// Reasoning behind default settings
// =================================
// Experiments show that GraphPruning speeds up k-means compared to BruteForce
// when d <= 16 on random data. The speedup is about 5x for d = 4, 2x for d = 8,
// and insignificant at d = 16. We use GraphPruning by default as there is
// almost no additional cost for high-dimensional data at typical problem sizes
// (n = 100_000, k = 256), and a clear benefit for low-dimensional data.
//
// The IterationThreshold is what typically determines the number of iterations.
// It is set to balance running time and quality. The DeltaThreshold only comes
// into effect after e.g. 50-100 iterations on random data.
//
// k-means++ initialization seems to provide slightly better WCSS on random data
// compared to random initialization. It does come at at a slight increase in
// cost, something like 3-5% of the total k-means running time, but the
// robustness and increased quality seems worth it.
//
// Notes
// =====
// There exists other heuristics to speed up k-means, see e.g. the paper
// "Making k-means even faster" by Greg Hamerly. We tried implementing this
// method, but it turned out that it mostly saves distance computations when the
// clusters are nearly stable, which only happens after e.g. 50+ iterations.
// Therefore the current heuristic (I don't know if it is published somewhere)
// seems to provide better speedups in the setting that is relewvant for
// product quantization (d = 4 or d = 8, and few iterations).
func New(k int, dimensions int, segment int) *KMeans {
	kMeans := &KMeans{
		K:                  k,
		DeltaThreshold:     0.01,
		IterationThreshold: 10,
		Initialization:     PlusPlusInitialization,
		Assignment:         GraphPruning,
		Seed:               rand.Uint64(),
		distance:           distancer.NewL2SquaredProvider(),
		dimensions:         dimensions,
		segment:            segment,
	}
	return kMeans
}

func (m *KMeans) seg(x []float32) []float32 {
	return x[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
}

func (m *KMeans) l2squared(x []float32, y []float32) float32 {
	dist, _ := m.distance.SingleDist(x, y)
	return dist
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

// initialize performs k-means++ initialization and also performs one iteration
// of Lloyd's algorithm (assigns points to centers and updates centroids) since
// we get the cluster assignment "for free" from the initialization.
func (m *KMeans) initializePlusPlus(data [][]float32) {
	n := len(data)
	copy(m.Centers[0], m.seg(data[m.tmp.rng.IntN(n)]))
	distances := make([]float32, n)
	for i := range distances {
		distances[i] = math.MaxFloat32
	}

	for c := range m.K {
		for i, x := range data {
			if dist := m.l2squared(m.seg(x), m.Centers[c]); dist < distances[i] {
				distances[i] = dist
				m.tmp.assignment[i] = uint32(c)
			}
		}
		if c < m.K-1 {
			idx := weightedSample(distances, m.tmp.rng)
			copy(m.Centers[c+1], m.seg(data[idx]))
		}
	}

	if m.IterationThreshold == 0 {
		return
	}

	m.updateCenters(data)
	var metrics IterationMetrics
	metrics.changes = n
	metrics.computations = n * m.K
	for _, dist := range distances {
		metrics.wcss += float64(dist)
	}
	m.Metrics.update(metrics)
}

type IndexOrder struct{}

func randomSubset(n int, k int, r *rand.Rand) []int {
	if k > n/2 {
		return r.Perm(n)[:k]
	}

	// We sample random integers and insert them in a map to create the random
	// subset. To produce a deterministic ordering we associate a random float
	// with each index and use it to order the subset.
	m := make(map[int]float64, k)
	for len(m) < k {
		m[r.IntN(n)] = r.Float64()
	}
	i := 0

	type IndexOrder struct {
		Index int
		Rank  float64
	}

	subset := make([]IndexOrder, k)
	for key, value := range m {
		subset[i].Index = key
		subset[i].Rank = value
		i++
	}

	slices.SortFunc(subset, func(a, b IndexOrder) int {
		return cmp.Compare(a.Rank, b.Rank)
	})

	indices := make([]int, k)
	for i := range k {
		indices[i] = subset[i].Index
	}

	return indices
}

// initializeRandom picks k random data points as centers. We also run a single
// iteration of LLoyd's algorithm in order to prepare the data structure for
// Fit() in a similar manner to initializePlusPlus().
func (m *KMeans) initializeRandom(data [][]float32) {
	subset := randomSubset(len(data), m.K, m.tmp.rng)
	for c := range m.K {
		copy(m.Centers[c], m.seg(data[subset[c]]))
	}

	if m.IterationThreshold == 0 {
		return
	}

	var metrics IterationMetrics
	for i, x := range data {
		nearest := m.nearestBruteForce(x)
		m.tmp.assignment[i] = nearest.index
		metrics.wcss += float64(nearest.distance)
		metrics.computations += nearest.computations
		metrics.changes++
	}
	m.Metrics.update(metrics)
	m.updateCenters(data)
}

// updateCenters computes new centroids according to the current assignment.
func (m *KMeans) updateCenters(data [][]float32) {
	// We perform intermediate computations of the centroids using float64
	// for improved precision. The overhead of doing this seems to be negligible
	// (< 1% of running time) as measured by BenchmarkKMeansFit().
	clear(m.tmp.sizes)
	for c := range m.K {
		clear(m.tmp.centroids[c])
	}

	for i, x := range data {
		c := m.tmp.assignment[i]
		m.tmp.sizes[c]++
		for j, z := range m.seg(x) {
			m.tmp.centroids[c][j] += float64(z)
		}
	}

	for c := range m.K {
		if m.tmp.sizes[c] == 0 {
			// This is not supposed to happen under normal circumstances.
			// If it happens it is likely due to duplicate data, but
			// k-means++ initialization should never pick duplicates except
			// as a fallback measure. We could pick another random center,
			// but it is unlikely to improve the situtation.
			continue // Keep the current center.
		}
		for j := range m.dimensions {
			m.Centers[c][j] = float32(m.tmp.centroids[c][j] / float64(m.tmp.sizes[c]))
		}
	}
}

type NearestCenter struct {
	index        uint32  // Index of nearest center.
	distance     float32 // L2 squared distance to nearest center.
	computations int     // Number of distance computations used to determine nearest center.
}

// nearestWithPruning returns the index of the nearest center to a point. It
// leverages the following information in order to prune unnecessary distance
// computations:
//
// 1) The center that the point was assigned to in the previous iteration, and
//
// 2) A list of the nearest centers to the previously assigned center, ordered
// by distance.
//
// We use the observation that a point is likely to remain in the same cluster,
// so we take the previous center as the starting point and compute the distance
// centerDist between it and the point. We can then prune away distance
// calculations to every center that lie at a distance greater than 2*centerDist
// from the previous center.
func (m *KMeans) nearestWithPruning(point []float32, prevCenterIdx uint32) NearestCenter {
	pointSegment := m.seg(point)
	minDist := m.l2squared(m.Centers[prevCenterIdx], pointSegment)
	centerDist := float32(math.Sqrt(float64(minDist)))
	idx := prevCenterIdx
	neighbors := m.tmp.centerNeighbors[prevCenterIdx]
	var computations int
	for _, v := range neighbors {
		if v.distance >= 2*centerDist {
			break // Remaining centers can be skipped.
		}
		if dist := m.l2squared(m.seg(point), m.Centers[v.index]); dist < minDist {
			minDist = dist
			idx = v.index
		}
	}
	return NearestCenter{index: idx, distance: minDist, computations: computations}
}

func (m *KMeans) nearestBruteForce(point []float32) NearestCenter {
	var minDist float32 = math.MaxFloat32
	var idx uint32
	for i := range m.Centers {
		if dist := m.l2squared(m.seg(point), m.Centers[i]); dist < minDist {
			minDist = dist
			idx = uint32(i)
		}
	}
	return NearestCenter{index: idx, distance: minDist, computations: m.K}
}

func (m *KMeans) nearest(point []float32, prevCenterIdx uint32) NearestCenter {
	var nearest NearestCenter
	switch m.Assignment {
	case GraphPruning:
		nearest = m.nearestWithPruning(point, prevCenterIdx)
	case BruteForce:
		nearest = m.nearestBruteForce(point)
	}
	return nearest
}

// updateCenterNeighbors computes for each center the list of nearest centers in
// ascending order by Euclidean distance.
func (m *KMeans) updateCenterNeighbors() {
	for c := range m.K {
		m.tmp.centerNeighbors[c] = m.tmp.centerNeighbors[c][:0]
	}

	for c1 := range m.K {
		for c2 := c1 + 1; c2 < m.K; c2++ {
			dist := m.l2squared(m.Centers[c1], m.Centers[c2])
			distEuclidean := float32(math.Sqrt(float64(dist)))
			m.tmp.centerNeighbors[c1] = append(m.tmp.centerNeighbors[c1], IndexAndDistance{index: uint32(c2), distance: distEuclidean})
			m.tmp.centerNeighbors[c2] = append(m.tmp.centerNeighbors[c2], IndexAndDistance{index: uint32(c1), distance: distEuclidean})
		}
	}

	for _, neighbors := range m.tmp.centerNeighbors {
		slices.SortFunc(neighbors, func(a, b IndexAndDistance) int {
			return cmp.Compare(a.distance, b.distance)
		})
	}
}

type IterationMetrics struct {
	changes      int
	computations int
	wcss         float64
}

func (m *KMeans) initMemory(n int) {
	m.tmp.init(n, m.dimensions, m.K, m.Seed, m.Assignment)
	m.Centers = make([][]float32, m.K)
	for c := range m.K {
		m.Centers[c] = make([]float32, m.dimensions)
	}
	m.Metrics = Metrics{}
}

func (m *KMeans) initializeCenters(data [][]float32) {
	switch m.Initialization {
	case RandomInitialization:
		m.initializeRandom(data)
	case PlusPlusInitialization:
		m.initializePlusPlus(data)
	}
}

func (m *KMeans) cleanupMemory() {
	m.tmp.free()
}

func (m *KMeans) computeCentroid(data [][]float32) {
	// We can skip Lloyd's algorithm and return the centroid directly.
	// We leverage existing methods to compute it.
	m.initMemory(len(data)) // Every data point is assigned to the first center.
	m.updateCenters(data)   // Compute the centroid according to the zero-assignment.
}

// Fit runs k-means clustering on the data according to the settings on the
// KMeans struct. After running Fit() the resulting cluster centers can be
// accessed through Centers().
// TODO: Consider refactoring to functions that explicitly pass around structs.
func (m *KMeans) Fit(data [][]float32) error {
	if len(data) < m.K {
		return errors.New("not enough data to fit k-means")
	}

	if m.K == 1 {
		m.computeCentroid(data)
		return nil
	}

	n := len(data)
	m.initMemory(n)
	m.initializeCenters(data)
	m.Metrics.Termination = MaxIterations
	for m.Metrics.Iterations < m.IterationThreshold {
		var metrics IterationMetrics
		if m.Assignment == GraphPruning {
			m.updateCenterNeighbors()
			metrics.computations += m.K * m.K / 2
		}
		for i, x := range data {
			prevCenterIdx := m.tmp.assignment[i]
			nearest := m.nearest(x, prevCenterIdx)
			if nearest.index != prevCenterIdx {
				metrics.changes++
				m.tmp.assignment[i] = nearest.index
			}
			metrics.wcss += float64(nearest.distance)
			metrics.computations += nearest.computations
		}
		m.Metrics.update(metrics)
		m.updateCenters(data)
		if float32(metrics.changes) <= m.DeltaThreshold*float32(n) {
			m.Metrics.Termination = ClusterStability
			break
		}
	}
	m.cleanupMemory()
	return nil
}

func (m *KMeans) DisableDeltaThreshold() {
	m.DeltaThreshold = -1
}

func (m *KMeans) DisableIterationThreshold() {
	m.IterationThreshold = math.MaxInt
}
