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

func repeatVector(v []float32, n int) [][]float32 {
	data := make([][]float32, n)
	for i := range data {
		data[i] = slices.Clone(v)
	}
	return data
}

func constantVector(d int, value float32) []float32 {
	v := make([]float32, d)
	for j := range v {
		v[j] = value
	}
	return v
}

// blobWithOutliers returns a dense blob of normally distributed points
// followed by nOutliers identical points at distance `offset` per dimension.
func blobWithOutliers(nBlob int, nOutliers int, d int, offset float32, seed uint64) [][]float32 {
	data := generateData(nBlob, d, normal, seed)
	for range nOutliers {
		data = append(data, constantVector(d, offset))
	}
	return data
}

// assertCentersMatchMembership asserts the core invariant of FitBalanced:
// for each cluster, the returned center is the mean of the vectors whose
// returned label points to it. The balanced membership legitimately violates
// nearest-centroid assignment for the points moved by balancing, so
// nearest-centroid is deliberately not asserted.
func assertCentersMatchMembership(t *testing.T, km *KMeans, data [][]float32, labels []uint32) {
	t.Helper()
	sums := make([][]float64, km.K)
	for c := range km.K {
		sums[c] = make([]float64, km.dimensions)
	}
	counts := make([]int, km.K)
	for i, c := range labels {
		counts[c]++
		for j, v := range km.seg(data[i]) {
			sums[c][j] += float64(v)
		}
	}
	for c := range km.K {
		if counts[c] == 0 {
			continue
		}
		for j := range km.dimensions {
			mean := sums[c][j] / float64(counts[c])
			assert.InDelta(t, mean, float64(km.Centers[c][j]), 1e-3,
				"cluster %d dimension %d: center must be the mean of the cluster's balanced members", c, j)
		}
	}
}

func balancedClusterSizes(t *testing.T, labels []uint32, k int) []int {
	t.Helper()
	counts := make([]int, k)
	for _, c := range labels {
		assert.Less(t, int(c), k)
		counts[c]++
	}
	return counts
}

func TestFitBalancedCentersMatchMembership(t *testing.T) {
	d := 4
	k := 2

	cases := []struct {
		name string
		data [][]float32
	}{
		{
			name: "normal data",
			data: generateData(300, d, normal, seed),
		},
		{
			// Most points are copies of one vector, so the initial centers
			// typically both land on a duplicate and one cluster starts out
			// empty, leaving its center at the initialization value until the
			// clusters separate.
			name: "duplicates with distinct minority",
			data: append(repeatVector(constantVector(d, 0.1), 194), repeatVector(constantVector(d, 0.9), 6)...),
		},
		{
			// Every point is identical: the second cluster stays empty for the
			// entire run and updateCenters keeps its initialization center,
			// while balancing still splits the membership roughly in half.
			name: "all duplicates",
			data: repeatVector(constantVector(d, 0.5), 200),
		},
	}

	for _, tc := range cases {
		for _, variant := range kMeansVariants {
			t.Run(fmt.Sprintf("%s-%v-%v", tc.name, variant.Initialization, variant.Assignment), func(t *testing.T) {
				km := newDeterministicKMeans(k, d, variant)
				labels, err := km.FitBalanced(tc.data)
				assert.NoError(t, err)
				assert.Len(t, labels, len(tc.data))

				n := len(tc.data)
				floor := (n + 3) / 4
				counts := balancedClusterSizes(t, labels, k)
				assert.GreaterOrEqual(t, counts[0], floor, "left child must hold at least ceil(n/4) vectors")
				assert.GreaterOrEqual(t, counts[1], floor, "right child must hold at least ceil(n/4) vectors")

				assertCentersMatchMembership(t, km, tc.data, labels)
			})
		}
	}
}

// A dense blob plus two far outliers, sized above the hfresh split floor of
// 192 vectors. Lloyd's iterations isolate the outliers in a two-point cluster;
// the size floor then tops that cluster up to exactly ceil(n/4) members with
// the blob points that have the lowest assignment regret. The returned center
// for that cluster must be the mean of the constrained membership, not the
// mean of just the two outliers.
func TestFitBalancedOutlierBlobSplit(t *testing.T) {
	d := 4
	k := 2
	nBlob := 198
	nOutliers := 2
	offset := float32(1000)
	data := blobWithOutliers(nBlob, nOutliers, d, offset, seed)
	n := nBlob + nOutliers
	floor := (n + 3) / 4

	for _, variant := range kMeansVariants {
		t.Run(fmt.Sprintf("%v-%v", variant.Initialization, variant.Assignment), func(t *testing.T) {
			km := newDeterministicKMeans(k, d, variant)
			labels, err := km.FitBalanced(data)
			assert.NoError(t, err)
			assert.Len(t, labels, n)

			counts := balancedClusterSizes(t, labels, k)
			assert.Equal(t, floor, min(counts[0], counts[1]),
				"the outlier cluster must be topped up to exactly the size floor")

			assertCentersMatchMembership(t, km, data, labels)
		})
	}
}

// Two well-separated groups whose natural split satisfies the size floor:
// the constrained assignment must not interfere — every point stays with its
// own group.
func TestFitBalancedNaturalSplitUntouched(t *testing.T) {
	d := 4
	nA, nB := 140, 60

	data := append(repeatVector(constantVector(d, 0.0), nA), repeatVector(constantVector(d, 10.0), nB)...)

	for _, variant := range kMeansVariants {
		t.Run(fmt.Sprintf("%v-%v", variant.Initialization, variant.Assignment), func(t *testing.T) {
			km := newDeterministicKMeans(2, d, variant)
			labels, err := km.FitBalanced(data)
			assert.NoError(t, err)

			counts := balancedClusterSizes(t, labels, 2)
			assert.ElementsMatch(t, []int{nA, nB}, counts,
				"a natural 70/30 split satisfies the floor and must be returned unchanged")

			for i := 1; i < nA; i++ {
				assert.Equal(t, labels[0], labels[i], "group A must stay together")
			}
			for i := nA + 1; i < nA+nB; i++ {
				assert.Equal(t, labels[nA], labels[i], "group B must stay together")
			}
			assert.NotEqual(t, labels[0], labels[nA], "the two groups must be separated")

			assertCentersMatchMembership(t, km, data, labels)
		})
	}
}

// When every vector is identical, all distance differences tie: the floor
// assigns exactly ceil(n/4) vectors to one side, deterministically in input
// order, with no vector dropped and no empty cluster.
func TestFitBalancedAllIdenticalDeterministic(t *testing.T) {
	d := 4
	n := 200
	floor := (n + 3) / 4
	data := repeatVector(constantVector(d, 0.5), n)

	for _, variant := range kMeansVariants {
		t.Run(fmt.Sprintf("%v-%v", variant.Initialization, variant.Assignment), func(t *testing.T) {
			km := newDeterministicKMeans(2, d, variant)
			labels, err := km.FitBalanced(data)
			assert.NoError(t, err)
			assert.Len(t, labels, n)

			counts := balancedClusterSizes(t, labels, 2)
			assert.Equal(t, floor, min(counts[0], counts[1]))

			// ties break by input order: the floor side is a contiguous prefix
			minority := labels[0]
			for i := range floor {
				assert.Equal(t, minority, labels[i], "floor side must be the input-order prefix")
			}
			for i := floor; i < n; i++ {
				assert.NotEqual(t, minority, labels[i])
			}
		})
	}
}

// Small odd sizes: the floor is ceil(n/4), mathematically at least 25%.
func TestFitBalancedSmallOddSizes(t *testing.T) {
	d := 4
	for _, n := range []int{2, 3, 5, 7, 9} {
		floor := (n + 3) / 4
		data := repeatVector(constantVector(d, 1.0), n)
		km := newDeterministicKMeans(2, d, kMeansVariants[0])
		labels, err := km.FitBalanced(data)
		assert.NoError(t, err)
		counts := balancedClusterSizes(t, labels, 2)
		assert.GreaterOrEqual(t, min(counts[0], counts[1]), floor, "n=%d", n)
		assert.LessOrEqual(t, max(counts[0], counts[1]), n-floor, "n=%d", n)
	}
}

// FitBalanced supports exactly two clusters; other K values must error rather
// than panic (the balancing logic is two-way by construction).
func TestFitBalancedRequiresTwoClusters(t *testing.T) {
	d := 4
	data := generateData(20, d, normal, seed)
	for _, k := range []int{1, 3, 4} {
		km := newDeterministicKMeans(k, d, kMeansVariants[0])
		_, err := km.FitBalanced(data)
		assert.ErrorContains(t, err, "exactly 2 clusters", "k=%d", k)
	}
}

// The metrics must describe the constrained algorithm: two distance
// computations per vector per assignment iteration, and WCSS computed from
// the assignment actually returned. When the run ends in a stable iteration
// (no changes), the recorded WCSS must equal the sum of squared distances of
// each vector to the center of its returned cluster.
func TestFitBalancedMetricsUseActualAssignment(t *testing.T) {
	d := 4
	nBlob := 198
	nOutliers := 2
	data := blobWithOutliers(nBlob, nOutliers, d, float32(1000), seed)
	n := nBlob + nOutliers

	km := newDeterministicKMeans(2, d, kMeansVariants[0])
	labels, err := km.FitBalanced(data)
	assert.NoError(t, err)

	// entry 0 is initialization; every assignment iteration computes both
	// distances for every vector
	for i := 1; i < len(km.Metrics.Computations); i++ {
		assert.Equal(t, 2*n, km.Metrics.Computations[i], "iteration %d", i)
	}

	if km.Metrics.Termination == ClusterStability && km.Metrics.Changes[len(km.Metrics.Changes)-1] == 0 {
		var expected float64
		for i, x := range data {
			var sum float64
			for j := range x {
				diff := float64(x[j] - km.Centers[labels[i]][j])
				sum += diff * diff
			}
			expected += sum
		}
		got := km.Metrics.WCSS[len(km.Metrics.WCSS)-1]
		assert.InEpsilon(t, expected, got, 1e-3,
			"WCSS must reflect the returned constrained assignment")
	}
}

// FitBalanced always runs at least one constrained assignment pass, even when
// the iteration threshold is consumed by initialization alone: the size floor
// and the centers-matching-membership guarantee both require the loop body to
// have executed.
func TestFitBalancedWithoutIterations(t *testing.T) {
	n := 100
	d := 4
	floor := (n + 3) / 4
	data := generateData(n, d, normal, seed)
	for _, variant := range kMeansVariants {
		for _, iterationThreshold := range []int{0, 1} {
			km := newDeterministicKMeans(2, d, variant)
			km.IterationThreshold = iterationThreshold
			labels, err := km.FitBalanced(data)
			assert.NoError(t, err)
			assert.Len(t, labels, n)
			assert.Equal(t, iterationThreshold+1, km.Metrics.Iterations,
				"exactly one constrained pass beyond the threshold")

			counts := balancedClusterSizes(t, labels, 2)
			assert.GreaterOrEqual(t, min(counts[0], counts[1]), floor)
			assert.LessOrEqual(t, max(counts[0], counts[1]), n-floor)
			assertCentersMatchMembership(t, km, data, labels)
		}
	}
}

// The assignment step in isolation: deltas[i] = d(i, left) - d(i, right).
func TestBalancedAssignment(t *testing.T) {
	cases := []struct {
		name     string
		deltas   []float32
		minCount int
		expected []uint32
	}{
		{
			name:     "natural split above floor is untouched",
			deltas:   []float32{-1, -2, 3, 4},
			minCount: 1,
			expected: []uint32{0, 0, 1, 1},
		},
		{
			name:     "natural split exactly at floor is untouched",
			deltas:   []float32{-1, -2, 3, 4},
			minCount: 2,
			expected: []uint32{0, 0, 1, 1},
		},
		{
			name:     "floor binds on the left",
			deltas:   []float32{1, 2, 3, 4},
			minCount: 1,
			expected: []uint32{0, 1, 1, 1},
		},
		{
			name:     "floor binds on the right",
			deltas:   []float32{-1, -2, -3, -4},
			minCount: 1,
			expected: []uint32{1, 0, 0, 0},
		},
		{
			name:     "forced movers are the lowest-regret vectors",
			deltas:   []float32{5, 1, 3, -2},
			minCount: 2,
			expected: []uint32{1, 0, 1, 0},
		},
		{
			name:     "ties break by input order",
			deltas:   []float32{0, 0, 0, 0},
			minCount: 1,
			expected: []uint32{0, 1, 1, 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := len(tc.deltas)
			labels := make([]uint32, n)
			offsets := make([]int, n)
			balancedAssignment(tc.deltas, tc.minCount, offsets, labels)
			assert.Equal(t, tc.expected, labels)
		})
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
