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

package emeans

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
)

const seed uint64 = 0x6a62976fa43149c9

func normalData(n int, d int, seed uint64) [][]float32 {
	r := rand.New(rand.NewPCG(seed, 8523456))
	data := make([][]float32, n)
	for i := range n {
		data[i] = make([]float32, d)
		for j := range d {
			data[i][j] = float32(r.NormFloat64())
		}
	}
	return data
}

// blobs generates well-separated unit-variance clusters and their true centers.
func blobs(n int, d int, k int, seed uint64) ([][]float32, [][]float32) {
	r := rand.New(rand.NewPCG(seed, 8523456))
	centers := make([][]float32, k)
	for c := range k {
		centers[c] = make([]float32, d)
		for j := range d {
			centers[c][j] = float32(100 * r.NormFloat64())
		}
	}
	data := make([][]float32, n)
	for i := range n {
		data[i] = make([]float32, d)
		for j := range d {
			data[i][j] = centers[i%k][j] + float32(r.NormFloat64())
		}
	}
	return data, centers
}

func l2squared(x []float32, y []float32) float64 {
	var sum float64
	for i := range x {
		diff := float64(x[i]) - float64(y[i])
		sum += diff * diff
	}
	return sum
}

func nearest(x []float32, centers [][]float32) (int, float64) {
	minDist, idx := math.MaxFloat64, 0
	for c := range centers {
		if dist := l2squared(x, centers[c]); dist < minDist {
			minDist, idx = dist, c
		}
	}
	return idx, minDist
}

func wcss(data [][]float32, centers [][]float32) float64 {
	var sum float64
	for _, x := range data {
		_, dist := nearest(x, centers)
		sum += dist
	}
	return sum
}

func newDeterministic(k int, d int) *EMeans {
	em := New(k, d, 0)
	em.Seed = seed
	return em
}

// Expected values are produced by faiss/contrib/e_means (_resolve_recipe).
func TestRecipeMatchesReference(t *testing.T) {
	cases := []struct {
		n, k, d, epochs int
		batchSize       int
		alpha0          float64
	}{
		{n: 100_000, k: 256, d: 8, epochs: 5, batchSize: 1024, alpha0: 0.15543381717430582},
		{n: 100_000, k: 256, d: 128, epochs: 5, batchSize: 8192, alpha0: 1},
		{n: 1_000_000, k: 1024, d: 128, epochs: 3, batchSize: 32768, alpha0: 0.6955211692520258},
		{n: 1000, k: 256, d: 8, epochs: 1, batchSize: 4, alpha0: 0.009928604274472001},
		{n: 300, k: 256, d: 32, epochs: 2, batchSize: 16, alpha0: 0.05524523185722609},
		{n: 2, k: 2, d: 4, epochs: 1, batchSize: 1, alpha0: 0.43827746604711676},
		{n: 10_000_000, k: 65536, d: 96, epochs: 5, batchSize: 1048576, alpha0: 0.7766657819733768},
		{n: 100_000, k: 2, d: 1536, epochs: 10, batchSize: 8192, alpha0: 1},
	}
	for _, tc := range cases {
		batchSize := batchLaw(AutoBatchLaw, tc.n, tc.k, tc.d, tc.epochs)
		assert.Equal(t, tc.batchSize, batchSize, "%+v", tc)
		assert.InEpsilon(t, tc.alpha0, stepLaw(tc.n, tc.k, tc.epochs, batchSize), 1e-9, "%+v", tc)
	}
}

func TestBatchLawSelection(t *testing.T) {
	n, k, epochs := 100_000, 256, 5
	quality := batchLaw(QualityBatchLaw, n, k, 128, epochs)
	throughput := batchLaw(ThroughputBatchLaw, n, k, 8, epochs)
	assert.Equal(t, 1024, quality)
	assert.Equal(t, 8192, throughput)
	assert.Equal(t, quality, batchLaw(AutoBatchLaw, n, k, qualityMaxDims, epochs))
	assert.Equal(t, throughput, batchLaw(AutoBatchLaw, n, k, qualityMaxDims+1, epochs))
}

func TestRoundPowerOfTwo(t *testing.T) {
	values := []float64{0.5, 1, 3, 2.9, 2.8, 5.6, 5.7, 6, 1000, 1500}
	expected := []int{1, 1, 4, 4, 2, 4, 8, 8, 1024, 2048}
	for i, value := range values {
		assert.Equal(t, expected[i], roundPowerOfTwo(value), "value %v", value)
	}
}

func TestScheduledAlpha(t *testing.T) {
	steps := []int{0, 5, 10, 12}
	expected := []float64{0.5, 0.275, 0.05, 0.05}
	for i, step := range steps {
		assert.InDelta(t, expected[i], scheduledAlpha(0.5, step, 10), 1e-12, "step %d", step)
	}
}

func TestValidation(t *testing.T) {
	data := normalData(100, 4, seed)
	cases := []struct {
		name   string
		mutate func(em *EMeans)
	}{
		{"zero clusters", func(em *EMeans) { em.K = 0 }},
		{"more clusters than points", func(em *EMeans) { em.K = 101 }},
		{"zero epochs", func(em *EMeans) { em.Epochs = 0 }},
		{"negative batch size", func(em *EMeans) { em.BatchSize = -1 }},
		{"negative step", func(em *EMeans) { em.Alpha0 = -0.1 }},
		{"step above one", func(em *EMeans) { em.Alpha0 = 1.1 }},
		{"NaN step", func(em *EMeans) { em.Alpha0 = math.NaN() }},
		{"unknown batch law", func(em *EMeans) { em.BatchLaw = "Fastest" }},
		{"unknown initialization", func(em *EMeans) { em.Initialization = "Furthest" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			em := newDeterministic(8, 4)
			tc.mutate(em)
			assert.Error(t, em.Fit(data))
		})
	}
}

func TestDeterministicForSeed(t *testing.T) {
	data := normalData(5000, 8, seed)
	a, b := newDeterministic(64, 8), newDeterministic(64, 8)
	require.NoError(t, a.Fit(data))
	require.NoError(t, b.Fit(data))
	assert.Equal(t, a.Centers, b.Centers)
	assert.Equal(t, a.Assignments, b.Assignments)

	c := newDeterministic(64, 8)
	c.Seed++
	require.NoError(t, c.Fit(data))
	assert.NotEqual(t, a.Centers, c.Centers)
}

func TestMetrics(t *testing.T) {
	n, k, d := 5000, 16, 8
	data := normalData(n, d, seed)
	cases := []struct {
		name      string
		epochs    int
		batchSize int
		steps     int
	}{
		{"batch divides n", 3, 500, 30},
		{"remainder batch is kept", 3, 512, 30},
		{"batch larger than n", 2, 2 * n, 2},
		{"single point batches", 1, 1, n},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			em := newDeterministic(k, d)
			em.Epochs = tc.epochs
			em.BatchSize = tc.batchSize
			require.NoError(t, em.Fit(data))
			assert.Equal(t, tc.steps, em.Metrics.Steps)
			assert.Equal(t, min(tc.batchSize, n), em.Metrics.BatchSize)
			assert.Equal(t, tc.epochs*n*k, em.Metrics.Computations)
			assert.Len(t, em.Metrics.WCSS, tc.epochs)
		})
	}
}

// The moving-center objective of an epoch upper bounds nothing in general,
// but on a static dataset it should come down as the centers settle.
func TestDecreasingWCSS(t *testing.T) {
	data := normalData(20_000, 16, seed)
	em := newDeterministic(64, 16)
	em.Epochs = 5
	require.NoError(t, em.Fit(data))
	for i := 1; i < len(em.Metrics.WCSS); i++ {
		assert.Less(t, em.Metrics.WCSS[i], em.Metrics.WCSS[i-1], "epoch %d", i)
	}
	assert.Less(t, wcss(data, em.Centers), em.Metrics.WCSS[len(em.Metrics.WCSS)-1])
}

// The returned centers must be the exact means of the returned assignments,
// for every batch size including the full-batch Lloyd special case.
func TestCentersAreMeansOfAssignments(t *testing.T) {
	n, k, d := 3000, 32, 6
	data := normalData(n, d, seed)
	for _, batchSize := range []int{0, 1, 7, 100, n} {
		em := newDeterministic(k, d)
		em.BatchSize = batchSize
		require.NoError(t, em.Fit(data))

		sums := make([][]float64, k)
		for c := range sums {
			sums[c] = make([]float64, d)
		}
		counts := make([]int, k)
		for i, x := range data {
			c := em.Assignments[i]
			require.Less(t, int(c), k)
			counts[c]++
			for j := range x {
				sums[c][j] += float64(x[j])
			}
		}
		for c := range k {
			require.NotZero(t, counts[c], "batch size %d cluster %d", batchSize, c)
			for j := range d {
				assert.InDelta(t, sums[c][j]/float64(counts[c]), em.Centers[c][j], 1e-5)
			}
		}
	}
}

// Random initialization rarely places one center in every blob, and like
// Lloyd's algorithm e-means does not move centers between distant blobs. With
// k-means++ initialization it has to find the true clusters.
func TestRecoversSeparatedClusters(t *testing.T) {
	for _, tc := range []struct{ n, d, k int }{
		{5000, 3, 8},
		{5000, 8, 16},
		{20_000, 32, 64},
		{20_000, 128, 32},
	} {
		data, truth := blobs(tc.n, tc.d, tc.k, seed)
		em := newDeterministic(tc.k, tc.d)
		em.Initialization = PlusPlusInitialization
		require.NoError(t, em.Fit(data))
		assert.InEpsilon(t, wcss(data, truth), wcss(data, em.Centers), 0.01, "%+v", tc)

		km := kmeans.New(tc.k, tc.d, 0)
		km.Seed = seed
		require.NoError(t, km.Fit(data))
		assert.InEpsilon(t, wcss(data, km.Centers), wcss(data, em.Centers), 0.01, "%+v", tc)
	}
}

func TestPlusPlusInitializationWithDuplicates(t *testing.T) {
	data := make([][]float32, 500)
	for i := range data {
		data[i] = []float32{float32(i % 3), 1}
	}
	em := newDeterministic(8, 2)
	em.Initialization = PlusPlusInitialization
	require.NoError(t, em.Fit(data))
	assertFinite(t, em.Centers)
	assert.InDelta(t, 0, wcss(data, em.Centers), 1e-9)
}

func TestCorrectnessAcrossSegments(t *testing.T) {
	n, d, k, segments := 2000, 4, 16, 3
	data := normalData(n, d*segments, seed)
	for segment := range segments {
		projected := make([][]float32, n)
		for i := range data {
			projected[i] = data[i][segment*d : (segment+1)*d]
		}
		whole := New(k, d, segment)
		whole.Seed = seed
		require.NoError(t, whole.Fit(data))
		part := newDeterministic(k, d)
		require.NoError(t, part.Fit(projected))
		assert.Equal(t, part.Centers, whole.Centers)
		assert.Len(t, whole.Centers[0], d)
	}
}

func TestFewDataPoints(t *testing.T) {
	for _, n := range []int{1, 2, 3, 17, 300} {
		data := normalData(n, 4, seed)
		em := newDeterministic(n, 4)
		require.NoError(t, em.Fit(data))
		assertFinite(t, em.Centers)
		// Revival runs before every cluster has seen a batch, so e-means does
		// not keep every point as its own center the way Lloyd's algorithm
		// does. The reference implementation behaves the same way.
		assert.Less(t, wcss(data, em.Centers), wcss(data, [][]float32{make([]float32, 4)}), "n = %d", n)
	}
}

func TestOneCenter(t *testing.T) {
	n, d := 1000, 5
	data := normalData(n, d, seed)
	mean := make([]float64, d)
	for _, x := range data {
		for j := range x {
			mean[j] += float64(x[j]) / float64(n)
		}
	}
	for _, epochs := range []int{1, 5} {
		em := newDeterministic(1, d)
		em.Epochs = epochs
		require.NoError(t, em.Fit(data))
		for j := range d {
			assert.InDelta(t, mean[j], em.Centers[0][j], 1e-5)
		}
	}
}

func assertFinite(t *testing.T, centers [][]float32) {
	t.Helper()
	for c := range centers {
		for _, z := range centers[c] {
			require.False(t, math.IsNaN(float64(z)) || math.IsInf(float64(z), 0), "center %d", c)
		}
	}
}

func countDistinct(centers [][]float32) int {
	distinct := make(map[string]struct{}, len(centers))
	for _, center := range centers {
		key := make([]byte, 0, 4*len(center))
		for _, z := range center {
			bits := math.Float32bits(z)
			key = append(key, byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24))
		}
		distinct[string(key)] = struct{}{}
	}
	return len(distinct)
}

// Duplicated data leaves most of the randomly initialized centers without
// points. Revival has to split the used clusters until every center is used.
func TestRevivalWithDuplicates(t *testing.T) {
	cases := []struct {
		name     string
		distinct int
		value    func(i int, j int) float32
	}{
		{"all points identical", 1, func(i, j int) float32 { return 3 }},
		{"all points at the origin", 1, func(i, j int) float32 { return 0 }},
		{"few distinct points", 4, func(i, j int) float32 { return float32(i%4*10 + j) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, d, k := 4000, 4, 16
			data := make([][]float32, n)
			for i := range data {
				data[i] = make([]float32, d)
				for j := range d {
					data[i][j] = tc.value(i, j)
				}
			}
			em := newDeterministic(k, d)
			require.NoError(t, em.Fit(data))
			assertFinite(t, em.Centers)
			assert.NotZero(t, em.Metrics.Splits)
			assert.InDelta(t, 0, wcss(data, em.Centers), 1e-3)

			disabled := newDeterministic(k, d)
			disabled.UnusedThreshold = 0
			require.NoError(t, disabled.Fit(data))
			assertFinite(t, disabled.Centers)
			assert.Zero(t, disabled.Metrics.Splits)
		})
	}
}

// Half of the centers start on a far-away duplicated point and are starved.
// Revival must move them to where the data is, which lowers the objective.
func TestRevivalImprovesStarvedInitialization(t *testing.T) {
	n, d, k := 20_000, 8, 64
	data := normalData(n, d, seed)
	for i := 0; i < n; i += 2 {
		for j := range d {
			data[i][j] = 1000
		}
	}
	revived := newDeterministic(k, d)
	require.NoError(t, revived.Fit(data))
	starved := newDeterministic(k, d)
	starved.UnusedThreshold = 0
	require.NoError(t, starved.Fit(data))

	assert.NotZero(t, revived.Metrics.Splits)
	assert.Less(t, wcss(data, revived.Centers), 0.9*wcss(data, starved.Centers))
	assert.Greater(t, countDistinct(revived.Centers), countDistinct(starved.Centers))
}

func TestAutoRevivalThreshold(t *testing.T) {
	cases := []struct {
		d      int
		splits bool
	}{
		{noRevivalMaxDims, false},
		{noRevivalMaxDims + 1, true},
	}
	for _, tc := range cases {
		data := make([][]float32, 2000)
		for i := range data {
			data[i] = make([]float32, tc.d)
		}
		em := newDeterministic(8, tc.d)
		require.NoError(t, em.Fit(data))
		assert.Equal(t, tc.splits, em.Metrics.Splits > 0, "d = %d", tc.d)
	}
}

// A full batch with a unit step is one iteration of Lloyd's algorithm.
func TestFullBatchUnitStepIsLloyd(t *testing.T) {
	n, d, k := 4000, 8, 16
	data := normalData(n, d, seed)
	em := newDeterministic(k, d)
	em.BatchSize = n
	em.Alpha0 = 1
	em.UnusedThreshold = 0
	em.Epochs = 1
	require.NoError(t, em.Fit(data))
	first := wcss(data, em.Centers)

	em.Epochs = 8
	require.NoError(t, em.Fit(data))
	assert.Less(t, wcss(data, em.Centers), first)
	// Lloyd's objective is non-increasing between epochs. The cosine schedule
	// lowers the step below one after the first epoch, which keeps that
	// property as every center stays a convex combination of means.
	for i := 1; i < len(em.Metrics.WCSS); i++ {
		assert.LessOrEqual(t, em.Metrics.WCSS[i], em.Metrics.WCSS[i-1], "epoch %d", i)
	}
}

func TestExtremeValuesStayFinite(t *testing.T) {
	cases := []struct {
		name  string
		scale float32
	}{
		{"tiny", 1e-30},
		{"large", 1e15},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := normalData(2000, 8, seed)
			for i := range data {
				for j := range data[i] {
					data[i][j] *= tc.scale
				}
			}
			em := newDeterministic(16, 8)
			require.NoError(t, em.Fit(data))
			assertFinite(t, em.Centers)
		})
	}
}

func assertBalancedContract(t *testing.T, em *EMeans, data [][]float32, labels []uint32) {
	t.Helper()
	n, d := len(data), len(data[0])
	require.Len(t, labels, n)
	sums := [2][]float64{make([]float64, d), make([]float64, d)}
	var counts [2]int
	for i, x := range data {
		require.Less(t, labels[i], uint32(2))
		counts[labels[i]]++
		for j := range x {
			sums[labels[i]][j] += float64(x[j])
		}
	}
	for c := range 2 {
		assert.GreaterOrEqual(t, counts[c], (n+3)/4, "cluster %d", c)
		for j := range d {
			assert.InDelta(t, sums[c][j]/float64(counts[c]), em.Centers[c][j], 1e-3, "cluster %d", c)
		}
	}
	assertFinite(t, em.Centers)
}

func TestFitBalanced(t *testing.T) {
	outliers := normalData(1000, 8, seed)
	for i := range 3 {
		for j := range outliers[i] {
			outliers[i][j] += 1000
		}
	}
	separated, _ := blobs(1000, 8, 2, seed)
	identical := make([][]float32, 500)
	for i := range identical {
		identical[i] = []float32{1, 2, 3, 4}
	}
	cases := []struct {
		name string
		data [][]float32
	}{
		{"normal", normalData(2000, 16, seed)},
		{"blob with outliers", outliers},
		{"two separated blobs", separated},
		{"all identical", identical},
		{"all at the origin", make2D(500, 4)},
		{"two points", normalData(2, 4, seed)},
		{"three points", normalData(3, 4, seed)},
		{"five points", normalData(5, 4, seed)},
		{"seven points", normalData(7, 4, seed)},
	}
	for _, tc := range cases {
		for _, epochs := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/%d epochs", tc.name, epochs), func(t *testing.T) {
				em := newDeterministic(2, len(tc.data[0]))
				em.Epochs = epochs
				labels, err := em.FitBalanced(tc.data)
				require.NoError(t, err)
				assertBalancedContract(t, em, tc.data, labels)
			})
		}
	}
}

func make2D(n int, d int) [][]float32 {
	data := make([][]float32, n)
	for i := range data {
		data[i] = make([]float32, d)
	}
	return data
}

// Separated blobs satisfy the size floor naturally, so the balanced result
// has to be the true clustering.
func TestFitBalancedNaturalSplitUntouched(t *testing.T) {
	data, _ := blobs(1000, 8, 2, seed)
	em := newDeterministic(2, 8)
	labels, err := em.FitBalanced(data)
	require.NoError(t, err)
	for i := range data {
		assert.Equal(t, labels[i%2], labels[i])
	}
	assert.NotEqual(t, labels[0], labels[1])
}

func TestFitBalancedRejectsOtherClusterCounts(t *testing.T) {
	data := normalData(100, 4, seed)
	for _, k := range []int{1, 3} {
		_, err := newDeterministic(k, 4).FitBalanced(data)
		assert.Error(t, err, "k = %d", k)
	}
	_, err := newDeterministic(2, 4).FitBalanced(data[:1])
	assert.Error(t, err)
}
