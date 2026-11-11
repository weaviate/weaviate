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

// Package emeans implements e-means, a mini-batch k-means variant that
// updates centroids with an exponential moving average of the per-batch sums
// and counts. It follows Algorithm 1 of "E-Means: Scaling Laws for Mini-Batch
// K-Means" (arXiv:2611.09501) and the NumPy reference implementation in
// faiss/contrib/e_means, including the fitted batch-size and step-size laws.
package emeans

import (
	"cmp"
	"errors"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
)

const (
	alphaMinFactor   = 0.1         // The cosine schedule ends at this fraction of the initial step.
	checkUnusedEvery = 5           // Number of batch updates between revival checks.
	initUsage        = 1.0 / 65536 // Initial EMA mass of every cluster.
	splitEpsilon     = 1e-4        // Relative coordinate perturbation applied when splitting a donor.
	unusedThreshold  = 0.1         // Clusters below this fraction of the mean mass are revived.
	noRevivalMaxDims = 2           // Revival hurts held-out error on data of at most this dimension.
	qualityMaxDims   = 16          // AutoBatchLaw picks the quality law at or below this dimension.
)

type BatchLaw string

const (
	AutoBatchLaw       BatchLaw = "Auto"       // Quality for low-dimensional data, throughput otherwise.
	ThroughputBatchLaw BatchLaw = "Throughput" // Larger batches, fewer centroid updates.
	QualityBatchLaw    BatchLaw = "Quality"    // Batches about 8 times smaller than throughput.
)

type InitializationStrategy string

const (
	RandomInitialization   InitializationStrategy = "Random"   // The initialization used in the paper.
	PlusPlusInitialization InitializationStrategy = "PlusPlus" // Costs about one extra epoch.
)

// AutoUnusedThreshold lets Fit pick the revival threshold from the dimension.
const AutoUnusedThreshold = -1

type Metrics struct {
	BatchSize    int       // Resolved batch size.
	Alpha0       float64   // Resolved initial EMA step.
	Steps        int       // Number of centroid updates.
	Splits       int       // Number of unused clusters revived by splitting a donor.
	Computations int       // Total number of distance computations.
	WCSS         []float64 // Per epoch sum of squared distances, measured against the moving centers.
}

type EMeans struct {
	K               int                    // How many centroids.
	Epochs          int                    // Number of passes over the data.
	BatchSize       int                    // Points per centroid update, zero derives it from BatchLaw.
	Alpha0          float64                // Initial EMA step in (0, 1], zero derives it from the step law.
	BatchLaw        BatchLaw               // Scaling law used to derive BatchSize.
	Initialization  InitializationStrategy // Algorithm used to initialize cluster centers.
	UnusedThreshold float64                // Revival threshold as a fraction of the mean cluster mass, zero disables revival.
	Seed            uint64                 // The seed for the RNG used during fitting.
	Metrics         Metrics                // Metrics for observability of the clustering algorithm.
	Centers         [][]float32            // The centers computed by Fit()
	// For each data point the cluster it was assigned to during the last epoch.
	// The centers moved during that epoch, so this is not guaranteed to be the
	// nearest of the returned centers.
	Assignments []uint32
	distance    distancer.Provider // The clustering algorithm is intended to work with L2 squared.
	dimensions  int                // Dimensions of the data.
	segment     int                // Segment where it operates.
}

func New(k int, dimensions int, segment int) *EMeans {
	return &EMeans{
		K:               k,
		Epochs:          5,
		BatchLaw:        AutoBatchLaw,
		Initialization:  RandomInitialization,
		UnusedThreshold: AutoUnusedThreshold,
		Seed:            rand.Uint64(),
		distance:        distancer.NewL2SquaredProvider(),
		dimensions:      dimensions,
		segment:         segment,
	}
}

func (m *EMeans) seg(x []float32) []float32 {
	return x[m.segment*m.dimensions : (m.segment+1)*m.dimensions]
}

func roundPowerOfTwo(value float64) int {
	if value <= 1 {
		return 1
	}
	low := math.Pow(2, math.Floor(math.Log2(value)))
	high := 2 * low
	if value/low < high/value {
		return int(low)
	}
	return int(high)
}

// batchLaw returns the batch size B = c * k^bk * n^bn * T^bT fitted in the
// paper. The quality law uses one eighth of the throughput coefficient.
func batchLaw(law BatchLaw, n int, k int, d int, epochs int) int {
	if law == AutoBatchLaw {
		law = ThroughputBatchLaw
		if d <= qualityMaxDims {
			law = QualityBatchLaw
		}
	}
	raw := math.Pow(float64(k), 0.134749) * math.Pow(float64(n), 0.876473) * math.Pow(float64(epochs), 0.813344)
	var batch int
	if law == QualityBatchLaw {
		batch = min(roundPowerOfTwo(0.00439175*raw), n/2)
	} else {
		batch = min(roundPowerOfTwo(0.035134*raw), roundPowerOfTwo(float64(n)/2))
	}
	return max(1, min(batch, n))
}

// stepLaw returns the initial EMA step fitted in the paper for a batch size.
func stepLaw(n int, k int, epochs int, batchSize int) float64 {
	alpha0 := 0.8244 * math.Pow(float64(k), -0.4617) * math.Pow(float64(batchSize), 0.9003) *
		math.Pow(float64(n), -0.4498) * math.Pow(float64(epochs), -0.1057)
	return min(max(alpha0, 1e-4), 1)
}

// scheduledAlpha anneals the EMA step with a cosine from alpha0 to a tenth of it.
func scheduledAlpha(alpha0 float64, step int, totalSteps int) float64 {
	fraction := float64(min(step, totalSteps)) / float64(max(1, totalSteps))
	cosine := 0.5 * (1 + math.Cos(math.Pi*fraction))
	return alpha0 * (alphaMinFactor + (1-alphaMinFactor)*cosine)
}

func (m *EMeans) validate(n int) error {
	if m.K < 1 {
		return errors.New("e-means requires at least one cluster")
	}
	if n < m.K {
		return errors.New("not enough data to fit e-means")
	}
	if m.Epochs < 1 {
		return errors.New("e-means requires at least one epoch")
	}
	if m.BatchSize < 0 {
		return errors.New("e-means batch size must not be negative")
	}
	if m.Alpha0 < 0 || m.Alpha0 > 1 || math.IsNaN(m.Alpha0) {
		return errors.New("e-means initial step must be in [0, 1]")
	}
	switch m.BatchLaw {
	case AutoBatchLaw, ThroughputBatchLaw, QualityBatchLaw:
	default:
		return errors.New("unknown e-means batch law")
	}
	switch m.Initialization {
	case RandomInitialization, PlusPlusInitialization:
	default:
		return errors.New("unknown e-means initialization strategy")
	}
	return nil
}

type candidate struct {
	index int
	key   float64
}

// state holds the temporary heap-allocated data used during fitting.
type state struct {
	usage      []float64   // EMA mass N_j of each cluster.
	sums       []float64   // Per batch sum of the points assigned to each cluster, k x d.
	counts     []uint32    // Per batch number of points assigned to each cluster.
	passSums   []float64   // Last epoch sum of the points assigned to each cluster, k x d.
	passCounts []uint64    // Last epoch number of points assigned to each cluster.
	order      []uint32    // Permutation of the data, reshuffled every epoch.
	donors     []candidate // Scratch space for sampling the clusters to split.
	unused     []int       // Scratch space for the clusters to revive.
	rng        *rand.Rand
}

func (m *EMeans) initialize(data [][]float32, s *state) {
	n := len(data)
	backing := make([]float32, m.K*m.dimensions)
	m.Centers = make([][]float32, m.K)
	for c := range m.K {
		m.Centers[c] = backing[c*m.dimensions : (c+1)*m.dimensions : (c+1)*m.dimensions]
	}
	s.order = make([]uint32, n)
	for i := range s.order {
		s.order[i] = uint32(i)
	}
	if m.Initialization == PlusPlusInitialization {
		m.initializePlusPlus(data, s)
		return
	}
	// A partial Fisher-Yates shuffle places a uniform random subset first.
	for c := range m.K {
		j := c + s.rng.IntN(n-c)
		s.order[c], s.order[j] = s.order[j], s.order[c]
		copy(m.Centers[c], m.seg(data[s.order[c]]))
	}
}

// initializePlusPlus picks each center among the data points with probability
// proportional to the squared distance to the nearest center picked so far.
func (m *EMeans) initializePlusPlus(data [][]float32, s *state) {
	n := len(data)
	copy(m.Centers[0], m.seg(data[s.rng.IntN(n)]))
	distances := make([]float32, n)
	for i := range distances {
		distances[i] = math.MaxFloat32
	}
	for c := range m.K - 1 {
		var total float64
		for i, x := range data {
			if dist, _ := m.distance.SingleDist(m.seg(x), m.Centers[c]); dist < distances[i] {
				distances[i] = dist
			}
			total += float64(distances[i])
		}
		// Falls back to the last point if rounding leaves the target unreached.
		idx := n - 1
		target := total * s.rng.Float64()
		var cumulative float64
		for i, dist := range distances {
			cumulative += float64(dist)
			if target < cumulative {
				idx = i
				break
			}
		}
		copy(m.Centers[c+1], m.seg(data[idx]))
	}
	m.Metrics.Computations += n * (m.K - 1)
}

// accumulate assigns a batch to the nearest centers and collects the sum and
// count of the points assigned to each cluster. It returns the batch's sum of
// squared distances.
func (m *EMeans) accumulate(data [][]float32, batch []uint32, s *state, capture bool) float64 {
	clear(s.sums)
	clear(s.counts)
	d := m.dimensions
	var wcss float64
	for _, i := range batch {
		x := m.seg(data[i])
		var minDist float32 = math.MaxFloat32
		var idx int
		for c := range m.Centers {
			if dist, _ := m.distance.SingleDist(x, m.Centers[c]); dist < minDist {
				minDist = dist
				idx = c
			}
		}
		wcss += float64(minDist)
		s.counts[idx]++
		sum := s.sums[idx*d : (idx+1)*d]
		for j, z := range x {
			sum[j] += float64(z)
		}
		if capture {
			m.Assignments[i] = uint32(idx)
		}
	}
	m.Metrics.Computations += len(batch) * m.K
	return wcss
}

// update applies the EMA to the mass and sum of each cluster, expressed in
// terms of the mass N_j and the center c_j = S_j / N_j:
//
//	N_new = (1 - alpha) * N_j + alpha * n_j
//	c_new = c_j + (alpha * n_j / N_new) * (s_j / n_j - c_j)
//
// Clusters without points in the batch keep their position but lose mass.
func (m *EMeans) update(s *state, alpha float64) {
	d := m.dimensions
	for c := range m.K {
		s.usage[c] *= 1 - alpha
		if s.counts[c] == 0 {
			continue
		}
		count := float64(s.counts[c])
		mass := s.usage[c] + alpha*count
		weight := alpha * count / mass
		sum := s.sums[c*d : (c+1)*d]
		center := m.Centers[c]
		for j := range center {
			mean := sum[j] / count
			center[j] = float32(float64(center[j]) + weight*(mean-float64(center[j])))
		}
		s.usage[c] = mass
	}
}

// revive replaces clusters whose mass fell below the threshold by splitting
// donors sampled without replacement in proportion to their mass. The donor
// and its child share the donor's mass and sit on either side of its center.
func (m *EMeans) revive(s *state, threshold float64) {
	if threshold <= 0 {
		return
	}
	var total float64
	for _, u := range s.usage {
		total += u
	}
	limit := threshold * total / float64(m.K)

	s.unused = s.unused[:0]
	s.donors = s.donors[:0]
	for c, u := range s.usage {
		if u < limit {
			s.unused = append(s.unused, c)
		} else if u > 0 {
			// Gumbel top-k trick: the largest keys form a sample without
			// replacement with probability proportional to the mass.
			uniform := max(s.rng.Float64(), math.SmallestNonzeroFloat64)
			s.donors = append(s.donors, candidate{index: c, key: math.Log(u) - math.Log(-math.Log(uniform))})
		}
	}
	splits := min(len(s.unused), len(s.donors))
	if splits == 0 {
		return
	}
	slices.SortFunc(s.donors, func(a, b candidate) int {
		return cmp.Compare(b.key, a.key)
	})

	for i := range splits {
		donor, child := m.Centers[s.donors[i].index], m.Centers[s.unused[i]]
		offset := float32(splitEpsilon)
		for j, z := range donor {
			donor[j] = z * (1 - offset)
			child[j] = z * (1 + offset)
			offset = -offset
		}
		if slices.Equal(donor, child) {
			// A center at the origin cannot be separated by scaling.
			child[0] = math.Nextafter32(child[0], float32(math.Inf(1)))
		}
		s.usage[s.donors[i].index] /= 2
		s.usage[s.unused[i]] = s.usage[s.donors[i].index]
	}
	m.Metrics.Splits += splits
}

// finalize replaces every center by the exact mean of the points assigned to
// it during the last epoch. Centers without points keep their position.
func (m *EMeans) finalize(s *state) {
	d := m.dimensions
	for c := range m.K {
		if s.passCounts[c] == 0 {
			continue
		}
		for j := range d {
			m.Centers[c][j] = float32(s.passSums[c*d+j] / float64(s.passCounts[c]))
		}
	}
}

// Fit runs e-means clustering on the data according to the settings on the
// EMeans struct. After running Fit() the resulting cluster centers can be
// accessed through Centers.
func (m *EMeans) Fit(data [][]float32) error {
	n := len(data)
	if err := m.validate(n); err != nil {
		return err
	}

	batchSize := min(m.BatchSize, n)
	if batchSize == 0 {
		batchSize = batchLaw(m.BatchLaw, n, m.K, m.dimensions, m.Epochs)
	}
	alpha0 := m.Alpha0
	if alpha0 == 0 {
		alpha0 = stepLaw(n, m.K, m.Epochs, batchSize)
	}
	threshold := m.UnusedThreshold
	if threshold < 0 {
		threshold = unusedThreshold
		if m.dimensions <= noRevivalMaxDims {
			threshold = 0
		}
	}
	stepsPerEpoch := (n + batchSize - 1) / batchSize
	totalSteps := stepsPerEpoch * m.Epochs

	s := &state{
		usage:      make([]float64, m.K),
		sums:       make([]float64, m.K*m.dimensions),
		counts:     make([]uint32, m.K),
		passSums:   make([]float64, m.K*m.dimensions),
		passCounts: make([]uint64, m.K),
		rng:        rand.New(rand.NewPCG(m.Seed, 0x9c1f6e0b7a5d3e21)),
	}
	for c := range s.usage {
		s.usage[c] = initUsage
	}
	m.Metrics = Metrics{BatchSize: batchSize, Alpha0: alpha0}
	m.Assignments = make([]uint32, n)
	m.initialize(data, s)

	for epoch := range m.Epochs {
		s.rng.Shuffle(n, func(i, j int) {
			s.order[i], s.order[j] = s.order[j], s.order[i]
		})
		last := epoch == m.Epochs-1
		var wcss float64
		for begin := 0; begin < n; begin += batchSize {
			batch := s.order[begin:min(begin+batchSize, n)]
			wcss += m.accumulate(data, batch, s, last)
			if last {
				for c, count := range s.counts {
					s.passCounts[c] += uint64(count)
				}
				for j, sum := range s.sums {
					s.passSums[j] += sum
				}
			}
			m.update(s, scheduledAlpha(alpha0, m.Metrics.Steps, totalSteps))
			m.Metrics.Steps++
			if m.Metrics.Steps%checkUnusedEvery == 0 {
				m.revive(s, threshold)
			}
		}
		m.Metrics.WCSS = append(m.Metrics.WCSS, wcss)
	}
	m.finalize(s)
	return nil
}

// FitBalanced clusters the data into exactly two groups whose sizes both hold
// at least ceil(n/4) of the points, matching the contract of
// kmeans.FitBalanced: the returned centers are the means of the returned
// membership, and the membership is not guaranteed to be nearest-centroid.
//
// Unlike kmeans.FitBalanced the size constraint is only applied once, to the
// centers of an unconstrained e-means run.
func (m *EMeans) FitBalanced(data [][]float32) ([]uint32, error) {
	if m.K != 2 {
		return nil, errors.New("balanced e-means requires exactly 2 clusters")
	}
	if err := m.Fit(data); err != nil {
		return nil, err
	}

	n := len(data)
	deltas := make([]float32, n)
	for i, x := range data {
		p := m.seg(x)
		d0, _ := m.distance.SingleDist(p, m.Centers[0])
		d1, _ := m.distance.SingleDist(p, m.Centers[1])
		deltas[i] = d0 - d1
	}
	m.Metrics.Computations += 2 * n
	kmeans.BalancedAssignment(deltas, (n+3)/4, make([]int, n), m.Assignments)

	sums := make([]float64, 2*m.dimensions)
	var counts [2]int
	for i, x := range data {
		c := int(m.Assignments[i])
		counts[c]++
		sum := sums[c*m.dimensions : (c+1)*m.dimensions]
		for j, z := range m.seg(x) {
			sum[j] += float64(z)
		}
	}
	for c := range 2 {
		for j := range m.dimensions {
			m.Centers[c][j] = float32(sums[c*m.dimensions+j] / float64(counts[c]))
		}
	}
	return m.Assignments, nil
}
