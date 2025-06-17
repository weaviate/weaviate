//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package compressionhelpers_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"gonum.org/v1/hdf5"
)

type ANNBenchDataDescriptor struct {
	Name     string
	Distance distancer.Provider
}

type ANNBenchData struct {
	Train     [][]float32
	Test      [][]float32
	Neighbors [][]uint64
	Distance  distancer.Provider
	Name      string
	Dimension int
}

// Consider randomly permuting the data..
func NewANNBenchData(dir string, name string, distance distancer.Provider) *ANNBenchData {
	path := dir + "/" + name + ".hdf5"
	file, _ := hdf5.OpenFile(path, hdf5.F_ACC_RDONLY)
	train := loadHdf5Float32(file, "train")
	test := loadHdf5Float32(file, "test")
	neighbors := loadHdf5Neighbors(file, "neighbors")

	// Perform the same normalization as the HNSW index does
	if distance.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		for i := range train {
			train[i] = distancer.Normalize(train[i])
		}
		for i := range test {
			test[i] = distancer.Normalize(test[i])
		}
	}

	file.Close()
	return &ANNBenchData{
		Train:     train,
		Test:      test,
		Neighbors: neighbors,
		Distance:  distance,
		Name:      name,
		Dimension: len(train[0]),
	}
}

func copyRandomSubset(vectors [][]float32, n int, seed uint64) [][]float32 {
	d := len(vectors[0])
	r := rand.New(rand.NewPCG(seed, 735755762))
	sample := make([][]float32, n)
	perm := r.Perm(len(vectors))[:n]
	for i, p := range perm {
		sample[i] = make([]float32, d)
		copy(sample[i], vectors[p])
	}
	return sample
}

type IdxDist struct {
	index    int
	distance float32
}

type SimplePriorityQueue struct {
	k   int
	knn []IdxDist
}

func NewSimplePriorityQueue(k int) *SimplePriorityQueue {
	pq := &SimplePriorityQueue{
		k:   k,
		knn: make([]IdxDist, k),
	}
	for i := range k {
		pq.knn[i].distance = math.MaxFloat32
	}
	return pq
}

func (pq *SimplePriorityQueue) Insert(idx int, dist float32) {
	if pq.knn[pq.k-1].distance < dist {
		return
	}
	pq.knn[pq.k-1] = IdxDist{index: idx, distance: dist}
	for j := pq.k - 1; j > 0 && pq.knn[j-1].distance > dist; j-- {
		pq.knn[j-1], pq.knn[j] = pq.knn[j], pq.knn[j-1]
	}
}

func (pq *SimplePriorityQueue) Neighbors() []int {
	neighbors := make([]int, pq.k)
	for i := range pq.knn {
		neighbors[i] = pq.knn[i].index
	}
	return neighbors
}

func (pq *SimplePriorityQueue) KNN() []IdxDist {
	return pq.knn
}

func (pq *SimplePriorityQueue) String() string {
	var sb strings.Builder
	for i, x := range pq.knn {
		sb.WriteString(fmt.Sprintf("%d: %v\n", i+1, x))
	}
	return sb.String()
}

// Product quantization

type PQSettings struct {
	Centroids     int
	SegmentLength int
	TrainingSize  int
}

func (s *PQSettings) BitsPerDimension() float64 {
	return math.Log2(float64(s.Centroids)) / float64(s.SegmentLength)
}

func (s *PQSettings) Description() string {
	return fmt.Sprintf("PQ(%d,%d)", int(math.Log2(float64(s.Centroids))), s.SegmentLength)
}

type PQNeighborProvider struct {
	quantizer     *compressionhelpers.ProductQuantizer
	quantizedData [][]byte
}

func NewPQNeighborProvider(data [][]float32, settings PQSettings, distance distancer.Provider) *PQNeighborProvider {
	dimension := len(data[0])
	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Centroids: settings.Centroids,
		Segments:  dimension / settings.SegmentLength,
	}
	quantizer, _ := compressionhelpers.NewProductQuantizer(
		cfg,
		distance,
		dimension,
		logger,
	)

	trainingSize := settings.TrainingSize
	if len(data) < trainingSize {
		trainingSize = len(data)
	}
	train := copyRandomSubset(data, trainingSize, 42)
	quantizer.Fit(train)

	// Encode the dataset.
	encoded := make([][]byte, len(data))
	for i := range data {
		encoded[i] = quantizer.Encode(data[i])
	}

	pq := &PQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: encoded,
	}
	return pq
}

func (pq *PQNeighborProvider) NearestNeighbors(query []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := pq.quantizer.NewDistancer(query)
	for i, c := range pq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of product quantization

// Scalar quantization

type SQSettings struct {
	TrainingSize int
}

func (s *SQSettings) BitsPerDimension() float64 {
	return 8.0
}

func (s *SQSettings) Description() string {
	return "SQ"
}

type SQNeighborProvider struct {
	quantizer     *compressionhelpers.ScalarQuantizer
	quantizedData [][]byte
}

func NewSQNeighborProvider(data [][]float32, settings SQSettings, distance distancer.Provider) *SQNeighborProvider {
	trainingSize := settings.TrainingSize
	if len(data) < trainingSize {
		trainingSize = len(data)
	}
	train := copyRandomSubset(data, trainingSize, 42)
	quantizer := compressionhelpers.NewScalarQuantizer(train, distance)

	quantizedData := make([][]byte, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
	}
	sq := &SQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: quantizedData,
	}
	return sq
}

func (sq *SQNeighborProvider) NearestNeighbors(query []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := sq.quantizer.NewDistancer(query)
	for i, c := range sq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of scalar quantization

// Binary quantization

type BQSettings struct{}

func (s *BQSettings) BitsPerDimension() float64 {
	return 1.0
}

func (s *BQSettings) Description() string {
	return "BQ"
}

type BQNeighborProvider struct {
	quantizer     *compressionhelpers.BinaryQuantizer
	quantizedData [][]uint64
}

func NewBQNeighborProvider(data [][]float32, settings BQSettings, distance distancer.Provider) *BQNeighborProvider {
	quantizer := compressionhelpers.NewBinaryQuantizer(distance)
	quantizedData := make([][]uint64, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
	}
	bq := &BQNeighborProvider{
		quantizer:     &quantizer,
		quantizedData: quantizedData,
	}
	return bq
}

func (bq *BQNeighborProvider) NearestNeighbors(query []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := bq.quantizer.NewDistancer(query)
	for i, c := range bq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of binary quantization.

// Rotational quantization

type RQSettings struct {
	Bits int
}

func (s *RQSettings) BitsPerDimension() float64 {
	return float64(s.Bits)
}

func (s *RQSettings) Description() string {
	return "RQ"
}

type RQNeighborProvider struct {
	quantizer     *compressionhelpers.RotationalQuantizer
	quantizedData [][]byte
	norms         []float32
	distance      distancer.Provider
	bits          int
}

func NewRQNeighborProvider(data [][]float32, settings RQSettings, distance distancer.Provider) *RQNeighborProvider {
	d := len(data[0])
	quantizer := compressionhelpers.NewRotationalQuantizer(d, 42, settings.Bits, distance)

	quantizedData := make([][]byte, len(data))
	norms := make([]float32, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
		var dot float32
		for j := range v {
			dot += v[j] * v[j]
		}
		norms[i] = float32(math.Sqrt(float64(dot)))
	}
	bq := &RQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: quantizedData,
		norms:         norms,
		distance:      distance,
		bits:          settings.Bits,
	}
	return bq
}

func (rq *RQNeighborProvider) NearestNeighbors(q []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := rq.quantizer.NewDistancer(q)
	for i, c := range rq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of rotational quantization

// Centered rotational quantization

type CRQSettings struct {
	TrainingSize int
	Centers      int
}

func (s *CRQSettings) BitsPerDimension() float64 {
	return float64(8)
}

func (s *CRQSettings) Description() string {
	return fmt.Sprintf("CRQ(%d)", s.Centers)
}

type CRQNeighborProvider struct {
	quantizer     *compressionhelpers.CenteredRotationalQuantizer
	quantizedData [][]byte
	distance      distancer.Provider
}

func NewCRQNeighborProvider(data [][]float32, settings CRQSettings, distance distancer.Provider) *CRQNeighborProvider {
	// Run k-means to find centers.
	d := len(data[0])
	trainingSize := settings.TrainingSize
	if len(data) < trainingSize {
		trainingSize = len(data)
	}
	train := copyRandomSubset(data, trainingSize, 42)

	km := kmeans.New(settings.Centers, d, 0)
	km.Fit(train)

	// Append origo
	origo := make([]float32, d)
	centers := make([][]float32, 1, settings.Centers+1)
	centers[0] = origo
	centers = append(centers, km.Centers...)

	quantizer := compressionhelpers.NewCenteredRotationalQuantizer(d, 42, distance, centers)

	quantizedData := make([][]byte, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
	}
	crq := &CRQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: quantizedData,
		distance:      distance,
	}
	return crq
}

func (rq *CRQNeighborProvider) NearestNeighbors(q []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := rq.quantizer.NewDistancer(q)
	for i, c := range rq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of centered rotational quantization

// Truncated rotational quantization

type TRQSettings struct {
	Order int
}

func (s *TRQSettings) BitsPerDimension() float64 {
	return float64(8)
}

func (s *TRQSettings) Description() string {
	return fmt.Sprintf("TRQ(%d)", s.Order)
}

type TRQNeighborProvider struct {
	quantizer     *compressionhelpers.TruncatedRotationalQuantizer
	quantizedData [][]byte
	distance      distancer.Provider
}

func NewTRQNeighborProvider(data [][]float32, settings TRQSettings, distance distancer.Provider) *TRQNeighborProvider {
	d := len(data[0])
	quantizer := compressionhelpers.NewTruncatedRotationalQuantizer(d, 42, distance, settings.Order)

	quantizedData := make([][]byte, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
	}
	trq := &TRQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: quantizedData,
		distance:      distance,
	}
	return trq
}

func (rq *TRQNeighborProvider) NearestNeighbors(q []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := rq.quantizer.NewDistancer(q)
	for i, c := range rq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of truncated rotational quantization

type NeighborProvider interface {
	NearestNeighbors(query []float32, k int) []int
}

type QuantizationSettings interface {
	BitsPerDimension() float64
	Description() string
}

func neighborProviderFactory(data [][]float32, distance distancer.Provider, settings QuantizationSettings) NeighborProvider {
	switch s := settings.(type) {
	case *PQSettings:
		return NewPQNeighborProvider(data, *s, distance)
	case *SQSettings:
		return NewSQNeighborProvider(data, *s, distance)
	case *BQSettings:
		return NewBQNeighborProvider(data, *s, distance)
	case *RQSettings:
		return NewRQNeighborProvider(data, *s, distance)
	case *CRQSettings:
		return NewCRQNeighborProvider(data, *s, distance)
	case *TRQSettings:
		return NewTRQNeighborProvider(data, *s, distance)
	default:
		return nil
	}
}

func bruteForceKNN(data [][]float32, query []float32, distancer distancer.Provider, k int) []int {
	queue := NewSimplePriorityQueue(k)
	for i, x := range data {
		dist, _ := distancer.SingleDist(query, x)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

func overlap(a []int, b []int) int {
	m := make(map[int]bool, len(a))
	for _, x := range a {
		m[x] = true
	}
	var count int
	for _, y := range b {
		if m[y] {
			count++
		}
	}
	return count
}

func BenchmarkQuantizationRecall(b *testing.B) {
	dataDir := "/Users/tobiaschristiani/code/datasets"
	datasets := []ANNBenchDataDescriptor{
		// {Name: "dbpedia-100k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "dbpedia-100k-openai-ada002-angular", Distance: distancer.NewCosineDistanceProvider()},
		// {Name: "dbpedia-100k-openai-3large-dot", Distance: distancer.NewDotProductProvider()},
		// {Name: "sift-128-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "gist-960-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "glove-200-angular", Distance: distancer.NewCosineDistanceProvider()},

		// Classic datasets
		{Name: "sift-128-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "gist-960-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "glove-200-angular", Distance: distancer.NewCosineDistanceProvider()},

		// Smaller OpenAI datasets
		// {Name: "dbpedia-100k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "dbpedia-100k-openai-ada002-angular", Distance: distancer.NewCosineDistanceProvider()},
		// {Name: "dbpedia-100k-openai-3large-dot", Distance: distancer.NewDotProductProvider()},

		// Bigger datasets
		{Name: "dbpedia-500k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "dbpedia-openai-1000k-angular", Distance: distancer.NewCosineDistanceProvider()},
		{Name: "sphere-1M-meta-dpr", Distance: distancer.NewDotProductProvider()},
		// {Name: "snowflake-msmarco-arctic-embed-m-v1.5-angular", Distance: distancer.NewCosineDistanceProvider()},
	}

	algorithms := []QuantizationSettings{
		&SQSettings{TrainingSize: 100_000},
		&RQSettings{Bits: 8},
		// &TRQSettings{Order: 1},
		// &TRQSettings{Order: 4},
		// &TRQSettings{Order: 8},
		&CRQSettings{TrainingSize: 100_000, Centers: 1},
		&CRQSettings{TrainingSize: 100_000, Centers: 16},
		&CRQSettings{TrainingSize: 100_000, Centers: 64},
	}

	maxVectors := 1_000_000
	maxQueries := 250

	for _, descriptor := range datasets {
		data := NewANNBenchData(dataDir, descriptor.Name, descriptor.Distance)
		n := min(len(data.Train), maxVectors)
		m := min(len(data.Test), maxQueries)
		train := copyRandomSubset(data.Train, n, 42)
		test := copyRandomSubset(data.Test, m, 42)
		k := 100

		kNN := make([][]int, m)
		for i, q := range test {
			kNN[i] = bruteForceKNN(train, q, descriptor.Distance, k)
		}

		for _, algorithm := range algorithms {
			b.Run(fmt.Sprintf("|%v|%v|", data.Name, algorithm.Description()), func(b *testing.B) {
				provider := neighborProviderFactory(train, descriptor.Distance, algorithm)
				var matches100At100 int
				var matches100At200 int
				for i := 0; i < b.N; i++ {
					for j, q := range test {
						neighbors := provider.NearestNeighbors(q, 200)
						matches100At100 += overlap(neighbors[:100], kNN[j])
						matches100At200 += overlap(neighbors[:200], kNN[j])
					}
				}
				b.ReportMetric(float64(matches100At100)/float64(k*m*b.N), "rec100@100")
				b.ReportMetric(float64(matches100At200)/float64(k*m*b.N), "rec100@200")
				b.ReportMetric(algorithm.BitsPerDimension(), "bits")
			})
		}

	}
}

func BenchmarkNorms(b *testing.B) {
	dataDir := "/Users/tobiaschristiani/code/datasets"
	datasets := []ANNBenchDataDescriptor{
		// // Smaller OpenAI datasets
		{Name: "dbpedia-100k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "dbpedia-100k-openai-ada002-angular", Distance: distancer.NewCosineDistanceProvider()},
		// {Name: "dbpedia-100k-openai-3large-dot", Distance: distancer.NewDotProductProvider()},

		// Classic datasets
		// {Name: "sift-128-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "gist-960-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "glove-200-angular", Distance: distancer.NewCosineDistanceProvider()},

		// // // Bigger datasets
		// {Name: "dbpedia-500k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "dbpedia-openai-1000k-angular", Distance: distancer.NewCosineDistanceProvider()},
		// {Name: "sphere-1M-meta-dpr", Distance: distancer.NewDotProductProvider()},
		//{Name: "snowflake-msmarco-arctic-embed-m-v1.5-angular", Distance: distancer.NewCosineDistanceProvider()},
	}

	// numCenters := []int{16, 64, 256} // The number of centers used for centering data points.
	// algorithms := []string{"kmeans", "kmeans++", "sampling"}
	numCenters := []int{1, 4, 16} // The number of centers used for centering data points.
	algorithms := []string{"sampling", "kmeans"}
	c := slices.Max(numCenters)
	m := 1000   // Number of data points where we will measure the norm before and after centering.
	t := 50_000 // Training size.
	var seed uint64 = 42

	for _, descriptor := range datasets {
		data := NewANNBenchData(dataDir, descriptor.Name, descriptor.Distance)
		sampleSize := t + m + c
		sample := copyRandomSubset(data.Train, sampleSize, seed)
		train, vectors, allCenters := sample[:t], sample[t:(t+m)], sample[(t+m):]
		d := len(train[0])
		for _, alg := range algorithms {
			for _, k := range numCenters {
				var centers [][]float32
				switch alg {
				case "kmeans", "kmeans++":
					km := kmeans.New(k, d, 0)
					if alg == "kmeans++" {
						km.Initialization = kmeans.PlusPlusInitialization
					}
					km.Fit(train)
					centers = km.Centers
				case "sampling":
					centers = allCenters[:k]
				}
				// Make sure that origo is in the list of unit centers. This is
				// important for sampling when the data is on the unit sphere as
				// otherwise we might see larger norms when centering because
				// data points are further from each other than they are from
				// origo.
				origo := make([]float32, len(centers[0]))
				centers = append(centers, origo)

				// Compute norms with and without centering.
				norms := make([]float64, len(vectors))
				centeredNorms := make([]float64, len(vectors))
				normRatios := make([]float64, len(vectors))
				for i, v := range vectors {
					centerIdx := bruteForceKNN(centers, v, distancer.NewL2SquaredProvider(), 1)[0]
					center := centers[centerIdx]
					cv := make([]float32, len(v))
					for j := range cv {
						cv[j] = v[j] - center[j]
					}
					norms[i] = norm(v)
					centeredNorms[i] = norm(cv)
					normRatios[i] = centeredNorms[i] / norms[i]
				}

				b.Run(fmt.Sprintf("%s-%s-%d", data.Name, alg, k), func(b *testing.B) {
					var something float64
					for b.Loop() {
						something += math.Pi
					}
					// reportSliceMetrics("norms", norms, b)
					// reportSliceMetrics("centered", centeredNorms, b)
					// reportSliceMetrics("ratios", normRatios, b)
					reportSliceMetrics("bits", ratiosToBitSavings(normRatios), b)
				})
			}
		}
	}
}

func ratiosToBitSavings(ratios []float64) []float64 {
	// The dot product estimation error from quantization is proportional to eps ~ ||x|| ||y|| / 2^B
	// ratios gives us the reduction in the norm of x, but we don't know the reduction in the norm of y.
	// We assume 75% because the distance computatons matter the most when q is close to x.
	queryReductionFactor := 1.0
	bitSavings := make([]float64, len(ratios))
	for i, r := range ratios {
		bitSavings[i] = (1 + queryReductionFactor) * math.Log2(1/r)
	}
	return bitSavings
}

type metrics struct {
	Min               float64
	Max               float64
	Average           float64
	StandardDeviation float64
}

func sliceMetrics(x []float64) metrics {
	min := math.MaxFloat64
	max := -math.MaxFloat64
	var avg float64
	for _, v := range x {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		avg += v
	}
	d := float64(len(x))
	avg = avg / d

	var dev2 float64
	for _, v := range x {
		dev2 = (v - avg) * (v - avg)
	}
	sd := math.Sqrt(dev2 / d)
	return metrics{Min: min, Max: max, Average: avg, StandardDeviation: sd}
}

func toFloat64(x []float32) []float64 {
	y := make([]float64, len(x))
	for i := range x {
		y[i] = float64(x[i])
	}
	return y
}

// min, max, average, standard deviation.
func reportSliceMetrics(name string, x []float64, b *testing.B) {
	m := sliceMetrics(x)

	b.ReportMetric(m.Min, name+"(a_min)")
	b.ReportMetric(m.Average, name+"(b_avg)")
	b.ReportMetric(m.Max, name+"(c_max)")
	b.ReportMetric(m.StandardDeviation, name+"(d_std)")
}

// The range max - min of the slice if we exlude the k largest and smallest entries.
func sliceRange(x []float64, k int) float64 {
	slices.Sort(x)
	return x[len(x)-k-1] - x[k]
}

// Try a number of rotations and pick the one with the smaller range of entries in order to reduce the quantization interval.
// We can get an idea about this by collecting information on the range for different independent rotations.
func BenchmarkRanges(b *testing.B) {
	dataDir := "/Users/tobiaschristiani/code/datasets"
	datasets := []ANNBenchDataDescriptor{
		// Classic datasets
		{Name: "sift-128-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "gist-960-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "glove-200-angular", Distance: distancer.NewCosineDistanceProvider()},

		// Smaller OpenAI datasets
		{Name: "dbpedia-100k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		{Name: "dbpedia-100k-openai-ada002-angular", Distance: distancer.NewCosineDistanceProvider()},
		{Name: "dbpedia-100k-openai-3large-dot", Distance: distancer.NewDotProductProvider()},

		// Bigger datasets
		// {Name: "dbpedia-500k-openai-ada002-euclidean", Distance: distancer.NewL2SquaredProvider()},
		// {Name: "dbpedia-openai-1000k-angular", Distance: distancer.NewCosineDistanceProvider()},
		{Name: "sphere-1M-meta-dpr", Distance: distancer.NewDotProductProvider()},
		// {Name: "snowflake-msmarco-arctic-embed-m-v1.5-angular", Distance: distancer.NewCosineDistanceProvider()},
	}

	numSamples := 1
	numRotations := 128
	rounds := []int{1}
	cutoff := []int{0, 1, 5} // remove smallest and largest entries from the rotated vector.
	rng := newRNG(1243)
	segmentLength := -1

	for _, descriptor := range datasets {
		data := NewANNBenchData(dataDir, descriptor.Name, descriptor.Distance)
		sample := copyRandomSubset(data.Train, numSamples, rng.Uint64())
		x := sample[0]
		d := len(x)
		if segmentLength > 0 {
			d = segmentLength
		}

		for _, r := range rounds {
			for _, k := range cutoff {
				ranges := make([]float64, numRotations)
				for i := range numRotations {
					rotation := compressionhelpers.NewFastRotation(d, r, rng.Uint64())
					rx := toFloat64(rotation.Rotate(x[:d]))
					ranges[i] = sliceRange(rx, k)
				}

				b.Run(fmt.Sprintf("%s-rounds-%d-cutoff-%d", data.Name, r, k), func(b *testing.B) {
					var something float64
					for b.Loop() {
						something += math.Pi
					}
					reportSliceMetrics("range", ranges, b)
				})
			}
		}
	}
}
