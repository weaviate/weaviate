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
	"strings"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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
	BitsPerEntry  int
}

func (s *PQSettings) BitsPerDimension() float64 {
	return math.Log2(float64(s.Centroids)) / float64(s.SegmentLength)
}

func (s *PQSettings) Description() string {
	return fmt.Sprintf("PQ(k=%d,d=%d,bits=%d)", s.Centroids, s.SegmentLength, s.BitsPerEntry)
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
	return fmt.Sprintf("RQ(%d)", s.Bits)
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

type BRQSettings struct{}

func (s *BRQSettings) BitsPerDimension() float64 {
	return 1.0
}

func (s *BRQSettings) Description() string {
	return "BRQ"
}

type BRQNeighborProvider struct {
	quantizer     *compressionhelpers.BinaryRotationalQuantizer
	quantizedData [][]uint64
	distance      distancer.Provider
}

func NewBRQNeighborProvider(data [][]float32, settings BRQSettings, distance distancer.Provider) *BRQNeighborProvider {
	d := len(data[0])
	quantizer := compressionhelpers.NewBinaryRotationalQuantizer(d, 42, distance)

	quantizedData := make([][]uint64, len(data))
	for i, v := range data {
		quantizedData[i] = quantizer.Encode(v)
	}
	bq := &BRQNeighborProvider{
		quantizer:     quantizer,
		quantizedData: quantizedData,
		distance:      distance,
	}
	return bq
}

func (rq *BRQNeighborProvider) NearestNeighbors(q []float32, k int) []int {
	queue := NewSimplePriorityQueue(k)
	distancer := rq.quantizer.NewDistancer(q)
	for i, c := range rq.quantizedData {
		dist, _ := distancer.Distance(c)
		queue.Insert(i, dist)
	}
	return queue.Neighbors()
}

// End of binary rotational quantization

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
	case *BRQSettings:
		return NewBRQNeighborProvider(data, *s, distance)
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
		//{Name: "sphere-1M-meta-dpr", Distance: distancer.NewDotProductProvider()},
		// {Name: "snowflake-msmarco-arctic-embed-m-v1.5-angular", Distance: distancer.NewCosineDistanceProvider()},
	}

	algorithms := []QuantizationSettings{
		&BQSettings{},
		&BRQSettings{},
		&PQSettings{SegmentLength: 8, Centroids: 256, TrainingSize: 10_000},
	}

	maxVectors := 100_000
	maxQueries := 100

	for _, descriptor := range datasets {
		data := NewANNBenchData(dataDir, descriptor.Name, descriptor.Distance)
		n := min(len(data.Train), maxVectors)
		m := min(len(data.Test), maxQueries)
		train := copyRandomSubset(data.Train, n, 42)
		test := copyRandomSubset(data.Test, m, 42)
		k := 10

		kNN := make([][]int, m)
		for i, q := range test {
			kNN[i] = bruteForceKNN(train, q, descriptor.Distance, k)
		}

		for _, algorithm := range algorithms {
			b.Run(fmt.Sprintf("|%v|%v|", data.Name, algorithm.Description()), func(b *testing.B) {
				provider := neighborProviderFactory(train, descriptor.Distance, algorithm)
				var matches10At10 int
				var matches10At20 int
				var matches10At50 int
				for i := 0; i < b.N; i++ {
					for j, q := range test {
						neighbors := provider.NearestNeighbors(q, 50)
						matches10At10 += overlap(neighbors[:10], kNN[j])
						matches10At20 += overlap(neighbors[:20], kNN[j])
						matches10At50 += overlap(neighbors[:50], kNN[j])
					}
				}
				b.ReportMetric(float64(matches10At10)/float64(k*m*b.N), "rec10@10")
				b.ReportMetric(float64(matches10At20)/float64(k*m*b.N), "rec10@20")
				b.ReportMetric(float64(matches10At50)/float64(k*m*b.N), "rec10@50")
			})
		}

	}
}
