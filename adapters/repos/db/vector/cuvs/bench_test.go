//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//  CONTACT: hello@weaviate.io
//

//go:build cuvs

package cuvs_index

import (
	"context"
	"errors"
	"fmt"

	// "fmt"
	"log"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/rapidsai/cuvs/go/cagra"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

// getHDF5ByteSize returns the size of an element in the dataset.
// Only 4 and 8 bytes (float32 and float64) are supported.
func getHDF5ByteSize(dataset *hdf5.Dataset) (uint, error) {
	datatype, err := dataset.Datatype()
	if err != nil {
		return 0, errors.New("unable to read datatype")
	}

	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		return 0, errors.New("unsupported dataset byte size")
	}
	return byteSize, nil
}

// convert1DChunk converts a one-dimensional slice (read from HDF5)
// into a two-dimensional slice (batchRows x dimensions) of float32.
func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := 0; i < batchRows; i++ {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

// convert1DChunk_int converts a one-dimensional slice (read from HDF5)
// into a two-dimensional slice (batchRows x dimensions) of int.
func convert1DChunk_int[D int](input []D, dimensions int, batchRows int) [][]int {
	chunkData := make([][]int, batchRows)
	for i := 0; i < batchRows; i++ {
		chunkData[i] = make([]int, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = int(input[i*dimensions+j])
		}
	}
	return chunkData
}

// loadHdf5Float32FromDataset reads an entire 2D dataset from an HDF5 file
// and returns its contents as a [][]float32.
func loadHdf5Float32FromDataset(dataset *hdf5.Dataset) [][]float32 {
	dataspace := dataset.Space()
	dims, _, err := dataspace.SimpleExtentDims()
	if err != nil {
		log.Fatalf("Error getting dataset dimensions: %v", err)
	}
	byteSize, err := getHDF5ByteSize(dataset)
	if err != nil {
		log.Fatalf("Error getting dataset byte size: %v", err)
	}
	if len(dims) != 2 {
		log.Fatalf("expected 2 dimensions, got %d", len(dims))
	}
	rows := dims[0]
	dimensions := dims[1]

	var data [][]float32
	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading dataset (float32): %v", err)
		}
		data = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading dataset (float64): %v", err)
		}
		data = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}
	return data
}

// loadHdf5IntFromDataset reads an entire 2D dataset that represents integers
// (e.g. ideal neighbor IDs) and returns its contents as a [][]int.
func loadHdf5IntFromDataset(dataset *hdf5.Dataset, expectedSecondDim int) [][]int {
	dataspace := dataset.Space()
	dims, _, err := dataspace.SimpleExtentDims()
	if err != nil {
		log.Fatalf("Error getting dataset dimensions: %v", err)
	}
	byteSize, err := getHDF5ByteSize(dataset)
	if err != nil {
		log.Fatalf("Error getting dataset byte size: %v", err)
	}
	if len(dims) != 2 {
		log.Fatalf("expected 2 dimensions, got %d", len(dims))
	}
	rows := dims[0]
	dimensions := dims[1]
	if int(dimensions) != expectedSecondDim {
		log.Fatalf("expected second dimension to be %d, got %d", expectedSecondDim, dimensions)
	}

	var data [][]int
	if byteSize == 4 {
		chunkData1D := make([]int, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading int dataset (int32): %v", err)
		}
		data = convert1DChunk_int[int](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]int, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading int dataset (int64): %v", err)
		}
		data = convert1DChunk_int[int](chunkData1D, int(dimensions), int(rows))
	}
	return data
}

// BenchResult stores metrics from the benchmark run.
type BenchResult struct {
	BuildTime   float64
	BuildQPS    float64
	QueryTime   float64
	QueryQPS    float64
	QueryRecall float64
}

// RunConfiguration sets up a cuvs index with the given configuration,
// loads the datasets, runs build and query benchmarks, and returns the results.
func RunConfiguration(cuvsIndexParams *cagra.IndexParams, cuvsSearchParams *cagra.SearchParams, b *testing.B) BenchResult {
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(
		filepath.Join(b.TempDir(), "store"),
		b.TempDir(),
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
	)
	if err != nil {
		b.Fatal("failed to create store")
	}
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		ID:               "a",
		TargetVector:     "vector",
		Logger:           logger,
		CuvsIndexParams:  cuvsIndexParams,
		CuvsSearchParams: cuvsSearchParams,
		RootPath:         b.TempDir(),
	}, cuvsEnt.UserConfig{}, store)
	if err != nil {
		panic(err)
	}
	index.PostStartup()

	// Open the HDF5 file and load the datasets.
	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	trainDataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatalf("Error opening train dataset: %v", err)
	}
	testDataset, err := file.OpenDataset("test")
	if err != nil {
		log.Fatalf("Error opening test dataset: %v", err)
	}
	neighborsDataset, err := file.OpenDataset("neighbors")
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}

	BuildTime, BuildQPS := LoadVectors(index, trainDataset)
	QueryRecall, QueryTime, QueryQPS := QueryVectors(index, testDataset, neighborsDataset)

	result := BenchResult{
		BuildTime:   BuildTime,
		BuildQPS:    BuildQPS,
		QueryTime:   QueryTime,
		QueryQPS:    QueryQPS,
		QueryRecall: QueryRecall,
	}

	if err := index.Shutdown(context.TODO()); err != nil {
		panic(err)
	}

	return result
}

// BenchmarkPareto runs multiple configurations defined by paretoConfigurations.
func BenchmarkPareto(b *testing.B) {
	type cagraConfig struct {
		graphDegree             int
		intermediateGraphDegree int
		buildAlgo               cagra.BuildAlgo
		itopkSize               int
		searchWidth             int
	}

	paretoConfigurations := []cagraConfig{
		{64, 128, cagra.NnDescent, 32, 64},
		{32, 64, cagra.NnDescent, 32, 1},
		{32, 32, cagra.NnDescent, 64, 1},
		{32, 64, cagra.NnDescent, 64, 1},
		{32, 32, cagra.NnDescent, 64, 4},
		{32, 64, cagra.NnDescent, 64, 4},
		{32, 32, cagra.NnDescent, 64, 8},
		{32, 64, cagra.NnDescent, 64, 8},
		{32, 64, cagra.NnDescent, 64, 8},
		{32, 32, cagra.NnDescent, 32, 16},
		{32, 64, cagra.NnDescent, 32, 16},
		{32, 64, cagra.NnDescent, 512, 16},
		{32, 32, cagra.NnDescent, 32, 32},
		{32, 64, cagra.NnDescent, 32, 32},
		{64, 64, cagra.NnDescent, 32, 32},
		{32, 32, cagra.NnDescent, 32, 64},
		{32, 32, cagra.NnDescent, 256, 64},
		{32, 64, cagra.NnDescent, 256, 64},
		{32, 128, cagra.NnDescent, 32, 64},
		{32, 64, cagra.NnDescent, 32, 64},
		{32, 128, cagra.NnDescent, 256, 64},
		{32, 128, cagra.NnDescent, 512, 64},
		{32, 128, cagra.NnDescent, 256, 64},
		{32, 128, cagra.NnDescent, 128, 64},
		{64, 128, cagra.NnDescent, 128, 64},
		{64, 128, cagra.NnDescent, 32, 64},
		{64, 128, cagra.NnDescent, 128, 64},
		{64, 128, cagra.NnDescent, 64, 64},
	}

	var benchResults []BenchResult

	for i, config := range paretoConfigurations {
		indexParams, err := cagra.CreateIndexParams()
		if err != nil {
			panic(err)
		}
		searchParams, err := cagra.CreateSearchParams()
		if err != nil {
			panic(err)
		}

		indexParams.SetGraphDegree(uintptr(config.graphDegree))
		indexParams.SetIntermediateGraphDegree(uintptr(config.intermediateGraphDegree))
		indexParams.SetBuildAlgo(config.buildAlgo)
		searchParams.SetItopkSize(uintptr(config.itopkSize))
		searchParams.SetSearchWidth(uintptr(config.searchWidth))

		benchResult := RunConfiguration(indexParams, searchParams, b)
		benchResults = append(benchResults, benchResult)

		fmt.Printf("Config: %+v\n", paretoConfigurations[i])
		fmt.Printf("Result: %+v\n", benchResult)
	}

	for i, res := range benchResults {
		println("--------------------------------")
		fmt.Printf("Config: %+v\n", paretoConfigurations[i])
		fmt.Printf("Result: %+v\n", res)
		println("--------------------------------")
	}
}

// BenchmarkDataset runs a single benchmark configuration (without pareto sweeps)
// using the default cuvs settings.
func BenchmarkDataset(b *testing.B) {
	// Exclude setup time.
	b.StopTimer()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(
		filepath.Join(b.TempDir(), "store"),
		b.TempDir(),
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
	)
	if err != nil {
		b.Fatal("failed to create store")
	}
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		ID:             "a",
		TargetVector:   "vector",
		Logger:         logger,
		RootPath:       b.TempDir(),
		CuvsPoolMemory: 90,
	}, cuvsEnt.UserConfig{}, store)
	if err != nil {
		b.Fatal(err)
	}

	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		b.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	trainDataset, err := file.OpenDataset("train")
	if err != nil {
		b.Fatalf("Error opening train dataset: %v", err)
	}
	testDataset, err := file.OpenDataset("test")
	if err != nil {
		b.Fatalf("Error opening test dataset: %v", err)
	}
	neighborsDataset, err := file.OpenDataset("neighbors")
	if err != nil {
		b.Fatalf("Error opening neighbors dataset: %v", err)
	}

	b.StartTimer()
	// Run the benchmark b.N times.
	for i := 0; i < b.N; i++ {
		BuildTime, BuildQPS := LoadVectors(index, trainDataset)
		QueryRecall, QueryTime, QueryQPS := QueryVectors(index, testDataset, neighborsDataset)

		b.ReportMetric(BuildQPS, "build-qps")
		b.ReportMetric(BuildTime, "build-time")
		b.ReportMetric(QueryQPS, "query-qps")
		b.ReportMetric(QueryTime, "query-time")
		b.ReportMetric(QueryRecall, "query-recall")
	}
}

// LoadVectors loads the training vectors from the given dataset into memory,
// then processes them in batches to add to the index.
func LoadVectors(index *cuvs_index, dataset *hdf5.Dataset) (float64, float64) {
	// Load entire dataset into memory.
	allVectors := loadHdf5Float32FromDataset(dataset)
	totalRows := len(allVectors)
	// Use the original constants.
	const batchSize = 990_000

	start := time.Now()

	// Optional: sanity check min/max values.
	minValC, maxValC := float32(math.Inf(1)), float32(math.Inf(-1))

	for i := 0; i < totalRows; i += batchSize {
		end := i + batchSize
		if end > totalRows {
			end = totalRows
		}
		batchVectors := allVectors[i:end]
		ids := make([]uint64, len(batchVectors))
		for j := 0; j < len(batchVectors); j++ {
			ids[j] = uint64(i + j)
		}

		// Update min/max (optional).
		for _, vec := range batchVectors {
			for _, val := range vec {
				if val < minValC {
					minValC = val
				}
				if val > maxValC {
					maxValC = val
				}
			}
		}
		// Print out current min/max values.
		println(minValC, maxValC)

		index.AddBatch(context.TODO(), ids, batchVectors)
		// require.NoError(b, err)
	}

	elapsed := time.Since(start).Seconds()
	qps := float64(totalRows) / elapsed
	return elapsed, qps
}

// QueryVectors loads the test vectors and their corresponding ideal neighbor IDs
// into memory, then processes them in batches to query the index and compute recall.
func QueryVectors(index *cuvs_index, testDataset *hdf5.Dataset, neighborsDataset *hdf5.Dataset) (float64, float64, float64) {
	// Constants as defined in the original.
	const batchSize = 100
	const K = 10

	allTestVectors := loadHdf5Float32FromDataset(testDataset)
	totalRows := len(allTestVectors)
	allIdealNeighbors := loadHdf5IntFromDataset(neighborsDataset, 100)

	numCorrect := 0
	start := time.Now()

	for i := 0; i < totalRows; i += batchSize {
		end := i + batchSize
		if end > totalRows {
			end = totalRows
		}
		batchTest := allTestVectors[i:end]
		batchIdeal := allIdealNeighbors[i:end]

		// Use batch search if more than one vector is present.
		if len(batchTest) > 1 {
			idsBatch, _, _ := index.SearchByVectorBatch(batchTest, K, nil)
			// require.NoError(b, err)
			for j, ids := range idsBatch {
				// Create a set of ideal neighbors for this query
				idealSet := make(map[uint64]struct{}, len(batchIdeal[j]))
				for _, ideal := range batchIdeal[j][:K] {
					idealSet[uint64(ideal)] = struct{}{}
				}
				// Check each returned id for a match regardless of order.
				for _, id := range ids {
					if _, found := idealSet[id]; found {
						numCorrect++
						// Remove the item so it isn't counted more than once.
						delete(idealSet, id)
					}
				}
			}
		} else if len(batchTest) == 1 {
			ids, _, _ := index.SearchByVector(context.TODO(), batchTest[0], K, nil)
			// require.NoError(b, err)
			idealSet := make(map[uint64]struct{}, len(batchIdeal[0]))
			for _, ideal := range batchIdeal[0][:K] {
				idealSet[uint64(ideal)] = struct{}{}
			}

			for _, id := range ids {
				// fmt.Printf("%v \n", id)
				if _, found := idealSet[id]; found {
					numCorrect++
					delete(idealSet, id)
				}
			}
		}
	}
	elapsed := time.Since(start).Seconds()
	println(elapsed)
	recall := float64(numCorrect) / float64(totalRows*K)
	qps := float64(totalRows) / elapsed
	return recall, elapsed, qps
}
