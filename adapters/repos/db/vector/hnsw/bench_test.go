//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	hnswEnt "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

//
// Utility functions for HDF5 data handling
//

// getHDF5ByteSize returns the size of an element in the dataset.
// Only 4 and 8 bytes (float32 and float64) are supported.
func getHDF5ByteSize(dataset *hdf5.Dataset) (uint, error) {
	datatype, err := dataset.Datatype()
	if err != nil {
		return 0, errors.New("Unable to read datatype")
	}

	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		return 0, errors.New("Unsupported dataset byte size")
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

//
// Structures and functions for benchmarking
//

// BenchResult stores metrics from the benchmark run.
type BenchResult struct {
	BuildTime   float64
	BuildQPS    float64
	QueryTime   float64
	QueryQPS    float64
	QueryRecall float64
}

// vectorForID is the on-demand vector lookup function.
// When the HNSW index needs to retrieve the vector for a given ID,
// we read that single vector (row) from the HDF5 "train" dataset.
func vectorForID(trainDataset *hdf5.Dataset, id uint64,
	dimensions uint, byteSize uint,
) ([]float32, error) {
	// Create a memory dataspace for a single vector (1 x dimensions).
	memspace, err := hdf5.CreateSimpleDataspace([]uint{1, dimensions},
		[]uint{1, dimensions})
	if err != nil {
		return nil, fmt.Errorf("creating memspace: %w", err)
	}
	defer memspace.Close()

	// Define the hyperslab: offset at the given row and full row length.
	offset := []uint{uint(id), 0}
	count := []uint{1, dimensions}

	// Get the file dataspace and select the hyperslab.
	dataspace := trainDataset.Space()
	if err = dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
		return nil, fmt.Errorf("selecting hyperslab for id %d: %w", id, err)
	}

	var vec []float32
	if byteSize == 4 {
		vec = make([]float32, dimensions)
		if err = trainDataset.ReadSubset(&vec, memspace, dataspace); err != nil {
			return nil, fmt.Errorf("reading subset (float32) for id %d: %w", id, err)
		}
	} else if byteSize == 8 {
		vecFloat64 := make([]float64, dimensions)
		if err = trainDataset.ReadSubset(&vecFloat64, memspace, dataspace); err != nil {
			return nil, fmt.Errorf("reading subset (float64) for id %d: %w", id, err)
		}
		vec = make([]float32, dimensions)
		for i, v := range vecFloat64 {
			vec[i] = float32(v)
		}
	} else {
		return nil, errors.New("unsupported byte size")
	}

	return vec, nil
}

// GetVectorsFromHdf5 loads a bulk chunk of vectors (and their
// ideal nearest-neighbor IDs) from the HDF5 file. This is useful
// for the query benchmark.
func GetVectorsFromHdf5(rows uint, dataset *hdf5.Dataset,
	ideal_neighbors *hdf5.Dataset, b *testing.B,
) ([][]float32, [][]int) {
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	idealNeighborsSpace := ideal_neighbors.Space()

	if len(dims) != 2 {
		b.Fatalf("expected 2 dimensions")
	}

	byteSize, err := getHDF5ByteSize(dataset)
	require.NoError(b, err)

	byteSizeIdeal, err := getHDF5ByteSize(ideal_neighbors)
	require.NoError(b, err)

	dimensions := dims[1]
	K := 10

	memspace, err := hdf5.CreateSimpleDataspace([]uint{rows, dimensions},
		[]uint{rows, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	memspaceIdeal, err := hdf5.CreateSimpleDataspace([]uint{rows, uint(K)},
		[]uint{rows, uint(K)})
	if err != nil {
		log.Fatalf("Error creating memspace for ideal neighbors: %v", err)
	}
	defer memspaceIdeal.Close()

	offset := []uint{0, 0}
	count := []uint{rows, dimensions}
	countIdeal := []uint{rows, uint(K)}

	if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
		log.Fatalf("Error selecting hyperslab: %v", err)
	}
	if err := idealNeighborsSpace.SelectHyperslab(offset, nil, countIdeal, nil); err != nil {
		log.Fatalf("Error selecting hyperslab for ideal neighbors: %v", err)
	}

	var chunkData [][]float32
	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Fatalf("Error reading subset (float32): %v", err)
		}
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Fatalf("Error reading subset (float64): %v", err)
		}
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	var chunkDataIdeal [][]int
	if byteSizeIdeal == 4 {
		chunkData1D := make([]int, int(rows)*K)
		if err := ideal_neighbors.ReadSubset(&chunkData1D, memspaceIdeal,
			idealNeighborsSpace); err != nil {
			log.Fatalf("Error reading subset for ideal neighbors (int32): %v", err)
		}
		chunkDataIdeal = convert1DChunk_int[int](chunkData1D, K, int(rows))
	} else if byteSizeIdeal == 8 {
		chunkData1D := make([]int, int(rows)*K)
		if err := ideal_neighbors.ReadSubset(&chunkData1D, memspaceIdeal,
			idealNeighborsSpace); err != nil {
			log.Fatalf("Error reading subset for ideal neighbors (int64): %v", err)
		}
		chunkDataIdeal = convert1DChunk_int[int](chunkData1D, K, int(rows))
	}

	return chunkData, chunkDataIdeal
}

//
// Benchmarking functions: building the index and querying it
//

// BenchmarkDataset is the main benchmark entry point. It builds the HNSW index
// from vectors in the "train" dataset and then runs queries (using "test" and
// "neighbors") to calculate performance and recall.
func BenchmarkDataset(b *testing.B) {
	// Stop timer to exclude setup time.
	b.StopTimer()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(filepath.Join(b.TempDir(), "store"), b.TempDir(),
		logger, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		b.Fatal("failed to create store")
	}
	defer store.Shutdown(context.Background())

	// Open the HDF5 file (adjust the path as needed)
	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5",
		hdf5.F_ACC_RDONLY)
	if err != nil {
		b.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Open the necessary datasets.
	// "train" is used for building the index.
	// "test" is used for queries.
	// "neighbors" is used as ground truth for recall.
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

	// Get dimensions and byte size information for the train dataset.
	trainSpace := trainDataset.Space()
	dims, _, err := trainSpace.SimpleExtentDims()
	if err != nil {
		b.Fatalf("Error getting train dataset dimensions: %v", err)
	}
	if len(dims) != 2 {
		b.Fatalf("expected train dataset to have 2 dimensions")
	}
	dimensions := dims[1]
	byteSize, err := getHDF5ByteSize(trainDataset)
	require.NoError(b, err)

	// Define vecForID to read vectors on demand from the train dataset.
	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return vectorForID(trainDataset, id, dimensions, byteSize)
	}

	// Create the HNSW index.
	index, err := New(Config{
		RootPath:              "noop-path",
		ID:                    "unittest",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:      vecForIDFn,
	}, hnswEnt.UserConfig{
		MaxConnections: 30,
		EFConstruction: 60,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	if err != nil {
		b.Fatal(err)
	}

	// Restart the timer and run the benchmark.
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		// Build the index from the "train" dataset.
		buildTime, buildQPS := LoadVectors(index, trainDataset, b, 100_000, 100_000)
		// Run queries using the "test" and "neighbors" datasets.
		queryRecall, queryTime, queryQPS := QueryVectors(index, testDataset, neighborsDataset, b, 100_000)

		b.ReportMetric(buildQPS, "build-qps")
		b.ReportMetric(buildTime, "build-time")
		b.ReportMetric(queryQPS, "query-qps")
		b.ReportMetric(queryTime, "query-time")
		b.ReportMetric(queryRecall, "query-recall")
	}
}

// LoadVectors builds the HNSW index by processing the train dataset in batches.
func LoadVectors(index *hnsw, dataset *hdf5.Dataset, b *testing.B, rows uint, batchSize uint) (float64, float64) {
	// const (
	// 	rows      = uint(10_000) // total number of rows for this run
	// 	batchSize = uint(10_000) // read 10k rows at a time
	// )

	dataspace := dataset.Space()
	dims, _, err := dataspace.SimpleExtentDims()
	require.NoError(b, err)
	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}
	byteSize, err := getHDF5ByteSize(dataset)
	require.NoError(b, err)
	dimensions := dims[1]

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions},
		[]uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	start := time.Now()
	minValC, maxValC := float32(math.Inf(1)), float32(math.Inf(-1))

	for i := uint(0); i < rows; i += batchSize {
		currentBatch := batchSize
		if i+batchSize > rows {
			currentBatch = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{currentBatch, dimensions},
				[]uint{currentBatch, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{currentBatch, dimensions}
		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32
		if byteSize == 4 {
			chunkData1D := make([]float32, currentBatch*dimensions)
			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Fatalf("Error reading subset (float32): %v", err)
			}
			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(currentBatch))
		} else if byteSize == 8 {
			chunkData1D := make([]float64, currentBatch*dimensions)
			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Fatalf("Error reading subset (float64): %v", err)
			}
			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(currentBatch))
		}

		// Create an ID for each vector in the batch.
		ids := make([]uint64, currentBatch)
		for j := uint(0); j < currentBatch; j++ {
			ids[j] = uint64(i + j)
		}

		// Update min/max values across the batch (optional sanity check).
		for _, vec := range chunkData {
			for _, val := range vec {
				if val < minValC {
					minValC = val
				}
				if val > maxValC {
					maxValC = val
				}
			}
		}
		println(minValC, maxValC)

		err := index.AddBatch(context.TODO(), ids, chunkData)
		require.NoError(b, err)
	}

	elapsed := time.Since(start)
	totalTime := elapsed.Seconds()
	qps := float64(rows) / totalTime

	return totalTime, qps
}

// QueryVectors runs searches on the index using the "test" dataset,
// comparing the returned neighbor IDs against those in the "neighbors" dataset
// to calculate recall and query throughput.
func QueryVectors(index *hnsw, dataset *hdf5.Dataset,
	ideal_neighbors *hdf5.Dataset, b *testing.B, rows uint,
) (float64, float64, float64) {
	// const (
	// 	rows = uint(10_000) // number of query vectors
	// )

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()
	idealNeighborsSpace := ideal_neighbors.Space()

	if len(dims) != 2 {
		b.Fatalf("expected 2 dimensions")
	}

	byteSize, err := getHDF5ByteSize(dataset)
	require.NoError(b, err)
	byteSizeIdeal, err := getHDF5ByteSize(ideal_neighbors)
	require.NoError(b, err)
	dimensions := dims[1]
	K := 10

	memspace, err := hdf5.CreateSimpleDataspace([]uint{rows, dimensions},
		[]uint{rows, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace for queries: %v", err)
	}
	defer memspace.Close()

	memspaceIdeal, err := hdf5.CreateSimpleDataspace([]uint{rows, uint(K)},
		[]uint{rows, uint(K)})
	if err != nil {
		log.Fatalf("Error creating memspace for ideal neighbors queries: %v", err)
	}
	defer memspaceIdeal.Close()

	start := time.Now()
	numCorrect := 0

	offset := []uint{0, 0}
	count := []uint{rows, dimensions}
	countIdeal := []uint{rows, uint(K)}

	if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
		log.Fatalf("Error selecting hyperslab for queries: %v", err)
	}
	if err := idealNeighborsSpace.SelectHyperslab(offset, nil, countIdeal, nil); err != nil {
		log.Fatalf("Error selecting hyperslab for ideal neighbors: %v", err)
	}

	var chunkData [][]float32
	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Fatalf("Error reading subset (float32) for queries: %v", err)
		}
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
			log.Fatalf("Error reading subset (float64) for queries: %v", err)
		}
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	var chunkDataIdeal [][]int
	if byteSizeIdeal == 4 {
		chunkDataIdeal1D := make([]int, int(rows)*K)
		if err := ideal_neighbors.ReadSubset(&chunkDataIdeal1D, memspaceIdeal,
			idealNeighborsSpace); err != nil {
			log.Fatalf("Error reading subset (int32) for ideal neighbors: %v", err)
		}
		chunkDataIdeal = convert1DChunk_int[int](chunkDataIdeal1D, K, int(rows))
	} else if byteSizeIdeal == 8 {
		chunkDataIdeal1D := make([]int, int(rows)*K)
		if err := ideal_neighbors.ReadSubset(&chunkDataIdeal1D, memspaceIdeal,
			idealNeighborsSpace); err != nil {
			log.Fatalf("Error reading subset (int64) for ideal neighbors: %v", err)
		}
		chunkDataIdeal = convert1DChunk_int[int](chunkDataIdeal1D, K, int(rows))
	}

	// For each query vector, perform the search and compare results.
	for i := range chunkData {
		ids, _, err := index.SearchByVector(context.TODO(), chunkData[i], K, nil)
		require.NoError(b, err)

		idealIDs := chunkDataIdeal[i]
		for j := range ids {
			if ids[j] == uint64(idealIDs[j]) {
				numCorrect++
			}
		}
	}

	elapsed := time.Since(start)
	totalTime := elapsed.Seconds()
	recall := float64(numCorrect) / float64(rows*uint(K))
	qps := float64(rows) / totalTime

	return recall, totalTime, qps
}
