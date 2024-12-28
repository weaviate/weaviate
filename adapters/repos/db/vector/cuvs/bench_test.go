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

//go:build cuvs

package cuvs_index

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/rapidsai/cuvs/go/cagra"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

func getHDF5ByteSize(dataset *hdf5.Dataset) (uint, error) {
	datatype, err := dataset.Datatype()
	if err != nil {
		return 0, errors.New("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		return 0, errors.New("Unable to load dataset with byte size")
	}
	return byteSize, nil
}

func convert1DChunk[D float32 | float64](input []D, dimensions int, batchRows int) [][]float32 {
	chunkData := make([][]float32, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

func convert1DChunk_int[D int](input []D, dimensions int, batchRows int) [][]int {
	chunkData := make([][]int, batchRows)
	for i := range chunkData {
		chunkData[i] = make([]int, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = int(input[i*dimensions+j])
		}
	}
	return chunkData
}

var paretoConfigurations [](func(indexParams *cagra.IndexParams, searchParams *cagra.SearchParams) (*cagra.IndexParams, *cagra.SearchParams))

func RunConfiguration(cuvsIndexParams *cagra.IndexParams, cuvsSearchParams *cagra.SearchParams, b *testing.B) BenchResult {
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(filepath.Join(b.TempDir(), "store"), b.TempDir(), logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	defer store.Shutdown(context.Background())

	index, err := New(Config{ID: "a", TargetVector: "vector", Logger: logger, CuvsIndexParams: cuvsIndexParams, CuvsSearchParams: cuvsSearchParams, RootPath: b.TempDir()}, cuvsEnt.UserConfig{}, store)
	if err != nil {
		panic(err)
	}

	index.PostStartup()

	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	dataset, err := file.OpenDataset("train")
	testdataset, err := file.OpenDataset("test")
	neighborsdataset, err := file.OpenDataset("neighbors")
	// distancesdataset, err := file.OpenDataset("distances")
	if err != nil {
		log.Fatalf("Error opening dataset: %v\n", err)
	}

	BuildTime, BuildQPS := LoadVectors(index, dataset, b)

	QueryRecall, QueryTime, QueryQPS := QueryVectors(index, testdataset, neighborsdataset, b)

	result := BenchResult{BuildTime, BuildQPS, QueryTime, QueryQPS, QueryRecall}

	err = index.Shutdown(context.TODO())
	if err != nil {
		panic(err)
	}

	return result
}

func BenchmarkPareto(b *testing.B) {
	type cagraConfig struct {
		graphDegree             int
		intermediateGraphDegree int
		buildAlgo               cagra.BuildAlgo
		itopkSize               int
		searchWidth             int
	}

	paretoConfigurations := []cagraConfig{
		{
			graphDegree:             64,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             1,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             1,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             1,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             4,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             4,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             8,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             8,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             8,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             16,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             16,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               512,
			searchWidth:             16,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             32,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             32,
		},
		{
			graphDegree:             64,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             32,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 32,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               256,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               256,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 64,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               256,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               512,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               256,
			searchWidth:             64,
		},
		{
			graphDegree:             32,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               128,
			searchWidth:             64,
		},
		{
			graphDegree:             64,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               128,
			searchWidth:             64,
		},
		{
			graphDegree:             64,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               32,
			searchWidth:             64,
		},
		{
			graphDegree:             64,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               128,
			searchWidth:             64,
		},
		{
			graphDegree:             64,
			intermediateGraphDegree: 128,
			buildAlgo:               cagra.NnDescent,
			itopkSize:               64,
			searchWidth:             64,
		},
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

	for i := range benchResults {
		println("--------------------------------")
		fmt.Printf("Config: %+v\n", paretoConfigurations[i])
		fmt.Printf("Result: %+v\n", benchResults[i])
		println("--------------------------------")
	}
}

type BenchResult struct {
	BuildTime   float64
	BuildQPS    float64
	QueryTime   float64
	QueryQPS    float64
	QueryRecall float64
}

func BenchmarkDataset(b *testing.B) {
	// Reset the timer to exclude setup time
	b.StopTimer()

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(filepath.Join(b.TempDir(), "store"), b.TempDir(), logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		b.Fatal("failed to create store")
	}
	defer store.Shutdown(context.Background())

	index, err := New(Config{ID: "a", TargetVector: "vector", Logger: logger, RootPath: b.TempDir(), CuvsPoolMemory: 90}, cuvsEnt.UserConfig{}, store)
	if err != nil {
		b.Fatal(err)
	}

	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		b.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	dataset, err := file.OpenDataset("train")
	testdataset, err := file.OpenDataset("test")
	neighborsdataset, err := file.OpenDataset("neighbors")
	if err != nil {
		b.Fatalf("Error opening dataset: %v\n", err)
	}

	b.StartTimer()
	// Run the benchmark b.N times
	for i := 0; i < b.N; i++ {
		BuildTime, BuildQPS := LoadVectors(index, dataset, b)
		QueryRecall, QueryTime, QueryQPS := QueryVectors(index, testdataset, neighborsdataset, b)

		// Report metrics
		b.ReportMetric(BuildQPS, "build-qps")
		b.ReportMetric(BuildTime, "build-time")
		b.ReportMetric(QueryQPS, "query-qps")
		b.ReportMetric(QueryTime, "query-time")
		b.ReportMetric(QueryRecall, "query-recall")
	}
}

func LoadVectors(index *cuvs_index, dataset *hdf5.Dataset, b *testing.B) (float64, float64) {
	const (
		rows      = uint(990_000)
		batchSize = uint(990_000)
	)

	dataspace := dataset.Space()
	dims, _, err := dataspace.SimpleExtentDims()
	require.NoError(b, err)

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	byteSize, err := getHDF5ByteSize(dataset)
	require.NoError(b, err)

	dimensions := dims[1]

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	start := time.Now()

	minValC, maxValC := float32(math.Inf(1)), float32(math.Inf(-1))

	for i := uint(0); i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		ids := make([]uint64, batchSize)

		for j := uint(0); j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)
		}

		for i := range chunkData {
			for j := range chunkData[i] {
				if math.IsNaN(float64(chunkData[i][j])) || math.IsInf(float64(chunkData[i][j]), 0) {
					log.Printf("Invalid value at [%d][%d]: %f", i, j, chunkData[i][j])
				}
			}
		}

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

		err := index.AddBatch(context.TODO(), ids, chunkData)
		require.NoError(b, err)

	}

	elapsed := time.Since(start)

	time := elapsed.Seconds()
	qps := float64(rows) / time

	return time, qps
}

func QueryVectors(index *cuvs_index, dataset *hdf5.Dataset, ideal_neighbors *hdf5.Dataset, b *testing.B) (float64, float64, float64) {
	const (
		rows      = uint(10_000)
		batchSize = uint(10_000)
	)

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	ideal_neighbors_dataspace := ideal_neighbors.Space()

	if len(dims) != 2 {
		b.Fatalf("expected 2 dimensions")
	}

	byteSize, error := getHDF5ByteSize(dataset)
	require.NoError(b, error)

	byteSize_ideal, error := getHDF5ByteSize(ideal_neighbors)
	require.NoError(b, error)

	dimensions := dims[1]

	K := 10

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	memspace_ideal, err := hdf5.CreateSimpleDataspace([]uint{batchSize, uint(K)}, []uint{batchSize, uint(K)})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace_ideal.Close()

	start := time.Now()

	numCorrect := 0

	for i := uint(0); i < rows; i += batchSize {

		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}

			memspace_ideal, err = hdf5.CreateSimpleDataspace([]uint{batchRows, uint(K)}, []uint{batchRows, uint(K)})
			if err != nil {
				log.Fatalf("Error creating final memspace: %v", err)
			}
		}

		offset := []uint{i, 0}
		count := []uint{batchRows, dimensions}

		count_ideal := []uint{batchRows, uint(K)}

		if err := dataspace.SelectHyperslab(offset, nil, count, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		if err := ideal_neighbors_dataspace.SelectHyperslab(offset, nil, count_ideal, nil); err != nil {
			log.Fatalf("Error selecting hyperslab: %v", err)
		}

		var chunkData [][]float32

		if byteSize == 4 {
			chunkData1D := make([]float32, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(batchRows))

		}

		var chunkData_ideal [][]int

		if byteSize_ideal == 4 {
			chunkData1D := make([]int, batchRows*uint(K))

			if err := ideal_neighbors.ReadSubset(&chunkData1D, memspace_ideal, ideal_neighbors_dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData_ideal = convert1DChunk_int[int](chunkData1D, int(K), int(batchRows))

		} else if byteSize_ideal == 8 {
			chunkData1D := make([]int, batchRows*uint(K))

			if err := ideal_neighbors.ReadSubset(&chunkData1D, memspace_ideal, ideal_neighbors_dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData_ideal = convert1DChunk_int[int](chunkData1D, int(K), int(batchRows))

		}

		ids := make([]uint64, batchSize)

		for j := uint(0); j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)
		}

		if batchSize > 1 {

			idsBatch, _, err := index.SearchByVectorBatch(chunkData, K, nil)
			require.NoError(b, err)
			for j := range idsBatch {
				for k := range idsBatch[j] {
					if idsBatch[j][k] == uint64(chunkData_ideal[j][k]) {
						numCorrect += 1
					}
				}
			}
		} else {
			ids, _, err := index.SearchByVector(context.TODO(), chunkData[0], K, nil)
			require.NoError(b, err)

			ids_ideal := chunkData_ideal[k]

			for j := range ids {
				if ids[j] == uint64(ids_ideal[j]) {
					numCorrect += 1
				}
			}

		}

	}

	elapsed := time.Since(start)

	time := float64(elapsed.Seconds())

	recall := float64(numCorrect) / float64(rows*uint(K))
	qps := float64(rows) / time

	return recall, time, qps
}
