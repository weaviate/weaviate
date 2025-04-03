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

package ivfpq_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ivfpq"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

type Batch struct {
	Vectors [][]float32
	offset  int
}

func getHDF5ByteSize(dataset *hdf5.Dataset) uint {

	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("Unabled to read datatype\n")
	}

	// log.WithFields(log.Fields{"size": datatype.Size()}).Printf("Parsing HDF5 byte format\n")
	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize
}

// Read an entire dataset from an hdf5 file at once
func loadHdf5Float32(file *hdf5.File, name string) [][]float32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening loadHdf5Float32 dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	byteSize := getHDF5ByteSize(dataset)

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	var chunkData [][]float32

	if byteSize == 4 {
		chunkData1D := make([]float32, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}

	return chunkData
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

func loadHdf5Neighbors(file *hdf5.File, name string) [][]uint64 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening neighbors dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	byteSize := getHDF5ByteSize(dataset)

	chunkData := make([][]uint64, rows)

	if byteSize == 4 {
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]uint64, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = uint64(chunkData1D[uint(i)*dimensions+j])
			}
		}
	} else if byteSize == 8 {
		chunkData1D := make([]uint64, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	}

	return chunkData
}

func captureMemoryProfile(name string) {
	// Create the file
	f, err := os.Create(fmt.Sprintf("%s.mem_profile.pprof", name))
	if err != nil {
		log.Fatalf("Failed to create memory profile file: %v", err)
	}
	defer f.Close()

	// Write heap profile directly to the file
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatalf("Failed to write memory profile: %v", err)
	}

	// Log memory usage statistics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	log.Printf("Memory at %s: %d MB", name, memStats.Alloc/1024/1024)
}

func startCPUProfile(name string) func() {
	// Create the CPU profile file
	cpuFile, err := os.Create(fmt.Sprintf("%s.cpu_profile.pprof", name))
	if err != nil {
		log.Fatalf("Failed to create CPU profile file: %v", err)
	}

	// Start CPU profiling
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		cpuFile.Close()
		log.Fatalf("Failed to start CPU profile: %v", err)
	}

	log.Printf("CPU profiling started...")

	// Return a function that will stop profiling
	return func() {
		pprof.StopCPUProfile()
		cpuFile.Close()
		log.Printf("CPU profiling completed and saved to %s.cpu_profile.pprof", name)
	}
}

func reverseFloat32Slice(slice []float32) []float32 {
	for i, j := 0, len(slice)-1; i < j; i, j = i+1, j-1 {
		slice[i], slice[j] = slice[j], slice[i]
	}
	return slice
}

func TestIVF(t *testing.T) {
	log.Println("Starting...")
	var ivf *ivfpq.FlatPQ
	//file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/dbpedia-100k-openai-ada002.hdf5", hdf5.F_ACC_RDONLY)
	file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	//file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/sphere-10M-meta-dpr.hdf5", hdf5.F_ACC_RDONLY)
	assert.Nil(t, err)
	// defer file.Close()
	dataset, err := file.OpenDataset("train")
	// defer dataset.Close()
	vectors := make([][]float32, 1_000_000)
	//vectors := make([][]float32, 10_000_000)
	batchSize := uint(1_000)
	dataspace := dataset.Space()
	byteSize := getHDF5ByteSize(dataset)
	assert.Nil(t, err)
	dims, _, _ := dataspace.SimpleExtentDims()
	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}
	rows := dims[0]
	dimensions := dims[1]
	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	assert.Nil(t, err)
	defer memspace.Close()
	var wg1 sync.WaitGroup
	wg1.Add(1)
	for i := uint(0); i < rows; i += batchSize {
		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			assert.Nil(t, err)
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

			chunkData = convert1DChunk(chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk(chunkData1D, int(dimensions), int(batchRows))

		}
		if i == 100_000 {
			go func() {
				defer wg1.Done()
				for idx := 0; idx < 100_000; idx++ {
					reverseFloat32Slice(vectors[idx])
				}
				ivf = ivfpq.NewFlatPQ(vectors[:100_000], distancer.NewCosineDistanceProvider(), 2, 64, testinghelpers.NewDummyStore(t))
				log.Println("PQ trained...")
			}()
		}
		copy(vectors[i:], chunkData)
	}
	wg1.Wait()
	log.Println("Data loaded...")
	var wg sync.WaitGroup

	before := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for current := id * (int(rows) / 10); current < (id+1)*(int(rows)/10); current++ {
				if current > 100_000 {
					reverseFloat32Slice(vectors[current])
				}
				ivf.Add(uint64(current), vectors[current])
			}
		}(i)
	}
	wg.Wait()
	log.Println("Indexing... ", time.Since(before))
	neighbors := loadHdf5Neighbors(file, "neighbors")
	testData := loadHdf5Float32(file, "test")

	before = time.Now()
	ivf.Store()
	ellapsed := time.Since(before)
	fmt.Println("Storing... ", ellapsed)

	file.Close()
	dataset.Close()
	vectors = nil
	runtime.GC()

	captureMemoryProfile("after_loading_data")

	stopProfiling := startCPUProfile("querying")
	var wg2 sync.WaitGroup
	rows = 100
	recall := float32(0)
	totaEllapsed := time.Duration(0)
	lock := sync.Mutex{}
	concurrencyLevel := 10
	totalProbed := 0
	latencies := make([]int, 0, rows)
	before = time.Now()
	for i := 0; i < concurrencyLevel; i++ {
		wg2.Add(1)
		go func(id int) {
			defer wg2.Done()
			for current := id * (int(rows) / concurrencyLevel); current < (id+1)*(int(rows)/concurrencyLevel); current++ {
				before := time.Now()
				ids, _, _ := ivf.SearchByVector(context.Background(), reverseFloat32Slice(testData[current]), 10)
				ellapsed := time.Since(before)
				rec := float32(testinghelpers.MatchesInLists(neighbors[current][:10], ids)) / 10

				lock.Lock()
				latencies = append(latencies, int(ellapsed.Milliseconds()))
				totaEllapsed += ellapsed
				recall += rec / float32(rows)
				totalProbed += len(ids)
				lock.Unlock()
			}
		}(i)
	}
	wg2.Wait()
	ellapsed = time.Since(before)
	stopProfiling()

	captureMemoryProfile("after_querying")
	sort.Ints(latencies)

	log.Println("Latency: ", totaEllapsed.Milliseconds()/int64(rows), "ms")
	log.Println("QPS: ", int64(rows)*1000/ellapsed.Milliseconds(), "QPS")
	log.Println("Recall: ", recall)
	log.Println("Memory: ", ivf.MemoryInUse()/1024/1024, "MB")
	log.Println("Average bytes read per query: ", float32(ivf.BytesRead())/1024/1024/float32(rows), "MB")
	log.Println("Active buckets: ", ivf.ActiveBuckets())
	p50 := calculatePercentile(latencies, 50)
	p90 := calculatePercentile(latencies, 90)
	p95 := calculatePercentile(latencies, 95)
	p99 := calculatePercentile(latencies, 99)
	log.Println(p50, p90, p95, p99)
	assert.NotNil(t, nil)
}

func calculatePercentile(sortedValues []int, percentile int) int {
	if len(sortedValues) == 0 {
		return 0
	}

	index := int(float64(percentile) / 100.0 * float64(len(sortedValues)-1))
	return sortedValues[index]
}
