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
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	hnswEnt "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

//
// Helper functions for HDF5
//

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
// into a two-dimensional slice (rows x dimensions) of float32.
func convert1DChunk[D float32 | float64](input []D, dimensions int, rows int) [][]float32 {
	chunkData := make([][]float32, rows)
	for i := 0; i < rows; i++ {
		chunkData[i] = make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			chunkData[i][j] = float32(input[i*dimensions+j])
		}
	}
	return chunkData
}

// loadHdf5Float32 reads an entire 2D dataset from an HDF5 file
// and returns its contents as a [][]float32.
func loadHdf5Float32(file *hdf5.File, name string) [][]float32 {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		log.Fatalf("Error opening dataset '%s': %v", name, err)
	}
	defer dataset.Close()

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()
	byteSize, err := getHDF5ByteSize(dataset)
	if err != nil {
		log.Fatalf("Error getting byte size for dataset '%s': %v", name, err)
	}
	if len(dims) != 2 {
		log.Fatalf("Expected 2 dimensions for dataset '%s'", name)
	}

	rows := dims[0]
	dimensions := dims[1]
	var data [][]float32

	if byteSize == 4 {
		// Read the entire dataset into a 1D slice of float32.
		chunkData1D := make([]float32, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading dataset '%s': %v", name, err)
		}
		data = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	} else if byteSize == 8 {
		// Read the entire dataset into a 1D slice of float64 and then convert.
		chunkData1D := make([]float64, rows*dimensions)
		if err := dataset.Read(&chunkData1D); err != nil {
			log.Fatalf("Error reading dataset '%s': %v", name, err)
		}
		data = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	}
	return data
}

// Read an entire dataset from an hdf5 file at once (neighbours)
func loadHdf5Neighbors(file *hdf5.File, name string) [][]int {
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

	byteSize, _ := getHDF5ByteSize(dataset)

	chunkData := make([][]int, rows)

	if byteSize == 4 {
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]int, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = int(chunkData1D[uint(i)*dimensions+j])
			}
		}
	} else if byteSize == 8 {
		chunkData1D := make([]int, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	}

	return chunkData
}

// Benchmark function: Build the index and run queries
func BenchmarkDataset(b *testing.B) {
	b.StopTimer()

	file, err := hdf5.OpenFile("/Users/trengrj/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		b.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})

	trainVectors := loadHdf5Float32(file, "train")
	testVectors := loadHdf5Float32(file, "test")
	idealNeighbors := loadHdf5Neighbors(file, "neighbors")

	vecForIDFn := func(ctx context.Context, id uint64) ([]float32, error) {
		return trainVectors[int(id)], nil
	}

	index, err := New(
		Config{
			RootPath:              "noop-path",
			ID:                    "unittest",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewL2SquaredProvider(),
			VectorForIDThunk:      vecForIDFn,
		},
		hnswEnt.UserConfig{
			MaxConnections: 16,
			EFConstruction: 384,
			EF:             16,
		},
		cyclemanager.NewCallbackGroupNoop(),
		testinghelpers.NewDummyStore(b),
	)
	if err != nil {
		b.Fatal(err)
	}

	const (
		batchSize   = 1000
		workerCount = 8
		logInterval = 10000
		K           = 10
		queryBatch  = 1 // Batch queries within each worker
	)

	b.StartTimer()

	// Build phase (unchanged from previous optimization)
	jobsForWorker := make([][][]float32, workerCount)
	for i := range jobsForWorker {
		baseCapacity := len(trainVectors) / workerCount
		extra := 0
		if i < len(trainVectors)%workerCount {
			extra = 1
		}
		jobsForWorker[i] = make([][]float32, 0, baseCapacity+extra)
	}

	for i, vec := range trainVectors {
		workerID := i % workerCount
		jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
	}

	startBuild := time.Now()
	var processed int64
	var mu sync.Mutex
	lastLogged := 0

	wg := &sync.WaitGroup{}
	for workerID, jobs := range jobsForWorker {
		if len(jobs) == 0 {
			continue
		}
		wg.Add(1)
		go func(workerID int, myJobs [][]float32) {
			defer wg.Done()
			batchIDs := make([]uint64, batchSize)

			for start := 0; start < len(myJobs); start += batchSize {
				end := min(start+batchSize, len(myJobs))
				batch := myJobs[start:end]

				for j := range batch {
					batchIDs[j] = uint64(workerID + (start+j)*workerCount)
				}

				if err := index.AddBatch(context.TODO(), batchIDs[:len(batch)], batch); err != nil {
					log.WithError(err).Error("Failed to add batch")
					return
				}

				newProcessed := atomic.AddInt64(&processed, int64(len(batch)))
				mu.Lock()
				if int(newProcessed)-lastLogged >= logInterval {
					log.Infof("Imported %d/%d rows", newProcessed, len(trainVectors))
					lastLogged = (int(newProcessed) / logInterval) * logInterval
				}
				mu.Unlock()
			}
		}(workerID, jobs)
	}
	wg.Wait()

	buildDuration := time.Since(startBuild)
	buildQPS := float64(len(trainVectors)) / buildDuration.Seconds()
	log.Infof("Build complete: %s (%.0f vectors/sec)", buildDuration.Round(time.Second), buildQPS)

	time.Sleep(10 * time.Second)

	// Optimized query benchmark
	numWorkers := 2 // runtime.NumCPU() // Simple worker count for search parallelization
	var totalCorrect int64
	queryWg := &sync.WaitGroup{}
	results := make(chan struct {
		correct int64
		time    time.Duration
	}, numWorkers)

	// Distribute test vectors among workers
	queues := make([][]int, numWorkers)
	for i := range queues {
		queues[i] = make([]int, 0, len(testVectors)/numWorkers+1)
	}
	for i := 0; i < len(testVectors); i++ {
		worker := i % numWorkers
		queues[worker] = append(queues[worker], i)
	}

	for workerID := 0; workerID < numWorkers; workerID++ {
		queryWg.Add(1)
		go func(workerID int) {
			defer queryWg.Done()
			var localCorrect int64
			var searchTime time.Duration

			idealSlice := make([]uint64, K)
			ctx := context.TODO()

			for _, idx := range queues[workerID] {
				before := time.Now()
				resultIDs, _, err := index.SearchByVector(ctx, testVectors[idx], K, nil)
				took := time.Since(before)
				if err != nil {
					log.WithError(err).Error("Search failed")
					return
				}
				searchTime += took

				// Process results
				for j, ideal := range idealNeighbors[idx][:K] {
					idealSlice[j] = uint64(ideal)
				}
				for _, id := range resultIDs {
					for _, ideal := range idealSlice {
						if id == ideal {
							localCorrect++
							break
						}
					}
				}
			}

			results <- struct {
				correct int64
				time    time.Duration
			}{localCorrect, searchTime}
		}(workerID)
	}

	go func() {
		queryWg.Wait()
		close(results)
	}()

	totalSearchTime := time.Duration(0)
	for result := range results {
		totalCorrect += result.correct
		totalSearchTime += result.time
	}

	queryQPS := float64(len(testVectors)) / (float64(totalSearchTime) / float64(time.Second))
	recall := float64(totalCorrect) / float64(len(testVectors)*K)

	log.Infof("Queries complete: %.0fs (%.0f queries/sec)", totalSearchTime.Seconds(), queryQPS)
	log.Infof("Recall: %.3f", recall)
}
