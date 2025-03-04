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
	"log"
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

func TestIVF(t *testing.T) {
	log.Println("Starting...")
	var ivf *ivfpq.IvfPQ
	file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/dbpedia-100k-openai-ada002.hdf5", hdf5.F_ACC_RDONLY)
	//file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	assert.Nil(t, err)
	defer file.Close()
	dataset, err := file.OpenDataset("train")
	defer dataset.Close()
	vectors := make([][]float32, 1_000_000)
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
		if i == 10_000 {
			go func() {
				defer wg1.Done()
				ivf = ivfpq.NewIvf(vectors[:10_000], distancer.NewCosineDistanceProvider())
				//ivf = ivfpq.NewIvfLSH(vectors[:10_000], distancer.NewCosineDistanceProvider())
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
				ivf.Add(uint64(current), vectors[current])
			}
		}(i)
	}
	wg.Wait()
	log.Println(time.Since(before))
	neighbors := loadHdf5Neighbors(file, "neighbors")
	testData := loadHdf5Float32(file, "test")

	var wg2 sync.WaitGroup
	rows = 500
	recall := float32(0)
	totaEllapsed := time.Duration(0)
	lock := sync.Mutex{}
	min, max := 100000, 0
	average := 0.0
	rmin, rmax := 100000, 0
	raverage := 0.0
	concurrencyLevel := 10
	for i := 0; i < concurrencyLevel; i++ {
		wg2.Add(1)
		go func(id int) {
			defer wg2.Done()
			for current := id * (int(rows) / concurrencyLevel); current < (id+1)*(int(rows)/concurrencyLevel); current++ {
				before = time.Now()
				ids, _, _ := ivf.SearchByVector(context.Background(), testData[current], 10)
				ellapsed := time.Since(before)
				rec := float32(testinghelpers.MatchesInLists(neighbors[current][:10], ids)) / 10

				currNeighbors := neighbors[current][:10]
				lastIndex := 0
				for index, x := range ids {
					found := false
					for _, y := range currNeighbors {
						if x == y {
							found = true
							break
						}
					}
					if found {
						lastIndex = index
					}
				}
				rlastIndex := len(ids)
				if min > lastIndex {
					min = lastIndex
				}
				if max < lastIndex {
					max = lastIndex
				}
				average += float64(lastIndex) / float64(rows)
				if rmin > rlastIndex {
					rmin = rlastIndex
				}
				if rmax < rlastIndex {
					rmax = rlastIndex
				}
				raverage += float64(rlastIndex) / float64(rows)

				lock.Lock()
				totaEllapsed += ellapsed
				recall += rec / float32(rows)
				lock.Unlock()
			}
		}(i)
	}
	wg2.Wait()
	log.Println("Latency: ", totaEllapsed.Milliseconds()/int64(rows), "ms")
	log.Println("Recall: ", recall)
	log.Println(min, max, average)
	log.Println(rmin, rmax, raverage)
	assert.Nil(t, ivf)
}
