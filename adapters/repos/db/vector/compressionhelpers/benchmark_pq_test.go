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

package compressionhelpers_test

import (
	"log"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	training_limit = 10_000
	vectors_size   = 100_000
	probe          = 100
	k              = 10
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

func BenchmarkPQ(b *testing.B) {
	distancerProvider := distancer.NewCosineDistanceProvider()
	pq, err := compressionhelpers.NewProductQuantizer(hnsw.PQConfig{
		Enabled:        true,
		BitCompression: false,
		Segments:       192,
		Centroids:      256,
		TrainingLimit:  training_limit,
		Encoder: hnsw.PQEncoder{
			Type:         hnsw.PQEncoderTypeKMeans,
			Distribution: hnsw.PQEncoderDistributionLogNormal,
		},
	}, distancerProvider, 1536, logrus.New())
	codes := make([][]byte, vectors_size)
	if err != nil {
		log.Fatal(err)
	}
	file, err := hdf5.OpenFile("/Users/abdel/Documents/datasets/dbpedia-100k-openai-ada002.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	dataset, err := file.OpenDataset("train")
	if err != nil {
		log.Fatal(err)
	}
	defer dataset.Close()
	vectors := make([][]float32, vectors_size)
	batchSize := uint(1_000)
	dataspace := dataset.Space()
	byteSize := getHDF5ByteSize(dataset)
	if err != nil {
		log.Fatal(err)
	}
	dims, _, _ := dataspace.SimpleExtentDims()
	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}
	rows := dims[0]
	dimensions := dims[1]
	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatal(err)
	}
	defer memspace.Close()
	var wg1 sync.WaitGroup
	wg1.Add(1)
	for i := uint(0); i < rows; i += batchSize {
		batchRows := batchSize
		// handle final smaller batch
		if i+batchSize > rows {
			batchRows = rows - i
			memspace, err = hdf5.CreateSimpleDataspace([]uint{batchRows, dimensions}, []uint{batchRows, dimensions})
			if err != nil {
				log.Fatal(err)
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

			chunkData = convert1DChunk(chunkData1D, int(dimensions), int(batchRows))

		} else if byteSize == 8 {
			chunkData1D := make([]float64, batchRows*dimensions)

			if err := dataset.ReadSubset(&chunkData1D, memspace, dataspace); err != nil {
				log.Printf("BatchRows = %d, i = %d, rows = %d", batchRows, i, rows)
				log.Fatalf("Error reading subset: %v", err)
			}

			chunkData = convert1DChunk(chunkData1D, int(dimensions), int(batchRows))

		}
		if i == training_limit {
			go func() {
				defer wg1.Done()
				pq.Fit(vectors[:training_limit])
			}()
		}
		copy(vectors[i:], chunkData)
	}
	wg1.Wait()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for current := id * (int(rows) / 10); current < (id+1)*(int(rows)/10); current++ {
				codes[current] = pq.Encode(vectors[current])
			}
		}(i)
	}
	wg.Wait()
	neighbors := loadHdf5Neighbors(file, "neighbors")
	testData := loadHdf5Float32(file, "test")

	rows = 100
	recall := float32(0)
	b.ResetTimer()
	N := b.N
	for i := 0; i < N; i++ {
		current := i % int(rows)
		heap := priorityqueue.NewMax[any](probe)
		distancer := pq.NewDistancer(testData[current])
		for j := 0; j < len(codes); j++ {
			d, _ := distancer.Distance(codes[j])
			heap.Insert(uint64(j), d)
			if heap.Len() > probe {
				heap.Pop()
			}
		}
		ids := make([]uint64, heap.Len())
		j := heap.Len() - 1
		for heap.Len() > 0 {
			ids[j] = heap.Pop().ID
			j--
		}
		//calculate recall
		recall += float32(matchesInLists(neighbors[current][:k], ids)) / 10
	}
	log.Println("Recall: ", recall/float32(N))
}
