//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build benchmarkBQRecall
// +build benchmarkBQRecall

package ssdhelpers_test

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	testinghelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

const datasetPath = "/Users/abdel/Documents/weaviate-chaos-engineering-grpc-batching/apps/ann-benchmarks/dbpedia-openai-1000k-angular.hdf5"

func TestBinaryQuantizer(t *testing.T) {
	vectors, err := parseAllVectorsFromHDF5Dataset(datasetPath)
	assert.Nil(t, err)
	assert.NotNil(t, vectors)
	queryVecs, neighbors, err := parseQueryVectorsFromHDF5Dataset(datasetPath)
	assert.Nil(t, err)
	assert.NotNil(t, queryVecs)
	assert.NotNil(t, neighbors)

	bq := ssdhelpers.NewBinaryQuantizer()
	bq.Fit(vectors[:10000])

	codes := make([][]uint64, len(vectors))
	ssdhelpers.Concurrently(uint64(len(vectors)), func(i uint64) {
		codes[i], _ = bq.Encode(vectors[i])
	})
	assert.NotNil(t, codes)
	k := 10
	correctedK := 200
	hits := uint64(0)
	mutex := sync.Mutex{}
	duration := time.Duration(0)
	ssdhelpers.Concurrently(uint64(len(queryVecs)), func(i uint64) {
		before := time.Now()
		query, _ := bq.Encode(queryVecs[i])
		heap := priorityqueue.NewMax(correctedK)
		for j := range codes {
			d, _ := bq.DistanceBetweenCompressedVectors(codes[j], query)
			if heap.Len() < correctedK || heap.Top().Dist > d {
				if heap.Len() == correctedK {
					heap.Pop()
				}
				heap.Insert(uint64(j), d)
			}
		}
		ids := make([]uint64, correctedK)
		for j := range ids {
			ids[j] = heap.Pop().ID
		}
		mutex.Lock()
		duration += time.Since(before)
		hits += testinghelpers.MatchesInLists(toUint64Slice(neighbors[i][:k]), ids)
		mutex.Unlock()
	})
	recall := float32(hits) / float32(k*len(queryVecs))
	latency := float32(duration.Microseconds()) / float32(len(queryVecs))
	fmt.Println(recall, latency)
	assert.True(t, recall == 1.1)
}

func toUint64Slice(x []int) []uint64 {
	res := make([]uint64, len(x))
	for i := range x {
		res[i] = uint64(x[i])
	}
	return res
}

func parseAllVectorsFromHDF5Dataset(path string) ([][]float32, error) {
	file, err := hdf5.OpenFile(path, hdf5.F_ACC_RDONLY)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return loadHdf5Float32(file, "train")
}

func parseQueryVectorsFromHDF5Dataset(path string) ([][]float32, [][]int, error) {
	file, err := hdf5.OpenFile(path, hdf5.F_ACC_RDONLY)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	test, err := loadHdf5Float32(file, "test")
	if err != nil {
		return nil, nil, err
	}
	neighbours, err := loadHdf5Neighbors(file, "neighbors")
	return test, neighbours, err
}

func loadHdf5Float32(file *hdf5.File, name string) ([][]float32, error) {
	dataset, err := file.OpenDataset(name)
	if err != nil {
		return nil, err
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	byteSize, err := getHDF5ByteSize(dataset)
	if err != nil {
		return nil, err
	}

	if len(dims) != 2 {
		return nil, errors.New("expected 2 dimensions")
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

	return chunkData, nil
}

func loadHdf5Neighbors(file *hdf5.File, name string) ([][]int, error) {
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

	byteSize, err := getHDF5ByteSize(dataset)
	if err != nil {
		return nil, err
	}

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

	return chunkData, nil
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

func getHDF5ByteSize(dataset *hdf5.Dataset) (uint, error) {
	datatype, err := dataset.Datatype()
	if err != nil {
		return 0, errors.New("Unabled to read datatype\n")
	}

	byteSize := datatype.Size()
	if byteSize != 4 && byteSize != 8 {
		return 0, fmt.Errorf("Unable to load dataset with byte size %d\n", byteSize)
	}
	return byteSize, nil
}
