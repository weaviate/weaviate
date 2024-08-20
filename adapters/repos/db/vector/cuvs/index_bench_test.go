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
	"log"
	"testing"
	"time"

	cuvs "github.com/rapidsai/cuvs/go"
	"github.com/sirupsen/logrus/hooks/test"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"

	"github.com/weaviate/hdf5"
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

func TestBench(t *testing.T) {

	logger, _ := test.NewNullLogger()

	index, err := New(Config{"a", "vector", logger, cuvs.DistanceL2}, cuvsEnt.UserConfig{}, nil)
	println("here")
	if err != nil {
		panic(err)
	}

	file, err := hdf5.OpenFile("/home/ajit/datasets/sift-128-euclidean.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		t.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	dataset, err := file.OpenDataset("train")

	LoadVectors(index, dataset, t)

	QueryVectors(index, dataset, t)

}

func LoadVectors(index *cuvs_index, dataset *hdf5.Dataset, t *testing.T) {

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		t.Fatal("expected 2 dimensions")
	}

	byteSize, _ := getHDF5ByteSize(dataset)

	rows := dims[0]
	dimensions := dims[1]

	rows = uint(1_000_000)

	// batchSize := uint(30_000)
	batchSize := uint(10_000)

	// Handle offsetting the data for product quantization
	// i := uint(0)

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	start := time.Now()

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

		// for i := range chunkData {
		// 	for j := range chunkData[i] {
		// 		println(chunkData[i][j])
		// 	}
		// }

		// for i := range chunkData {

		ids := make([]uint64, batchSize)

		for j := uint(0); j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)

		}

		err := index.AddBatch(context.Background(), ids, chunkData)
		if err != nil {
			panic(err)
		}

		// }

	}

	elapsed := time.Since(start)
	println("elapsed time: ", elapsed.Seconds())
	println("QPS: ", float64(rows)/elapsed.Seconds())
}

func QueryVectors(index *cuvs_index, dataset *hdf5.Dataset, t *testing.T) {

	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		t.Fatal("expected 2 dimensions")
	}

	byteSize, _ := getHDF5ByteSize(dataset)

	rows := dims[0]
	dimensions := dims[1]

	rows = uint(100_000)

	// batchSize := uint(30_000)
	batchSize := uint(20_000)

	// Handle offsetting the data for product quantization
	// i := uint(0)

	memspace, err := hdf5.CreateSimpleDataspace([]uint{batchSize, dimensions}, []uint{batchSize, dimensions})
	if err != nil {
		log.Fatalf("Error creating memspace: %v", err)
	}
	defer memspace.Close()

	start := time.Now()

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

		// for i := range chunkData {
		// 	for j := range chunkData[i] {
		// 		println(chunkData[i][j])
		// 	}
		// }

		// for i := range chunkData {

		ids := make([]uint64, batchSize)

		for j := uint(0); j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)

		}

		K := 10

		_, _, err := index.SearchByVector(chunkData, K, nil)
		if err != nil {
			panic(err)
		}

		// }

	}

	elapsed := time.Since(start)
	println("elapsed time (query): ", elapsed.Seconds())
	println("QPS: ", float64(rows)/elapsed.Seconds())
}
