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
	"log"

	"gonum.org/v1/hdf5"
)

// NOTE: To get HDF5 to work I had to install hdf5 using homebrew and set the
// environment variables:
// CGO_CFLAGS="-I/opt/homebrew/include"
// CGO_LDFLAGS="-L/opt/homebrew/lib"

// The HDF5 files we are going to read have this layout:
//
// <HDF5 file "dbpedia-100k-openai-ada002-angular.hdf5" (mode r)>
// ├── neighbors [int64 (973, 100)]
// ├── test [float64 (973, 1536)]
// └── train [float64 (100000, 1536)]

// <HDF5 file "dbpedia-100k-openai-3large-dot.hdf5" (mode r)>
// ├── neighbors [int64 (1000, 100)]
// ├── test [float64 (1000, 3072)]
// └── train [float64 (100000, 3072)]

// <HDF5 file "deep-image-96-angular.hdf5" (mode r)>
// ├── distances [float32 (10000, 100)]
// ├── neighbors [int32 (10000, 100)]
// ├── test [float32 (10000, 96)]
// └── train [float32 (9990000, 96)]

// <HDF5 file "fashion-mnist-784-euclidean.hdf5" (mode r)>
// ├── distances [float32 (10000, 100)]
// ├── neighbors [int32 (10000, 100)]
// ├── test [int64 (10000, 784)]
// └── train [int64 (60000, 784)]

// <HDF5 file "gist-960-euclidean.hdf5" (mode r)>
// ├── distances [float32 (1000, 100)]
// ├── neighbors [int32 (1000, 100)]
// ├── test [float32 (1000, 960)]
// └── train [float32 (1000000, 960)]

// <HDF5 file "snowflake-msmarco-arctic-embed-m-v1.5-angular.hdf5" (mode r)>
// ├── distances [float32 (100000, 100)]
// ├── neighbors [int64 (100000, 100)]
// ├── test [float32 (100000, 768)]
// └── train [float32 (8741823, 768)]

// <HDF5 file "dbpedia-500k-openai-ada002-euclidean.hdf5" (mode r)>
// ├── neighbors [int64 (2000, 100)]
// ├── test [float64 (2000, 1536)]
// └── train [float64 (500000, 1536)]

// <HDF5 file "dbpedia-100k-openai-ada002-euclidean.hdf5" (mode r)>
// ├── neighbors [int64 (500, 100)]
// ├── test [float64 (500, 1536)]
// └── train [float64 (100000, 1536)]

// <HDF5 file "glove-200-angular.hdf5" (mode r)>
// ├── distances [float32 (10000, 100)]
// ├── neighbors [int32 (10000, 100)]
// ├── test [float32 (10000, 200)]
// └── train [float32 (1183514, 200)]

// <HDF5 file "sphere-1M-meta-dpr.hdf5" (mode r)>
// ├── neighbors [int64 (10000, 100)]
// ├── test [float64 (10000, 768)]
// └── train [float64 (990000, 768)]

// <HDF5 file "sift-128-euclidean.hdf5" (mode r)>
// ├── distances [float32 (10000, 100)]
// ├── neighbors [int32 (10000, 100)]
// ├── test [float32 (10000, 128)]
// └── train [float32 (1000000, 128)]

// <HDF5 file "dbpedia-openai-1000k-angular.hdf5" (mode r)>
// ├── distances [float64 (10000, 100)]
// ├── neighbors [int64 (10000, 100)]
// ├── test [float32 (10000, 1536)]
// └── train [float32 (990000, 1536)]

func getHDF5ByteSize(dataset *hdf5.Dataset) uint {
	datatype, err := dataset.Datatype()
	if err != nil {
		log.Fatalf("Unabled to read datatype\n")
	}

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

	switch byteSize {
	case 4:
		chunkData1D := make([]float32, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float32](chunkData1D, int(dimensions), int(rows))
	case 8:
		chunkData1D := make([]float64, rows*dimensions)
		dataset.Read(&chunkData1D)
		chunkData = convert1DChunk[float64](chunkData1D, int(dimensions), int(rows))
	default:
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
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

	switch byteSize {
	case 4:
		chunkData1D := make([]int32, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = make([]uint64, dimensions)
			for j := uint(0); j < dimensions; j++ {
				chunkData[i][j] = uint64(chunkData1D[uint(i)*dimensions+j])
			}
		}
	case 8:
		chunkData1D := make([]uint64, rows*dimensions)
		dataset.Read(&chunkData1D)
		for i := range chunkData {
			chunkData[i] = chunkData1D[i*int(dimensions) : (i+1)*int(dimensions)]
		}
	default:
		log.Fatalf("Unable to load dataset with byte size %d\n", byteSize)
	}

	return chunkData
}
