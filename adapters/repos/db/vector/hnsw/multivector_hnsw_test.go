//go:build multiVectorTest
// +build multiVectorTest

package hnsw

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"gonum.org/v1/hdf5"
)

// go get gonum.org/v1/hdf5
// export CGO_LDFLAGS="-L/opt/homebrew/lib"
// export CGO_CFLAGS="-I/opt/homebrew/include"

func TestMultiVector(t *testing.T) {

	ctx := context.Background()
	maxConnections := 64
	efConstruction := 256
	ef := 256

	var dataset string
	var vectors [][][]float32
	var queries [][][]float32
	var vectorIndex *hnsw

	t.Run("load vectors", func(t *testing.T) {
		// load vectors from hdf5 file
		dataset = "lotte-recreation-reduced_-1_-1"
		dataset_path = "/Users/roberto/Desktop/colbert/" + dataset + ".hdf5"

		vectors = loadVectors(dataset)

		queries = loadHdf5Queries(dataset_path, "queries")
		fmt.Printf("Queries shape: %d x %d x %d\n", len(queries), len(queries[0]), len(queries[0][0]))
	})

	t.Run("importing into hnsw", func(t *testing.T) {

		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			MultipleVectorForIDThunk: func(ctx context.Context, docID uint64, relativeVecID uint64) ([]float32, error) {
				return vectors[docID][relativeVecID], nil
			},
		}, ent.UserConfig{
			MaxConnections: maxConnections,
			EFConstruction: efConstruction,
			EF:             ef,
			Multivector:    true,
		}, cyclemanager.NewCallbackGroupNoop(), nil)
		require.Nil(t, err)
		vectorIndex = index
		fmt.Printf("hnsw created\n")
		//workerCount := runtime.GOMAXPROCS(0)
		workerCount := runtime.GOMAXPROCS(0)
		jobsForWorker := make([][][][]float32, workerCount)

		before := time.Now()
		for i, vec := range vectors {
			workerID := i % workerCount
			jobsForWorker[workerID] = append(jobsForWorker[workerID], vec)
		}

		wg := &sync.WaitGroup{}
		for workerID, jobs := range jobsForWorker {
			wg.Add(1)
			go func(workerID int, myJobs [][][]float32) {
				defer wg.Done()
				for i, vec := range myJobs {
					originalIndex := (i * workerCount) + workerID
					err := vectorIndex.AddMulti(ctx, uint64(originalIndex), vec)
					require.Nil(t, err)
				}
			}(workerID, jobs)
		}

		wg.Wait()
		fmt.Printf("importing took %s\n", time.Since(before))
	})

	t.Run("inspect a query", func(t *testing.T) {
		k := 10

		f, err := os.Create("results-" + dataset + ".txt")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		for i, query := range queries {
			ids, distances, err := vectorIndex.SearchByMultipleVector(ctx, query, k, nil)
			require.Nil(t, err)
			f.WriteString(fmt.Sprintf("Query %d: %v, %v\n", i, ids, distances))
		}

	})
}

func loadVectors(dataset string) [][][]float32 {

	vectors := loadHdf5Float32("/Users/roberto/Desktop/colbert/"+dataset+".hdf5", "vectors")
	ids := loadHdf5Int32("/Users/roberto/Desktop/colbert/"+dataset+".hdf5", "ids")

	data := make([][][]float32, ids[len(ids)-1]+1)

	id := ids[0]
	for j, vec := range vectors {
		if ids[j] != id {
			id = ids[j]
		}
		data[id] = append(data[id], vec)
	}

	fmt.Printf("Number of documents: %d\n", len(data))
	fmt.Printf("Vectors shape: %d x %d\n", len(vectors), len(vectors[0]))
	return data
}

func loadHdf5Float32(filename string, dataname string) [][]float32 {

	// Open HDF5 file
	file, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	// Open dataset
	dataset, err := file.OpenDataset(dataname)
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

func loadHdf5Queries(filename string, dataname string) [][][]float32 {

	// Open HDF5 file
	file, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	// Open dataset
	dataset, err := file.OpenDataset(dataname)
	if err != nil {
		log.Fatalf("Error opening queries dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	byteSize := getHDF5ByteSize(dataset)

	if len(dims) != 3 {
		log.Fatal("expected 3 dimensions")
	}

	num_queries := dims[0]
	num_token := dims[1]
	num_dim := dims[2]

	var chunkData [][][]float32

	if byteSize == 4 {
		chunkData1D := make([]float32, num_queries*num_token*num_dim)
		dataset.Read(&chunkData1D)
		chunkData = convert3DChunk[float32](chunkData1D, int(num_queries), int(num_token), int(num_dim))
	} else if byteSize == 8 {
		chunkData1D := make([]float64, num_queries*num_token*num_dim)
		dataset.Read(&chunkData1D)
		chunkData = convert3DChunk[float64](chunkData1D, int(num_queries), int(num_token), int(num_dim))
	}

	return chunkData
}

func convert3DChunk[D float32 | float64](input []D, num_queries int, num_token int, num_dim int) [][][]float32 {
	chunkData := make([][][]float32, num_queries)
	for i := range chunkData {
		chunkData[i] = make([][]float32, num_token)
		for j := 0; j < num_token; j++ {
			chunkData[i][j] = make([]float32, num_dim)
			for k := 0; k < num_dim; k++ {
				chunkData[i][j][k] = float32(input[i*num_dim*num_token+j*num_dim+k])
			}
		}
	}
	return chunkData
}

func loadHdf5Int32(filename string, dataname string) []uint64 {

	// Open HDF5 file
	file, err := hdf5.OpenFile(filename, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatalf("Error opening file: %v\n", err)
	}
	defer file.Close()

	// Open dataset
	dataset, err := file.OpenDataset(dataname)
	if err != nil {
		log.Fatalf("Error opening loadHdf5Float32 dataset: %v", err)
	}
	defer dataset.Close()
	dataspace := dataset.Space()
	dims, _, _ := dataspace.SimpleExtentDims()

	if len(dims) != 2 {
		log.Fatal("expected 2 dimensions")
	}

	rows := dims[0]
	dimensions := dims[1]

	var chunkData []uint64

	data_int32 := make([]int32, rows*dimensions)
	dataset.Read(&data_int32)
	chunkData = convertint32toUint64(data_int32)

	return chunkData
}

func convertint32toUint64(input []int32) []uint64 {
	chunkData := make([]uint64, len(input))
	for i := range chunkData {
		chunkData[i] = uint64(input[i])
	}
	return chunkData
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
