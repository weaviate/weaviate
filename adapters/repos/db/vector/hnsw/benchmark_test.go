package hnsw

import (
	"io"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"gopkg.in/yaml.v2"
)

var datasets = map[string]string{
	"random-xs":              "datasets/big-ann-benchmarks/random10000/data_10000_20",
	"random-xs-clustered":    "datasets/big-ann-benchmarks/random-clustered10000/clu-random.fbin.crop_nb_10000",
	"msturing-1M":            "datasets/big-ann-benchmarks/MSTuringANNS/base1b.fbin",
	"msturing-10M":           "datasets/big-ann-benchmarks/MSTuringANNS/base1b.fbin",
	"msspacev-1M":            "datasets/big-ann-benchmarks/MSSPACEV1B/spacev1b_base.i8bin",
	"msspacev-10M":           "datasets/big-ann-benchmarks/MSSPACEV1B/spacev1b_base.i8bin",
	"msturing-10M-clustered": "datasets/big-ann-benchmarks/MSTuring-10M-clustered/msturing-10M-clustered.fbin",
}

var queries = map[string]string{
	"random-xs":              "datasets/big-ann-benchmarks/random10000/queries_1000_20",
	"random-xs-clustered":    "datasets/big-ann-benchmarks/random-clustered10000/queries_1000_20.fbin",
	"msturing-1M":            "datasets/big-ann-benchmarks/MSTuringANNS/query100K.fbin",
	"msturing-10M":           "datasets/big-ann-benchmarks/MSTuringANNS/query100K.fbin",
	"msspacev-1M":            "datasets/big-ann-benchmarks/MSSPACEV1B/query.i8bin",
	"msspacev-10M":           "datasets/big-ann-benchmarks/MSSPACEV1B/query.i8bin",
	"msturing-10M-clustered": "datasets/big-ann-benchmarks/MSTuring-10M-clustered/testQuery10K.fbin",
}

func BenchmarkHnswNeurips23(b *testing.B) {
	runbooks := []string{
		"datasets/neurips23/simple_runbook.yaml",
		"datasets/neurips23/clustered_runbook.yaml",
	}

	type datasetPoints struct {
		dataset string
		points  int
	}

	readDatasets := make(map[datasetPoints][][]float32)

	for _, runbookFile := range runbooks {
		b.Run(runbookFile, func(b *testing.B) {
			runbook := readRunbook(b, runbookFile)

			for _, step := range runbook.Steps {
				b.Run(step.Dataset, func(b *testing.B) {
					// Read the dataset if we haven't already
					vectors, ok := readDatasets[datasetPoints{step.Dataset, step.MaxPts}]
					if !ok {
						file, ok := datasets[step.Dataset]
						if !ok {
							b.Skipf("Neurips23 dataset %s not found", step.Dataset)
						}

						if _, err := os.Stat(file); err != nil {
							b.Skipf("Neurips23 dataset %s not found", step.Dataset)
						}

						readDatasets[datasetPoints{step.Dataset, step.MaxPts}] = readBigAnnDataset(b, file, step.MaxPts)
						vectors = readDatasets[datasetPoints{step.Dataset, step.MaxPts}]
					}

					var queryVectors [][]float32

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						index := createEmptyHnswIndexForTests(b, idVectorSize(len(vectors[0])))

						for _, op := range step.Operations {
							switch op.Operation {
							case "insert":
								ssdhelpers.Concurrently(uint64(op.End-op.Start), func(i uint64) {
									err := index.Add(uint64(op.Start+int(i)), vectors[op.Start+int(i)])
									require.NoError(b, err)
								})
							case "delete":
								ssdhelpers.Concurrently(uint64(op.End-op.Start), func(i uint64) {
									err := index.Delete(uint64(op.Start + int(i)))
									require.NoError(b, err)
								})
							case "search":
								if len(queryVectors) == 0 {
									file, ok := queries[step.Dataset]
									if !ok {
										b.Errorf("query file: not found for %s dataset", step.Dataset)
									}

									queryVectors = readBigAnnDataset(b, file, 0)
								}

								ssdhelpers.Concurrently(uint64(len(queryVectors)), func(i uint64) {
									_, _, err := index.SearchByVector(queryVectors[i], 0, nil)
									require.NoError(b, err)
								})
							default:
								b.Errorf("Unknown operation %s", op.Operation)
							}
						}
					}
				})
			}
		})
	}
}

func readBigAnnDataset(t testing.TB, file string, maxObjects int) [][]float32 {
	var vectors [][]float32

	f, err := os.Open(file)
	if err != nil {
		panic(errors.Wrap(err, "Could not open SIFT file"))
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(errors.Wrap(err, "Could not get SIFT file properties"))
	}
	fileSize := fi.Size()

	b := make([]byte, 4)

	// The data is a binary file containing floating point vectors
	// It starts with 8 bytes of header data
	// The first 4 bytes are the number of vectors in the file
	// The second 4 bytes are the dimensionality of the vectors in the file
	// The vector data needs to be converted from bytes to float
	// Note that the vector entries are of type float but are integer numbers eg 2.0

	// The first 4 bytes are the number of vectors in the file
	_, err = f.Read(b)
	require.NoError(t, err)
	n := int32FromBytes(b)

	// The second 4 bytes are the dimensionality of the vectors in the file
	_, err = f.Read(b)
	require.NoError(t, err)
	d := int32FromBytes(b)

	bytesPerF := 4

	require.Equal(t, 8+n*d*bytesPerF, int(fileSize))

	vectorBytes := make([]byte, d*bytesPerF)
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		vectorFloat := make([]float32, 0, d)
		for j := 0; j < d; j++ {
			start := j * bytesPerF
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}

		vectors = append(vectors, vectorFloat)

		if maxObjects > 0 && i >= maxObjects {
			break
		}
	}

	if maxObjects > 0 {
		require.Equal(t, maxObjects, len(vectors))
	}

	return vectors
}

type runbook struct {
	Steps []runbookStep
}
type runbookStep struct {
	Dataset    string
	MaxPts     int
	Operations []runbookOperation
}

type runbookOperation struct {
	Operation string
	Start     int
	End       int
}

func readRunbook(t testing.TB, file string) *runbook {
	f, err := os.Open(file)
	require.NoError(t, err, "Could not open runbook file")
	defer f.Close()

	d := yaml.NewDecoder(f)

	var runbook runbook

	var m map[string]map[string]any
	err = d.Decode(&m)
	require.NoError(t, err)

	var datasets []string
	for datasetName := range m {
		datasets = append(datasets, datasetName)
	}

	sort.Strings(datasets)

	for _, datasetName := range datasets {
		stepInfo := m[datasetName]
		var step runbookStep

		step.Dataset = datasetName
		step.MaxPts = stepInfo["max_pts"].(int)
		i := 1
		for {
			s := strconv.Itoa(i)
			if _, ok := stepInfo[s]; !ok {
				break
			}

			opInfo := stepInfo[s].(map[any]any)

			var op runbookOperation
			op.Operation = opInfo["operation"].(string)
			if op.Operation == "insert" || op.Operation == "delete" {
				op.Start = opInfo["start"].(int)
				op.End = opInfo["end"].(int)
			}

			step.Operations = append(step.Operations, op)

			i++
		}

		runbook.Steps = append(runbook.Steps, step)
	}

	return &runbook
}
