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

package hnsw

import (
	"flag"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"gopkg.in/yaml.v2"
)

var download = flag.Bool("download", false, "download datasets if not found locally")

var datasets = map[string]string{
	"random-xs":              "datasets/big-ann-benchmarks/random10000/data_10000_20",
	"random-xs-clustered":    "datasets/big-ann-benchmarks/random-clustered10000/clu-random.fbin.crop_nb_10000",
	"msturing-1M":            "datasets/big-ann-benchmarks/MSTuringANNS/base1b.fbin.crop_nb_1000000",
	"msturing-10M":           "datasets/big-ann-benchmarks/MSTuringANNS/base1b.fbin.crop_nb_10000000",
	"msspacev-1M":            "datasets/big-ann-benchmarks/MSSPACEV1B/spacev1b_base.i8bin.crop_nb_1000000",
	"msspacev-10M":           "datasets/big-ann-benchmarks/MSSPACEV1B/spacev1b_base.i8bin.crop_nb_10000000",
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
							if !*download {
								b.Skipf(`Neurips23 dataset %s not found.
Run test with -download to automatically download the dataset.
Ex: go test -v -benchmem -bench ^BenchmarkHnswNeurips23$ -download`, step.Dataset)
							}
							downloadDataset(b, step.Dataset)
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
								compressionhelpers.Concurrently(uint64(op.End-op.Start), func(i uint64) {
									err := index.Add(uint64(op.Start+int(i)), vectors[op.Start+int(i)])
									require.NoError(b, err)
								})
							case "delete":
								compressionhelpers.Concurrently(uint64(op.End-op.Start), func(i uint64) {
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

								compressionhelpers.Concurrently(uint64(len(queryVectors)), func(i uint64) {
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

func downloadDataset(t testing.TB, name string) {
	t.Helper()

	ds, ok := datasets[name]
	if !ok {
		t.Fatalf("Dataset %s not found", name)
	}

	qs, ok := queries[name]
	if !ok {
		t.Fatalf("Query file not found for %s dataset", name)
	}

	for _, f := range []string{ds, qs} {
		downloadDatasetFile(t, f)
	}
}

func downloadDatasetFile(t testing.TB, file string) {
	t.Helper()

	if _, err := os.Stat(file); err == nil {
		return
	}

	err := os.MkdirAll(filepath.Dir(file), 0o755)
	require.NoError(t, err)

	path := strings.TrimPrefix(file, "datasets/")

	u, err := url.JoinPath("https://storage.googleapis.com/ann-datasets/", path)
	require.NoError(t, err)

	t.Logf("Downloading dataset from %s", u)

	client := http.Client{
		Timeout: 60 * time.Second,
	}

	resp, err := client.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Could not download dataset. Status code: %d", resp.StatusCode)
	}

	f, err := os.Create(file)
	require.NoError(t, err)
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	require.NoError(t, err)

	t.Logf("Downloaded dataset %s", file)
}

func readBigAnnDataset(t testing.TB, file string, maxObjects int) [][]float32 {
	t.Helper()

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

	// The data is a binary file containing either floating point vectors or int8 vectors
	// It starts with 8 bytes of header data
	// The first 4 bytes are the number of vectors in the file
	// The second 4 bytes are the dimensionality of the vectors in the file
	// If the file is in fbin format, the vector data needs to be converted from bytes to float.
	// If the file is in i8bin format, the vector data needs to be converted from bytes to int8 then to float.

	// The first 4 bytes are the number of vectors in the file
	_, err = f.Read(b)
	require.NoError(t, err)
	n := int32FromBytes(b)

	// The second 4 bytes are the dimensionality of the vectors in the file
	_, err = f.Read(b)
	require.NoError(t, err)
	d := int32FromBytes(b)

	var bytesPerVector int
	switch {
	case strings.Contains(file, "i8bin"):
		bytesPerVector = 1
	case strings.Contains(file, "fbin"):
		fallthrough
	default:
		bytesPerVector = 4
	}

	require.Equal(t, 8+n*d*bytesPerVector, int(fileSize))

	vectorBytes := make([]byte, d*bytesPerVector)
	if maxObjects > 0 && maxObjects < n {
		n = maxObjects
	}

	for i := 0; i < n; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		vectorFloat := make([]float32, 0, d)
		for j := 0; j < d; j++ {
			start := j * bytesPerVector
			var f float32
			if bytesPerVector == 1 {
				f = float32(vectorBytes[start])
			} else {
				f = float32FromBytes(vectorBytes[start : start+bytesPerVector])
			}

			vectorFloat = append(vectorFloat, f)
		}

		vectors = append(vectors, vectorFloat)
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
