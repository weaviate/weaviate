//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build benchmark

package emeans

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
)

// Compares e-means with the k-means implementation used for product
// quantization on wall-clock time and mean squared error. Run with
//
//	EMEANS_BENCH_DIR=<dir with .fvecs files> go test -tags benchmark \
//	  -run TestCompareWithKMeans -v -timeout 0 ./adapters/repos/db/vector/emeans/
//
// EMEANS_BENCH_CASES filters cases by substring, EMEANS_BENCH_SEEDS sets the
// number of seeds (default 3, large cases always use one). Without a directory only synthetic data is used.

func readFvecs(t *testing.T, path string, limit int) [][]float32 {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	d := int(binary.LittleEndian.Uint32(raw))
	n := min(limit, len(raw)/(4*(d+1)))
	data := make([][]float32, n)
	for i := range data {
		row := raw[i*4*(d+1)+4 : (i+1)*4*(d+1)]
		data[i] = make([]float32, d)
		for j := range d {
			data[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(row[4*j:]))
		}
	}
	return data
}

// meanSquaredError assigns every point to its nearest center in parallel.
func meanSquaredError(data [][]float32, centers [][]float32, d int) float64 {
	provider := distancer.NewL2SquaredProvider()
	workers := 8
	sums := make([]float64, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < len(data); i += workers {
				var minDist float32 = math.MaxFloat32
				for c := range centers {
					if dist, _ := provider.SingleDist(data[i][:d], centers[c]); dist < minDist {
						minDist = dist
					}
				}
				sums[w] += float64(minDist)
			}
		}()
	}
	wg.Wait()
	var total float64
	for _, sum := range sums {
		total += sum
	}
	return total / float64(len(data))
}

type fitter struct {
	name string
	fit  func(data [][]float32, k int, d int, seed uint64) ([][]float32, int, error)
}

func kmeansFitter(name string, init kmeans.InitializationStrategy, assignment kmeans.AssignmentStrategy, iterations int, delta float32) fitter {
	return fitter{name: name, fit: func(data [][]float32, k int, d int, seed uint64) ([][]float32, int, error) {
		km := kmeans.New(k, d, 0)
		km.Seed = seed
		km.Initialization = init
		km.Assignment = assignment
		km.IterationThreshold = iterations
		km.DeltaThreshold = delta
		err := km.Fit(data)
		return km.Centers, km.Metrics.Iterations, err
	}}
}

func emeansFitter(name string, epochs int, mutate func(em *EMeans)) fitter {
	return fitter{name: name, fit: func(data [][]float32, k int, d int, seed uint64) ([][]float32, int, error) {
		em := New(k, d, 0)
		em.Seed = seed
		em.Epochs = epochs
		if mutate != nil {
			mutate(em)
		}
		err := em.Fit(data)
		return em.Centers, epochs, err
	}}
}

var fitters = []fitter{
	// The configuration used by the product quantization encoder.
	kmeansFitter("kmeans-pq(rand,prune,10it,delta.01)", kmeans.RandomInitialization, kmeans.GraphPruning, 10, 0.01),
	// The defaults of kmeans.New().
	kmeansFitter("kmeans-default(++,prune,10it,delta.01)", kmeans.PlusPlusInitialization, kmeans.GraphPruning, 10, 0.01),
	kmeansFitter("kmeans-brute(rand,brute,10it)", kmeans.RandomInitialization, kmeans.BruteForce, 10, -1),
	kmeansFitter("lloyd-25(rand,prune,25it)", kmeans.RandomInitialization, kmeans.GraphPruning, 25, -1),
	emeansFitter("emeans-1", 1, nil),
	emeansFitter("emeans-2", 2, nil),
	emeansFitter("emeans-3", 3, nil),
	emeansFitter("emeans-5", 5, nil),
	emeansFitter("emeans-10", 10, nil),
	emeansFitter("emeans-3-quality", 3, func(em *EMeans) { em.BatchLaw = QualityBatchLaw }),
	emeansFitter("emeans-3-throughput", 3, func(em *EMeans) { em.BatchLaw = ThroughputBatchLaw }),
	emeansFitter("emeans-3-plusplus", 3, func(em *EMeans) { em.Initialization = PlusPlusInitialization }),
}

type compareCase struct {
	dataset string // File name without extension, or "normal" / "blobs" for synthetic data.
	n       int    // Training points.
	d       int    // Leading dimensions used, as for the first segment of a product quantizer.
	k       int
}

func (c compareCase) String() string {
	return fmt.Sprintf("%s/n%d/d%d/k%d", c.dataset, c.n, c.d, c.k)
}

var compareCases = []compareCase{
	// Product quantization: 100k training vectors, 256 centroids, short segments.
	{"normal", 100_000, 4, 256},
	{"normal", 100_000, 8, 256},
	{"normal", 100_000, 32, 256},
	{"blobs", 100_000, 8, 256},
	{"blobs", 100_000, 32, 256},
	{"glove25", 100_000, 5, 256},
	{"sift", 100_000, 8, 256},
	{"sift", 100_000, 16, 256},
	{"dbpedia-ada002", 90_000, 8, 256},
	// Few training vectors per centroid.
	{"sift", 2560, 8, 256},
	{"sift", 10_000, 8, 256},
	// Whole vectors, as for coarse quantizers and posting splits.
	{"glove25", 100_000, 25, 256},
	{"glove25", 990_000, 25, 1024},
	{"sift", 100_000, 128, 256},
	{"sift", 100_000, 128, 1024},
	{"sift", 990_000, 128, 1024},
	{"dbpedia-ada002", 90_000, 1536, 256},
	{"dbpedia-ada002", 90_000, 1536, 2},
}

const compareTestPoints = 10_000

func loadCompareData(t *testing.T, c compareCase, cache map[string][][]float32) (train [][]float32, test [][]float32) {
	total := c.n + compareTestPoints
	switch c.dataset {
	case "normal":
		data := normalData(total, c.d, seed)
		return data[:c.n], data[c.n:]
	case "blobs":
		data, _ := blobs(total, c.d, c.k, seed)
		// blobs assigns point i to cluster i % k, so any prefix is balanced.
		return data[:c.n], data[c.n:]
	}
	dir := os.Getenv("EMEANS_BENCH_DIR")
	if dir == "" {
		t.Skip("EMEANS_BENCH_DIR is not set")
	}
	data, ok := cache[c.dataset]
	if !ok {
		data = readFvecs(t, filepath.Join(dir, c.dataset+".fvecs"), math.MaxInt)
		cache[c.dataset] = data
	}
	if len(data) < total {
		t.Skipf("%s has %d vectors, need %d", c.dataset, len(data), total)
	}
	// The held-out points are always the tail of the file.
	return data[:c.n], data[len(data)-compareTestPoints:]
}

func TestCompareWithKMeans(t *testing.T) {
	seeds := 3
	if value := os.Getenv("EMEANS_BENCH_SEEDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		seeds = parsed
	}
	filter := os.Getenv("EMEANS_BENCH_CASES")
	cache := make(map[string][][]float32)

	for _, c := range compareCases {
		if !strings.Contains(c.String(), filter) {
			continue
		}
		t.Run(c.String(), func(t *testing.T) {
			train, test := loadCompareData(t, c, cache)
			seeds := seeds
			if c.n*c.d*c.k > 1e10 {
				seeds = 1 // A single fit already takes minutes.
			}
			var baseline float64
			for _, f := range fitters {
				times := make([]float64, 0, seeds)
				var trainMSE, testMSE float64
				var iterations int
				for s := range seeds {
					start := time.Now()
					centers, iters, err := f.fit(train, c.k, c.d, seed+uint64(s))
					times = append(times, time.Since(start).Seconds())
					require.NoError(t, err)
					trainMSE += meanSquaredError(train, centers, c.d) / float64(seeds)
					testMSE += meanSquaredError(test, centers, c.d) / float64(seeds)
					iterations += iters
				}
				slices.Sort(times)
				median := times[len(times)/2]
				if baseline == 0 {
					baseline = testMSE
				}
				// The fit time is the median, the errors are means over the seeds.
				fmt.Fprintf(os.Stderr, "RESULT\t%s\t%d\t%d\t%d\t%s\t%.1f\t%.4f\t%.6g\t%.6g\t%+.2f%%\n",
					c.dataset, c.n, c.d, c.k, f.name, float64(iterations)/float64(seeds), median,
					trainMSE, testMSE, 100*(testMSE/baseline-1))
			}
		})
	}
}
