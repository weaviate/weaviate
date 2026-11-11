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

package compressionhelpers_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/emeans"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Compares product quantization codebooks trained with k-means and e-means
// on training time, reconstruction error and brute-force recall. Run with
//
//	PQ_BENCH_DIR=<dir with .fvecs files> go test -tags benchmark \
//	  -run TestPQCodebookTraining -v -timeout 0 ./adapters/repos/db/vector/compressionhelpers/
//
// PQ_BENCH_CASES filters cases by substring and PQ_BENCH_SEEDS sets the
// number of seeds (default 3). The last vectors of a file serve as queries.

type codebookTrainer struct {
	name string
	fit  func(data [][]float32, k int, d int, segment int, seed uint64) ([][]float32, error)
}

func kmeansCodebookTrainer(name string, iterations int, delta float32) codebookTrainer {
	return codebookTrainer{name: name, fit: func(data [][]float32, k int, d int, segment int, seed uint64) ([][]float32, error) {
		km := kmeans.New(k, d, segment)
		km.Seed = seed
		km.Initialization = kmeans.RandomInitialization
		km.Assignment = kmeans.GraphPruning
		km.IterationThreshold = iterations
		km.DeltaThreshold = delta
		err := km.Fit(data)
		return km.Centers, err
	}}
}

func emeansCodebookTrainer(epochs int) codebookTrainer {
	return codebookTrainer{name: fmt.Sprintf("emeans-%d", epochs), fit: func(data [][]float32, k int, d int, segment int, seed uint64) ([][]float32, error) {
		em := emeans.New(k, d, segment)
		em.Seed = seed
		em.Epochs = epochs
		err := em.Fit(data)
		return em.Centers, err
	}}
}

var codebookTrainers = []codebookTrainer{
	// The settings of KMeansEncoder.Fit, which trains the codebooks today.
	kmeansCodebookTrainer("kmeans-pq", 10, 0.01),
	kmeansCodebookTrainer("lloyd-25", 25, -1),
	kmeansCodebookTrainer("lloyd-100", 100, -1),
	emeansCodebookTrainer(1),
	emeansCodebookTrainer(2),
	emeansCodebookTrainer(3),
	emeansCodebookTrainer(5),
	emeansCodebookTrainer(10),
	emeansCodebookTrainer(25),
}

type codebookCase struct {
	dataset  string
	metric   string // "l2-squared" or "cosine-dot", which normalizes the vectors.
	n        int    // Indexed vectors, the first TrainingLimit of them train the codebooks.
	segments int    // Zero uses the segments Weaviate picks by default.
}

var codebookCases = []codebookCase{
	{"sift", "l2-squared", 500_000, 0},
	{"sift", "l2-squared", 500_000, 32},
	{"sift", "l2-squared", 500_000, 16},
	{"dbpedia-ada002", "cosine-dot", 99_000, 0},
	{"dbpedia-ada002", "cosine-dot", 99_000, 96},
	{"glove25", "cosine-dot", 500_000, 5},
}

func readFvecsPrefixAndTail(t *testing.T, path string, n int, tail int) ([][]float32, [][]float32) {
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	d := int(binary.LittleEndian.Uint32(raw))
	total := len(raw) / (4 * (d + 1))
	require.GreaterOrEqual(t, total, n+tail)
	read := func(from int, count int) [][]float32 {
		data := make([][]float32, count)
		for i := range data {
			row := raw[(from+i)*4*(d+1)+4 : (from+i+1)*4*(d+1)]
			data[i] = make([]float32, d)
			for j := range d {
				data[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(row[4*j:]))
			}
		}
		return data
	}
	return read(0, n), read(total-tail, tail)
}

func TestPQCodebookTraining(t *testing.T) {
	dir := os.Getenv("PQ_BENCH_DIR")
	if dir == "" {
		t.Skip("PQ_BENCH_DIR is not set")
	}
	seeds := 3
	if value := os.Getenv("PQ_BENCH_SEEDS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		seeds = parsed
	}
	filter := os.Getenv("PQ_BENCH_CASES")
	logger, _ := test.NewNullLogger()
	const k, rescore, queryCount = 10, 100, 1000

	for _, c := range codebookCases {
		name := fmt.Sprintf("%s/segments%d", c.dataset, c.segments)
		if !strings.Contains(name, filter) {
			continue
		}
		t.Run(name, func(t *testing.T) {
			vectors, queries := readFvecsPrefixAndTail(t, dir+"/"+c.dataset+".fvecs", c.n, queryCount)
			if c.metric == "cosine-dot" {
				normalizeVectors(vectors)
				normalizeVectors(queries)
			}
			provider := datasetDistancer(c.metric)
			dims := len(vectors[0])
			segments := c.segments
			if segments == 0 {
				segments = common.CalculateOptimalSegments(dims)
			}
			ds := dims / segments
			cfg := ent.PQConfig{
				Enabled:       true,
				Segments:      segments,
				Centroids:     ent.DefaultPQCentroids,
				TrainingLimit: ent.DefaultPQTrainingLimit,
				Encoder:       ent.PQEncoder{Type: ent.PQEncoderTypeKMeans, Distribution: ent.PQEncoderDistributionLogNormal},
			}
			training := vectors[:min(len(vectors), cfg.TrainingLimit)]

			truths := make([]map[int]struct{}, len(queries))
			compressionhelpers.Concurrently(logger, uint64(len(queries)), func(q uint64) {
				top := newTopK(k)
				for i, v := range vectors {
					dist, _ := provider.SingleDist(queries[q], v)
					top.insert(i, dist)
				}
				truths[q] = make(map[int]struct{}, k)
				for _, id := range top.ids {
					truths[q][id] = struct{}{}
				}
			})

			for _, trainer := range codebookTrainers {
				var fitSeconds, mse, recall, rescored float64
				recalls := make([]float64, seeds)
				for s := range seeds {
					encoders := make([]compression.PQSegmentEncoder, segments)
					start := time.Now()
					// Segments are trained concurrently, as in ProductQuantizer.Fit.
					err := compressionhelpers.ConcurrentlyWithError(logger, uint64(segments), func(segment uint64) error {
						centers, err := trainer.fit(training, cfg.Centroids, ds, int(segment), uint64(1000*s)+segment)
						encoders[segment] = compressionhelpers.NewKMeansEncoderWithCenters(cfg.Centroids, ds, int(segment), centers)
						return err
					})
					require.NoError(t, err)
					fitSeconds += time.Since(start).Seconds() / float64(seeds)

					pq, err := compressionhelpers.NewProductQuantizerWithEncoders(cfg, provider, dims, encoders, logger)
					require.NoError(t, err)

					codes := make([][]byte, len(vectors))
					var mutex sync.Mutex
					var squaredError float64
					compressionhelpers.Concurrently(logger, uint64(len(vectors)), func(i uint64) {
						codes[i] = pq.Encode(vectors[i])
						decoded := pq.Decode(codes[i])
						var sum float64
						for j, z := range vectors[i] {
							diff := float64(z) - float64(decoded[j])
							sum += diff * diff
						}
						mutex.Lock()
						squaredError += sum
						mutex.Unlock()
					})
					mse += squaredError / float64(len(vectors)) / float64(seeds)

					var hits, rescoredHits int
					compressionhelpers.Concurrently(logger, uint64(len(queries)), func(q uint64) {
						distancer := pq.NewDistancer(queries[q])
						defer pq.ReturnDistancer(distancer)
						top, wide := newTopK(k), newTopK(rescore)
						for i := range codes {
							dist, _ := distancer.Distance(codes[i])
							top.insert(i, dist)
							wide.insert(i, dist)
						}
						var found, foundWide int
						for _, id := range top.ids {
							if _, ok := truths[q][id]; ok {
								found++
							}
						}
						for _, id := range wide.ids {
							if _, ok := truths[q][id]; ok {
								foundWide++
							}
						}
						mutex.Lock()
						hits += found
						rescoredHits += foundWide
						mutex.Unlock()
					})
					recalls[s] = float64(hits) / float64(k*len(queries))
					recall += recalls[s] / float64(seeds)
					rescored += float64(rescoredHits) / float64(k*len(queries)) / float64(seeds)
				}
				var spread float64
				for _, r := range recalls {
					spread = max(spread, math.Abs(r-recall))
				}
				// recall@10 ranks by compressed distance alone, 10@100 is the
				// recall after rescoring the best 100 compressed candidates.
				fmt.Fprintf(os.Stderr, "RESULT\t%s\tn=%d\tsegments=%d\tds=%d\t%s\tfit=%.2fs\tmse=%.6g\trecall@10=%.4f\t±%.4f\t10@100=%.4f\n",
					c.dataset, len(vectors), segments, ds, trainer.name, fitSeconds, mse, recall, spread, rescored)
			}
		})
	}
}
