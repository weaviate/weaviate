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
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Compares quantizers on real embedding datasets from the
// weaviate/ann-datasets HuggingFace repo. The datasets are downloaded on
// first use and served from the local HuggingFace cache afterwards.
//
// Run with:
//
//	go test -run xxx -bench BenchmarkQuantizerDataset -benchtime 1x \
//	  ./adapters/repos/db/vector/compressionhelpers/
//
// Reported metrics per dataset and quantizer:
//   - recall@10: brute-force top-10 by quantized distance vs ground truth.
//   - rescored@10: top-50 by quantized distance, re-ranked with exact float
//     distances, then top-10 vs ground truth.
//   - m.dists/sec: quantized distance computations per second, including the
//     per-query distancer setup amortized over the corpus.
type datasetConfig struct {
	subset     string
	metric     string
	numQueries int
}

var datasetConfigs = []datasetConfig{
	{subset: "fiqa-st-minilm-384-dot-12k", metric: "dot", numQueries: 1000},
	{subset: "beir-cohere-v3-1024-euclidean-20k", metric: "l2-squared", numQueries: 1000},
	{subset: "dbpedia-openai-ada002-1536-angular-20k", metric: "cosine-dot", numQueries: 1000},
}

// benchDatasetConfigs is the default set plus, if QUANTIZER_BENCH_DATASET is
// set, one locally cached subset given as subset:metric[:numQueries] — for a
// large dataset that should not be part of the default set. numQueries
// defaults to 100.
func benchDatasetConfigs(b *testing.B) []datasetConfig {
	extra := os.Getenv("QUANTIZER_BENCH_DATASET")
	if extra == "" {
		return datasetConfigs
	}
	parts := strings.SplitN(extra, ":", 3)
	if len(parts) < 2 {
		b.Fatalf("QUANTIZER_BENCH_DATASET must be subset:metric[:numQueries], got %q", extra)
	}
	nq := 100
	if len(parts) == 3 {
		var err error
		if nq, err = strconv.Atoi(parts[2]); err != nil || nq < 1 {
			b.Fatalf("bad numQueries in QUANTIZER_BENCH_DATASET %q: %v", extra, err)
		}
	}
	return append(datasetConfigs, datasetConfig{subset: parts[0], metric: parts[1], numQueries: nq})
}

// loadBenchDataset loads a subset from the local HuggingFace cache, normalizes
// it when the metric is cosine, and truncates the query set to cfg.numQueries.
// It skips rather than fails when the dataset is not cached, so the benchmarks
// stay runnable on a machine that has only some of them.
func loadBenchDataset(b *testing.B, cfg datasetConfig) (
	trainIds []uint64, vectors [][]float32, neighbors [][]uint64, queries [][]float32,
) {
	hf := datasets.NewHubDataset("weaviate/ann-datasets", cfg.subset)
	trainIds, vectors, err := hf.LoadTrainData()
	if err != nil {
		b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
	}
	neighbors, queries, err = hf.LoadTestData()
	if err != nil {
		b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
	}
	if cfg.metric == "cosine-dot" {
		normalizeVectors(vectors)
		normalizeVectors(queries)
	}
	if len(queries) > cfg.numQueries {
		queries = queries[:cfg.numQueries]
		neighbors = neighbors[:cfg.numQueries]
	}
	return trainIds, vectors, neighbors, queries
}

func datasetDistancer(metric string) distancer.Provider {
	switch metric {
	case "dot":
		return distancer.NewDotProductProvider()
	case "cosine-dot":
		return distancer.NewCosineDistanceProvider()
	default:
		return distancer.NewL2SquaredProvider()
	}
}

// datasetQuantizer adapts the different compressed types ([]byte, []uint64)
// to a common shape for the benchmark.
type datasetQuantizer struct {
	name           string
	encodeAll      func(vectors [][]float32)
	queryDist      func(q []float32) func(i int) float32
	compressedSize func() int
}

func newRQ4Adapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, m)
	var codes [][]byte
	return &datasetQuantizer{
		name: "rq4",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

// newPureRaBitQ4Adapter uses the reference extended-RaBitQ encoder (symmetric
// zero-centered grid, per-vector optimal scale) with the standard 4-bit
// distancer, isolating the grid parameterization as the only difference from
// rq4.
func newPureRaBitQ4Adapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, m)
	var codes [][]byte
	return &datasetQuantizer{
		name: "xrbq4",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.EncodePureRaBitQ4(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

func newRQ4CAdapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	var rq *compressionhelpers.FourBitRotationalQuantizer
	var codes [][]byte
	return &datasetQuantizer{
		name: "rq4c",
		encodeAll: func(vectors [][]float32) {
			mean := compressionhelpers.MeanVector(vectors, dim)
			rq, _ = compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, m, mean)
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

// rq4cTrainSample caps how many vectors the centering mean trains on,
// mirroring the production trainingLimit: a stable seeded random sample.
const rq4cTrainSample = 10000

func trainSample(vectors [][]float32, seed uint64) [][]float32 {
	if len(vectors) <= rq4cTrainSample {
		return vectors
	}
	rng := rand.New(rand.NewSource(int64(seed)))
	idx := rng.Perm(len(vectors))[:rq4cTrainSample]
	sample := make([][]float32, rq4cTrainSample)
	for i, j := range idx {
		sample[i] = vectors[j]
	}
	return sample
}

// newRQ4CT10kAdapter is the centered tier fit on the production-sized 10k
// random sample (the honest training protocol) rather than the full corpus
func newRQ4CT10kAdapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	var rq *compressionhelpers.FourBitRotationalQuantizer
	var codes [][]byte
	return &datasetQuantizer{
		name: "rq4c-t10k",
		encodeAll: func(vectors [][]float32) {
			mean := compressionhelpers.MeanVector(trainSample(vectors, seed), dim)
			var err error
			rq, err = compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, m, mean)
			if err != nil {
				panic(err)
			}
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

func newRQ4RAdapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	rq := compressionhelpers.NewFourBitResidualQuantizer(dim, seed, m)
	var codes [][]byte
	return &datasetQuantizer{
		name: "rq4r",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

func newRQ8Adapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	rq := compressionhelpers.NewRotationalQuantizer(dim, seed, 8, m)
	var codes [][]byte
	return &datasetQuantizer{
		name: "rq8",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return len(codes[0]) },
	}
}

func newBRQAdapter(dim int, seed uint64, m distancer.Provider) *datasetQuantizer {
	rq, _ := compressionhelpers.NewBinaryRotationalQuantizer(dim, seed, m)
	var codes [][]uint64
	return &datasetQuantizer{
		name: "brq1",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]uint64, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			d := rq.NewDistancer(q)
			return func(i int) float32 {
				dist, _ := d.Distance(codes[i])
				return dist
			}
		},
		compressedSize: func() int { return 8 * len(codes[0]) },
	}
}

// newBQAdapter uses the classic sign-bit BinaryQuantizer. BQ has no
// asymmetric distancer: the query is encoded too and compared via Hamming
// distance, matching the flat/HNSW BQ search path.
func newBQAdapter(m distancer.Provider) *datasetQuantizer {
	bq := compressionhelpers.NewBinaryQuantizer(m)
	var codes [][]uint64
	return &datasetQuantizer{
		name: "bq",
		encodeAll: func(vectors [][]float32) {
			codes = make([][]uint64, len(vectors))
			for i, v := range vectors {
				codes[i] = bq.Encode(v)
			}
		},
		queryDist: func(q []float32) func(i int) float32 {
			cq := bq.Encode(q)
			return func(i int) float32 {
				dist, _ := bq.DistanceBetweenCompressedVectors(cq, codes[i])
				return dist
			}
		},
		compressedSize: func() int { return 8 * len(codes[0]) },
	}
}

// topK keeps the k smallest distances seen so far. Insertion cost is O(k) but
// only on improvement, which happens O(k log(n/k)) times on random input.
type topK struct {
	dists []float32
	ids   []int
	worst int // index of the largest distance in dists
}

func newTopK(k int) *topK {
	return &topK{dists: make([]float32, 0, k), ids: make([]int, 0, k)}
}

func (t *topK) insert(id int, dist float32) {
	if len(t.dists) < cap(t.dists) {
		t.dists = append(t.dists, dist)
		t.ids = append(t.ids, id)
		if dist > t.dists[t.worst] {
			t.worst = len(t.dists) - 1
		}
		return
	}
	if dist >= t.dists[t.worst] {
		return
	}
	t.dists[t.worst] = dist
	t.ids[t.worst] = id
	t.worst = 0
	for i, d := range t.dists {
		if d > t.dists[t.worst] {
			t.worst = i
		}
	}
}

func normalizeVectors(vectors [][]float32) {
	for _, v := range vectors {
		var norm float32
		for _, x := range v {
			norm += x * x
		}
		if norm == 0 {
			continue
		}
		inv := float32(1.0 / math.Sqrt(float64(norm)))
		for i := range v {
			v[i] *= inv
		}
	}
}

func recallAt(groundTruth []uint64, ids []int, trainIds []uint64, k int) float64 {
	truth := make(map[uint64]struct{}, k)
	for _, id := range groundTruth[:k] {
		truth[id] = struct{}{}
	}
	var hits int
	for _, idx := range ids {
		if _, ok := truth[trainIds[idx]]; ok {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

func BenchmarkQuantizerDataset(b *testing.B) {
	const (
		k       = 10
		rescore = 50
	)

	seed := uint64(42)
	if s := os.Getenv("QUANTIZER_BENCH_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			b.Fatalf("bad QUANTIZER_BENCH_SEED %q: %v", s, err)
		}
		seed = v
	}
	for _, cfg := range benchDatasetConfigs(b) {
		trainIds, vectors, neighbors, queries := loadBenchDataset(b, cfg)
		m := datasetDistancer(cfg.metric)
		dim := len(vectors[0])

		quantizers := []*datasetQuantizer{
			newRQ4Adapter(dim, seed, m),
			newRQ4CAdapter(dim, seed, m),
			newRQ4CT10kAdapter(dim, seed, m),
			newRQ8Adapter(dim, seed, m),
			newBRQAdapter(dim, seed, m),
			newBQAdapter(m),
			// Below are quantizers not used in production, but are included for comparison.
			newPureRaBitQ4Adapter(dim, seed, m),
			newRQ4RAdapter(dim, seed, m),
		}
		for _, quant := range quantizers {
			encoded := false
			b.Run(fmt.Sprintf("%s/%s/encode", cfg.subset, quant.name), func(b *testing.B) {
				for b.Loop() {
					quant.encodeAll(vectors)
				}
				encoded = true
				b.ReportMetric(float64(b.N)*float64(len(vectors))/b.Elapsed().Seconds(), "vecs/sec")
				b.ReportMetric(float64(quant.compressedSize()), "bytes/vec")
			})

			b.Run(fmt.Sprintf("%s/%s/recall", cfg.subset, quant.name), func(b *testing.B) {
				// The encode subbench may have been filtered out via -bench;
				// the codes must exist before distances can be computed.
				if !encoded {
					quant.encodeAll(vectors)
					encoded = true
					b.ResetTimer()
				}
				var recallSum, rescoreSum float64
				for b.Loop() {
					recallSum, rescoreSum = 0, 0
					for qi, q := range queries {
						dist := quant.queryDist(q)
						approx := newTopK(rescore)
						for i := range vectors {
							approx.insert(i, dist(i))
						}
						// recall@10 straight from the quantized top-k.
						quantized := newTopK(k)
						for j, id := range approx.ids {
							quantized.insert(id, approx.dists[j])
						}
						recallSum += recallAt(neighbors[qi], quantized.ids, trainIds, k)

						// Re-rank the rescore window with exact distances.
						exact := newTopK(k)
						for _, id := range approx.ids {
							d, _ := m.SingleDist(q, vectors[id])
							exact.insert(id, d)
						}
						rescoreSum += recallAt(neighbors[qi], exact.ids, trainIds, k)
					}
				}
				totalDists := float64(len(queries)) * float64(len(vectors))
				b.ReportMetric(recallSum/float64(len(queries)), "recall@10")
				b.ReportMetric(rescoreSum/float64(len(queries)), "rescored@10")
				b.ReportMetric(float64(b.N)*totalDists/1e6/b.Elapsed().Seconds(), "m.dists/sec")
			})
		}
	}
}
