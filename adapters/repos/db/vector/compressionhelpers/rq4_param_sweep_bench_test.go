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

package compressionhelpers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
)

// Verifies a parameter choice across rotation seeds on the highest-dimension
// dataset, to separate real improvements from seed luck.
func BenchmarkRQ4SeedCheck(b *testing.B) {
	const k = 10
	cfg := datasetConfigs[len(datasetConfigs)-1] // dbpedia 1536
	hf := datasets.NewHubDataset("weaviate/ann-datasets", cfg.subset)
	trainIds, vectors, err := hf.LoadTrainData()
	if err != nil {
		b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
	}
	neighbors, queries, err := hf.LoadTestData()
	if err != nil {
		b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
	}
	if len(queries) > cfg.numQueries {
		queries = queries[:cfg.numQueries]
		neighbors = neighbors[:cfg.numQueries]
	}
	m := datasetDistancer(cfg.metric)
	normalizeVectors(vectors)
	normalizeVectors(queries)
	dim := len(vectors[0])

	configs := []struct {
		name    string
		factors []float32
		sample  int
	}{
		{"f9-s256", []float32{0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95}, 256},
		{"f4-s512", []float32{0.6, 0.7, 0.8, 0.9}, 512},
	}
	for _, seed := range []uint64{42, 7, 987654321} {
		for _, c := range configs {
			b.Run(fmt.Sprintf("seed%d/%s", seed, c.name), func(b *testing.B) {
				prevClip := compressionhelpers.SetRQ4ClipFactors(c.factors)
				prevSample := compressionhelpers.SetRQ4ClipSearchSample(c.sample)
				defer func() {
					compressionhelpers.SetRQ4ClipFactors(prevClip)
					compressionhelpers.SetRQ4ClipSearchSample(prevSample)
				}()
				rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, m)
				codes := make([][]byte, len(vectors))
				for i, v := range vectors {
					codes[i] = rq.Encode(v)
				}
				var recallSum float64
				for b.Loop() {
					recallSum = 0
					for qi, q := range queries {
						d := rq.NewDistancer(q)
						top := newTopK(k)
						for i := range codes {
							dist, _ := d.Distance(codes[i])
							top.insert(i, dist)
						}
						recallSum += recallAt(neighbors[qi], top.ids, trainIds, k)
					}
				}
				b.ReportMetric(recallSum/float64(len(queries)), "recall@10")
			})
		}
	}
}

// Sweeps the encode-time parameters of the 4-bit quantizer (clip factor grid
// and clip search sample size) over the real datasets, reporting raw
// recall@10 and encode throughput for each combination.
func BenchmarkRQ4ParamSweep(b *testing.B) {
	const (
		k    = 10
		seed = 42
	)
	factorGrids := []struct {
		name    string
		factors []float32
	}{
		{"f4", []float32{0.6, 0.7, 0.8, 0.9}},
		{"f9", []float32{0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95}},
		{"f13", []float32{0.60, 0.625, 0.65, 0.675, 0.70, 0.725, 0.75, 0.775, 0.80, 0.825, 0.85, 0.875, 0.90}},
	}
	samples := []int{128, 256, 512}

	for _, cfg := range datasetConfigs {
		hf := datasets.NewHubDataset("weaviate/ann-datasets", cfg.subset)
		trainIds, vectors, err := hf.LoadTrainData()
		if err != nil {
			b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
		}
		neighbors, queries, err := hf.LoadTestData()
		if err != nil {
			b.Skipf("failed to load dataset %s: %v", cfg.subset, err)
		}
		if len(queries) > cfg.numQueries {
			queries = queries[:cfg.numQueries]
			neighbors = neighbors[:cfg.numQueries]
		}
		m := datasetDistancer(cfg.metric)
		if cfg.metric == "cosine-dot" {
			normalizeVectors(vectors)
			normalizeVectors(queries)
		}
		dim := len(vectors[0])
		rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, m)

		for _, fg := range factorGrids {
			for _, sample := range samples {
				b.Run(fmt.Sprintf("%s/%s-s%d", cfg.subset, fg.name, sample), func(b *testing.B) {
					prevClip := compressionhelpers.SetRQ4ClipFactors(fg.factors)
					prevSample := compressionhelpers.SetRQ4ClipSearchSample(sample)
					defer func() {
						compressionhelpers.SetRQ4ClipFactors(prevClip)
						compressionhelpers.SetRQ4ClipSearchSample(prevSample)
					}()

					start := time.Now()
					codes := make([][]byte, len(vectors))
					for i, v := range vectors {
						codes[i] = rq.Encode(v)
					}
					encodeRate := float64(len(vectors)) / time.Since(start).Seconds()

					var recallSum float64
					for b.Loop() {
						recallSum = 0
						for qi, q := range queries {
							d := rq.NewDistancer(q)
							top := newTopK(k)
							for i := range codes {
								dist, _ := d.Distance(codes[i])
								top.insert(i, dist)
							}
							recallSum += recallAt(neighbors[qi], top.ids, trainIds, k)
						}
					}
					b.ReportMetric(recallSum/float64(len(queries)), "recall@10")
					b.ReportMetric(encodeRate, "enc.vecs/sec")
				})
			}
		}
	}
}
