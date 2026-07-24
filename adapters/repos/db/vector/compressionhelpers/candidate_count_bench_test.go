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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
)

// Measures how the interval-search candidate count affects raw recall for
// both grid parameterizations (affine rq4 and symmetric xrbq4).
func BenchmarkRecallByCandidateCount(b *testing.B) {
	const (
		k    = 10
		seed = 42
	)
	ten := make([]float32, 10)
	for i := range ten {
		ten[i] = 0.55 + 0.05*float32(i)
	}
	dense := make([]float32, 25)
	for i := range dense {
		dense[i] = 0.40 + 0.025*float32(i)
	}

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

		configs := []struct {
			name    string
			factors []float32
			encode  func([]float32) []byte
		}{
			{"rq4-10cand", ten, rq.Encode},
			{"rq4-25cand", dense, rq.Encode},
			{"xrbq4-10cand", ten, rq.EncodePureRaBitQ4},
			{"xrbq4-25cand", dense, rq.EncodePureRaBitQ4},
		}
		for _, c := range configs {
			b.Run(fmt.Sprintf("%s/%s", cfg.subset, c.name), func(b *testing.B) {
				prevClip := compressionhelpers.SetRQ4ClipFactors(c.factors)
				prevScale := compressionhelpers.PureRaBitQ4ScaleFactors
				compressionhelpers.PureRaBitQ4ScaleFactors = c.factors
				defer func() {
					compressionhelpers.SetRQ4ClipFactors(prevClip)
					compressionhelpers.PureRaBitQ4ScaleFactors = prevScale
				}()

				codes := make([][]byte, len(vectors))
				for i, v := range vectors {
					codes[i] = c.encode(v)
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
