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
	"os"
	"strconv"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

func BenchmarkRQ4CenteredEstimator(b *testing.B) {
	const (
		numQueries = 20
		numPairs   = 500
		topK       = 50
	)
	seed := compactSeed(b)
	for _, cfg := range benchDatasetConfigs(b) {
		_, vectors, _, queries := loadBenchDataset(b, cfg)
		dim := len(vectors[0])
		m := datasetDistancer(cfg.metric)
		b.Run(fmt.Sprintf("%s/estimator", cfg.subset), func(b *testing.B) {
			for b.Loop() {
			}
			mean := compressionhelpers.MeanVector(vectors, dim)
			rq, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, m, mean)
			if err != nil {
				b.Fatal(err)
			}
			corpus := vectors
			if len(corpus) > numPairs {
				corpus = corpus[:numPairs]
			}
			base := make([][]byte, len(corpus))
			full := make([][]byte, len(corpus))
			for i, v := range corpus {
				base[i] = rq.EncodeWithoutSidecar(v)
				full[i] = rq.Encode(v)
			}

			qs := queries
			if len(qs) > numQueries {
				qs = qs[:numQueries]
			}
			var randBase, randFull, topBase, topFull float64
			var randN, topN int
			for _, q := range qs {
				d := rq.NewDistancer(q)
				exact := make([]float64, len(corpus))
				order := make([]int, len(corpus))
				for i, v := range corpus {
					e, _ := m.SingleDist(q, v)
					exact[i] = float64(e)
					order[i] = i
				}
				for i, id := range order {
					eb, _ := d.Distance(base[id])
					ef, _ := d.Distance(full[id])
					randBase += sq(float64(eb) - exact[id])
					randFull += sq(float64(ef) - exact[id])
					randN++
					_ = i
				}
				// Exact top-K by true distance: the ranking-critical pairs.
				for _, id := range topKIndices(exact, topK) {
					eb, _ := d.Distance(base[id])
					ef, _ := d.Distance(full[id])
					topBase += sq(float64(eb) - exact[id])
					topFull += sq(float64(ef) - exact[id])
					topN++
				}
			}
			rmse := func(sum float64, n int) float64 { return math.Sqrt(sum / float64(n)) }
			rb, rf := rmse(randBase, randN), rmse(randFull, randN)
			tb, tf := rmse(topBase, topN), rmse(topFull, topN)
			fmt.Printf("%s estimator: random %.6g -> %.6g (%+.1f%%) | top%d %.6g -> %.6g (%+.1f%%)\n",
				cfg.subset, rb, rf, 100*(rf/rb-1), topK, tb, tf, 100*(tf/tb-1))
		})
	}
}

func sq(x float64) float64 { return x * x }

// topKIndices returns the indices of the k smallest values.
func topKIndices(vals []float64, k int) []int {
	idx := make([]int, 0, k)
	for i := range vals {
		pos := len(idx)
		for pos > 0 && vals[i] < vals[idx[pos-1]] {
			pos--
		}
		if pos < k {
			if len(idx) < k {
				idx = append(idx, 0)
			}
			copy(idx[pos+1:], idx[pos:])
			idx[pos] = i
		}
	}
	return idx
}

func compactSeed(b *testing.B) uint64 {
	seed := uint64(42)
	if s := os.Getenv("QUANTIZER_BENCH_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			b.Fatalf("bad QUANTIZER_BENCH_SEED %q: %v", s, err)
		}
		seed = v
	}
	return seed
}
