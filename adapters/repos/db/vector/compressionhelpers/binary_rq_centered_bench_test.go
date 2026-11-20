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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// BenchmarkCenteredBRQDistance is BenchmarkBRQDistance for the centered 1-bit
// quantizer: one query against one code, at the usual widths and metrics.
func BenchmarkCenteredBRQDistance(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 512, 768, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			// A mean apart from the query: correlatedVectors returns the same
			// pair every time, and a query equal to the mean centers to zero.
			mean := make([]float32, dim)
			for i := range mean {
				mean[i] = float32(rng.NormFloat64()) / 32
			}
			quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(dim, rng.Uint64(), mean, m)
			if err != nil {
				b.Fatal(err)
			}
			q, x := correlatedVectors(dim, 0.5)
			cx := quantizer.Encode(x)
			distancer := quantizer.NewDistancer(q)
			b.Run(fmt.Sprintf("d%d-%s", dim, m.Type()), func(b *testing.B) {
				for b.Loop() {
					distancer.Distance(cx)
				}
				b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
			})
		}
	}
}
