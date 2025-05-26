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

package compressionhelpers_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TODO: Compare against asymmetric (float32 vs 8 bit) and RaBitQ
// TODO: Try to minimize floating point errors.
func TestFastRQDotEstimates(t *testing.T) {
	rng := newRNG(123)
	n := 100
	for range n {
		d := 2 + rng.IntN(1000)
		alpha := rng.Float32()
		x := make([]float32, d)
		x[0] = 1.0
		y := make([]float32, d)
		y[0] = alpha
		y[1] = float32(math.Sqrt(float64(1 - alpha*alpha)))
		rq := compressionhelpers.NewFastRotationalQuantizer(d, rng.Uint64())
		xCode := rq.Encode(x, 8)
		yCode := rq.Encode(y, 8)
		dotEstimate := compressionhelpers.EstimateDotProduct(xCode, yCode)
		assert.Less(t, math.Abs(float64(alpha-dotEstimate)), 0.004)
	}
}

func TestFastRQDistancer(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}

	rng := newRNG(678)
	n := 100
	for range n {
		d := 2 + rng.IntN(1000)
		rq := compressionhelpers.NewFastRotationalQuantizer(d, rng.Uint64())

		alpha := rng.Float32()
		q := make([]float32, d)
		q[0] = 1.0
		x := make([]float32, d)
		x[0] = alpha
		x[1] = float32(math.Sqrt(float64(1 - alpha*alpha)))

		xCode := rq.Encode(x, 8)
		for _, m := range metrics {
			expected, _ := m.SingleDist(q, x)
			distancer := rq.NewFastRQDistancer(q, m, 8)
			estimated, _ := distancer.Distance(xCode)
			assert.Less(t, math.Abs(float64(estimated-expected)), 0.004)
		}

	}
}

func BenchmarkFastRQEncode(b *testing.B) {
	rng := newRNG(42)
	vg := NewVectorGenerator()
	dimensions := []int{64, 128, 256, 512, 1024, 2048}
	bits := []int{4, 8}
	for _, dim := range dimensions {
		for _, k := range bits {
			quantizer := compressionhelpers.NewFastRotationalQuantizer(dim, rng.Uint64())
			x := vg.RandomUnitVector(dim)
			b.Run(fmt.Sprintf("FastRQEncode-d%d-b%d", dim, k), func(b *testing.B) {
				for b.Loop() {
					quantizer.Encode(x, k)
				}
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
				b.ReportMetric(float64(b.N)/float64(b.Elapsed().Seconds()), "ops/sec")
			})
		}
	}
}

// Around 25% slower than SQ in some instances. Not sure why, maybe due to branching or inlining or alignment of data.
// Nevertheless, it should be fast enough as it is. It might not make sense to optimize for a specific architecture.
func BenchmarkFastRQDistancer(b *testing.B) {
	rng := newRNG(42)
	vg := NewVectorGenerator()
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	bits := []int{8}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, k := range bits {
			for _, m := range metrics {
				quantizer := compressionhelpers.NewFastRotationalQuantizer(dim, rng.Uint64())
				q := vg.RandomUnitVector(dim)
				x := vg.RandomUnitVector(dim)
				xCode := quantizer.Encode(x, k)
				distancer := quantizer.NewFastRQDistancer(q, m, 8)
				b.Run(fmt.Sprintf("FastRQDistancer-d%d-b%d-%s", dim, k, m.Type()), func(b *testing.B) {
					for b.Loop() {
						distancer.Distance(xCode)
					}
					b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
				})
			}
		}
	}
}

func BenchmarkSQDistancer(b *testing.B) {
	vg := NewVectorGenerator()
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			train := [][]float32{
				vg.RandomUnitVector(dim),
			}
			quantizer := compressionhelpers.NewScalarQuantizer(train, m)
			q := vg.RandomUnitVector(dim)
			x := vg.RandomUnitVector(dim)
			xCode := quantizer.Encode(x)
			distancer := quantizer.NewDistancer(q)
			b.Run(fmt.Sprintf("SQDistancer-d%d-%s", dim, m.Type()), func(b *testing.B) {
				for b.Loop() {
					distancer.Distance(xCode)
				}
				b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
			})
		}
	}
}
