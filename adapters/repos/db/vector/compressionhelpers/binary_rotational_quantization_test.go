//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

func TestBRQDistanceEstimates(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(123)
	n := 100

	// What kind of error do we expect from 1-bit RQ under each of the different metrics?
	// We know that the absolute error of the dot product of unit vectors <q,x> decreases by 1/SQRT(D)
	// As we scale the vectors the absolute error should scale with the product of the scaling factors.
	for _, m := range metrics {
		for range n {
			// Create two unit vectors with a uniform random correlation
			// between -1 and 1 and scale them randomly.
			dim := 2 + rng.IntN(2000)
			q, x := correlatedVectors(dim, 1-2*rng.Float32())
			var sq, sx float32 = 1.0, 1.0
			if m.Type() != "cosine-dot" {
				var scaleFactor float32 = 1.0
				if rng.Float32() < 0.5 {
					scaleFactor = 1e3
				}
				sq = scaleFactor * rng.Float32()
				sx = scaleFactor * rng.Float32()
			}
			scale(q, sq)
			scale(x, sx)

			rq := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), m)
			distancer := rq.NewDistancer(q)
			cx := rq.Encode(x)
			distancerEstimate, _ := distancer.Distance(cx)
			target, _ := m.SingleDist(q, x)

			baseEps := 2.3 / math.Sqrt(float64(dim))
			eps := float64(sq*sx) * baseEps
			if m.Type() == "l2-squared" {
				eps *= 2
			}
			err := absDiff(distancerEstimate, target)
			assert.Less(t, err, eps,
				"Metric: %s, Dimension: %d, Target: %.4f, Estimate: %.4f, Estimate/Target: %.4f, Error: %.4f, Eps: %.4f",
				m.Type(), dim, target, distancerEstimate, distancerEstimate/target, err, eps)
		}
	}
}

func TestBRQCompressedDistanceEstimates(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(123)
	n := 100

	// What kind of error do we expect from 1-bit RQ under each of the different metrics?
	// We know that the absolute error of the dot product of unit vectors <q,x> decreases by 1/SQRT(D)
	// As we scale the vectors the absolute error should scale with the product of the scaling factors.
	for _, m := range metrics {
		for range n {
			// Create two unit vectors with a uniform random correlation
			// between -1 and 1 and scale them randomly.
			dim := 2 + rng.IntN(2000)
			q, x := correlatedVectors(dim, 1-2*rng.Float32())
			var sq, sx float32 = 1.0, 1.0
			if m.Type() != "cosine-dot" {
				var scaleFactor float32 = 1.0
				if rng.Float32() < 0.5 {
					scaleFactor = 1e3
				}
				sq = scaleFactor * rng.Float32()
				sx = scaleFactor * rng.Float32()
			}
			scale(q, sq)
			scale(x, sx)

			rq := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), m)
			cq := rq.Encode(q)
			cx := rq.Encode(x)
			estimate, _ := rq.DistanceBetweenCompressedVectors(cq, cx)
			target, _ := m.SingleDist(q, x)

			baseEps := 3.0 / math.Sqrt(float64(dim))
			eps := float64(sq*sx) * baseEps
			if m.Type() == "l2-squared" {
				eps *= 2
			}
			err := absDiff(estimate, target)
			assert.Less(t, err, eps,
				"Metric: %s, Dimension: %d, Target: %.4f, Estimate: %.4f, Estimate/Target: %.4f, Error: %.4f, Eps: %.4f",
				m.Type(), dim, target, estimate, estimate/target, err, eps)
		}
	}
}

// The absolute error when estimating the dot product between unit vectors
// should scale according to 1/SQRT(D). Therefore we should roughly be seeing
// that quadrupling the dimensionality halves the error.
//
// The average bias should be small compared to the average absolute error. The
// sign of the average bias should also appear random (the estimator should be
// unbiased in expectation).
func BenchmarkBRQError(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	for _, dim := range dimensions {
		var absErr float64
		var bias float64
		b.Run(fmt.Sprintf("d%d", dim), func(b *testing.B) {
			rng := newRNG(43)
			for b.Loop() {
				alpha := 1 - 2*rng.Float32()
				q, x := correlatedVectors(dim, float32(alpha))
				quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), distancer.NewDotProductProvider())
				distancer := quantizer.NewDistancer(q)
				cx := quantizer.Encode(x)
				dotEstimate, _ := distancer.Distance(cx)
				dotEstimate = -dotEstimate // The distancer estimate is the negative dot product.
				absErr += math.Abs(float64(dotEstimate - alpha))
				bias += float64(dotEstimate - alpha)
			}
			b.ReportMetric(absErr/float64(b.N), "avg.err")
			b.ReportMetric(bias/float64(b.N), "avg.bias")
		})
	}
}

func BenchmarkBRQCompressedError(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	correlation := []float32{-0.9, -0.7, -0.5, -0.25, 0.0, 0.25, 0.5, 0.7, 0.9}
	for _, dim := range dimensions {
		for _, alpha := range correlation {
			var absErr float64
			var bias float64
			b.Run(fmt.Sprintf("d:%d-alpha:%.2f", dim, alpha), func(b *testing.B) {
				rng := newRNG(43)
				for b.Loop() {
					q, x := correlatedVectors(dim, float32(alpha))
					quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), distancer.NewDotProductProvider())
					cx := quantizer.Encode(x)
					cq := quantizer.Encode(q)
					dotEstimate, _ := quantizer.DistanceBetweenCompressedVectors(cq, cx)
					dotEstimate = -dotEstimate // The distancer estimate is the negative dot product.
					absErr += math.Abs(float64(dotEstimate - alpha))
					bias += float64(dotEstimate - alpha)
				}
				b.ReportMetric(absErr/float64(b.N), "avg.err")
				b.ReportMetric(bias/float64(b.N), "avg.bias")
			})
		}
	}
}

func BenchmarkBRQEncode(b *testing.B) {
	dimensions := []int{256, 1024, 1536}
	rng := newRNG(42)
	for _, dim := range dimensions {
		quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), distancer.NewDotProductProvider())
		x := make([]float32, dim)
		x[0] = 1
		b.Run(fmt.Sprintf("d%d", dim), func(b *testing.B) {
			for b.Loop() {
				quantizer.Encode(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			b.ReportMetric(float64(b.N)/float64(b.Elapsed().Seconds()), "ops/sec")
		})
	}
}

func BenchmarkBRQNewDistancer(b *testing.B) {
	dimensions := []int{128, 256, 1024, 1536}
	rng := newRNG(42)
	for _, dim := range dimensions {
		quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), distancer.NewDotProductProvider())
		q := make([]float32, dim)
		q[0] = 1
		b.Run(fmt.Sprintf("d%d", dim), func(b *testing.B) {
			for b.Loop() {
				quantizer.NewDistancer(q)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			b.ReportMetric(float64(b.N)/float64(b.Elapsed().Seconds()), "ops/sec")
		})
	}
}

func BenchmarkBRQDistance(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 512, 768, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), m)
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

func BenchmarkBRQCompressedDistance(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 512, 768, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			quantizer := compressionhelpers.NewBinaryRotationalQuantizer(dim, rng.Uint64(), m)
			q, x := correlatedVectors(dim, 0.5)
			cq := quantizer.Encode(q)
			cx := quantizer.Encode(x)
			b.Run(fmt.Sprintf("d%d-%s", dim, m.Type()), func(b *testing.B) {
				for b.Loop() {
					quantizer.DistanceBetweenCompressedVectors(cq, cx)
				}
				b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
			})
		}
	}
}

func BenchmarkSignDot(b *testing.B) {
	words := []int{4, 8, 16, 24, 32}
	for _, w := range words {
		bits := 64 * w
		x, y := make([]uint64, w), make([]uint64, w)
		b.Run(fmt.Sprintf("Go-d%d", bits), func(b *testing.B) {
			for b.Loop() {
				compressionhelpers.HammingDist(x, y)
			}
			b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
		})
		b.Run(fmt.Sprintf("SIMD-d%d", bits), func(b *testing.B) {
			for b.Loop() {
				compressionhelpers.HammingDistSIMD(x, y)
			}
			b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
		})
	}
}
