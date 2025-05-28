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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func defaultRotationalQuantizer(dim int, seed uint64) *compressionhelpers.RotationalQuantizer {
	return compressionhelpers.NewRotationalQuantizer(dim, seed, 8, 8, distancer.NewCosineDistanceProvider())
}

// Create two d-dimensional unit vectors with a cosine similarity of alpha.
func correlatedVectors(d int, alpha float32) ([]float32, []float32) {
	x := make([]float32, d)
	x[0] = 1.0
	y := make([]float32, d)
	y[0] = alpha
	y[1] = float32(math.Sqrt(float64(1 - alpha*alpha)))
	return x, y
}

// There is perhaps no reason to expose the RQCode type at all..
func TestRQCodeToFromBytes(t *testing.T) {
	rng := newRNG(42)
	length := 634
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(rng.UintN(256))
	}
	c := compressionhelpers.NewRQCodeFromBackingBytes(bytes)
	assert.True(t, slices.Equal(bytes, c.Bytes()))
}

func TestRQDistanceEstimate(t *testing.T) {
	d := 1024
	rq := defaultRotationalQuantizer(d, 43)

	var alpha float32 = 0.5
	x, y := correlatedVectors(d, alpha)

	target := 1 - alpha
	cx, cy := rq.Encode(x), rq.Encode(y)
	estimate, _ := rq.DistanceBetweenCompressedVectors(cx, cy)
	assert.Less(t, math.Abs(float64(target-estimate)), 1e-4)
}

// Consider adding an inverse transform to restore the input vector.
func TestRQEncodeRestore(t *testing.T) {
	d := 1536
	rq := defaultRotationalQuantizer(d, 42)
	x := make([]float32, d)
	x[0] = 1.0
	cx := rq.Encode(x)
	target := rq.Rotate(x)
	restored := rq.Restore(cx)
	for i := range target {
		assert.Less(t, math.Abs(float64(target[i]-restored[i])), 4e-4)
	}
}

// TODO: Test that RQ obeys the empirical extended RaBitQ bounds (test exists in previous version).
func TestRQDistancer(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(678)
	n := 250
	for range n {
		d := 2 + rng.IntN(1000)
		alpha := -1.0 + 2*rng.Float32()
		q, x := correlatedVectors(d, alpha)
		for _, m := range metrics {
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, bits, m)
			distancer := rq.NewDistancer(q)
			expected, _ := m.SingleDist(q, x)
			cx := rq.Encode(x)
			estimated, _ := distancer.Distance(cx)
			assert.Less(t, math.Abs(float64(estimated-expected)), 0.004)
		}
	}
}

func randomUnitVector(d int, rng *rand.Rand) []float32 {
	x := make([]float32, d)
	for i := range x {
		x[i] = float32(rng.NormFloat64())
	}
	xNorm := float32(norm(x))
	for i := range x {
		x[i] = x[i] / xNorm
	}
	return x
}

func scale(x []float32, s float32) {
	for i := range x {
		x[i] *= s
	}
}

func TestRQDistancerRandomVectorsWithScaling(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(7743)
	n := 100
	for range n {
		d := 2 + rng.IntN(1000)
		q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
		s1 := 1000 * rng.Float32()
		s2 := 1000 * rng.Float32()
		scale(q, s1)
		scale(x, s2)
		for _, m := range metrics {
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, bits, m)
			distancer := rq.NewDistancer(q)
			expected, _ := m.SingleDist(q, x)
			cx := rq.Encode(x)
			estimated, _ := distancer.Distance(cx)
			assert.Less(t, math.Abs(float64(estimated-expected)), float64(s1*s2*0.004))
		}
	}
}

func BenchmarkRQEncode(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{64, 128, 256, 512, 1024, 2048}
	for _, dim := range dimensions {
		quantizer := defaultRotationalQuantizer(dim, rng.Uint64())
		x := make([]float32, dim)
		b.Run(fmt.Sprintf("FastRQEncode-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				quantizer.Encode(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			b.ReportMetric(float64(b.N)/float64(b.Elapsed().Seconds()), "ops/sec")
		})
	}
}

// Could be made faster if we didn't have to do the conversion to an RQCode internally.
func BenchmarkRQDistancer(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			bits := 8
			quantizer := compressionhelpers.NewRotationalQuantizer(dim, rng.Uint64(), bits, bits, m)
			q, x := correlatedVectors(dim, 0.5)
			cx := quantizer.Encode(x)
			distancer := quantizer.NewDistancer(q)
			b.Run(fmt.Sprintf("RQDistancer-d%d-%s", dim, m.Type()), func(b *testing.B) {
				for b.Loop() {
					distancer.Distance(cx)
				}
				b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
			})
		}
	}
}
