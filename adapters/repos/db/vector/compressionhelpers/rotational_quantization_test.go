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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func defaultRotationalQuantizer(dim int, seed uint64) *compressionhelpers.RotationalQuantizer {
	return compressionhelpers.NewRotationalQuantizer(dim, seed, 8, distancer.NewCosineDistanceProvider())
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

// RQDistancer.Distance and RotationalQuantizer.DistanceBetweenCompressedVectors
// are implemented separately for performance reasons. Verify that they return
// the same distance estimates up to floating point errors.
func TestRQDistanceEstimatesAreIdentical(t *testing.T) {
	rng := newRNG(64521467)

	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	n := 100
	for range n {
		d := 2 + rng.IntN(2000)
		for _, m := range metrics {
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, m)
			q, x := randomUnitVector(d, rng), randomUnitVector(d, rng)
			cq, cx := rq.Encode(q), rq.Encode(x)
			distancer := rq.NewDistancer(q)
			distancerEstimate, _ := distancer.Distance(cx)
			compressedEstimate, _ := rq.DistanceBetweenCompressedVectors(cq, cx)
			eps := 2e-6 // Unfortunately the deviation can be quite big. Perhaps the intermediate calculations can be done using float64?
			assert.Less(t, math.Abs(float64(distancerEstimate-compressedEstimate)), eps)
		}
	}
}

func absDiff(a float32, b float32) float64 {
	return math.Abs(float64(a - b))
}

func TestRQDistanceEstimate(t *testing.T) {
	a := float32(1.0 / math.Sqrt2)
	q := []float32{1.0, 0.0}
	x := []float32{a, a}

	dim := 2
	bits := 8
	var seed uint64 = 42

	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}

	for _, m := range metrics {
		rq := compressionhelpers.NewRotationalQuantizer(dim, seed, bits, m)
		cq, cx := rq.Encode(q), rq.Encode(x)
		distancer := rq.NewDistancer(q)
		distancerEstimate, _ := distancer.Distance(cx)
		compressedEstimate, _ := rq.DistanceBetweenCompressedVectors(cq, cx)

		target, _ := m.SingleDist(q, x)
		eps := 1e-3
		assert.Less(t, absDiff(distancerEstimate, target), eps)
		assert.Less(t, absDiff(compressedEstimate, target), eps)
	}
}

func randomUniformVector(d int, rng *rand.Rand) []float32 {
	x := make([]float32, d)
	for i := range x {
		x[i] = 2*rng.Float32() - 1.0
	}
	return x
}

func TestRQEncodeRestore(t *testing.T) {
	n := 10
	rng := newRNG(7542)
	for range n {
		d := 2 + rng.IntN(1000)
		rq := defaultRotationalQuantizer(d, rng.Uint64())

		s := 1000 * rng.Float32()
		x := randomUniformVector(d, rng)

		// Each entry of the scaled uniform vector ranges from [-s, s]
		// So the euclidean norm is going to be something like sd/3
		// Once we rotate the absolute magnitude of the entries should not exceed
		// something like (sd/3)*(6/sqrt(D)) < 2*s*sqrt(d) where D >= d is the output dimension.
		// So suppose the entries range between [-2*s*sqrt(d), 2*s*sqrt(d)] and we quantize this interval using 256 evenly distributed values.
		// Then the absolute quantization error in any one entry should not exceed 2*s*sqrt(d)/256
		errorBoundUpper := float64(s) * math.Sqrt(float64(d)) / 128
		eps := 0.1 * errorBoundUpper // The actual error is much smaller.

		scale(x, s)
		cx := rq.Encode(x)
		target := rq.Rotate(x)
		restored := rq.Restore(cx)

		for i := range target {
			assert.Less(t, math.Abs(float64(target[i]-restored[i])), eps)
		}
	}
}

func TestRQDistancer(t *testing.T) {
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(6789)
	n := 250
	for range n {
		d := 2 + rng.IntN(2000)
		alpha := -1.0 + 2*rng.Float32()
		// Note that we are testing on vectors where all mass is concentrated in
		// the first two entries. It might be more realistic to test on vectors
		// that have already been rotated randomly.
		q, x := correlatedVectors(d, alpha)
		for _, m := range metrics {
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, m)
			distancer := rq.NewDistancer(q)
			expected, _ := m.SingleDist(q, x)
			cx := rq.Encode(x)
			estimated, _ := distancer.Distance(cx)
			assert.Less(t, math.Abs(float64(estimated-expected)), 0.0051)
		}
	}
}

// Verify that the estimator behaves according to the concentration bounds
// specified in the paper. In the paper they use an asymmetric encoding of
// (float32, B-bits) while we use (queryBits, dataBits), so we cannot expect to
// satisfy their bounds exactly in all cases. This is especially the case when
// using few bits, something this quantization scheme is not optimized for.
func TestRQEstimationConcentrationBounds(t *testing.T) {
	rng := newRNG(12345)
	n := 100
	for range n {
		d := 2 + rng.IntN(2000)
		alpha := -1.0 + 2*rng.Float32()
		bits := 8

		// With probability > 0.999 the absolute error should be less than eps.
		// For d = 256 the error for b bits is 2^(-b) * 0.36, so 0.18, 0.09,
		// 0.045.. for b = 1, 2, 3,...
		eps := math.Pow(2.0, -float64(bits)) * 5.75 / math.Sqrt(float64(d))

		// With the optimizations we are adding, such as reducing the number of
		// rotation rounds and removing randomized rounding from the encoding we
		// are seeing a loss. We keep track of this loss as a factor that we
		// have to increase eps by in order to pass this test. For the initial
		// implementation this factor was 1.0. Note that a factor of 2
		// corresponds to a loss of 1 bit compared to extended RabitQ. A factor
		// of 4 corresponds to 2 bits and so on...
		additionalErrorFactor := 1.5
		eps *= additionalErrorFactor

		q, x := correlatedVectors(d, alpha)
		rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, distancer.NewDotProductProvider())
		cx := rq.Encode(x)
		dist := rq.NewDistancer(q)
		estimate, _ := dist.Distance(cx)
		cosineSimilarityEstimate := -estimate // Holds for unit vectors.
		assert.Less(t, math.Abs(float64(cosineSimilarityEstimate-alpha)), eps)
	}
}

func scale(x []float32, s float32) {
	for i := range x {
		x[i] *= s
	}
}

// Verify that the error scales as expected with the norm of the vectors.
// i.e. we can handle vectors of different norms.
func TestRQDistancerRandomVectorsWithScaling(t *testing.T) {
	// We do not test for cosine similarity here since it assumes normalized vectors.
	metrics := []distancer.Provider{
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	rng := newRNG(77433)
	n := 100
	for range n {
		d := 2 + rng.IntN(1000)
		alpha := -1.0 + 2*rng.Float32()
		q, x := correlatedVectors(d, alpha)
		s1 := 1000 * rng.Float32()
		s2 := 1000 * rng.Float32()
		scale(q, s1)
		scale(x, s2)
		for _, m := range metrics {
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(d, rng.Uint64(), bits, m)
			distancer := rq.NewDistancer(q)
			cx := rq.Encode(x)
			target, _ := m.SingleDist(q, x)
			estimate, _ := distancer.Distance(cx)

			// Suppose we are seeing absolute errors of estimation of size eps when working with unit vectors.
			// Then the error when scaling should scale roughly with the product of the scaling factors for the inner product.
			// For the Euclidean distance things are slightly more complex.
			assert.Less(t, math.Abs(float64(estimate-target)), float64(s1*s2*0.004), "Failure at a dimensionality of %d, metric %s", d, m.Type())
		}
	}
}

func TestRQCodePointDistribution(t *testing.T) {
	rng := newRNG(999)
	n := 100
	bits := 8
	codePoints := 1 << bits
	for range n {
		inDim := 2 + rng.IntN(1024)
		rq := compressionhelpers.NewRotationalQuantizer(inDim, rng.Uint64(), bits, distancer.NewDotProductProvider())

		// Encode m random unit vectors and mark the bytes that were used.
		m := 100
		byteCount := make([]int, codePoints)
		for range m {
			x := randomUnitVector(inDim, rng)
			var c compressionhelpers.RQCode = rq.Encode(x)
			codeBytes := c.Bytes()
			for _, b := range codeBytes {
				byteCount[b]++
			}
		}

		uniformExpectation := float64(m) * float64(rq.OutputDimension()) / float64(codePoints)
		for i := range byteCount {
			// The code was designed to guarantee that the min and max are
			// always included, so they will have an abnormally high count,
			// especially in low dimensions.
			if i == 0 || i == (len(byteCount)-1) {
				continue
			}
			errorMsg := fmt.Sprintf("Byte %d was seen %d times (%.3f times its expectation). Input dimension: %d, Output dimension: %d",
				i, byteCount[i], float64(byteCount[i])/uniformExpectation, inDim, rq.OutputDimension())
			assert.Greater(t, byteCount[i], 0, errorMsg)
			assert.Less(t, float64(byteCount[i]), 3.0*uniformExpectation, errorMsg)
		}
	}
}

func TestRQHandlesAbnormalVectorsGracefully(t *testing.T) {
	inDim := 97
	bits := 8
	rq := compressionhelpers.NewRotationalQuantizer(inDim, 42, bits, distancer.NewDotProductProvider())
	outDim := rq.OutputDimension()
	zeroCode := compressionhelpers.ZeroRQCode(outDim)

	var nilVector []float32
	assert.True(t, slices.Equal(rq.Encode(nilVector), zeroCode))

	lengthZeroVector := make([]float32, 0)
	assert.True(t, slices.Equal(rq.Encode(lengthZeroVector), zeroCode))

	longVectorOfZeroes := make([]float32, 572)
	shortVectorOfZeroes := make([]float32, 15)
	assert.True(t, slices.Equal(rq.Encode(longVectorOfZeroes), zeroCode))
	assert.True(t, slices.Equal(rq.Encode(shortVectorOfZeroes), zeroCode))

	// Only the first at most outDim entries are used for the encoding, the rest is ignored.
	x := make([]float32, 243)
	for i := range x {
		x[i] = float32(i)
	}
	assert.True(t, slices.Equal(rq.Encode(x[:outDim]), rq.Encode(x)))
}

func BenchmarkRQEncode(b *testing.B) {
	dimensions := []int{256, 1024, 1536}
	rng := newRNG(42)
	for _, dim := range dimensions {
		quantizer := defaultRotationalQuantizer(dim, rng.Uint64())
		x := make([]float32, dim)
		x[0] = 1
		b.Run(fmt.Sprintf("FastRQEncode-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				quantizer.Encode(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			b.ReportMetric(float64(b.N)/float64(b.Elapsed().Seconds()), "ops/sec")
		})
	}
}

func BenchmarkRQDistancer(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	rng := newRNG(42)
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			// Rotational quantization.
			bits := 8
			rq := compressionhelpers.NewRotationalQuantizer(dim, rng.Uint64(), bits, m)
			q, x := correlatedVectors(dim, 0.5)
			cx := rq.Encode(x)
			distancer := rq.NewDistancer(q)
			b.Run(fmt.Sprintf("RQDistancer-d%d-%s", dim, m.Type()), func(b *testing.B) {
				for b.Loop() {
					distancer.Distance(cx)
				}
				b.ReportMetric((float64(b.N)/1e6)/float64(b.Elapsed().Seconds()), "m.ops/sec")
			})
		}
	}
}

// For comparison.
func BenchmarkSQDistancer(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{64, 128, 256, 512, 1024, 1536, 2048}
	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}
	for _, dim := range dimensions {
		for _, m := range metrics {
			train := [][]float32{
				randomUnitVector(dim, rng),
			}
			quantizer := compressionhelpers.NewScalarQuantizer(train, m)
			q, x := correlatedVectors(dim, 0.5)
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
