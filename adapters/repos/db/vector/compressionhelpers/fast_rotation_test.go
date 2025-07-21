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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"golang.org/x/exp/slices"
)

// Euclidean norm.
func norm(x []float32) float64 {
	return math.Sqrt(dot(x, x))
}

// Euclidean distance.
func dist(u []float32, v []float32) float64 {
	x := make([]float32, len(v))
	for i := range x {
		x[i] = u[i] - v[i]
	}
	return norm(x)
}

func dot(u []float32, v []float32) float64 {
	var sum float64
	for i := range u {
		sum += float64(u[i]) * float64(v[i])
	}
	return sum
}

func randomNormalVector(d int, rng *rand.Rand) []float32 {
	z := make([]float32, d)
	for i := range d {
		z[i] = float32(rng.NormFloat64())
	}
	return z
}

func randomUnitVector(d int, rng *rand.Rand) []float32 {
	x := randomNormalVector(d, rng)
	normalize := float32(1.0 / norm(x))
	for i := range x {
		x[i] *= normalize
	}
	return x
}

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
}

func TestFastRotationPreservesNorm(t *testing.T) {
	rng := newRNG(65234)
	n := 100
	for range n {
		d := 2 + rng.IntN(2000)
		r := 1 + rng.IntN(5)
		rotation := compressionhelpers.NewFastRotation(d, r, rng.Uint64())
		z := randomNormalVector(d, rng)
		rz := rotation.Rotate(z)
		assert.Less(t, math.Abs(norm(rz)-norm(z)), 5e-6)
	}
}

func TestFastRotationPreservesDistance(t *testing.T) {
	rng := newRNG(4242)
	n := 100
	for range n {
		d := 2 + rng.IntN(2000)
		rounds := 1 + rng.IntN(5)
		rotation := compressionhelpers.NewFastRotation(d, rounds, rng.Uint64())
		z1 := randomNormalVector(d, rng)
		z2 := randomNormalVector(d, rng)
		rz1 := rotation.Rotate(z1)
		rz2 := rotation.Rotate(z2)
		assert.Less(t, math.Abs(dist(rz1, rz2)-dist(z1, z2)), 6e-6)
	}
}

func TestFastRotationOutputLength(t *testing.T) {
	rng := newRNG(42)
	n := 100
	for range n {
		d := rng.IntN(1000)
		r := 1 + rng.IntN(5)
		rotation := compressionhelpers.NewFastRotation(d, r, rng.Uint64())
		x := make([]float32, d)
		rx := rotation.Rotate(x)
		if d < 64 {
			assert.Equal(t, len(rx), 64)
		} else {
			assert.GreaterOrEqual(t, len(rx), 64)
			assert.Less(t, len(rx), d+64)
		}
	}
}

// Rotate the standard basis and verify that the rotated vectors are orthogonal.
func TestFastRotationPreservesOrthogonality(t *testing.T) {
	rng := newRNG(424242)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129, 255, 257, 512, 768}
	for _, d := range dimensions {
		unitVectors := make([][]float32, d)
		for i := range d {
			v := make([]float32, d)
			v[i] = 1.0
			unitVectors[i] = v
		}

		r := compressionhelpers.NewFastRotation(d, 3, rng.Uint64())
		rotatedVectors := make([][]float32, d)
		for i := range d {
			rotatedVectors[i] = r.Rotate(unitVectors[i])
		}

		for i := range d {
			u := rotatedVectors[i]
			for j := range d {
				v := rotatedVectors[j]
				if i == j {
					assert.Less(t, math.Abs(dot(u, v)-1.0), 1e-6)
				} else {
					assert.Less(t, math.Abs(dot(u, v)), 1e-6)
				}
			}
		}
	}
}

// Smoothing doesn't work great unless we use a high number of rounds.
// This in turn probably comes with more noticeable floating point errors.
func TestFastRotationSmoothensVector(t *testing.T) {
	rng := newRNG(424242)
	n := 10
	for range n {
		dim := 2 + rng.IntN(1000)
		rounds := 5
		r := compressionhelpers.NewFastRotation(dim, rounds, rng.Uint64())

		bound := 5.8 / math.Sqrt(float64(r.OutputDim))
		for i := range dim {
			x := make([]float32, dim)
			x[i] = 1.0
			rx := r.Rotate(x)
			for j := range rx {
				assert.Less(t, math.Abs(float64(rx[j])), bound,
					"Failure in index %d of %d-dim vector e%d rotated into %d-dims using %d rounds", j, dim, i, len(rx), rounds)
			}
		}
	}
}

// The uniform distribution on the d-dimensional sphere can be produced by
// normalizing a d-dimensional vector of i.i.d standard normal distributed
// random variables to unit length.
//
// The ith entry of a randomly rotated unit vector should therefore follow the
// distribution Zi / SQRT(Z1^2 + ... + Zd^2).
//
// When d is relatively large, e.g. d >= 32, the denominator should be tightly
// concentrated around sqrt(d). We can therefore test that (sqrt(d)*Rx)_i
// behaves like a standard normal distributed variable.
func TestFastRotatedEntriesAreNormalizedGaussian(t *testing.T) {
	type CumulativeProbabilityCount struct {
		Value       float64
		Probability float64
		Count       int
	}
	cdf := []CumulativeProbabilityCount{
		{-3.0, 1.0 - 0.9987, 0},
		{-2.0, 1.0 - 0.9772, 0},
		{-1.0, 1.0 - 0.8413, 0},
		{-0.5, 1.0 - 0.6915, 0},
		{-0.25, 1.0 - 0.5987, 0},
		{0.0, 0.5, 0},
		{0.25, 0.5987, 0},
		{0.5, 0.6915, 0},
		{1.0, 0.8413, 0},
		{2.0, 0.9772, 0},
		{3.0, 0.9987, 0},
	}

	dimensions := make([]int, 0)
	for i := range 15 {
		dimensions = append(dimensions, 64+i*64)
	}

	for _, d := range dimensions {
		rounds := 5
		var seed uint64 = 424242
		r := compressionhelpers.NewFastRotation(d, rounds, seed)
		for i := range d {
			v := make([]float32, d)
			v[i] = 1.0
			rv := r.Rotate(v)
			for j := range d {
				z := math.Sqrt(float64(d)) * float64(rv[j])
				for k := range cdf {
					if z < cdf[k].Value {
						cdf[k].Count++
					}
				}
			}
		}
		acceptableDeviation := 0.012 // Acceptable absolute deviation from standard normal CDF.
		n := d * d                   // Number of measurements.
		for i := range cdf {
			p := float64(cdf[i].Count) / float64(n)
			assert.Less(t, math.Abs(p-cdf[i].Probability), acceptableDeviation)
			// Reset for next run.
			cdf[i].Count = 0
		}
	}
}

func TestFastRotatedVectorsAreUniformOnSphere(t *testing.T) {
	d := 128
	rounds := 3
	v := make([]float32, d)
	v[0] = 1.0
	target := 100
	n := 8 * target
	var count int
	for i := range n {
		r := compressionhelpers.NewFastRotation(d, rounds, uint64(i))
		rv := r.Rotate(v)
		if rv[17] < 0 && rv[32] > 0 && rv[41] < 0 {
			count++
		}
	}
	dev := 15
	assert.Greater(t, count, target-dev)
	assert.Less(t, count, target+dev)
}

// For testing that the unrolled recursion gives the same result.
func fastWalshHadamardTransform(x []float32, normalize float32) {
	if len(x) == 2 {
		x[0], x[1] = normalize*(x[0]+x[1]), normalize*(x[0]-x[1])
		return
	}
	m := len(x) / 2
	fastWalshHadamardTransform(x[:m], normalize)
	fastWalshHadamardTransform(x[m:], normalize)
	for i := range m {
		x[i], x[m+i] = x[i]+x[m+i], x[i]-x[m+i]
	}
}

func TestFastWalshHadamardTransform64(t *testing.T) {
	rng := newRNG(7212334)
	n := 1000
	dim := 64
	for range n {
		x := make([]float32, dim)
		for i := range x {
			x[i] = 1
			if rng.Float64() < 0.5 {
				x[i] = -1
			}
		}
		target := make([]float32, dim)
		copy(target, x)
		fastWalshHadamardTransform(target, 0.125)
		compressionhelpers.FastWalshHadamardTransform64(x)
		assert.True(t, slices.Equal(x, target))
	}
}

func TestFastWalshHadamardTransform256(t *testing.T) {
	rng := newRNG(7212334)
	n := 1000
	dim := 256
	for range n {
		x := make([]float32, dim)
		for i := range x {
			x[i] = 1
			if rng.Float64() < 0.5 {
				x[i] = -1
			}
		}
		target := make([]float32, 256)
		copy(target, x)
		fastWalshHadamardTransform(target, 0.0625)
		compressionhelpers.FastWalshHadamardTransform256(x)
		assert.True(t, slices.Equal(x, target))
	}
}

func BenchmarkFastRotation(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 512, 768, 1024, 1536, 2048}
	rounds := []int{3, 5}
	for _, dim := range dimensions {
		x := make([]float32, dim)
		x[0] = 1.0
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastRotation(dim, r, rng.Uint64())
			b.Run(fmt.Sprintf("Rotate-d%d-r%d", dim, r), func(b *testing.B) {
				for b.Loop() {
					rotation.Rotate(x)
				}
			})
		}
	}
}

func BenchmarkFastRotationError(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{64, 128, 256, 512, 768, 1024, 1536, 2048}
	rounds := []int{3, 5}
	for _, dim := range dimensions {
		x := randomUnitVector(dim, rng)
		y := randomUnitVector(dim, rng)
		target := dot(x, y)
		for _, r := range rounds {
			b.Run(fmt.Sprintf("Rotate-d%d-r%d", dim, r), func(b *testing.B) {
				var errorSum float64
				var maxError float64
				for b.Loop() {
					b.StopTimer()
					rotation := compressionhelpers.NewFastRotation(dim, r, rng.Uint64())
					b.StartTimer()
					rx := rotation.Rotate(x)
					ry := rotation.Rotate(y)
					b.StopTimer()
					err := math.Abs(dot(rx, ry) - target)
					errorSum += err
					if err > maxError {
						maxError = err
					}
				}
				// The absolute errors are approximately of size 1e-7.
				errorScale := 1e7
				b.ReportMetric(errorScale*errorSum/float64(b.N), "error(avg)")
				b.ReportMetric(errorScale*maxError, "error(max)")
			})
		}
	}
}

// The uniform distribution on the d-dimensional sphere can be produced by
// normalizing a d-dimensional vector of i.i.d standard normal distributed
// random variables to unit length.
//
// The ith entry of a randomly rotated unit vector should therefore follow the
// distribution Zi / SQRT(Z1^2 + ... + Zd^2).
//
// When d is relatively large, e.g. d >= 32, the denominator should be tightly
// concentrated around sqrt(d). We can therefore test that (sqrt(d)*Rx)_i
// behaves like a standard normal distributed variable.
func BenchmarkFastRotationOutputDistribution(b *testing.B) {
	rounds := []int{1, 2, 3, 4, 5}
	inputDimensions := []int{1536}
	rng := newRNG(1234)

	for _, inputDim := range inputDimensions {
		for _, r := range rounds {
			b.Run(fmt.Sprintf("Rotation-d%d-r%d", inputDim, r), func(b *testing.B) {
				type CumulativeProbabilityCount struct {
					Value       float64
					Probability float64
					Count       int
				}
				cdf := []CumulativeProbabilityCount{
					{-3.0, 1.0 - 0.9987, 0},
					{-2.0, 1.0 - 0.9772, 0},
					{-1.0, 1.0 - 0.8413, 0},
					{-0.5, 1.0 - 0.6915, 0},
					{-0.25, 1.0 - 0.5987, 0},
					{0.0, 0.5, 0},
					{0.25, 0.5987, 0},
					{0.5, 0.6915, 0},
					{1.0, 0.8413, 0},
					{2.0, 0.9772, 0},
					{3.0, 0.9987, 0},
				}

				var intervalSum float64
				var intervalMax float64
				var outDim int
				for b.Loop() {
					rotation := compressionhelpers.NewFastRotation(inputDim, r, rng.Uint64())
					outDim = int(rotation.OutputDim)
					// We rotate each of the d unit vectors and collect measurements.
					for i := range outDim {
						v := make([]float32, outDim)
						v[i] = 1.0
						rv := rotation.Rotate(v)
						for j := range outDim {
							z := math.Sqrt(float64(outDim)) * float64(rv[j])
							for k := range cdf {
								if z < cdf[k].Value {
									cdf[k].Count++
								}
							}
						}
						interval := float64(slices.Max(rv) - slices.Min(rv))
						intervalSum += interval
						if interval > intervalMax {
							intervalMax = interval
						}
					}

				}

				// Report the max deviation from the normal CDF.
				var maxDeviation float64
				for i := range cdf {
					numEntries := outDim * outDim * b.N
					p := float64(cdf[i].Count) / float64(numEntries)
					dev := math.Abs(p - cdf[i].Probability)
					if dev > maxDeviation {
						maxDeviation = dev
					}
				}
				b.ReportMetric(maxDeviation, "cdf_dev(max)")

				// Report the average and max interval length.
				numIntervals := outDim * b.N
				avgInterval := intervalSum / float64(numIntervals)
				b.ReportMetric(avgInterval, "interval(avg)")
				b.ReportMetric(intervalMax, "interval(max)")
			})
		}
	}
}
