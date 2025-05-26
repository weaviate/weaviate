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
)

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
}

func TestFastRotationPreservesNorm(t *testing.T) {
	rng := newRNG(42)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129}
	rounds := []int{1, 2, 3}
	n := 10
	for _, d := range dimensions {
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastBlock64Rotation(d, r, rng.Uint64())
			for range n {
				z := randomNormalVector(d, rng)
				rz := rotation.Rotate(z)
				assert.Less(t, math.Abs(norm(rz)-norm(z)), 1e-6)
			}
		}
	}
}

func TestFastRotationOutputLength(t *testing.T) {
	rng := newRNG(42)
	n := 100
	for range n {
		d := rng.IntN(1000)
		r := 1 + rng.IntN(5)
		rotation := compressionhelpers.NewFastBlock64Rotation(d, r, rng.Uint64())
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

func TestFastRotationPreservesDistance(t *testing.T) {
	rng := newRNG(4242)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129}
	rounds := []int{1, 2, 3}
	n := 10
	for _, d := range dimensions {
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastBlock64Rotation(d, r, rng.Uint64())
			for range n {
				z1 := randomNormalVector(d, rng)
				z2 := randomNormalVector(d, rng)
				rz1 := rotation.Rotate(z1)
				rz2 := rotation.Rotate(z2)
				assert.Less(t, math.Abs(dist(rz1, rz2)-dist(z1, z2)), 1.5e-6)
			}
		}
	}
}

// Rotate the standard basis and verify that the rotated vectors are orthogonal.
func TestFastRotationPreservesOrthogonality(t *testing.T) {
	rng := newRNG(424242)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129}
	for _, d := range dimensions {
		unitVectors := make([][]float32, d)
		for i := range d {
			v := make([]float32, d)
			v[i] = 1.0
			unitVectors[i] = v
		}

		r := compressionhelpers.NewFastBlock64Rotation(d, 3, rng.Uint64())
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
		dim := rng.IntN(300)
		rounds := 1 + rng.IntN(5)
		r := compressionhelpers.NewFastBlock64Rotation(dim, rounds, rng.Uint64())

		bound := 5.5 / math.Sqrt(float64(r.OutputDimension()))
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

	dimensions := []int{412, 764, 1024}
	var seed uint64 = 42
	rounds := 3
	for _, d := range dimensions {
		r := compressionhelpers.NewFastBlock64Rotation(d, rounds, seed)
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

		acceptableDeviation := 0.01 // Acceptable absolute deviation from standard normal CDF.
		n := d * d                  // Number of measurements.
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
	rounds := 5
	v := make([]float32, d)
	v[0] = 1.0
	target := 100
	n := 8 * target
	var count int
	for i := range n {
		r := compressionhelpers.NewFastBlock64Rotation(d, rounds, uint64(i))
		rv := r.Rotate(v)
		if rv[17] < 0 && rv[32] > 0 && rv[41] < 0 {
			count++
		}
	}

	dev := 15
	assert.Greater(t, count, target-dev)
	assert.Less(t, count, target+dev)
}

func TestFastWalshHadamardTransform64(t *testing.T) {
	x := []float64{-1, -1, 1, 1, 1, -1, 1, 1}
	compressionhelpers.FastWalshHadamardTransform64(x)
	xExpected := []float64{2, 2, -6, 2, -2, -2, -2, -2}
	for i := range x {
		assert.Equal(t, xExpected[i], x[i])
	}
}

func TestPermutation(t *testing.T) {
	rng := newRNG(435564)
	n := 1000
	for range n {
		d := 2 + rng.IntN(10000)
		s := compressionhelpers.RandomSignsInt8(d, rng)
		p := compressionhelpers.RandomPermutationInt32(d, rng)

		x := make([]float64, d)
		for j := range d {
			x[j] = rng.Float64()
		}

		expected := make([]float64, d)
		for j := range d {
			expected[p[j]] = float64(s[p[j]]) * x[j]
		}

		compressionhelpers.PermuteAndApplySigns(x, p, s)

		assert.True(t, slices.Equal(expected, x))
		t.Log("expected", expected)
		t.Log("x", x)
	}
}

func BenchmarkFastRotation(b *testing.B) {
	rng := newRNG(42)
	numDimensions := 100
	dimensions := make([]int, numDimensions)
	for i := range dimensions {
		dimensions[i] = 2 + rng.IntN(5000)
	}
	// dimensions := []int{256, 512, 511}
	slices.Sort(dimensions)
	rounds := []int{3, 5}
	for _, dim := range dimensions {
		for _, r := range rounds {
			// rotation := compressionhelpers.NewFastBlock64Rotation(dim, r, rng.Uint64())
			rotation := compressionhelpers.NewFastBlock64Rotation(dim, r, rng.Uint64())
			var rangeSum float32
			var max float32 = -1e15
			var min float32 = 1e15
			var maxRange float32
			var symmetrySum float64
			var minMaxSum float64
			entrySums := make([]float64, rotation.OutputDimension())

			b.Run(fmt.Sprintf("Rotation-d%d-r%d", dim, r), func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					x := make([]float32, dim)
					x[rng.IntN(dim)] = 1.0
					b.StartTimer()
					rx := rotation.Rotate(x)
					b.StopTimer()
					// Collect metrics
					l := len(rx) - 1
					slices.Sort(rx)
					for i := range rx {
						entrySums[i] += float64(rx[i])
					}

					if rx[0] < min {
						min = rx[0]
					}
					if rx[l] > max {
						max = rx[l]
					}
					if rx[l]-rx[0] > maxRange {
						maxRange = rx[l] - rx[0]
					}
					// Find location where sign changes
					s := 0
					for rx[s] < 0 {
						s++
					}
					symmetrySum += float64(s) / float64(l+1)
					minMaxSum += float64(rx[l] + rx[0])
					rangeSum += rx[l] - rx[0]
				}

				u := math.Sqrt(float64(rotation.OutputDimension()))
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
				b.ReportMetric(symmetrySum/float64(b.N), "sym(0.5)")
				b.ReportMetric(minMaxSum/float64(b.N), "sym(0.0)")
				b.ReportMetric(float64(min)*u, "min")
				b.ReportMetric(float64(max)*u, "max")
				b.ReportMetric(float64(rangeSum)/float64(b.N), "r-avg")
				b.ReportMetric(float64(max)-float64(min), "r-max")
				// Report some entry sums
				absoluteQuantiles := []int{0, 1, 5, 10}
				outDim := rotation.OutputDimension()
				for _, q := range absoluteQuantiles {
					b.ReportMetric(u*entrySums[q]/float64(b.N), fmt.Sprintf("q%d", q))
					b.ReportMetric(u*entrySums[outDim-1-q]/float64(b.N), fmt.Sprintf("q%d", outDim-q))
				}
			})
		}
	}
}

func BenchmarkFastWalshHadamardTransform(b *testing.B) {
	vg := NewVectorGenerator()
	dimensions := []int{64, 256, 1024, 2048}
	for _, dim := range dimensions {
		blueprint := vg.RandomUnitVector(dim)
		x := make([]float32, dim)
		b.Run(fmt.Sprintf("Rotation-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				copy(x, blueprint)
				b.StartTimer()
				compressionhelpers.FastWalshHadamardTransform(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
	}
}
