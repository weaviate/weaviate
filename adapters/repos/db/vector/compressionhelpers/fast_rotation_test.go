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

func newRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
}

func TestFastRotationPreservesNorm(t *testing.T) {
	rng := newRNG(42)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129, 407}
	rounds := []int{1, 2, 3}
	n := 10
	for _, d := range dimensions {
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastRotation(d, r, rng.Uint64())
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

func TestFastRotationPreservesDistance(t *testing.T) {
	rng := newRNG(4242)
	dimensions := []int{3, 8, 26, 32, 33, 61, 127, 128, 129}
	rounds := []int{1, 2, 3}
	n := 10
	for _, d := range dimensions {
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastRotation(d, r, rng.Uint64())
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
		dim := 2 + rng.IntN(300)
		rounds := 1 + rng.IntN(5)
		r := compressionhelpers.NewFastRotation(dim, rounds, rng.Uint64())

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

func TestFastWalshHadamardTransform64(t *testing.T) {
	x := []float64{-1, -1, 1, 1, 1, -1, 1, 1}
	compressionhelpers.FastWalshHadamardTransform64(x)
	xExpected := []float64{2, 2, -6, 2, -2, -2, -2, -2}
	for i := range x {
		assert.Equal(t, xExpected[i], x[i])
	}
}

func BenchmarkFastRotation(b *testing.B) {
	rng := newRNG(42)
	dimensions := []int{128, 256, 512, 1024, 1536}
	slices.Sort(dimensions)
	rounds := []int{1, 3, 5}
	for _, dim := range dimensions {
		x := make([]float32, dim)
		x[0] = 1.0
		for _, r := range rounds {
			rotation := compressionhelpers.NewFastRotation(dim, r, rng.Uint64())
			b.Run(fmt.Sprintf("Rotation-d%d-r%d", dim, r), func(b *testing.B) {
				for b.Loop() {
					rotation.Rotate(x)
				}
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			})
		}
	}
}
