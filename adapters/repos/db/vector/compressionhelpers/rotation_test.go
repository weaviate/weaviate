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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// The error from the projection is surprisingly large. Could be due to the
// numerical stability of the implementation of the QR factorization, or because
// we cast from float64 to float32. We could try implementing the Modified
// Gram-Schmidt algorithm directly using float32, but the QR factorization from
// gonum should be better as it uses BLAS/LAPACK and there are well-known more
// stable QR factorization algorithms.
const eps = 1e-6

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

func TestRotationPreservesNorm(t *testing.T) {
	var seed1 uint64 = 0xf1ceff93c6927ddf
	var seed2 uint64 = 0x2b5ae5debbb4d66c
	rng := rand.New(rand.NewPCG(seed1, 0x385ab5285169b1ac))
	for d := 2; d < 15; d++ {
		z := randomNormalVector(d, rng)
		r := compressionhelpers.NewRotation(d, seed2^uint64(d))
		rz := r.Rotate(z)
		assert.Less(t, math.Abs(norm(rz)-norm(z)), eps)
	}
}

func TestRotationPreservesDistance(t *testing.T) {
	var seed1 uint64 = 0xf1ceff93c6927ddf
	var seed2 uint64 = 0x2b5ae5debbb4d66c
	rng := rand.New(rand.NewPCG(seed1, 0x385ab5285169b1ac))
	for d := 2; d < 15; d++ {
		z1 := randomNormalVector(d, rng)
		z2 := randomNormalVector(d, rng)
		r := compressionhelpers.NewRotation(d, seed2^uint64(d))
		rz1 := r.Rotate(z1)
		rz2 := r.Rotate(z2)
		assert.Less(t, math.Abs(dist(rz1, rz2)-dist(z1, z2)), eps)
	}
}

// Rotate the standard basis and verify that the rotated vectors are orthogonal.
func TestRotationPreservesOrthogonality(t *testing.T) {
	d := 64
	unitVectors := make([][]float32, d)
	for i := range d {
		v := make([]float32, d)
		v[i] = 1.0
		unitVectors[i] = v
	}

	var seed uint64 = 42
	r := compressionhelpers.NewRotation(d, seed)
	rotatedVectors := make([][]float32, d)
	for i := range d {
		rotatedVectors[i] = r.Rotate(unitVectors[i])
	}

	for i := range d {
		u := rotatedVectors[i]
		for j := range d {
			v := rotatedVectors[j]
			if i == j {
				assert.Less(t, math.Abs(dot(u, v)-1.0), eps)
			} else {
				assert.Less(t, math.Abs(dot(u, v)), eps)
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
func TestRotatedEntriesAreNormalizedGaussian(t *testing.T) {
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

	d := 128
	var seed uint64 = 42
	r := compressionhelpers.NewRotation(d, seed)
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
	for _, c := range cdf {
		p := float64(c.Count) / float64(n)
		assert.Less(t, math.Abs(p-c.Probability), acceptableDeviation)

	}
}

func orthant(x []float32) int {
	var s int
	for i := range x {
		if x[i] > 0 {
			s ^= 1 << i
		}
	}
	return s
}

// Test that approximately the same number of vectors end up in each orthant
// when we apply different random rotations to the same vector.
func TestRotatedVectorsAreUniformOnSphere(t *testing.T) {
	d := 4
	orthants := 1 << d
	count := make([]int, 1<<d) // Count of how many vectors are rotated into each orthant.

	v := make([]float32, d)
	v[0] = 1.0

	m := 250          // Target number of vectors in each orthant.
	n := m * orthants // Number of random repetitions.
	for i := range n {
		r := compressionhelpers.NewRotation(d, uint64(i))
		rv := r.Rotate(v)
		count[orthant(rv)]++
	}

	dev := 25
	for _, c := range count {
		assert.Greater(t, c, m-dev)
		assert.Less(t, c, m+dev)
	}
}

func BenchmarkRotation(b *testing.B) {
	vg := NewVectorGenerator()
	dimensions := []int{64, 256, 1024, 2048}
	for _, dim := range dimensions {
		r := compressionhelpers.NewRotation(dim, 42)
		x := vg.RandomUnitVector(dim)
		b.Run(fmt.Sprintf("Rotation-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				r.Rotate(x)
			}
			b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
		})
	}
}
