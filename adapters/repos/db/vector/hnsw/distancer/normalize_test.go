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

package distancer

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalize(t *testing.T) {
	t.Run("normalize produces unit vector", func(t *testing.T) {
		v := []float32{3, 4}
		result := Normalize(v)

		// magnitude should be 1
		var mag float32
		for _, x := range result {
			mag += x * x
		}
		assert.InDelta(t, 1.0, mag, 0.0001)

		// original should be unchanged
		assert.Equal(t, []float32{3, 4}, v)
	})

	t.Run("zero vector returns zero vector", func(t *testing.T) {
		v := []float32{0, 0, 0}
		result := Normalize(v)
		assert.Equal(t, []float32{0, 0, 0}, result)
	})

	t.Run("large components do not collapse to zero", func(t *testing.T) {
		// 1e20*1e20 is 1e40, past the float32 max, so the sum of squares
		// overflows to +Inf and the vector used to scale to all zeros.
		v := []float32{1e20, 1e20, 1e20, 1e20}
		assertUnitLength(t, Normalize(v))
	})

	t.Run("tiny components do not collapse to zero", func(t *testing.T) {
		// The mirror case: 1e-25*1e-25 underflows to 0 in float32, so the sum
		// of squares reads as a zero vector.
		v := []float32{1e-25, 1e-25, 1e-25, 1e-25}
		assertUnitLength(t, Normalize(v))
	})
}

// assertUnitLength checks the magnitude in float64 so that the check itself
// does not overflow on the inputs it is meant to cover.
func assertUnitLength(t *testing.T, v []float32) {
	t.Helper()
	var mag float64
	for _, x := range v {
		mag += float64(x) * float64(x)
	}
	assert.InDelta(t, 1.0, math.Sqrt(mag), 0.0001)
}

func TestNormalizeInPlace(t *testing.T) {
	t.Run("produces same result as Normalize", func(t *testing.T) {
		v1 := []float32{3, 4, 5, 6, 7, 8}
		v2 := make([]float32, len(v1))
		copy(v2, v1)

		expected := Normalize(v1)
		NormalizeInPlace(v2)

		assert.Equal(t, expected, v2)
	})

	t.Run("modifies vector in place", func(t *testing.T) {
		v := []float32{3, 4}
		NormalizeInPlace(v)

		// magnitude should be 1
		var mag float32
		for _, x := range v {
			mag += x * x
		}
		assert.InDelta(t, 1.0, mag, 0.0001)

		// check expected values: 3/5 = 0.6, 4/5 = 0.8
		assert.InDelta(t, 0.6, v[0], 0.0001)
		assert.InDelta(t, 0.8, v[1], 0.0001)
	})

	t.Run("zero vector remains zero", func(t *testing.T) {
		v := []float32{0, 0, 0}
		NormalizeInPlace(v)
		assert.Equal(t, []float32{0, 0, 0}, v)
	})

	t.Run("single element vector", func(t *testing.T) {
		v := []float32{5}
		NormalizeInPlace(v)
		assert.InDelta(t, 1.0, v[0], 0.0001)
	})

	t.Run("negative values", func(t *testing.T) {
		v1 := []float32{-3, 4, -5}
		v2 := make([]float32, len(v1))
		copy(v2, v1)

		expected := Normalize(v1)
		NormalizeInPlace(v2)

		assert.Equal(t, expected, v2)
	})

	t.Run("empty vector", func(t *testing.T) {
		v := []float32{}
		NormalizeInPlace(v)
		assert.Equal(t, []float32{}, v)
	})

	t.Run("large components do not collapse to zero", func(t *testing.T) {
		v := []float32{1e20, -1e20, 1e20, -1e20}
		NormalizeInPlace(v)
		assertUnitLength(t, v)
	})
}

// TestNormalizeMatchesScalarReference checks the SIMD-backed implementation
// against the straightforward scalar definition. The implementation
// multiplies by the reciprocal norm rather than dividing, so results may
// differ by one ulp per element.
func TestNormalizeMatchesScalarReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 22))
	for _, n := range []int{1, 2, 3, 7, 15, 64, 100, 384, 1536} {
		v := make([]float32, n)
		for i := range v {
			v[i] = float32(rng.NormFloat64() * 100)
		}
		var norm float32
		for _, x := range v {
			norm += x * x
		}
		norm = float32(math.Sqrt(float64(norm)))
		got := Normalize(v)
		for i := range v {
			assert.InDelta(t, v[i]/norm, got[i], 1e-6, "n=%d index=%d", n, i)
		}
	}
}

func TestNormalizeInto(t *testing.T) {
	t.Run("matches Normalize with dirty undersized buffer", func(t *testing.T) {
		v := []float32{3, -4, 5, -6, 7}
		got := NormalizeInto([]float32{99}, v)
		assert.Equal(t, Normalize(v), got)
		assert.Equal(t, []float32{3, -4, 5, -6, 7}, v, "input must be preserved")
	})

	t.Run("reuses oversized buffer", func(t *testing.T) {
		buf := make([]float32, 8)
		for i := range buf {
			buf[i] = float32(math.NaN())
		}
		v := []float32{3, 4}
		got := NormalizeInto(buf, v)
		assert.Equal(t, 2, len(got))
		assert.Same(t, &buf[0], &got[0], "oversized buffer must be reused")
		assert.Equal(t, Normalize(v), got)
	})

	t.Run("zero vector fills buffer with zeros", func(t *testing.T) {
		buf := []float32{1, 2, 3}
		got := NormalizeInto(buf, []float32{0, 0, 0})
		assert.Equal(t, []float32{0, 0, 0}, got)
	})

	t.Run("large components do not collapse to zero", func(t *testing.T) {
		v := []float32{1e20, 1e20, 1e20, 1e20}
		got := NormalizeInto([]float32{99}, v)
		assertUnitLength(t, got)
		assert.Equal(t, []float32{1e20, 1e20, 1e20, 1e20}, v, "input must be preserved")
	})
}

func BenchmarkNormalize(b *testing.B) {
	rng := rand.New(rand.NewPCG(23, 24))
	v := make([]float32, 1536)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	b.Run("alloc", func(b *testing.B) {
		for b.Loop() {
			Normalize(v)
		}
	})
	buf := make([]float32, 1536)
	b.Run("into", func(b *testing.B) {
		for b.Loop() {
			NormalizeInto(buf, v)
		}
	})
}
