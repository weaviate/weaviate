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
	"github.com/stretchr/testify/require"
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

// TestNormalizeExtremeMagnitudes covers vectors that are finite and nonzero
// but whose float32 squares underflow to zero (subnormals, tiny normals) or
// overflow to +Inf (huge components). See issues #13751 and #11671.
func TestNormalizeExtremeMagnitudes(t *testing.T) {
	// smallest positive float32 subnormal: 1.4012984643248171e-45 (2^-149)
	const minSubnormal = float32(0x1p-149)

	t.Run("issue 13751: subnormal vector normalizes to unit vector", func(t *testing.T) {
		mk := func() []float32 {
			return []float32{0, 0, 0, minSubnormal, 0, 0, 0}
		}
		want := []float32{0, 0, 0, 1, 0, 0, 0}

		assert.Equal(t, want, Normalize(mk()))

		v := mk()
		NormalizeInPlace(v)
		assert.Equal(t, want, v)

		got := NormalizeInto([]float32{99, 99, 99, 99, 99, 99, 99}, mk())
		assert.Equal(t, want, got)
	})

	t.Run("smallest subnormal normalizes to one", func(t *testing.T) {
		assert.Equal(t, []float32{1}, Normalize([]float32{minSubnormal}))
	})

	t.Run("tiny normal components do not read as zero", func(t *testing.T) {
		v := []float32{1e-23, -1e-23}
		got := Normalize(v)
		assert.InDelta(t, math.Sqrt2/2, got[0], 1e-6)
		assert.InDelta(t, -math.Sqrt2/2, got[1], 1e-6)
	})

	// The float32 dot product of these components rounds up to a squared
	// norm that is positive but subnormal (3e-45 < 2^-126). It must still
	// take the float64 path: the fast path's reciprocal is computed from a
	// norm that lost ~55% of its true value to underflow rounding, so
	// scaling by it yields components around 0.5667 instead of 0.7071.
	// This is the case a weakening of the cutoff to `norm2 > 0` would
	// silently pass; the 1e-23 case above squares all the way to zero and
	// so does not exercise this branch.
	t.Run("positive subnormal squared norm keeps the float64 path", func(t *testing.T) {
		v := []float32{3e-23, -3e-23}
		norm2 := squaredNorm(v)
		require.True(t, norm2 > 0 && norm2 < minNormalFloat32,
			"precondition: float32 squared norm must be positive and subnormal, got %g", norm2)

		got := Normalize(v)
		assert.InDelta(t, math.Sqrt2/2, got[0], 1e-6)
		assert.InDelta(t, -math.Sqrt2/2, got[1], 1e-6)

		inPlace := []float32{3e-23, -3e-23}
		NormalizeInPlace(inPlace)
		assert.InDelta(t, math.Sqrt2/2, inPlace[0], 1e-6)
		assert.InDelta(t, -math.Sqrt2/2, inPlace[1], 1e-6)
	})

	t.Run("large vectors do not collapse to zero", func(t *testing.T) {
		// squares overflow float32 (+Inf) even though the vector is finite
		v := []float32{1e20, 1e20}
		got := Normalize(v)
		assert.InDelta(t, math.Sqrt2/2, got[0], 1e-6)
		assert.InDelta(t, math.Sqrt2/2, got[1], 1e-6)
	})

	t.Run("genuine zero vectors keep zero semantics", func(t *testing.T) {
		assert.Equal(t, []float32{0, 0, 0}, Normalize([]float32{0, 0, 0}))

		v := []float32{0, 0, 0}
		NormalizeInPlace(v)
		assert.Equal(t, []float32{0, 0, 0}, v)

		got := NormalizeInto([]float32{99, 99, 99}, []float32{0, 0, 0})
		assert.Equal(t, []float32{0, 0, 0}, got)
	})
}
