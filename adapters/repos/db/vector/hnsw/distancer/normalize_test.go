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

func TestNormalizeOut(t *testing.T) {
	t.Run("matches Normalize without touching src", func(t *testing.T) {
		src := []float32{3, 4, 0, -5}
		srcCopy := append([]float32(nil), src...)
		want := Normalize(src)

		dst := make([]float32, len(src))
		got := NormalizeOut(dst, src)

		assert.Equal(t, want, got)
		assert.Equal(t, srcCopy, src, "src must not be mutated")
	})

	t.Run("dst exactly src behaves like NormalizeInPlace", func(t *testing.T) {
		v := []float32{1, 2, 2}
		want := append([]float32(nil), v...)
		NormalizeInPlace(want)

		got := NormalizeOut(v, v)
		assert.Equal(t, want, got)
	})

	t.Run("zero vector stays zero", func(t *testing.T) {
		src := []float32{0, 0, 0}
		dst := []float32{9, 9, 9}
		got := NormalizeOut(dst, src)
		assert.Equal(t, []float32{0, 0, 0}, got)
	})

	t.Run("oversized dst is trimmed to len(src)", func(t *testing.T) {
		src := []float32{1, 0}
		dst := make([]float32, 8)
		got := NormalizeOut(dst, src)
		require.Len(t, got, 2)
		assert.Equal(t, []float32{1, 0}, got)
	})

	t.Run("empty src", func(t *testing.T) {
		got := NormalizeOut(nil, nil)
		assert.Empty(t, got)
	})
}
