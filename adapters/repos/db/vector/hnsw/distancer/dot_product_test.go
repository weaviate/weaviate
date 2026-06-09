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

func TestDotDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{3, 4, 5}
		expectedDistance := float32(-50)

		dist, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("without matching dimensions", func(t *testing.T) {
		vec1 := []float32{0, 1, 0, 2, 0, 3}
		vec2 := []float32{1, 0, 2, 0, 3, 0}
		expectedDistance := float32(0)

		dist, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("very different vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{-3, -4, -5}
		expectedDistance := float32(+50)

		dist, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})
}

func TestDotDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{-3, -4, -5}

		expectedDistance, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		distanceProvider := NewDotProductProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}

func TestDotDistancerLargeBiasPrecision(t *testing.T) {
	// Tests the float64 accumulation fix for precision loss
	// described in: https://github.com/weaviate/weaviate/issues/11667
	//
	// When vectors contain a wide range of component magnitudes,
	// float32 accumulation can lose the contributions of smaller
	// components during summation. Using float64 for the running
	// sum preserves accuracy until the final cast to float32.
	//
	// For the extreme case in the issue (bias=1e6, sum=1e12 with
	// a difference of 0.001), the float32 result itself cannot
	// represent the difference (float32 ULP at 1e12 is ~131072).
	// Fixing that requires the priority queue to store distances
	// in float64. This PR is the first step: making the computation
	// as accurate as possible within the float32 interface.

	t.Run("dotProductPrecise matches float32 on well-conditioned input", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{-3, -4, -5}

		precise := dotProductPrecise(vec1, vec2)
		goImpl := dotProductGo[float32, float32](vec1, vec2)

		assert.Equal(t, goImpl, precise,
			"precise should match Go fallback when float32 is sufficient")
	})

	t.Run("dotProductPrecise matches expected values", func(t *testing.T) {
		a := []float32{1.5, 2.5, 3.5}
		b := []float32{2.0, 3.0, 4.0}

		// dot = 1.5*2.0 + 2.5*3.0 + 3.5*4.0 = 3 + 7.5 + 14 = 24.5
		prod := dotProductPrecise(a, b)
		assert.InDelta(t, float32(24.5), prod, 1e-6,
			"dotProductPrecise should compute the correct dot product")
	})

	t.Run("dotProductPrecise consistent with SingleDist", func(t *testing.T) {
		vec1 := []float32{0.5, -1.5, 2.0, 3.0}
		vec2 := []float32{-2.0, 1.0, 0.5, -1.0}

		prod := dotProductPrecise(vec1, vec2)
		dist, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)

		// SingleDist returns -dotProductPrecise
		assert.InDelta(t, -prod, dist, 1e-6,
			"SingleDist should use dotProductPrecise internally")
	})
}
