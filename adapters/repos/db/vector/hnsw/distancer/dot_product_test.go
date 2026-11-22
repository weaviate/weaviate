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
	"encoding/json"
	"math"
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

// Regression test for weaviate/weaviate#13578: the dot product of finite
// vectors with large components overflows float32, which used to surface as a
// -Inf self-distance (and broke JSON serialization of the whole REST/GraphQL
// response with a 422). Distances of finite vectors must stay finite.
func TestDotDistancerLargeVectors(t *testing.T) {
	t.Run("large vector self-distance is finite and saturates", func(t *testing.T) {
		large := []float32{1e20, 1e20}

		dist, err := NewDotProductProvider().New(large).Distance(large)
		require.NoError(t, err)
		assert.False(t, math.IsInf(float64(dist), 0), "distance must not be -Inf")
		assert.False(t, math.IsNaN(float64(dist)), "distance must not be NaN")
		// 1e20*1e20 + 1e20*1e20 = 2e40 overflows float32, so the negated
		// distance saturates at the most negative finite float32 value.
		assert.Equal(t, float32(-math.MaxFloat32), dist)

		control, err := NewDotProductProvider().SingleDist(large, large)
		require.NoError(t, err)
		assert.Equal(t, dist, control)

		// A saturated distance must still serialize to JSON, otherwise one
		// row would fail the entire REST/GraphQL response.
		_, err = json.Marshal(dist)
		require.NoError(t, err)
	})

	t.Run("large vector against normal vector stays ordered", func(t *testing.T) {
		large := []float32{1e20, 1e20}
		normal := []float32{0.1, 0.1}

		distLarge, err := NewDotProductProvider().New(large).Distance(large)
		require.NoError(t, err)
		distNormal, err := NewDotProductProvider().New(large).Distance(normal)
		require.NoError(t, err)

		// -2e40 (saturated) < -2e19: the identical vector is still nearer.
		assert.Less(t, distLarge, distNormal)
		assert.False(t, math.IsInf(float64(distNormal), 0))
		// 1e20*0.1 + 1e20*0.1 = 2e19, representable in float32.
		assert.InDelta(t, -2e19, float64(distNormal), 2e19*1e-6)
	})

	t.Run("opposite large vectors saturate at positive bound", func(t *testing.T) {
		large := []float32{1e20, 1e20}
		opposite := []float32{-1e20, -1e20}

		dist, err := NewDotProductProvider().New(large).Distance(opposite)
		require.NoError(t, err)
		assert.False(t, math.IsInf(float64(dist), 0), "distance must not be +Inf")
		// Dot product -2e40, negated distance +2e40 saturates at +MaxFloat32.
		assert.Equal(t, float32(math.MaxFloat32), dist)
	})

	t.Run("step by step with large vectors is finite", func(t *testing.T) {
		large := []float32{1e20, 1e20}
		provider := NewDotProductProvider()

		sum := float32(0.0)
		for i := range large {
			step := provider.Step([]float32{large[i]}, []float32{large[i]})
			assert.False(t, math.IsInf(float64(step), 0), "step must not be +Inf")
			sum += step
		}
		dist := provider.Wrap(sum)
		assert.False(t, math.IsInf(float64(dist), 0), "distance must not be -Inf")
		assert.Equal(t, float32(-math.MaxFloat32), dist)
	})
}
