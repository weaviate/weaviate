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

package distancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCosineDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := Normalize([]float32{0.1, 0.3, 0.7})
		vec2 := Normalize([]float32{0.1, 0.3, 0.7})
		expectedDistance := float32(0.0)

		dist, ok, err := NewCosineDistanceProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewCosineDistanceProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("different vectors, but identical angle", func(t *testing.T) {
		vec1 := Normalize([]float32{0.1, 0.3, 0.7})
		vec2 := Normalize([]float32{0.2, 0.6, 1.4})
		expectedDistance := float32(0.0)

		dist, ok, err := NewCosineDistanceProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewCosineDistanceProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("different vectors", func(t *testing.T) {
		vec1 := Normalize([]float32{0.1, 0.3, 0.7})
		vec2 := Normalize([]float32{0.2, 0.2, 0.2})
		expectedDistance := float32(0.173)

		dist, ok, err := NewCosineDistanceProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewCosineDistanceProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.InDelta(t, expectedDistance, dist, 0.01)
	})

	t.Run("opposite vectors", func(t *testing.T) {
		// This is unique to cosine/angular distance.
		vec1 := Normalize([]float32{0.1, 0.3, 0.7})
		vec2 := Normalize([]float32{-0.1, -0.3, -0.7})
		expectedDistance := float32(2)

		dist, ok, err := NewCosineDistanceProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewCosineDistanceProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.InDelta(t, expectedDistance, dist, 0.01)
	})
}

func TestCosineDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := Normalize([]float32{3, 4, 5})
		vec2 := Normalize([]float32{-3, -4, -5})

		expectedDistance, ok, err := NewCosineDistanceProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)

		distanceProvider := NewCosineDistanceProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}
