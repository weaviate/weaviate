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

func TestHammingDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{3, 4, 5}
		expectedDistance := float32(0)

		dist, ok, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("same angle, different euclidean position", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{1.5, 2, 2.5}
		expectedDistance := float32(3) // all three positions are different

		dist, ok, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("one position different", func(t *testing.T) {
		vec1 := []float32{10, 11}
		vec2 := []float32{10, 15}
		expectedDistance := float32(1)

		dist, ok, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("three positions different", func(t *testing.T) {
		vec1 := []float32{10, 11, 15, 25, 31}
		vec2 := []float32{10, 15, 16, 25, 30}
		expectedDistance := float32(3)

		dist, ok, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})
}

func TestHammingDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := []float32{10, 11, 15, 25, 31}
		vec2 := []float32{10, 15, 16, 25, 30}

		expectedDistance, ok, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)

		distanceProvider := NewHammingProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}
