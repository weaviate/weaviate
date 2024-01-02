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

func TestManhattanDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{3, 4, 5}
		expectedDistance := float32(0)

		dist, ok, err := NewManhattanProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewManhattanProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("same angle, different euclidean position", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{1.5, 2, 2.5}
		// distance will be abs(3-1.5) + abs(4-2) + abs(5-2.5) = 1.5 + 2 + 2.5 = 6
		expectedDistance := float32(6)

		dist, ok, err := NewManhattanProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewManhattanProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("different vectors", func(t *testing.T) {
		vec1 := []float32{10, 11}
		vec2 := []float32{13, 15}
		// distance will be calculated as abs(10-13) + abs(11-15) = 3 + 4 = 7
		expectedDistance := float32(7)

		dist, ok, err := NewManhattanProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewManhattanProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})
}

func TestManhattanDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{1.5, 2, 2.5}

		expectedDistance, ok, err := NewManhattanProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)

		distanceProvider := NewManhattanProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}
