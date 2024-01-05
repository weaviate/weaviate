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

func TestDotDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{3, 4, 5}
		expectedDistance := float32(-50)

		dist, ok, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("without matching dimensions", func(t *testing.T) {
		vec1 := []float32{0, 1, 0, 2, 0, 3}
		vec2 := []float32{1, 0, 2, 0, 3, 0}
		expectedDistance := float32(0)

		dist, ok, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("very different vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{-3, -4, -5}
		expectedDistance := float32(+50)

		dist, ok, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)
		control, ok, err := NewDotProductProvider().SingleDist(vec1, vec2)
		require.True(t, ok)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})
}

func TestDotDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{-3, -4, -5}

		expectedDistance, ok, err := NewDotProductProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)
		require.True(t, ok)

		distanceProvider := NewDotProductProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}
