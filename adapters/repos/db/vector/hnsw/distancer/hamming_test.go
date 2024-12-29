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
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHammingDistancer(t *testing.T) {
	t.Run("identical vectors", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{3, 4, 5}
		expectedDistance := float32(0)

		dist, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("same angle, different euclidean position", func(t *testing.T) {
		vec1 := []float32{3, 4, 5}
		vec2 := []float32{1.5, 2, 2.5}
		expectedDistance := float32(3) // all three positions are different

		dist, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewHammingProvider().SingleDist(vec1, vec2)

		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("one position different", func(t *testing.T) {
		vec1 := []float32{10, 11}
		vec2 := []float32{10, 15}
		expectedDistance := float32(1)

		dist, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})

	t.Run("three positions different", func(t *testing.T) {
		vec1 := []float32{10, 11, 15, 25, 31}
		vec2 := []float32{10, 15, 16, 25, 30}
		expectedDistance := float32(3)

		dist, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		control, err := NewHammingProvider().SingleDist(vec1, vec2)
		require.Nil(t, err)
		assert.Equal(t, control, dist)
		assert.Equal(t, expectedDistance, dist)
	})
}

func TestHammingDistancerStepbyStep(t *testing.T) {
	t.Run("step by step equals SingleDist", func(t *testing.T) {
		vec1 := []float32{10, 11, 15, 25, 31}
		vec2 := []float32{10, 15, 16, 25, 30}

		expectedDistance, err := NewHammingProvider().New(vec1).Distance(vec2)
		require.Nil(t, err)

		distanceProvider := NewHammingProvider()
		sum := float32(0.0)
		for i := range vec1 {
			sum += distanceProvider.Step([]float32{vec1[i]}, []float32{vec2[i]})
		}
		control := distanceProvider.Wrap(sum)

		assert.Equal(t, control, expectedDistance)
	})
}

func TestCompareHammingDistanceImplementations(t *testing.T) {
	sizes := []uint{
		1,
		2,
		3,
		4,
		5,
		6,
		8,
		10,
		12,
		16,
		24,
		30,
		31,
		32,
		64,
		67,
		128,
		256,
		260,
		299,
		300,
		384,
		390,
		600,
		768,
		777,
		784,
		1024,
		1536,
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("with size %d", size), func(t *testing.T) {
			r := getRandomSeed()
			count := 1
			countFailed := 0

			vec1s := make([][]float32, count)
			vec2s := make([][]float32, count)

			for i := 0; i < count; i++ {
				vec1 := make([]float32, size)
				vec2 := make([]float32, size)
				for j := range vec1 {
					equal := r.Float32() < 0.5
					if equal {
						randomValue := r.Float32()
						vec1[j] = randomValue
						vec2[j] = randomValue
					} else {
						vec1[j] = r.Float32()
						vec2[j] = r.Float32() + 10
					}
				}
				vec1s[i] = vec1
				vec2s[i] = vec2
			}

			for i := 0; i < count; i++ {
				res, err := NewHammingProvider().New(vec1s[i]).Distance(vec2s[i])

				require.NoError(t, err)

				resControl := HammingDistanceGo(vec1s[i], vec2s[i])

				if resControl != res {
					countFailed++
					t.Fatalf("run %d: match: %f != %f, %d\n", i, resControl, res, (unsafe.Pointer(&vec1s[i][0])))
				}

			}
			fmt.Printf("total failed: %d\n", countFailed)
		})
	}
}
