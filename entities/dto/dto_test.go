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

package dto

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestVectors(t *testing.T) {
	var vectors []models.Vector

	t.Run("insert", func(t *testing.T) {
		regularVector := []float32{0.1, 0.2, 0.3}
		multiVector := [][]float32{{0.1, 0.2, 0.3}, {0.1, 0.2, 0.3}}

		vectors = append(vectors, regularVector)
		vectors = append(vectors, multiVector)

		require.Len(t, vectors, 2)
		assert.IsType(t, []float32{}, vectors[0])
		assert.IsType(t, [][]float32{}, vectors[1])
	})

	t.Run("type check", func(t *testing.T) {
		isMultiVectorFn := func(in models.Vector) (bool, error) {
			switch in.(type) {
			case []float32:
				return false, nil
			case [][]float32:
				return true, nil
			default:
				return false, fmt.Errorf("unsupported type: %T", in)
			}
		}
		isMultiVector, err := isMultiVectorFn(vectors[0])
		require.NoError(t, err)
		assert.False(t, isMultiVector)
		isMultiVector, err = isMultiVectorFn(vectors[1])
		require.NoError(t, err)
		assert.True(t, isMultiVector)
	})

	t.Run("as vector slice", func(t *testing.T) {
		searchVectors := [][]float32{{0.1, 0.2}, {0.11, 0.22}, {0.111, 0.222}}

		var asVectorSlice []models.Vector
		for _, vector := range searchVectors {
			asVectorSlice = append(asVectorSlice, vector)
		}

		require.Len(t, asVectorSlice, len(searchVectors))
		assert.ElementsMatch(t, searchVectors, asVectorSlice)
	})

	t.Run("case to vector types", func(t *testing.T) {
		searchVectors := []models.Vector{[]float32{0.1, 0.2}, [][]float32{{0.11, 0.22}, {0.111, 0.222, 0.333}}, []float32{0.111, 0.222}}

		regularVector, ok := searchVectors[0].([]float32)
		require.True(t, ok)
		assert.Len(t, regularVector, 2)

		multiVector, ok := searchVectors[1].([][]float32)
		require.True(t, ok)
		require.Len(t, multiVector, 2)
		assert.Len(t, multiVector[0], 2)
		assert.Len(t, multiVector[1], 3)
	})
}
