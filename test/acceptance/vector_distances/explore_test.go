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

package test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testExplore(t *testing.T) {
	className := "L2Squared_Class_2"
	defer deleteObjectClass(t, className)

	t.Run("create second L2 class", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class:      className,
			Vectorizer: "none",
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
			VectorIndexConfig: map[string]interface{}{
				"distance": "l2-squared",
			},
		})
	})

	t.Run("create objects for the new class", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"name": "object_1",
			},
			Vector: []float32{
				6, 7, 8,
			},
		})

		createObject(t, &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"name": "object_2",
			},
			Vector: []float32{
				1, 2, 3,
			},
		})
	})

	t.Run("run Explore and assert results", func(t *testing.T) {
		res := AssertGraphQL(t, nil, `
		{
			Explore(nearVector: {vector: [3, 4, 5], distance: 365}) {
				distance
				className
			}
		}
		`)

		explore := res.Get("Explore").AsSlice()

		expected := []struct {
			className string
			dist      json.Number
		}{
			{className: "L2Squared_Class_2", dist: "12"},
			{className: "L2Squared_Class_2", dist: "27"},
			{className: "L2Squared_Class", dist: "50"},
			{className: "L2Squared_Class", dist: "147"},
			{className: "L2Squared_Class", dist: "365"},
		}

		for i, item := range explore {
			m, ok := item.(map[string]interface{})
			assert.True(t, ok)
			assert.Equal(t, expected[i].dist, m["distance"])
			assert.Equal(t, expected[i].className, m["className"])
		}
	})

	t.Run("run Explore with certainty arg and expect failure", func(t *testing.T) {
		res := ErrorGraphQL(t, nil, `
		{
			Explore(nearVector: {vector: [3, 4, 5], certainty: 0.4}) {
				distance
				className
			}
		}
		`)

		assert.Len(t, res, 1)
		expectedErr := "can't compute and return certainty when vector index is configured with l2-squared distance"
		errorMsg := res[0].Message
		assert.Equal(t, expectedErr, errorMsg)
	})

	t.Run("run Explore with certainty prop and expect failure", func(t *testing.T) {
		res := ErrorGraphQL(t, nil, `
		{
			Explore(nearVector: {vector: [3, 4, 5], distance: 0.4}) {
				certainty
				className
			}
		}
		`)

		assert.Len(t, res, 1)
		expectedErr := "can't compute and return certainty when vector index is configured with l2-squared distance"
		errorMsg := res[0].Message
		assert.Equal(t, expectedErr, errorMsg)
	})
}
