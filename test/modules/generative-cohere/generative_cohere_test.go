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

package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/planets"
)

func testGenerativeCohere(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		data := planets.Planets
		// Define class
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
			"description": {
				Vectorizer: map[string]interface{}{
					"text2vec-transformers": map[string]interface{}{
						"properties":         []interface{}{"description"},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
		}
		tests := []struct {
			name            string
			generativeModel string
		}{
			{
				name:            "command-r-plus",
				generativeModel: "command-r-plus",
			},
			{
				name:            "command-r",
				generativeModel: "command-r",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				class.ModuleConfig = map[string]interface{}{
					"generative-cohere": map[string]interface{}{
						"model": tt.generativeModel,
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					planets.InsertObjects(t, class.Class)
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, planet := range data {
						t.Run(planet.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, planet.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "cohere:{temperature:0.9 k:400}"
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
			})
		}
	}
}
