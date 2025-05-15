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

func testGenerativeManyModules(host, ollamaApiEndpoint, region, gcpProject string) func(t *testing.T) {
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
		class.ModuleConfig = map[string]interface{}{
			"generative-aws": map[string]interface{}{
				"service": "bedrock",
				"region":  region,
				"model":   "amazon.titan-text-lite-v1",
			},
			"generative-google": map[string]interface{}{
				"projectId": gcpProject,
				"modelId":   "gemini-1.0-pro",
			},
			"generative-ollama": map[string]interface{}{
				"apiEndpoint": ollamaApiEndpoint,
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
					require.IsType(t, []float32{}, obj.Vectors["description"])
					assert.True(t, len(obj.Vectors["description"].([]float32)) > 0)
				})
			}
		})
		// generative task with params
		tests := []struct {
			name   string
			params string
		}{
			{
				name:   "ollama",
				params: `ollama:{temperature:0.1 model:"tinyllama"}`,
			},
			{
				name:   "aws",
				params: `aws:{temperature:0.9 model:"ai21.j2-mid-v1"}`,
			},
			{
				name:   "google",
				params: `google:{topP:0.8 topK:30 model:"chat-bison"}`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("create a tweet with params", func(t *testing.T) {
					planets.CreateTweetTestWithParams(t, class.Class, tt.params)
				})
			})
		}
	}
}
