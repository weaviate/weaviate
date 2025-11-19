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
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecCohere(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		className := "BooksGenerativeTest"
		data := companies.Companies
		class := companies.BaseClass(className)
		tests := []struct {
			name       string
			model      string
			dimensions int
		}{
			{
				name:       "embed-v4.0",
				model:      "embed-v4.0",
				dimensions: 256,
			},
			{
				name:  "embed-english-light-v3.0",
				model: "embed-english-light-v3.0",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				settings := map[string]any{
					"properties":         []any{"description"},
					"vectorizeClassName": false,
					"model":              tt.model,
				}
				if tt.dimensions > 0 {
					settings["dimensions"] = tt.dimensions
				}
				// Define class
				class.VectorConfig = map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]any{
							"text2vec-cohere": settings,
						},
						VectorIndexType: "flat",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					companies.InsertObjects(t, host, class.Class)
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, company := range data {
						t.Run(company.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
							require.IsType(t, []float32{}, obj.Vectors["description"])
							assert.True(t, len(obj.Vectors["description"].([]float32)) > 0)
							if tt.dimensions > 0 {
								assert.Equal(t, tt.dimensions, len(obj.Vectors["description"].([]float32)))
							}
						})
					}
				})
				// vector search
				t.Run("perform vector search", func(t *testing.T) {
					companies.PerformVectorSearchTest(t, host, class.Class)
				})
				// hybird search
				t.Run("perform hybrid search", func(t *testing.T) {
					companies.PerformHybridSearchTest(t, host, class.Class)
				})
			})
		}
	}
}
