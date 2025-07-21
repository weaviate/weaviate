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

func testText2MultivecJinaAI(host, grpc, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		className := "BooksGenerativeTest"
		data := companies.Companies
		class := companies.BaseClass(className)
		tests := []struct {
			name  string
			model string
		}{
			{
				name:  "jina-colbert-v2",
				model: "jina-colbert-v2",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define class
				class.VectorConfig = map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]interface{}{
							vectorizerName: map[string]interface{}{
								"properties":         []interface{}{"description"},
								"vectorizeClassName": false,
								"model":              tt.model,
								"dimensions":         64,
							},
						},
						VectorIndexType: "hnsw",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// check that multivector is enabled
				t.Run("check multivector config", func(t *testing.T) {
					cls := helper.GetClass(t, class.Class)
					assert.True(t, cls.VectorConfig["description"].VectorIndexConfig.(map[string]any)["multivector"].(map[string]any)["enabled"].(bool))
				})
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
							require.IsType(t, [][]float32{}, obj.Vectors["description"])
							assert.True(t, len(obj.Vectors["description"].([][]float32)) > 0)
						})
					}
				})
				t.Run("search tests", func(t *testing.T) {
					companies.PerformAllSearchTests(t, host, grpc, class.Class)
				})
			})
		}
	}
}
