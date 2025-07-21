//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecHuggingFace(host string) func(t *testing.T) {
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
				name:  "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
				model: "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define class
				class.VectorConfig = map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]interface{}{
							"text2vec-huggingface": map[string]interface{}{
								"properties":         []interface{}{"description"},
								"vectorizeClassName": false,
								"model":              tt.model,
								"options": map[string]interface{}{
									"waitForModel": true,
									"useGPU":       false,
									"useCache":     true,
								},
							},
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
							require.NotEmpty(t, obj.Vectors["description"])
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
