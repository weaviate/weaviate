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

func testText2VecVoyageAI(rest, grpc string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
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
				name:  "voyage-3.5",
				model: "voyage-3.5",
			},
			{
				name:       "voyage-3.5-lite 512 dimensions",
				model:      "voyage-3.5-lite",
				dimensions: 512,
			},
			{
				name:  "voyage-3.5-lite",
				model: "voyage-3.5-lite",
			},
			{
				name:  "voyage-3",
				model: "voyage-3",
			},
			{
				name:  "voyage-3-lite",
				model: "voyage-3-lite",
			},
			{
				name:  "voyage-context-3",
				model: "voyage-context-3",
			},
			{
				name:       "voyage-context-3 256 dimensions",
				model:      "voyage-context-3",
				dimensions: 256,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define settings
				text2vecVoyageaiSettings := map[string]any{
					"properties":         []any{"description"},
					"vectorizeClassName": false,
					"model":              tt.model,
				}
				if tt.dimensions != 0 {
					text2vecVoyageaiSettings["dimensions"] = tt.dimensions
				}
				// Define class
				class.VectorConfig = map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]any{
							"text2vec-voyageai": text2vecVoyageaiSettings,
						},
						VectorIndexType: "flat",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					companies.InsertObjects(t, rest, class.Class)
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
						})
					}
				})
				t.Run("search tests", func(t *testing.T) {
					companies.PerformAllSearchTests(t, rest, grpc, class.Class)
				})
			})
		}
	}
}
