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

func testText2VecGoogle(host, gcpProject, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		data := companies.Companies
		className := "VectorizerTest"
		class := companies.BaseClass(className)
		tests := []struct {
			name  string
			model string
		}{
			{
				name:  "textembedding-gecko@001",
				model: "textembedding-gecko@001",
			},
			{
				name:  "textembedding-gecko@latest",
				model: "textembedding-gecko@latest",
			},
			{
				name:  "textembedding-gecko-multilingual@latest",
				model: "textembedding-gecko-multilingual@latest",
			},
			{
				name:  "textembedding-gecko@003",
				model: "textembedding-gecko@003",
			},
			{
				name:  "textembedding-gecko@002",
				model: "textembedding-gecko@002",
			},
			{
				name:  "textembedding-gecko-multilingual@001",
				model: "textembedding-gecko-multilingual@001",
			},
			{
				name:  "text-embedding-preview-0409",
				model: "text-embedding-preview-0409",
			},
			{
				name:  "text-multilingual-embedding-preview-0409",
				model: "text-multilingual-embedding-preview-0409",
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
								"projectId":          gcpProject,
								"modelId":            tt.model,
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
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// vector search
				t.Run("perform vector search", func(t *testing.T) {
					companies.PerformVectorSearchTest(t, host, class.Class)
				})
				// hybrid search
				t.Run("perform hybrid search", func(t *testing.T) {
					companies.PerformHybridSearchTest(t, host, class.Class)
				})
			})
		}
	}
}
