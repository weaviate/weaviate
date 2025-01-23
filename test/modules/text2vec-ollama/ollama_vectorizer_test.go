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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecOllama(host, ollamaApiEndpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// companiesList data
		className := "OllamaVectorizerTest"
		companiesList := companies.Companies
		class := companies.BaseClass(className)
		// Define class
		class.VectorConfig = map[string]models.VectorConfig{
			"description": {
				Vectorizer: map[string]interface{}{
					"text2vec-ollama": map[string]interface{}{
						"properties":         []interface{}{"description"},
						"vectorizeClassName": false,
						"apiEndpoint":        ollamaApiEndpoint,
						"model":              "nomic-embed-text",
					},
				},
				VectorIndexType: "flat",
			},
		}
		tests := []struct {
			name     string
			useBatch bool
		}{
			{
				name: "without batch", useBatch: false,
			},
			{
				name: "with batch", useBatch: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				if tt.useBatch {
					t.Run("batch create objects", func(t *testing.T) {
						companies.BatchInsertObjects(t, host, class.Class)
					})
				} else {
					t.Run("create objects", func(t *testing.T) {
						companies.InsertObjects(t, host, class.Class)
					})
				}
				t.Run("check objects existence", func(t *testing.T) {
					for _, company := range companiesList {
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
				t.Run("perform vector search", func(t *testing.T) {
					companies.PerformVectorSearchTest(t, host, class.Class)
				})
				t.Run("perform hybrid search", func(t *testing.T) {
					companies.PerformHybridSearchTest(t, host, class.Class)
				})
				t.Run("perform second vector search", func(t *testing.T) {
					query := fmt.Sprintf(`
						{
							Get {
								%s(
									nearText:{
										concepts: "Space flight"
									}
								){
									name
									_additional {
										id
										certainty
									}
								}
							}
						}
					`, class.Class)
					result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
					objs := result.Get("Get", class.Class).AsSlice()
					require.Len(t, objs, 2)
					for i, obj := range objs {
						name := obj.(map[string]interface{})["name"]
						assert.NotEmpty(t, name)
						additional, ok := obj.(map[string]interface{})["_additional"].(map[string]interface{})
						require.True(t, ok)
						require.NotNil(t, additional)
						id, ok := additional["id"].(string)
						require.True(t, ok)
						expectedID := companies.SpaceX.String()
						if i > 0 {
							expectedID = companies.OpenAI.String()
						}
						require.Equal(t, expectedID, id)
						certainty := additional["certainty"].(json.Number)
						assert.NotNil(t, certainty)
						certaintyValue, err := certainty.Float64()
						require.NoError(t, err)
						assert.Greater(t, certaintyValue, 0.1)
					}
				})
			})
		}
	}
}
