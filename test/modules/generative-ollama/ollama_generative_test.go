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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testGenerativeOllama(host, ollamaApiEndpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		companies := companies.Companies()
		// Define class
		className := "CompaniesGenerativeTest"
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "description", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"description": {
					Vectorizer: map[string]interface{}{
						"text2vec-transformers": map[string]interface{}{
							"properties":         []interface{}{"description"},
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "flat",
				},
			},
		}
		tests := []struct {
			name            string
			generativeModel string
		}{
			{
				name:            "tinyllama",
				generativeModel: "tinyllama",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				class.ModuleConfig = map[string]interface{}{
					"generative-ollama": map[string]interface{}{
						"apiEndpoint": ollamaApiEndpoint,
						"modelId":     tt.generativeModel,
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					for _, company := range companies {
						obj := &models.Object{
							Class: class.Class,
							ID:    company.ID,
							Properties: map[string]interface{}{
								"name":        company.Name,
								"description": company.Description,
							},
						}
						helper.CreateObject(t, obj)
						helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
					}
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, company := range companies {
						t.Run(company.ID.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, company.ID, "vector")
							require.NoError(t, err)
							require.NotNil(t, obj)
							require.Len(t, obj.Vectors, 1)
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					prompt := "Generate a funny tweet out of this content: {description}"
					query := fmt.Sprintf(`
						{
							Get {
								%s{
									name
									_additional {
										generate(
											singleResult: {
												prompt: """
													%s
												"""
											}
										) {
											singleResult
											error
										}
									}
								}
							}
						}
					`, class.Class, prompt)
					timeout := 60 * time.Second
					result := graphqlhelper.AssertGraphQLWithTimeout(t, helper.RootAuth, timeout, query)
					objs := result.Get("Get", class.Class).AsSlice()
					require.Len(t, objs, 2)
					for _, obj := range objs {
						name := obj.(map[string]interface{})["name"]
						assert.NotEmpty(t, name)
						additional, ok := obj.(map[string]interface{})["_additional"].(map[string]interface{})
						require.True(t, ok)
						require.NotNil(t, additional)
						generate, ok := additional["generate"].(map[string]interface{})
						require.True(t, ok)
						require.NotNil(t, generate)
						require.Nil(t, generate["error"])
						require.NotNil(t, generate["singleResult"])
						singleResult, ok := generate["singleResult"].(string)
						require.True(t, ok)
						require.NotEmpty(t, singleResult)
					}
				})
			})
		}
	}
}
