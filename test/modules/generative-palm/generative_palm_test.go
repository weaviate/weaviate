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

package generative_palm_tests

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func testGenerativePaLM(host, gcpProject string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		planets := []struct {
			ID                strfmt.UUID
			Name, Description string
		}{
			{
				ID:   strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				Name: "Earth",
				Description: `
				The Earth's surface is predominantly covered by oceans, accounting for about 71% of its total area, while continents provide 
				the stage for bustling cities, towering mountains, and sprawling forests. Its atmosphere, composed mostly of nitrogen and oxygen, 
				protects life from harmful solar radiation and regulates the planet's climate, creating the conditions necessary for life to flourish.

				Humans, as the dominant species, have left an indelible mark on Earth, shaping its landscapes and ecosystems in profound ways. 
				However, with this influence comes the responsibility to steward and preserve our planet for future generations.
				`,
			},
			{
				ID:   strfmt.UUID("00000000-0000-0000-0000-000000000002"),
				Name: "Mars",
				Description: `
				Mars, often called the "Red Planet" due to its rusty reddish hue, is the fourth planet from the Sun in our solar system. 
				It's a world of stark contrasts and mysterious allure, captivating the imaginations of scientists, explorers, and dreamers alike.

				With its barren, rocky terrain and thin atmosphere primarily composed of carbon dioxide, Mars presents a harsh environment vastly 
				different from Earth. Yet, beneath its desolate surface lie tantalizing clues about its past, including evidence of ancient rivers, 
				lakes, and even the possibility of microbial life.
				`,
			},
		}
		// Define class
		className := "PlanetsGenerativeTest"
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
						"text2vec-palm": map[string]interface{}{
							"properties":         []interface{}{"description"},
							"vectorizeClassName": false,
							"projectId":          gcpProject,
							"modelId":            "textembedding-gecko@001",
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
				name:            "chat-bison",
				generativeModel: "chat-bison",
			},
			{
				name:            "chat-bison-32k",
				generativeModel: "chat-bison-32k",
			},
			{
				name:            "chat-bison@002",
				generativeModel: "chat-bison@002",
			},
			{
				name:            "chat-bison-32k@002",
				generativeModel: "chat-bison-32k@002",
			},
			{
				name:            "chat-bison@001",
				generativeModel: "chat-bison@001",
			},
			{
				name:            "gemini-1.5-pro-preview-0514",
				generativeModel: "gemini-1.5-pro-preview-0514",
			},
			{
				name:            "gemini-1.5-pro-preview-0409",
				generativeModel: "gemini-1.5-pro-preview-0409",
			},
			{
				name:            "gemini-1.5-flash-preview-0514",
				generativeModel: "gemini-1.5-flash-preview-0514",
			},
			{
				name:            "gemini-1.0-pro-002",
				generativeModel: "gemini-1.0-pro-002",
			},
			{
				name:            "gemini-1.0-pro-001",
				generativeModel: "gemini-1.0-pro-001",
			},
			{
				name:            "gemini-1.0-pro",
				generativeModel: "gemini-1.0-pro",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				class.ModuleConfig = map[string]interface{}{
					"generative-palm": map[string]interface{}{
						"projectId": gcpProject,
						"modelId":   tt.generativeModel,
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				// create objects
				t.Run("create objects", func(t *testing.T) {
					for _, company := range planets {
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
					for _, company := range planets {
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
					prompt := "Write a short description about {name}"
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
					result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
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
