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

func testGenerativeAWS(host, region string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		companies := []struct {
			id                strfmt.UUID
			name, description string
		}{
			{
				id:   strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				name: "OpenAI",
				description: `
					OpenAI is a research organization and AI development company that focuses on artificial intelligence (AI) and machine learning (ML).
					Founded in December 2015, OpenAI's mission is to ensure that artificial general intelligence (AGI) benefits all of humanity.
					The organization has been at the forefront of AI research, producing cutting-edge advancements in natural language processing,
					reinforcement learning, robotics, and other AI-related fields.

					OpenAI has garnered attention for its work on various projects, including the development of the GPT (Generative Pre-trained Transformer)
					series of models, such as GPT-2 and GPT-3, which have demonstrated remarkable capabilities in generating human-like text.
					Additionally, OpenAI has contributed to advancements in reinforcement learning through projects like OpenAI Five, an AI system
					capable of playing the complex strategy game Dota 2 at a high level.
				`,
			},
			{
				id:   strfmt.UUID("00000000-0000-0000-0000-000000000002"),
				name: "SpaceX",
				description: `
					SpaceX, short for Space Exploration Technologies Corp., is an American aerospace manufacturer and space transportation company
					founded by Elon Musk in 2002. The company's primary goal is to reduce space transportation costs and enable the colonization of Mars,
					among other ambitious objectives.

					SpaceX has made significant strides in the aerospace industry by developing advanced rocket technology, spacecraft,
					and satellite systems. The company is best known for its Falcon series of rockets, including the Falcon 1, Falcon 9, 
					and Falcon Heavy, which have been designed with reusability in mind. Reusability has been a key innovation pioneered by SpaceX,
					aiming to drastically reduce the cost of space travel by reusing rocket components multiple times.
				`,
			},
		}
		// Define class
		className := "BooksGenerativeTest"
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
						"text2vec-aws": map[string]interface{}{
							"properties":         []interface{}{"description"},
							"vectorizeClassName": false,
							"service":            "bedrock",
							"region":             region,
							"model":              "amazon.titan-embed-text-v2:0",
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
				name:            "cohere.command-text-v14",
				generativeModel: "cohere.command-text-v14",
			},
			{
				name:            "cohere.command-light-text-v14",
				generativeModel: "cohere.command-light-text-v14",
			},
			{
				name:            "cohere.command-r-v1:0",
				generativeModel: "cohere.command-r-v1:0",
			},
			{
				name:            "cohere.command-r-plus-v1:0",
				generativeModel: "cohere.command-r-plus-v1:0",
			},
			{
				name:            "anthropic.claude-v2",
				generativeModel: "anthropic.claude-v2",
			},
			{
				name:            "anthropic.claude-v2:1",
				generativeModel: "anthropic.claude-v2:1",
			},
			{
				name:            "anthropic.claude-instant-v1",
				generativeModel: "anthropic.claude-instant-v1",
			},
			{
				name:            "anthropic.claude-3-sonnet-20240229-v1:0",
				generativeModel: "anthropic.claude-3-sonnet-20240229-v1:0",
			},
			{
				name:            "anthropic.claude-3-haiku-20240307-v1:0",
				generativeModel: "anthropic.claude-3-haiku-20240307-v1:0",
			},
			{
				name:            "ai21.j2-ultra-v1",
				generativeModel: "ai21.j2-ultra-v1",
			},
			{
				name:            "ai21.j2-mid-v1",
				generativeModel: "ai21.j2-mid-v1",
			},
			{
				name:            "amazon.titan-text-lite-v1",
				generativeModel: "amazon.titan-text-lite-v1",
			},
			{
				name:            "amazon.titan-text-premier-v1:0",
				generativeModel: "amazon.titan-text-premier-v1:0",
			},
			{
				name:            "amazon.titan-text-express-v1",
				generativeModel: "amazon.titan-text-express-v1",
			},
			{
				name:            "mistral.mistral-7b-instruct-v0:2",
				generativeModel: "mistral.mistral-7b-instruct-v0:2",
			},
			{
				name:            "mistral.mixtral-8x7b-instruct-v0:1",
				generativeModel: "mistral.mixtral-8x7b-instruct-v0:1",
			},
			{
				name:            "mistral.mistral-large-2402-v1:0",
				generativeModel: "mistral.mistral-large-2402-v1:0",
			},
			{
				name:            "meta.llama3-8b-instruct-v1:0",
				generativeModel: "meta.llama3-8b-instruct-v1:0",
			},
			{
				name:            "meta.llama3-70b-instruct-v1:0",
				generativeModel: "meta.llama3-70b-instruct-v1:0",
			},
			{
				name:            "meta.llama2-13b-chat-v1",
				generativeModel: "meta.llama2-13b-chat-v1",
			},
			{
				name:            "meta.llama2-70b-chat-v1",
				generativeModel: "meta.llama2-70b-chat-v1",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				class.ModuleConfig = map[string]interface{}{
					"generative-aws": map[string]interface{}{
						"projectId": "semi-random-dev",
						"service":   "bedrock",
						"region":    region,
						"model":     tt.generativeModel,
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
							ID:    company.id,
							Properties: map[string]interface{}{
								"name":        company.name,
								"description": company.description,
							},
						}
						helper.CreateObject(t, obj)
						helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
					}
				})
				t.Run("check objects existence", func(t *testing.T) {
					for _, company := range companies {
						t.Run(company.id.String(), func(t *testing.T) {
							obj, err := helper.GetObject(t, class.Class, company.id, "vector")
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
