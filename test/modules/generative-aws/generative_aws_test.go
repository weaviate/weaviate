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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/planets"
)

func testGenerativeAWS(host, region string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		data := planets.Planets
		// Define class
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
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
						"service": "bedrock",
						"region":  region,
						"model":   tt.generativeModel,
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
							assert.True(t, len(obj.Vectors["description"]) > 0)
						})
					}
				})
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "aws:{temperature:0.1}"
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
			})
		}
	}
}
