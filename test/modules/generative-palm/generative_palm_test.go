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

func testGenerativePaLM(host, gcpProject string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Data
		data := planets.Planets
		// Define class
		class := planets.BaseClass("PlanetsGenerativeTest")
		class.VectorConfig = map[string]models.VectorConfig{
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
					planets.InsertObjects(t, class.Class)
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
				// generative task
				t.Run("create a tweet", func(t *testing.T) {
					planets.CreateTweetTest(t, class.Class)
				})
				t.Run("create a tweet with params", func(t *testing.T) {
					params := "google:{topP:0.1 topK:40}"
					planets.CreateTweetTestWithParams(t, class.Class, params)
				})
			})
		}
	}
}
