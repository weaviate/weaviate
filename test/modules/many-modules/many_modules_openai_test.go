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

package test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func createSchemaOpenAISanityChecks(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(endpoint)

		vectorizer := "text2vec-openai"
		class := &models.Class{
			Class: "OpenAI",
			Properties: []*models.Property{
				{
					Name:     "text",
					DataType: []string{schema.DataTypeText.String()},
				},
			},
			Vectorizer: vectorizer,
		}
		tests := []struct {
			name                  string
			text2vecOpenAI        map[string]interface{}
			expectDefaultSettings bool
		}{
			{
				name: "model: text-embedding-3-large, dimensions: 256, vectorizeClassName: false",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": false,
					"model":              "text-embedding-3-large",
					"dimensions":         256,
				},
			},
			{
				name: "model: text-embedding-3-large, dimensions: 1024, vectorizeClassName: false",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": false,
					"model":              "text-embedding-3-large",
					"dimensions":         1024,
				},
			},
			{
				name: "model: text-embedding-3-large, dimensions: 3072, vectorizeClassName: false",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": false,
					"model":              "text-embedding-3-large",
					"dimensions":         3072,
				},
			},
			{
				name: "model: text-embedding-3-small, dimensions: 512, vectorizeClassName: true",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": true,
					"model":              "text-embedding-3-small",
					"dimensions":         512,
				},
			},
			{
				name: "model: text-embedding-3-small, dimensions: 1536, vectorizeClassName: false",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": false,
					"model":              "text-embedding-3-small",
					"dimensions":         1536,
				},
			},
			{
				name: "model: text-embedding-3-small, dimensions: 1536, vectorizeClassName: true",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": true,
					"model":              "text-embedding-3-small",
					"dimensions":         1536,
				},
			},
			{
				name: "model: ada, vectorizeClassName: false",
				text2vecOpenAI: map[string]interface{}{
					"vectorizeClassName": false,
					"model":              "ada",
				},
			},
			{
				name:                  "nil settings",
				text2vecOpenAI:        nil,
				expectDefaultSettings: true,
			},
			{
				name:                  "empty settings",
				text2vecOpenAI:        map[string]interface{}{},
				expectDefaultSettings: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				class.ModuleConfig = map[string]interface{}{
					vectorizer: tt.text2vecOpenAI,
				}
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)
				verifyClass := helper.GetClass(t, class.Class)
				moduleConfig, ok := verifyClass.ModuleConfig.(map[string]interface{})
				require.True(t, ok)
				require.NotEmpty(t, moduleConfig)
				text2vecOpenAI, ok := moduleConfig[vectorizer].(map[string]interface{})
				require.True(t, ok)
				require.NotEmpty(t, text2vecOpenAI)
				if tt.expectDefaultSettings {
					assert.Equal(t, "text-embedding-3-small", text2vecOpenAI["model"])
					assert.Equal(t, true, text2vecOpenAI["vectorizeClassName"])
				} else {
					assert.Equal(t, tt.text2vecOpenAI["model"], text2vecOpenAI["model"])
					assert.Equal(t, tt.text2vecOpenAI["vectorizeClassName"], text2vecOpenAI["vectorizeClassName"])
					expectedDimensions, ok := tt.text2vecOpenAI["dimensions"]
					if ok {
						dimensions, ok := text2vecOpenAI["dimensions"].(json.Number)
						require.True(t, ok)
						dimensionsInt64, err := dimensions.Int64()
						require.NoError(t, err)
						assert.Equal(t, expectedDimensions, int(dimensionsInt64))
					}
				}
			})
		}
	}
}
