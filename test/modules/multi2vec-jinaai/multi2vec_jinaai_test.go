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

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2VecJinaAI(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		tests := []struct {
			name                  string
			model                 string
			clipWeightsDimensions int
		}{
			{
				name: "default settings",
			},
			{
				name:                  "jina-embeddings-v4",
				model:                 "jina-embeddings-v4",
				clipWeightsDimensions: 2048,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define settings
				clipSettings := map[string]interface{}{
					"imageFields":        []interface{}{multimodal.PropertyImage},
					"vectorizeClassName": false,
					"dimensions":         600,
				}
				if tt.model != "" {
					clipSettings["model"] = tt.model
				}
				clipWeightsSettings := map[string]interface{}{
					"textFields":  []interface{}{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
					"imageFields": []interface{}{multimodal.PropertyImage},
					"weights": map[string]interface{}{
						"textFields":  []interface{}{0.05, 0.05},
						"imageFields": []interface{}{0.9},
					},
					"vectorizeClassName": false,
				}
				if tt.model != "" {
					clipWeightsSettings["model"] = tt.model
				}
				if tt.clipWeightsDimensions > 0 {
					clipWeightsSettings["dimensions"] = tt.clipWeightsDimensions
				}
				// Define class
				vectorizerName := "multi2vec-jinaai"
				className := "ClipTest"
				class := multimodal.BaseClass(className, false)
				class.VectorConfig = map[string]models.VectorConfig{
					"clip": {
						Vectorizer: map[string]interface{}{
							vectorizerName: clipSettings,
						},
						VectorIndexType: "flat",
					},
					"clip_weights": {
						Vectorizer: map[string]interface{}{
							vectorizerName: clipWeightsSettings,
						},
						VectorIndexType: "flat",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)

				t.Run("import data", func(t *testing.T) {
					multimodal.InsertObjects(t, dataFolderPath, class.Class, false)
				})

				t.Run("check objects", func(t *testing.T) {
					multimodal.CheckObjects(t, dataFolderPath, class.Class, []string{"clip", "clip_weights"}, nil)
				})

				t.Run("nearImage", func(t *testing.T) {
					blob, err := multimodal.GetImageBlob(dataFolderPath, 2)
					require.NoError(t, err)
					targetVector := "clip"
					nearMediaArgument := fmt.Sprintf(`
						nearImage: {
							image: "%s"
							targetVectors: ["%s"]
						}
					`, blob, targetVector)
					titleProperty := multimodal.PropertyImageTitle
					titlePropertyValue := "waterfalls"
					clipWeightsDimensions := 1024
					if tt.clipWeightsDimensions > 0 {
						clipWeightsDimensions = tt.clipWeightsDimensions
					}
					targetVectors := map[string]int{
						"clip":         600,
						"clip_weights": clipWeightsDimensions,
					}
					multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
				})
			})
		}
	}
}
