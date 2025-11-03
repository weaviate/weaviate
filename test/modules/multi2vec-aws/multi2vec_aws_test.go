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

package tests

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2VecAWS(host, region string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		getModel := func(model string) any {
			if model != "" {
				return model
			}
			return nil
		}
		tests := []struct {
			name              string
			model             string
			defaultDimensions int
		}{
			{
				name:              "default",
				defaultDimensions: 1024,
			},
			{
				name:              "amazon.nova-2-multimodal-embeddings-v1:0",
				model:             "amazon.nova-2-multimodal-embeddings-v1:0",
				defaultDimensions: 3072,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Define class
				className := "AWSClipTest"
				vectorizerName := "multi2vec-aws"
				class := multimodal.BaseClass(className, true)
				class.VectorConfig = map[string]models.VectorConfig{
					"clip_aws": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"model":              getModel(tt.model),
								"imageFields":        []any{multimodal.PropertyImage},
								"vectorizeClassName": false,
								"region":             region,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_aws_256": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"model":              getModel(tt.model),
								"imageFields":        []any{multimodal.PropertyImage},
								"vectorizeClassName": false,
								"region":             region,
								"dimensions":         256,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_aws_weights": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"model":       getModel(tt.model),
								"textFields":  []any{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
								"imageFields": []any{multimodal.PropertyImage},
								"weights": map[string]any{
									"textFields":  []any{0.05, 0.05},
									"imageFields": []any{0.9},
								},
								"vectorizeClassName": false,
								"region":             region,
								"dimensions":         384,
							},
						},
						VectorIndexType: "flat",
					},
				}
				// create schema
				helper.CreateClass(t, class)
				defer helper.DeleteClass(t, class.Class)

				t.Run("import data", func(t *testing.T) {
					multimodal.InsertObjects(t, dataFolderPath, class.Class, true)
				})

				t.Run("check objects", func(t *testing.T) {
					vectorNames := []string{"clip_aws", "clip_aws_256", "clip_aws_weights"}
					multimodal.CheckObjects(t, dataFolderPath, class.Class, vectorNames, nil)
				})

				t.Run("nearImage", func(t *testing.T) {
					blob, err := multimodal.GetImageBlob(dataFolderPath, 2)
					require.NoError(t, err)
					targetVector := "clip_aws"
					nearMediaArgument := fmt.Sprintf(`
						nearImage: {
							image: "%s"
							targetVectors: ["%s"]
						}
					`, blob, targetVector)
					titleProperty := multimodal.PropertyImageTitle
					titlePropertyValue := "waterfalls"
					targetVectors := map[string]int{
						"clip_aws":         tt.defaultDimensions,
						"clip_aws_256":     256,
						"clip_aws_weights": 384,
					}
					multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
				})

				t.Run("nearText", func(t *testing.T) {
					targetVector := "clip_aws"
					nearMediaArgument := fmt.Sprintf(`
						nearText: {
							concepts: ["waterfalls"]
							targetVectors: ["%s"]
						}
					`, targetVector)
					titleProperty := multimodal.PropertyImageTitle
					titlePropertyValue := "waterfalls"
					targetVectors := map[string]int{
						"clip_aws":         tt.defaultDimensions,
						"clip_aws_256":     256,
						"clip_aws_weights": 384,
					}
					multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
				})
			})
		}
	}
}
