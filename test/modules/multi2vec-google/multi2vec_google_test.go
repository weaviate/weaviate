//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

func testMulti2VecGoogleVertex(host, gcpProject, location, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define tests
		tests := []testCase{
			{
				name: "default model",
			},
			{
				name:  "multimodalembedding@001",
				model: "multimodalembedding@001",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, multimodalTests(tt, gcpProject, location, vectorizerName))
		}
	}
}

type testCase struct {
	name         string
	model        string
	apiEndpoint  string
	withoutVideo bool
}

func multimodalTests(tt testCase, gcpProject, location, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		defaultDimensions := 1408
		if tt.apiEndpoint != "" {
			defaultDimensions = 3072
		}
		clipGoogleSettings := map[string]any{
			"imageFields":        []any{multimodal.PropertyImage},
			"vectorizeClassName": false,
			"location":           location,
			"projectId":          gcpProject,
		}
		clipGoogle128Settings := map[string]any{
			"imageFields":        []any{multimodal.PropertyImage},
			"vectorizeClassName": false,
			"location":           location,
			"projectId":          gcpProject,
			"dimensions":         128,
		}
		clipGoogle256Settings := map[string]any{
			"imageFields":        []any{multimodal.PropertyImage},
			"vectorizeClassName": false,
			"location":           location,
			"projectId":          gcpProject,
			"dimensions":         256,
		}
		clipGoogleWeightsSettings := map[string]any{
			"textFields":  []any{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
			"imageFields": []any{multimodal.PropertyImage},
			"weights": map[string]any{
				"textFields":  []any{0.05, 0.05},
				"imageFields": []any{0.9},
			},
			"vectorizeClassName": false,
			"location":           location,
			"projectId":          gcpProject,
			"dimensions":         512,
		}
		if tt.model != "" {
			clipGoogleSettings["model"] = tt.model
			clipGoogle128Settings["model"] = tt.model
			clipGoogle256Settings["model"] = tt.model
			clipGoogleWeightsSettings["model"] = tt.model
		}
		if tt.apiEndpoint != "" {
			clipGoogleSettings["apiEndpoint"] = tt.apiEndpoint
			clipGoogle128Settings["apiEndpoint"] = tt.apiEndpoint
			clipGoogle256Settings["apiEndpoint"] = tt.apiEndpoint
			clipGoogleWeightsSettings["apiEndpoint"] = tt.apiEndpoint
		}

		// Define class
		className := "GoogleClipTest"
		class := multimodal.BaseClass(className, true)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip_google": {
				Vectorizer: map[string]any{
					vectorizerName: clipGoogleSettings,
				},
				VectorIndexType: "flat",
			},
			"clip_google_128": {
				Vectorizer: map[string]any{
					vectorizerName: clipGoogle128Settings,
				},
				VectorIndexType: "flat",
			},
			"clip_google_256": {
				Vectorizer: map[string]any{
					vectorizerName: clipGoogle256Settings,
				},
				VectorIndexType: "flat",
			},
			"clip_google_weights": {
				Vectorizer: map[string]any{
					vectorizerName: clipGoogleWeightsSettings,
				},
				VectorIndexType: "flat",
			},
		}
		if !tt.withoutVideo {
			clipGoogleVideoSettings := map[string]any{
				"videoFields":        []any{multimodal.PropertyVideo},
				"vectorizeClassName": false,
				"location":           location,
				"projectId":          gcpProject,
			}
			if tt.model != "" {
				clipGoogleVideoSettings["model"] = tt.model
			}
			if tt.apiEndpoint != "" {
				clipGoogleVideoSettings["apiEndpoint"] = tt.apiEndpoint
			}
			class.VectorConfig["clip_google_video"] = models.VectorConfig{
				Vectorizer: map[string]any{
					vectorizerName: clipGoogleVideoSettings,
				},
				VectorIndexType: "flat",
			}
		}
		// create schema
		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import data", func(t *testing.T) {
			multimodal.InsertObjects(t, dataFolderPath, class.Class, true)
		})

		t.Run("check objects", func(t *testing.T) {
			vectorNames := []string{"clip_google", "clip_google_128", "clip_google_256", "clip_google_weights"}
			if !tt.withoutVideo {
				vectorNames = append(vectorNames, "clip_google_video")
			}
			multimodal.CheckObjects(t, dataFolderPath, class.Class, vectorNames, nil)
		})

		t.Run("nearImage", func(t *testing.T) {
			blob, err := multimodal.GetImageBlob(dataFolderPath, 2)
			require.NoError(t, err)
			targetVector := "clip_google"
			nearMediaArgument := fmt.Sprintf(`
					nearImage: {
						image: "%s"
						targetVectors: ["%s"]
					}
				`, blob, targetVector)
			titleProperty := multimodal.PropertyImageTitle
			titlePropertyValue := "waterfalls"
			targetVectors := map[string]int{
				"clip_google":         defaultDimensions,
				"clip_google_128":     128,
				"clip_google_256":     256,
				"clip_google_weights": 512,
			}
			if !tt.withoutVideo {
				targetVectors["clip_google_video"] = defaultDimensions
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})

		if !tt.withoutVideo {
			t.Run("nearVideo", func(t *testing.T) {
				blob, err := multimodal.GetVideoBlob(dataFolderPath, 2)
				require.NoError(t, err)
				targetVector := "clip_google_video"
				nearMediaArgument := fmt.Sprintf(`
						nearVideo: {
							video: "%s"
							targetVectors: ["%s"]
						}
					`, blob, targetVector)
				titleProperty := multimodal.PropertyVideoTitle
				titlePropertyValue := "dog"
				targetVectors := map[string]int{
					"clip_google":         defaultDimensions,
					"clip_google_128":     128,
					"clip_google_256":     256,
					"clip_google_video":   defaultDimensions,
					"clip_google_weights": 512,
				}
				multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
			})
		}
	}
}
