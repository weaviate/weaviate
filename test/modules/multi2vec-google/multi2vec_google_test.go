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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2VecGoogle(host, gcpProject, location, vectorizerName string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		// Define class
		className := "GoogleClipTest"
		class := multimodal.BaseClass(className, true)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip_google": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"location":           location,
						"projectId":          gcpProject,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_google_128": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"location":           location,
						"projectId":          gcpProject,
						"dimensions":         128,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_google_256": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"location":           location,
						"projectId":          gcpProject,
						"dimensions":         256,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_google_video": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"videoFields":        []interface{}{multimodal.PropertyVideo},
						"vectorizeClassName": false,
						"location":           location,
						"projectId":          gcpProject,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_google_weights": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"textFields":  []interface{}{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
						"imageFields": []interface{}{multimodal.PropertyImage},
						"weights": map[string]interface{}{
							"textFields":  []interface{}{0.05, 0.05},
							"imageFields": []interface{}{0.9},
						},
						"vectorizeClassName": false,
						"location":           location,
						"projectId":          gcpProject,
						"dimensions":         512,
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
				"clip_google":         1408,
				"clip_google_128":     128,
				"clip_google_256":     256,
				"clip_google_video":   1408,
				"clip_google_weights": 512,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})

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
				"clip_google":         1408,
				"clip_google_128":     128,
				"clip_google_256":     256,
				"clip_google_video":   1408,
				"clip_google_weights": 512,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}
