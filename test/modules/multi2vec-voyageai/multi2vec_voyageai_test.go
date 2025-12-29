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
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2VecVoyageAI(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		// Define class
		vectorizerName := "multi2vec-voyageai"
		className := "VoyageAIClipTest"
		class := multimodal.BaseClass(className, false)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_weights": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"model":       "voyage-multimodal-3",
						"textFields":  []interface{}{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
						"imageFields": []interface{}{multimodal.PropertyImage},
						"weights": map[string]interface{}{
							"textFields":  []interface{}{0.05, 0.05},
							"imageFields": []interface{}{0.9},
						},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_multimodal_3_5": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"model":       "voyage-multimodal-3.5",
						"textFields":  []interface{}{multimodal.PropertyImageTitle},
						"imageFields": []interface{}{multimodal.PropertyImage},
						"weights": map[string]interface{}{
							"textFields":  []interface{}{0.5},
							"imageFields": []interface{}{0.5},
						},
						"vectorizeClassName": false,
					},
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
			targetVectors := map[string]int{
				"clip":                1024,
				"clip_weights":        1024,
				"clip_multimodal_3_5": 1024,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}

func testMulti2VecVoyageAIWithVideo(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)

		// Use small test videos that fit within VoyageAI token limits
		videoFolderPath := "../../../test/helper/sample-schema/multimodal/data/videos_small"

		vectorizerName := "multi2vec-voyageai"
		className := "VoyageAIVideoTest"

		// Create class with video support
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{schema.DataTypeText.String()}},
				{Name: "video", DataType: []string{schema.DataTypeBlob.String()}},
			},
			VectorConfig: map[string]models.VectorConfig{
				"video_vec": {
					Vectorizer: map[string]interface{}{
						vectorizerName: map[string]interface{}{
							"model":              "voyage-multimodal-3.5",
							"textFields":         []interface{}{"title"},
							"videoFields":        []interface{}{"video"},
							"vectorizeClassName": false,
							"weights": map[string]interface{}{
								"textFields":  []interface{}{0.3},
								"videoFields": []interface{}{0.7},
							},
						},
					},
					VectorIndexType: "flat",
				},
			},
		}

		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import video data", func(t *testing.T) {
			// Read video file and encode as base64
			videoBytes, err := os.ReadFile(fmt.Sprintf("%s/1.mp4", videoFolderPath))
			require.NoError(t, err)
			videoBlob := base64.StdEncoding.EncodeToString(videoBytes)

			obj := &models.Object{
				Class: className,
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				Properties: map[string]interface{}{
					"title": "test video red",
					"video": videoBlob,
				},
			}

			err = helper.CreateObjectWithTimeout(t, obj, multimodal.DefaultTimeout)
			require.NoError(t, err)

			// Verify object was created with vector
			createdObj := helper.AssertGetObjectEventually(t, className, obj.ID)
			require.NotNil(t, createdObj)
		})

		t.Run("verify video vector dimensions", func(t *testing.T) {
			obj, err := helper.GetObject(t, className, strfmt.UUID("00000000-0000-0000-0000-000000000001"), "vector")
			require.NoError(t, err)
			require.NotNil(t, obj)
			require.NotNil(t, obj.Vectors)

			videoVec, ok := obj.Vectors["video_vec"]
			require.True(t, ok, "video_vec should exist")

			vecSlice, ok := videoVec.([]float32)
			require.True(t, ok, "video_vec should be []float32")
			require.Equal(t, 1024, len(vecSlice), "voyage-multimodal-3.5 should return 1024 dimensions")
		})
	}
}
