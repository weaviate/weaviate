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
	"encoding/csv"
	"fmt"
	"io"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
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
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "image_title", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "image_description", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "video_title", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "video_description", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "image", DataType: []string{schema.DataTypeBlob.String()},
				},
				{
					Name: "video", DataType: []string{schema.DataTypeBlob.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"clip_google": {
					Vectorizer: map[string]interface{}{
						vectorizerName: map[string]interface{}{
							"imageFields":        []interface{}{"image"},
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
							"imageFields":        []interface{}{"image"},
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
							"imageFields":        []interface{}{"image"},
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
							"videoFields":        []interface{}{"video"},
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
							"textFields":  []interface{}{"image_title", "image_description"},
							"imageFields": []interface{}{"image"},
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
			},
		}
		// create schema
		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import data", func(t *testing.T) {
			f, err := multimodal.GetCSV(dataFolderPath)
			require.NoError(t, err)
			defer f.Close()
			var objs []*models.Object
			i := 0
			csvReader := csv.NewReader(f)
			for {
				line, err := csvReader.Read()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				if i > 0 {
					id := line[1]
					imageTitle := line[2]
					imageDescription := line[3]
					imageBlob, err := multimodal.GetImageBlob(dataFolderPath, i)
					require.NoError(t, err)
					videoTitle := line[4]
					videoDescription := line[5]
					videoBlob, err := multimodal.GetVideoBlob(dataFolderPath, i)
					require.NoError(t, err)
					obj := &models.Object{
						Class: class.Class,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"image_title":       imageTitle,
							"image_description": imageDescription,
							"image":             imageBlob,
							"video_title":       videoTitle,
							"video_description": videoDescription,
							"video":             videoBlob,
						},
					}
					objs = append(objs, obj)
				}
				i++
			}
			for _, obj := range objs {
				helper.CreateObject(t, obj)
				helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
			}
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
			titleProperty := "image_title"
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
			titleProperty := "video_title"
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
