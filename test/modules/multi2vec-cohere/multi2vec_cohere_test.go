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

func testMulti2VecCohere(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		// Define class
		vectorizerName := "multi2vec-cohere"
		className := "CohereClipTest"
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
					Name: "image", DataType: []string{schema.DataTypeBlob.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"clip": {
					Vectorizer: map[string]interface{}{
						vectorizerName: map[string]interface{}{
							"imageFields":        []interface{}{"image"},
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "flat",
				},
				"clip_weights": {
					Vectorizer: map[string]interface{}{
						vectorizerName: map[string]interface{}{
							"model":       "embed-english-light-v3.0",
							"textFields":  []interface{}{"image_title", "image_description"},
							"imageFields": []interface{}{"image"},
							"weights": map[string]interface{}{
								"textFields":  []interface{}{0.05, 0.05},
								"imageFields": []interface{}{0.9},
							},
							"vectorizeClassName": false,
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
					obj := &models.Object{
						Class: class.Class,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"image_title":       imageTitle,
							"image_description": imageDescription,
							"image":             imageBlob,
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
			targetVector := "clip"
			nearMediaArgument := fmt.Sprintf(`
				nearImage: {
					image: "%s"
					targetVectors: ["%s"]
				}
			`, blob, targetVector)
			titleProperty := "image_title"
			titlePropertyValue := "waterfalls"
			targetVectors := map[string]int{
				"clip":         1024,
				"clip_weights": 384,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}
