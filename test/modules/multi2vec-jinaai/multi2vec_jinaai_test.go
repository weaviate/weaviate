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
		// Define class
		vectorizerName := "multi2vec-jinaai"
		className := "ClipTest"
		class := multimodal.BaseClass(className, false)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"dimensions":         600,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_weights": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
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
				"clip":         600,
				"clip_weights": 1024,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}
