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
		// Define class
		className := "AWSClipTest"
		vectorizerName := "multi2vec-aws"
		class := multimodal.BaseClass(className, true)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip_aws": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"region":             region,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_aws_256": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields":        []interface{}{multimodal.PropertyImage},
						"vectorizeClassName": false,
						"region":             region,
						"dimensions":         256,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_aws_weights": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"textFields":  []interface{}{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
						"imageFields": []interface{}{multimodal.PropertyImage},
						"weights": map[string]interface{}{
							"textFields":  []interface{}{0.05, 0.05},
							"imageFields": []interface{}{0.9},
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
				"clip_aws":         1024,
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
				"clip_aws":         1024,
				"clip_aws_256":     256,
				"clip_aws_weights": 384,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}
