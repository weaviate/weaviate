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
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2MultivecJinaAI(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		// Define class
		vectorizerName := "multi2multivec-jinaai"
		className := "ClipTest"
		class := multimodal.BaseClass(className, false)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip": {
				Vectorizer: map[string]interface{}{
					vectorizerName: map[string]interface{}{
						"imageFields": []interface{}{multimodal.PropertyImage},
					},
				},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: hnsw.MultivectorConfig{Enabled: true},
			},
		}
		// create schema
		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import data", func(t *testing.T) {
			multimodal.InsertObjects(t, dataFolderPath, class.Class, false)
		})

		t.Run("check objects", func(t *testing.T) {
			multimodal.CheckObjects(t, dataFolderPath, class.Class, nil, []string{"clip"})
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
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, nil)
		})

		t.Run("nearText", func(t *testing.T) {
			titleProperty := multimodal.PropertyImageTitle
			titlePropertyValue := "waterfalls"
			targetVector := "clip"
			nearMediaArgument := fmt.Sprintf(`
				nearText: {
					concepts: "%s"
					targetVectors: ["%s"]
				}
			`, titlePropertyValue, targetVector)
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, nil)
		})
	}
}
