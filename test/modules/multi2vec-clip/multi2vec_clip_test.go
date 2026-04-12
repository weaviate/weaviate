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

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	schemaDef "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multimodal"
)

func testMulti2VecClip(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		// Define path to test/helper/sample-schema/multimodal/data folder
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		// Define class
		vectorizerName := "multi2vec-clip"
		className := "ClipTest"
		class := multimodal.BaseClass(className, false, false)
		class.VectorConfig = map[string]models.VectorConfig{
			"clip": {
				Vectorizer: map[string]any{
					vectorizerName: map[string]any{
						"imageFields":        []any{multimodal.PropertyImage},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			"clip_weights": {
				Vectorizer: map[string]any{
					vectorizerName: map[string]any{
						"textFields":  []any{multimodal.PropertyImageTitle, multimodal.PropertyImageDescription},
						"imageFields": []any{multimodal.PropertyImage},
						"weights": map[string]any{
							"textFields":  []any{0.05, 0.05},
							"imageFields": []any{0.9},
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
			multimodal.InsertObjects(t, dataFolderPath, class.Class, false, false)
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
			targetVectors := map[string]int{
				"clip":         512,
				"clip_weights": 512,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument, titleProperty, titlePropertyValue, targetVectors)
		})
	}
}

func testBlobHashValidation(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		vectorizerName := "multi2vec-clip"

		t.Run("single named vector: blobHash alone is accepted", func(t *testing.T) {
			className := "BlobHashAlone"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "img", DataType: []string{schemaDef.DataTypeBlobHash.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"clip": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"img"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			helper.CreateClass(t, class)
			defer helper.DeleteClass(t, class.Class)
		})

		t.Run("single named vector: blobHash with other media field is rejected", func(t *testing.T) {
			className := "BlobHashWithMedia"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "img", DataType: []string{schemaDef.DataTypeBlobHash.String()}},
					{Name: "photo", DataType: []string{schemaDef.DataTypeBlob.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"clip": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"img", "photo"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "blobHash property cannot be combined with other vectorizable fields")
		})

		t.Run("single named vector: blobHash with text fields is rejected", func(t *testing.T) {
			className := "BlobHashWithText"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "img", DataType: []string{schemaDef.DataTypeBlobHash.String()}},
					{Name: "title", DataType: []string{schemaDef.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"clip": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"img"},
								"textFields":         []any{"title"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "blobHash property cannot be combined with other vectorizable fields")
		})

		t.Run("multiple named vectors: only the misconfigured one is rejected", func(t *testing.T) {
			className := "BlobHashMultiVecBad"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "img", DataType: []string{schemaDef.DataTypeBlobHash.String()}},
					{Name: "photo", DataType: []string{schemaDef.DataTypeBlob.String()}},
					{Name: "title", DataType: []string{schemaDef.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"clip_text": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"textFields":         []any{"title"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_image": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"photo"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_bad": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"img", "photo"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "blobHash property cannot be combined with other vectorizable fields")
		})

		t.Run("multiple named vectors: all correctly configured is accepted", func(t *testing.T) {
			className := "BlobHashMultiVecOK"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "img", DataType: []string{schemaDef.DataTypeBlobHash.String()}},
					{Name: "photo", DataType: []string{schemaDef.DataTypeBlob.String()}},
					{Name: "title", DataType: []string{schemaDef.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{
					"clip_text": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"textFields":         []any{"title"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_image": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"photo"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
					"clip_hash": {
						Vectorizer: map[string]any{
							vectorizerName: map[string]any{
								"imageFields":        []any{"img"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			helper.CreateClass(t, class)
			defer helper.DeleteClass(t, class.Class)
		})
	}
}
