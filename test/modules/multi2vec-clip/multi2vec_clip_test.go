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

	"github.com/go-openapi/strfmt"
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

func testBlobHashVectorization(host string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(host)
		dataFolderPath := "../../../test/helper/sample-schema/multimodal/data"
		vectorizerName := "multi2vec-clip"
		className := "ClipBlobHashTest"

		class := multimodal.BaseClass(className, false, false, multimodal.WithBlobHash)
		class.VectorConfig = map[string]models.VectorConfig{
			"blob": {
				Vectorizer: map[string]any{
					vectorizerName: map[string]any{
						"imageFields":        []any{multimodal.PropertyImage},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
			"blobhash": {
				Vectorizer: map[string]any{
					vectorizerName: map[string]any{
						"imageFields":        []any{multimodal.PropertyImageHash},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "flat",
			},
		}

		helper.CreateClass(t, class)
		defer helper.DeleteClass(t, class.Class)

		t.Run("import data", func(t *testing.T) {
			multimodal.InsertObjects(t, dataFolderPath, class.Class, false, false,
				multimodal.WithImageHash(dataFolderPath))
		})

		t.Run("check vectors exist and are equal", func(t *testing.T) {
			multimodal.CheckObjects(t, dataFolderPath, class.Class, []string{"blob", "blobhash"}, nil)
			for _, id := range multimodal.GetIDs(t, dataFolderPath) {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)

				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok, "blob vector should be []float32")
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok, "blobhash vector should be []float32")

				require.Len(t, blobVec, 512)
				require.Len(t, blobHashVec, 512)
				assert.Equal(t, blobVec, blobHashVec,
					"blob and blobhash vectors should be identical for the same image data")
			}
		})

		t.Run("update with same blob data does not change vectors", func(t *testing.T) {
			// Capture vectors and image_hash property before update
			type objectSnapshot struct {
				blobVec       []float32
				blobHashVec   []float32
				imageHashProp string
			}
			snapshots := make(map[string]objectSnapshot)
			ids := multimodal.GetIDs(t, dataFolderPath)
			for _, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)
				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)
				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok, "image_hash property should be a string (SHA-256 hash)")
				require.NotEmpty(t, imageHash, "image_hash should not be empty")
				snapshots[id] = objectSnapshot{
					blobVec:       append([]float32{}, blobVec...),
					blobHashVec:   append([]float32{}, blobHashVec...),
					imageHashProp: imageHash,
				}
			}

			// Update each object: same image data, different image_title
			for i, id := range ids {
				imageBlob, err := multimodal.GetImageBlob(dataFolderPath, i+1)
				require.NoError(t, err)
				updatedObj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(id),
					Properties: map[string]any{
						multimodal.PropertyImageTitle:       fmt.Sprintf("updated_title_%d", i),
						multimodal.PropertyImageDescription: fmt.Sprintf("updated_desc_%d", i),
						multimodal.PropertyImage:            imageBlob,
						multimodal.PropertyImageHash:        imageBlob,
					},
				}
				err = helper.UpdateObject(t, updatedObj)
				require.NoError(t, err)
			}

			// Verify vectors did not change and titles were updated
			for i, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)

				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)

				snap := snapshots[id]
				assert.Equal(t, snap.blobVec, blobVec,
					"blob vector should not change when updating with same image data (object %s)", id)
				assert.Equal(t, snap.blobHashVec, blobHashVec,
					"blobhash vector should not change when updating with same image data (object %s)", id)

				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, fmt.Sprintf("updated_title_%d", i), props[multimodal.PropertyImageTitle],
					"image_title should be updated (object %s)", id)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok, "image_hash property should be a string after update")
				assert.Equal(t, snap.imageHashProp, imageHash,
					"image_hash stored value should not change when updating with same blob data (object %s)", id)
			}
		})

		t.Run("update only blob image does not change blobhash vector", func(t *testing.T) {
			// Snapshot vectors before update
			type vectorSnapshot struct {
				blobVec     []float32
				blobHashVec []float32
				imageHash   string
			}
			ids := multimodal.GetIDs(t, dataFolderPath)
			snapshots := make(map[string]vectorSnapshot)
			for _, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)
				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)
				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok)
				snapshots[id] = vectorSnapshot{
					blobVec:     append([]float32{}, blobVec...),
					blobHashVec: append([]float32{}, blobHashVec...),
					imageHash:   imageHash,
				}
			}

			// Update each object with a different image but keep the same blob data for image_hash
			for i, id := range ids {
				// Swap images: object 0 gets image 2, object 1 gets image 1
				swappedIdx := len(ids) - i
				newBlob, err := multimodal.GetImageBlob(dataFolderPath, swappedIdx)
				require.NoError(t, err)
				// Keep the original image blob for the hash field so the hash stays the same
				originalBlob, err := multimodal.GetImageBlob(dataFolderPath, i+1)
				require.NoError(t, err)

				updatedObj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(id),
					Properties: map[string]any{
						multimodal.PropertyImageTitle:       fmt.Sprintf("swapped_title_%d", i),
						multimodal.PropertyImageDescription: fmt.Sprintf("swapped_desc_%d", i),
						multimodal.PropertyImage:            newBlob,
						multimodal.PropertyImageHash:        originalBlob,
					},
				}
				err = helper.UpdateObject(t, updatedObj)
				require.NoError(t, err)
			}

			// Verify blobhash vector is unchanged, blob vector changed
			for _, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)

				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)

				snap := snapshots[id]
				assert.Equal(t, snap.blobHashVec, blobHashVec,
					"blobhash vector should NOT change when only blob image is updated (object %s)", id)
				assert.NotEqual(t, snap.blobVec, blobVec,
					"blob vector SHOULD change when image is swapped (object %s)", id)

				// image_hash property should remain the same
				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok)
				assert.Equal(t, snap.imageHash, imageHash,
					"image_hash should remain unchanged (object %s)", id)
			}
		})

		t.Run("patch only PropertyImage does not change blobhash vector", func(t *testing.T) {
			// Snapshot vectors before patch
			type vectorSnapshot struct {
				blobVec     []float32
				blobHashVec []float32
				imageHash   string
			}
			ids := multimodal.GetIDs(t, dataFolderPath)
			snapshots := make(map[string]vectorSnapshot)
			for _, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)
				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)
				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok)
				snapshots[id] = vectorSnapshot{
					blobVec:     append([]float32{}, blobVec...),
					blobHashVec: append([]float32{}, blobHashVec...),
					imageHash:   imageHash,
				}
			}

			// Patch each object with only PropertyImage (swap back)
			for i, id := range ids {
				newBlob, err := multimodal.GetImageBlob(dataFolderPath, i+1)
				require.NoError(t, err)

				patchObj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(id),
					Properties: map[string]any{
						multimodal.PropertyImage: newBlob,
					},
				}
				err = helper.PatchObject(t, patchObj)
				require.NoError(t, err)
			}

			// Verify blobhash vector is unchanged, blob vector changed
			for _, id := range ids {
				obj, err := helper.GetObject(t, className, strfmt.UUID(id), "vector")
				require.NoError(t, err)
				require.NotNil(t, obj)

				blobVec, ok := obj.Vectors["blob"].([]float32)
				require.True(t, ok)
				blobHashVec, ok := obj.Vectors["blobhash"].([]float32)
				require.True(t, ok)

				snap := snapshots[id]
				assert.Equal(t, snap.blobHashVec, blobHashVec,
					"blobhash vector should NOT change when only PropertyImage is patched (object %s)", id)
				assert.NotEqual(t, snap.blobVec, blobVec,
					"blob vector SHOULD change when PropertyImage is patched with different data (object %s)", id)

				// image_hash property should remain the same
				props, ok := obj.Properties.(map[string]any)
				require.True(t, ok)
				imageHash, ok := props[multimodal.PropertyImageHash].(string)
				require.True(t, ok)
				assert.Equal(t, snap.imageHash, imageHash,
					"image_hash should remain unchanged after patching only PropertyImage (object %s)", id)
			}
		})

		t.Run("nearImage search with blob target vector", func(t *testing.T) {
			blob, err := multimodal.GetImageBlob(dataFolderPath, 2)
			require.NoError(t, err)
			nearMediaArgument := fmt.Sprintf(`
				nearImage: {
					image: "%s"
					targetVectors: ["blob"]
				}
			`, blob)
			targetVectors := map[string]int{
				"blob":     512,
				"blobhash": 512,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument,
				multimodal.PropertyImageTitle, "swapped_title_1", targetVectors)
		})

		t.Run("nearImage search with blobhash target vector", func(t *testing.T) {
			blob, err := multimodal.GetImageBlob(dataFolderPath, 2)
			require.NoError(t, err)
			nearMediaArgument := fmt.Sprintf(`
				nearImage: {
					image: "%s"
					targetVectors: ["blobhash"]
				}
			`, blob)
			targetVectors := map[string]int{
				"blob":     512,
				"blobhash": 512,
			}
			multimodal.TestQuery(t, class.Class, nearMediaArgument,
				multimodal.PropertyImageTitle, "swapped_title_1", targetVectors)
		})
	}
}

func requireCreateClassError(t *testing.T, class *models.Class, expectedMsg string) {
	t.Helper()
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	require.Error(t, err)
	var errResp *schema.SchemaObjectsCreateUnprocessableEntity
	require.ErrorAs(t, err, &errResp)
	require.NotNil(t, errResp.Payload)
	require.NotEmpty(t, errResp.Payload.Error)
	assert.Contains(t, errResp.Payload.Error[0].Message, expectedMsg)
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
			class := &models.Class{
				Class: "BlobHashWithMedia",
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
			requireCreateClassError(t, class, "blobHash property cannot be combined with other vectorizable fields")
		})

		t.Run("single named vector: blobHash with text fields is rejected", func(t *testing.T) {
			class := &models.Class{
				Class: "BlobHashWithText",
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
			requireCreateClassError(t, class, "blobHash property cannot be combined with other vectorizable fields")
		})

		t.Run("multiple named vectors: only the misconfigured one is rejected", func(t *testing.T) {
			class := &models.Class{
				Class: "BlobHashMultiVecBad",
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
			requireCreateClassError(t, class, "blobHash property cannot be combined with other vectorizable fields")
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
