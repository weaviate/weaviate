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

package named_vectors_tests

import (
	"context"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testBatchObject(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		idsToDelete := []string{id1, id2}
		hasBeenDeleted := func(id string) bool {
			for _, deletedId := range idsToDelete {
				if deletedId == id {
					return true
				}
			}
			return false
		}

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()

			t.Run("create schema", func(t *testing.T) {
				createNamedVectorsClass(t, client)
			})

			t.Run("batch create objects", func(t *testing.T) {
				objs := []*models.Object{}
				for id, book := range fixtures.Books() {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"text": book.Description,
						},
					}
					objs = append(objs, obj)
				}

				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(objs...).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
			})

			t.Run("check existence", func(t *testing.T) {
				for id := range fixtures.Books() {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})

			t.Run("GraphQL get vectors", func(t *testing.T) {
				for id := range fixtures.Books() {
					resultVectors := getVectors(t, client, className, id, targetVectors...)
					checkTargetVectors(t, resultVectors)
				}
			})

			t.Run("GraphQL near<Media> check", func(t *testing.T) {
				for id, book := range fixtures.Books() {
					for _, targetVector := range targetVectors {
						nearText := client.GraphQL().NearTextArgBuilder().
							WithConcepts([]string{book.Title}).
							WithTargetVectors(targetVector)
						resultVectors := getVectorsWithNearText(t, client, className, id, nearText, targetVectors...)
						checkTargetVectors(t, resultVectors)
					}
				}
			})

			t.Run("batch delete objects", func(t *testing.T) {
				where := filters.Where().
					WithPath([]string{"id"}).
					WithOperator(filters.ContainsAny).
					WithValueText(idsToDelete...)
				resp, err := client.Batch().ObjectsBatchDeleter().
					WithClassName(className).
					WithDryRun(true).
					WithOutput("verbose").
					WithWhere(where).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.Results)
				require.Len(t, resp.Results.Objects, 2)
				for _, res := range resp.Results.Objects {
					require.Nil(t, res.Errors)
					require.NotNil(t, res.Status)
					assert.Equal(t, models.BatchDeleteResponseResultsObjectsItems0StatusDRYRUN, *res.Status)
				}

				resp, err = client.Batch().ObjectsBatchDeleter().
					WithClassName(className).
					WithDryRun(false).
					WithOutput("verbose").
					WithWhere(where).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.NotNil(t, resp.Results)
				require.Len(t, resp.Results.Objects, 2)
				for _, res := range resp.Results.Objects {
					require.Nil(t, res.Errors)
					require.NotNil(t, res.Status)
					assert.Equal(t, models.BatchDeleteResponseResultsObjectsItems0StatusSUCCESS, *res.Status)
				}

				for id := range fixtures.Books() {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					isDeleted := false
					for _, deletedID := range idsToDelete {
						if id == deletedID {
							isDeleted = true
							break
						}
					}
					require.Equal(t, !isDeleted, exists)
				}
			})

			t.Run("batch update objects and check if vectors changed", func(t *testing.T) {
				existingIds := []string{}
				for id := range fixtures.Books() {
					if !hasBeenDeleted(id) {
						existingIds = append(existingIds, id)
					}
				}
				beforeUpdateVectorsMap := map[string]map[string][]float32{}
				for _, id := range existingIds {
					beforeUpdateVectors := getVectors(t, client, className, id, targetVectors...)
					checkTargetVectors(t, beforeUpdateVectors)
				}

				objs := []*models.Object{}
				for id, book := range fixtures.Books() {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"text": book.Title,
						},
					}
					objs = append(objs, obj)
				}
				resp, err := client.Batch().ObjectsBatcher().
					WithObjects(objs...).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, resp)

				for _, id := range existingIds {
					afterUpdateVectors := getVectors(t, client, className, id, targetVectors...)
					checkTargetVectors(t, afterUpdateVectors)
					beforeUpdateVectors := beforeUpdateVectorsMap[id]
					for _, targetVector := range targetVectors {
						assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
					}
				}
			})
		})

		t.Run("bring your own vectors", func(t *testing.T) {
			cleanup()
			tests := []struct {
				name                   string
				className              string
				vectorConfig           map[string]models.VectorConfig
				vectors                models.Vectors
				generatedTargetVectors []string
			}{
				{
					name:      "with 2 none vectorizers",
					className: "OnlyNoneVectorizers",
					vectorConfig: map[string]models.VectorConfig{
						"none1": {
							Vectorizer: map[string]interface{}{
								"none": nil,
							},
							VectorIndexType: "hnsw",
						},
						"none2": {
							Vectorizer: map[string]interface{}{
								"none": nil,
							},
							VectorIndexType: "flat",
						},
					},
					vectors: models.Vectors{
						"none1": []float32{1, 2},
						"none2": []float32{0.11, 0.22, 0.33},
					},
				},
				{
					name:      "with 2 none vectorizers and 2 generated vectors",
					className: "NoneAndGeneratedVectorizers",
					vectorConfig: map[string]models.VectorConfig{
						"none1": {
							Vectorizer: map[string]interface{}{
								"none": nil,
							},
							VectorIndexType: "hnsw",
						},
						"none2": {
							Vectorizer: map[string]interface{}{
								"none": nil,
							},
							VectorIndexType: "flat",
						},
						c11y: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "hnsw",
						},
						transformers_flat: {
							Vectorizer: map[string]interface{}{
								text2vecTransformers: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "flat",
						},
					},
					vectors: models.Vectors{
						"none1": []float32{1, 2},
						"none2": []float32{0.11, 0.22, 0.33},
					},
					generatedTargetVectors: []string{c11y, transformers_flat},
				},
			}
			for _, tt := range tests {
				class := &models.Class{
					Class: tt.className,
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: tt.vectorConfig,
				}
				err = client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)
				// perform batch import
				id := strfmt.UUID("00000000-0000-0000-0000-000000000001")
				vectors := tt.vectors
				object := &models.Object{
					ID:    id,
					Class: tt.className,
					Properties: map[string]interface{}{
						"name": "some name",
					},
					Vectors: vectors,
				}
				batchResponse, err := client.Batch().ObjectsBatcher().WithObjects(object).Do(ctx)
				require.NoError(t, err)
				assert.NotNil(t, batchResponse)
				assert.Equal(t, 1, len(batchResponse))
				// get object
				objs, err := client.Data().ObjectsGetter().
					WithClassName(tt.className).WithID(id.String()).WithVector().
					Do(ctx)
				require.NoError(t, err)
				require.NotEmpty(t, objs)
				require.Len(t, objs, 1)
				assert.Equal(t, id, objs[0].ID)
				require.Len(t, objs[0].Vectors, len(tt.vectors)+len(tt.generatedTargetVectors))
				for targetVector, vector := range objs[0].Vectors {
					assert.Equal(t, vector, objs[0].Vectors[targetVector])
				}
				for _, generatedTargetVector := range tt.generatedTargetVectors {
					assert.NotEmpty(t, objs[0].Vectors[generatedTargetVector])
				}
			}
		})
	}
}
