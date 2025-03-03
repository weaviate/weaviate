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

package test_suits

import (
	"context"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testVectorIndexTypesConfigurations(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		vectorIndexTypeClassName := "NamedVectorsWithVectorIndexType"
		vectorIndexTargetVectors := []string{"description"}
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

		tests := []struct {
			name              string
			vectorIndexType   string
			vectorIndexConfig map[string]interface{}
		}{
			{
				name:              "flat",
				vectorIndexType:   "flat",
				vectorIndexConfig: nil,
			},
			{
				name:              "flat_bq",
				vectorIndexType:   "flat",
				vectorIndexConfig: bqFlatIndexConfig(),
			},
			{
				name:              "hnsw",
				vectorIndexType:   "hnsw",
				vectorIndexConfig: nil,
			},
			{
				name:              "hnsw_pq",
				vectorIndexType:   "hnsw",
				vectorIndexConfig: pqVectorIndexConfig(),
			},
			{
				name:              "hnsw_sq",
				vectorIndexType:   "hnsw",
				vectorIndexConfig: sqVectorIndexConfig(),
			},
			{
				name:              "hnsw_bq",
				vectorIndexType:   "hnsw",
				vectorIndexConfig: bqFlatIndexConfig(),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cleanup()

				t.Run("create schema", func(t *testing.T) {
					ctx := context.Background()
					class := &models.Class{
						Class: vectorIndexTypeClassName,
						Properties: []*models.Property{
							{
								Name: "author", DataType: []string{schema.DataTypeText.String()},
							},
							{
								Name: "title", DataType: []string{schema.DataTypeText.String()},
							},
							{
								Name: "description", DataType: []string{schema.DataTypeText.String()},
							},
							{
								Name: "genre", DataType: []string{schema.DataTypeText.String()},
							},
						},
						VectorConfig: map[string]models.VectorConfig{
							vectorIndexTargetVectors[0]: {
								Vectorizer: map[string]interface{}{
									text2vecTransformers: map[string]interface{}{
										"vectorizeClassName": false,
										"properties":         []interface{}{"description"},
									},
								},
								VectorIndexType:   tt.vectorIndexType,
								VectorIndexConfig: tt.vectorIndexConfig,
							},
						},
					}

					err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
					require.NoError(t, err)

					cls, err := client.Schema().ClassGetter().WithClassName(vectorIndexTypeClassName).Do(ctx)
					require.NoError(t, err)
					assert.Equal(t, class.Class, cls.Class)
					require.NotEmpty(t, cls.VectorConfig)
					require.Len(t, cls.VectorConfig, len(vectorIndexTargetVectors))
					for _, name := range vectorIndexTargetVectors {
						require.NotEmpty(t, cls.VectorConfig[name])
						assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
						vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
						require.True(t, ok)
						require.NotEmpty(t, vectorizerConfig[text2vecTransformers])
					}
				})

				t.Run("batch create objects", func(t *testing.T) {
					objs := []*models.Object{}
					for id, book := range fixtures.Books() {
						obj := &models.Object{
							Class: vectorIndexTypeClassName,
							ID:    strfmt.UUID(id),
							Properties: map[string]interface{}{
								"author":      book.Author,
								"title":       book.Title,
								"description": book.Description,
								"genre":       book.Genre,
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
							WithClassName(vectorIndexTypeClassName).
							Do(ctx)
						require.NoError(t, err)
						require.True(t, exists)
					}
				})

				t.Run("GraphQL get vectors", func(t *testing.T) {
					for id := range fixtures.Books() {
						resultVectors := getVectors(t, client, vectorIndexTypeClassName, id, vectorIndexTargetVectors...)
						require.NotEmpty(t, resultVectors[vectorIndexTargetVectors[0]])
					}
				})

				t.Run("GraphQL near<Media> check", func(t *testing.T) {
					for id, book := range fixtures.Books() {
						for _, targetVector := range vectorIndexTargetVectors {
							nearText := client.GraphQL().NearTextArgBuilder().
								WithConcepts([]string{book.Title}).
								WithTargetVectors(targetVector)
							resultVectors := getVectorsWithNearText(t, client, vectorIndexTypeClassName, id, nearText, vectorIndexTargetVectors...)
							require.NotEmpty(t, resultVectors[vectorIndexTargetVectors[0]])
						}
					}
				})

				t.Run("batch delete objects", func(t *testing.T) {
					where := filters.Where().
						WithPath([]string{"id"}).
						WithOperator(filters.ContainsAny).
						WithValueText(idsToDelete...)
					resp, err := client.Batch().ObjectsBatchDeleter().
						WithClassName(vectorIndexTypeClassName).
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
						WithClassName(vectorIndexTypeClassName).
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
							WithClassName(vectorIndexTypeClassName).
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
						beforeUpdateVectors := getVectors(t, client, vectorIndexTypeClassName, id, vectorIndexTargetVectors...)
						require.NotEmpty(t, beforeUpdateVectors[vectorIndexTargetVectors[0]])
					}

					objs := []*models.Object{}
					for id, book := range fixtures.Books() {
						obj := &models.Object{
							Class: vectorIndexTypeClassName,
							ID:    strfmt.UUID(id),
							Properties: map[string]interface{}{
								"description": book.Title,
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
						afterUpdateVectors := getVectors(t, client, vectorIndexTypeClassName, id, vectorIndexTargetVectors...)
						require.NotEmpty(t, afterUpdateVectors[vectorIndexTargetVectors[0]])
						beforeUpdateVectors := beforeUpdateVectorsMap[id]
						for _, targetVector := range vectorIndexTargetVectors {
							assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
						}
					}
				})
			})
		}
	}
}
