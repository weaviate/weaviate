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
	"acceptance_tests_with_client/fixtures"
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate/entities/models"
)

func testBatchObject(t *testing.T, host string) func(t *testing.T) {
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
	}
}
