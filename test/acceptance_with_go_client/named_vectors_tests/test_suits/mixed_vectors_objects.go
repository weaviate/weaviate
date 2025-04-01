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
	"fmt"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testMixedVectorsObject(host string) func(t *testing.T) {
	return func(t *testing.T) {
		var (
			ctx     = context.Background()
			idField = graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
				},
			}
		)
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))
		class := createMixedVectorsSchema(t, client)

		// create objects
		for i, data := range []struct{ id, text string }{
			{id: id1, text: "I like reading books"},
			{id: id2, text: "I like programming"},
		} {
			_, err := client.Data().Creator().
				WithClassName(class.Class).
				WithID(data.id).
				WithProperties(map[string]interface{}{
					"text":   data.text,
					"number": i,
				}).
				Do(context.Background())
			require.NoError(t, err)
		}
		testAllObjectsIndexed(t, client, class.Class)

		t.Run("get object", func(t *testing.T) {
			objWrappers, err := client.Data().ObjectsGetter().
				WithClassName(class.Class).
				WithID(id1).
				WithVector().
				Do(ctx)
			require.NoError(t, err)

			require.Len(t, objWrappers, 1)
			obj := objWrappers[0]
			require.NotNil(t, obj)

			assert.Len(t, obj.Vector, 300)

			require.Len(t, obj.Vectors, 3)
			assert.Equal(t, []float32(obj.Vector), obj.Vectors["contextionary"].([]float32))
			assert.Len(t, obj.Vectors["contextionary_with_class_name"], 300)
			assert.Len(t, obj.Vectors["transformers"], 384)

			// as these vectors were made using different module parameters, they should be different
			assert.NotEqual(t, obj.Vector, obj.Vectors["contextionary_with_class_name"], 300)
		})

		t.Run("GraphQL get vectors", func(t *testing.T) {
			resultVectors := getVectors(t, client, class.Class, id1, "", transformers)
			require.Len(t, resultVectors, 2)
			require.Len(t, resultVectors[""], 300)
			require.Len(t, resultVectors[transformers], 384)
		})

		for _, targetVector := range []string{"", contextionary} {
			t.Run(fmt.Sprintf("targetVector=%q", targetVector), func(t *testing.T) {
				t.Run("nearText search", func(t *testing.T) {
					nearText := client.GraphQL().NearTextArgBuilder().
						WithConcepts([]string{"book"}).
						WithCertainty(0.9)

					if targetVector != "" {
						nearText = nearText.WithTargetVectors(targetVector)
					}

					res, err := client.GraphQL().Get().WithClassName(class.Class).
						WithNearText(nearText).
						WithFields(idField).Do(ctx)
					require.NoError(t, err)

					require.Equal(t, []string{id1}, acceptance_with_go_client.GetIds(t, res, class.Class))
				})

				t.Run("nearObject search", func(t *testing.T) {
					nearObject := client.GraphQL().NearObjectArgBuilder().WithID(id1)
					if targetVector != "" {
						nearObject = nearObject.WithTargetVectors(targetVector)
					}

					res, err := client.GraphQL().Get().WithClassName(class.Class).
						WithNearObject(nearObject).
						WithFields(idField).
						WithLimit(1).Do(ctx)
					require.NoError(t, err)

					require.Equal(t, []string{id1}, acceptance_with_go_client.GetIds(t, res, class.Class))
				})

				t.Run("nearVector search", func(t *testing.T) {
					vectors := getVectors(t, client, class.Class, id1, contextionary)
					require.Len(t, vectors, 1)
					require.Len(t, vectors[contextionary], 300)
					obj1C11YVector := vectors[contextionary].([]float32)

					nearVector := client.GraphQL().NearVectorArgBuilder().
						WithVector(obj1C11YVector).
						WithCertainty(0.9)

					if targetVector != "" {
						nearVector = nearVector.WithTargetVectors(targetVector)
					}

					res, err := client.GraphQL().Get().WithClassName(class.Class).
						WithNearVector(nearVector).
						WithFields(idField).Do(ctx)
					require.NoError(t, err)

					require.Equal(t, []string{id1}, acceptance_with_go_client.GetIds(t, res, class.Class))
				})
			})
		}

		t.Run("update object", func(t *testing.T) {
			vectorsToCheck := []string{"", contextionary}

			beforeUpdate := getVectors(t, client, class.Class, id1, vectorsToCheck...)
			require.NoError(t, client.Data().Updater().
				WithClassName(className).
				WithID(id1).
				WithProperties(map[string]interface{}{
					"text": "I like reading science-fiction books",
				}).
				Do(ctx))
			afterUpdate := getVectors(t, client, class.Class, id1, vectorsToCheck...)

			// expect the vectors to change
			require.Equal(t, len(beforeUpdate), len(afterUpdate))
			for _, targetVector := range vectorsToCheck {
				require.NotEqual(t, beforeUpdate[targetVector], afterUpdate[targetVector])
			}
		})

		t.Run("update object with merge", func(t *testing.T) {
			vectorsToCheck := []string{"", contextionary}
			beforeUpdate := getVectors(t, client, class.Class, id1, vectorsToCheck...)
			require.NoError(t, client.Data().Updater().
				WithClassName(className).
				WithMerge().
				WithID(id1).
				WithProperties(map[string]interface{}{
					"text": "I like reading books about dinosaurs",
				}).
				Do(ctx))
			afterUpdate := getVectors(t, client, class.Class, id1, vectorsToCheck...)

			// expect the vectors to change
			require.Equal(t, len(beforeUpdate), len(afterUpdate))
			for _, targetVector := range vectorsToCheck {
				require.NotEqual(t, beforeUpdate[targetVector], afterUpdate[targetVector])
			}
		})
	}
}

func testMixedVectorsBatchBYOV(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		require.NoError(t, client.Schema().AllDeleter().Do(ctx))
		class := createMixedVectorsSchema(t, client)

		var objects []*models.Object
		for i, data := range []struct {
			id     strfmt.UUID
			text   string
			vector []float32
		}{
			{id: UUID1, text: "I like reading books", vector: []float32{1, 2, 3, 4}},
			{id: UUID2, text: "I like programming", vector: []float32{5, 6, 7, 8}},
		} {
			objects = append(objects, &models.Object{
				Class: class.Class,
				ID:    data.id,
				Properties: map[string]interface{}{
					"text":   data.text,
					"number": i,
				},
				Vector: data.vector,
				Vectors: models.Vectors{
					contextionary: data.vector,
				},
			})
		}

		resp, err := client.Batch().
			ObjectsBatcher().
			WithObjects(objects...).
			Do(ctx)
		require.NoError(t, err)
		for _, r := range resp {
			require.Nil(t, r.Result.Errors, spew.Sdump(r.Result.Errors))
		}

		for _, obj := range objects {
			vectors := getVectors(t, client, class.Class, obj.ID.String(), "", contextionary, transformers)
			require.Len(t, vectors, 3)
			require.Equal(t, []float32(obj.Vector), vectors[""].([]float32))
			require.Equal(t, []float32(obj.Vector), vectors[contextionary].([]float32))
			require.Len(t, vectors[transformers], 384)
		}
	}
}

func createMixedVectorsSchema(t *testing.T, client *wvt.Client) *models.Class {
	contextionaryConfig := map[string]interface{}{
		text2vecContextionary: map[string]interface{}{
			"vectorizeClassName": false,
		},
	}

	return createMixedVectorsSchemaHelper(t, client, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "number",
				DataType: schema.DataTypeInt.PropString(),
			},
		},
		Vectorizer:   text2vecContextionary,
		ModuleConfig: contextionaryConfig,
		VectorConfig: map[string]models.VectorConfig{
			contextionary: {
				Vectorizer:      contextionaryConfig,
				VectorIndexType: "hnsw",
			},
			"contextionary_with_class_name": {
				Vectorizer: map[string]interface{}{
					text2vecContextionary: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				VectorIndexType: "hnsw",
			},
			transformers: {
				Vectorizer: map[string]interface{}{
					text2vecTransformers: map[string]interface{}{},
				},
				VectorIndexType: "flat",
			},
		},
	})
}
