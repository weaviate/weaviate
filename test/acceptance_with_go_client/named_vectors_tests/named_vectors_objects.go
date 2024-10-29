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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testCreateObject(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()

			t.Run("create schema", func(t *testing.T) {
				createNamedVectorsClass(t, client)
			})

			t.Run("create objects", func(t *testing.T) {
				objects := []struct {
					id   string
					text string
				}{
					{id: id1, text: "I like reading books"},
					{id: id2, text: "I like programming"},
				}
				for _, object := range objects {
					objWrapper, err := client.Data().Creator().
						WithClassName(className).
						WithID(object.id).
						WithProperties(map[string]interface{}{
							"text": object.text,
						}).
						Do(ctx)
					require.NoError(t, err)
					require.NotNil(t, objWrapper)
					assert.Len(t, objWrapper.Object.Vectors, len(targetVectors))

					objs, err := client.Data().ObjectsGetter().
						WithClassName(className).
						WithID(object.id).
						WithVector().
						Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.NotNil(t, objs[0])
					assert.Len(t, objs[0].Vectors, len(targetVectors))
					properties, ok := objs[0].Properties.(map[string]interface{})
					require.True(t, ok)
					assert.Equal(t, object.text, properties["text"])
				}
			})

			t.Run("check existence", func(t *testing.T) {
				for _, id := range []string{id1, id2} {
					exists, err := client.Data().Checker().
						WithID(id).
						WithClassName(className).
						Do(ctx)
					require.NoError(t, err)
					require.True(t, exists)
				}
			})

			t.Run("GraphQL get vectors", func(t *testing.T) {
				resultVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, resultVectors)
			})

			t.Run("GraphQL near<Media> check", func(t *testing.T) {
				for _, targetVector := range targetVectors {
					nearText := client.GraphQL().NearTextArgBuilder().
						WithConcepts([]string{"book"}).
						WithTargetVectors(targetVector)
					resultVectors := getVectorsWithNearText(t, client, className, id1, nearText, targetVectors...)
					checkTargetVectors(t, resultVectors)
				}
			})

			t.Run("certainty checks", func(t *testing.T) {
				var vectorC11y []float32
				t.Run("nearText", func(t *testing.T) {
					nearText := client.GraphQL().NearTextArgBuilder().
						WithConcepts([]string{"book"}).
						WithTargetVectors(c11y)
					resultVectors := getVectorsWithNearTextWithCertainty(t, client, className, id1, nearText, c11y)
					require.NotEmpty(t, resultVectors[c11y])
					vectorC11y = resultVectors[c11y]
				})

				t.Run("nearVector", func(t *testing.T) {
					nearVector := client.GraphQL().NearVectorArgBuilder().
						WithVector(vectorC11y).
						WithTargetVectors(c11y)
					resultVectors := getVectorsWithNearVectorWithCertainty(t, client, className, id1, nearVector, c11y)
					require.NotEmpty(t, resultVectors[c11y])
				})

				t.Run("nearObject", func(t *testing.T) {
					nearObject := client.GraphQL().NearObjectArgBuilder().
						WithID(id1).
						WithTargetVectors(c11y)
					resultVectors := getVectorsWithNearObjectWithCertainty(t, client, className, id1, nearObject, c11y)
					require.NotEmpty(t, resultVectors[c11y])
				})
			})

			t.Run("delete 1 object", func(t *testing.T) {
				err := client.Data().Deleter().
					WithClassName(className).
					WithID(id2).
					Do(ctx)
				require.NoError(t, err)

				exists, err := client.Data().Checker().
					WithID(id2).
					WithClassName(className).
					Do(ctx)
				require.NoError(t, err)
				require.False(t, exists)

				objs, err := client.Data().ObjectsGetter().
					WithClassName(className).
					WithID(id1).
					WithVector().
					Do(ctx)
				require.NoError(t, err)
				require.Len(t, objs, 1)
				require.NotNil(t, objs[0])
				assert.Len(t, objs[0].Vectors, len(targetVectors))
				properties, ok := objs[0].Properties.(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, properties["text"])
			})

			t.Run("update object and check if vectors changed", func(t *testing.T) {
				beforeUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, beforeUpdateVectors)

				err := client.Data().Updater().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"text": "I like reading science-fiction books",
					}).
					Do(ctx)
				require.NoError(t, err)
				afterUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, afterUpdateVectors)
				for _, targetVector := range targetVectors {
					assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
				}
			})

			t.Run("merge object and check if vectors changed", func(t *testing.T) {
				beforeUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, beforeUpdateVectors)

				err := client.Data().Updater().
					WithMerge().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"text": "This is a new property value that should be merged",
					}).
					Do(ctx)
				require.NoError(t, err)
				afterUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, afterUpdateVectors)
				for _, targetVector := range targetVectors {
					assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
				}
			})

			t.Run("add new property", func(t *testing.T) {
				property := &models.Property{
					Name:     "dont_vectorize_property",
					DataType: []string{schema.DataTypeText.String()},
					ModuleConfig: map[string]interface{}{
						text2vecContextionary: map[string]interface{}{
							"skip":                  true,
							"vectorizePropertyName": "false",
						},
						text2vecTransformers: map[string]interface{}{
							"skip":                  true,
							"vectorizePropertyName": "false",
						},
					},
				}

				err = client.Schema().PropertyCreator().WithClassName(className).WithProperty(property).Do(ctx)
				require.NoError(t, err)

				class, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, class)
				require.Len(t, class.Properties, 2)

				beforeUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, beforeUpdateVectors)

				err = client.Data().Updater().
					WithMerge().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"dont_vectorize_property": "This change should not change vector",
					}).
					Do(ctx)
				require.NoError(t, err)
				afterUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, afterUpdateVectors)
				for _, targetVector := range targetVectors {
					assert.Equal(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
				}
			})

			// note that vectors are different afterwards and you cant use checkTargetVectors anymore
			t.Run("Update with a vector - use the given vector and dont revectorize", func(t *testing.T) {
				targetVecUpdate := targetVectors[0]
				beforeUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				checkTargetVectors(t, beforeUpdateVectors)
				vecForNewObject := make([]float32, len(beforeUpdateVectors[targetVecUpdate]))
				copy(vecForNewObject, beforeUpdateVectors[targetVecUpdate])
				vecForNewObject[0] = vecForNewObject[0] + 0.1

				require.NoError(t, client.Data().Updater().
					WithMerge().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"text": "Apple",
					}).WithVectors(models.Vectors{targetVecUpdate: vecForNewObject}).
					Do(ctx))

				// vector from update should be used and it should be different from before
				afterUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				assert.NotEqual(t, beforeUpdateVectors[targetVecUpdate], afterUpdateVectors[targetVecUpdate])

				// no vectorization
				assert.Equal(t, vecForNewObject, afterUpdateVectors[targetVecUpdate])

				// vectorization for vectors that have not been sent with the update
				for _, targetVector := range targetVectors[1:] {
					assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
				}
			})
		})
	}
}
