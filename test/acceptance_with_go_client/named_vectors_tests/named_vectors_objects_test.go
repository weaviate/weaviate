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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testCreateObject(t *testing.T, host string) func(t *testing.T) {
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

			className := "NamedVectors"
			text2vecContextionaryName1 := "c11y1"
			text2vecContextionaryName2 := "c11y2_flat"
			text2vecContextionaryName3 := "c11y2_pq"
			transformersName1 := "transformers1"
			transformersName2 := "transformers2_flat"
			transformersName3 := "transformers2_pq"
			text2vecContextionary := "text2vec-contextionary"
			text2vecTransformers := "text2vec-transformers"
			id1 := "00000000-0000-0000-0000-000000000001"
			id2 := "00000000-0000-0000-0000-000000000002"

			t.Run("create schema", func(t *testing.T) {
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name: "text", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						text2vecContextionaryName1: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "hnsw",
						},
						text2vecContextionaryName2: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "flat",
						},
						text2vecContextionaryName3: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType:   "hnsw",
							VectorIndexConfig: pqVectorIndexConfig(),
						},
						transformersName1: {
							Vectorizer: map[string]interface{}{
								text2vecTransformers: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "hnsw",
						},
						transformersName2: {
							Vectorizer: map[string]interface{}{
								text2vecTransformers: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "flat",
						},
						transformersName3: {
							Vectorizer: map[string]interface{}{
								text2vecTransformers: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType:   "hnsw",
							VectorIndexConfig: pqVectorIndexConfig(),
						},
					},
					Vectorizer: text2vecContextionary,
				}

				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)

				cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
				require.NoError(t, err)
				assert.Equal(t, class.Class, cls.Class)
				require.NotEmpty(t, cls.VectorConfig)
				require.Len(t, cls.VectorConfig, 6)
				targetVectors := []string{
					text2vecContextionaryName1, text2vecContextionaryName2, text2vecContextionaryName3,
					transformersName1, transformersName2, transformersName3,
				}
				for _, name := range targetVectors {
					require.NotEmpty(t, cls.VectorConfig[name])
					assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
					vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
					require.True(t, ok)
					vectorizerName := text2vecContextionary
					if strings.HasPrefix(name, "transformers") {
						vectorizerName = text2vecTransformers
					}
					require.NotEmpty(t, vectorizerConfig[vectorizerName])
				}
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
					assert.Len(t, objWrapper.Object.Vectors, 6)

					objs, err := client.Data().ObjectsGetter().
						WithClassName(className).
						WithID(object.id).
						WithVector().
						Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.NotNil(t, objs[0])
					assert.Len(t, objs[0].Vectors, 6)
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
				targetVectors := []string{
					text2vecContextionaryName1, text2vecContextionaryName2, text2vecContextionaryName3,
					transformersName1, transformersName2, transformersName3,
				}
				resultVectors := getVectors(t, client, className, id1, targetVectors...)
				require.NotEmpty(t, resultVectors[text2vecContextionaryName1])
				require.NotEmpty(t, resultVectors[text2vecContextionaryName2])
				require.NotEmpty(t, resultVectors[text2vecContextionaryName3])
				require.NotEmpty(t, resultVectors[transformersName1])
				require.NotEmpty(t, resultVectors[transformersName2])
				require.NotEmpty(t, resultVectors[transformersName3])
				assert.Equal(t, resultVectors[text2vecContextionaryName1], resultVectors[text2vecContextionaryName2])
				assert.Equal(t, resultVectors[text2vecContextionaryName2], resultVectors[text2vecContextionaryName3])
				assert.Equal(t, resultVectors[transformersName1], resultVectors[transformersName2])
				assert.Equal(t, resultVectors[transformersName2], resultVectors[transformersName3])
				assert.NotEqual(t, resultVectors[text2vecContextionaryName1], resultVectors[transformersName1])
				assert.NotEqual(t, resultVectors[text2vecContextionaryName2], resultVectors[transformersName2])
				assert.NotEqual(t, resultVectors[text2vecContextionaryName3], resultVectors[transformersName3])
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
				assert.Len(t, objs[0].Vectors, 6)
				properties, ok := objs[0].Properties.(map[string]interface{})
				require.True(t, ok)
				assert.NotNil(t, properties["text"])
			})

			t.Run("update object and check if vectors changed", func(t *testing.T) {
				targetVectors := []string{
					text2vecContextionaryName1, text2vecContextionaryName2, text2vecContextionaryName3,
					transformersName1, transformersName2, transformersName3,
				}
				beforeUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				require.NotEmpty(t, beforeUpdateVectors[text2vecContextionaryName1])
				require.NotEmpty(t, beforeUpdateVectors[text2vecContextionaryName2])
				require.NotEmpty(t, beforeUpdateVectors[text2vecContextionaryName3])
				require.NotEmpty(t, beforeUpdateVectors[transformersName1])
				require.NotEmpty(t, beforeUpdateVectors[transformersName2])
				require.NotEmpty(t, beforeUpdateVectors[transformersName3])
				assert.Equal(t, beforeUpdateVectors[text2vecContextionaryName1], beforeUpdateVectors[text2vecContextionaryName2])
				assert.Equal(t, beforeUpdateVectors[text2vecContextionaryName2], beforeUpdateVectors[text2vecContextionaryName3])
				assert.Equal(t, beforeUpdateVectors[transformersName1], beforeUpdateVectors[transformersName2])
				assert.Equal(t, beforeUpdateVectors[transformersName2], beforeUpdateVectors[transformersName3])
				assert.NotEqual(t, beforeUpdateVectors[text2vecContextionaryName1], beforeUpdateVectors[transformersName1])
				assert.NotEqual(t, beforeUpdateVectors[text2vecContextionaryName2], beforeUpdateVectors[transformersName2])
				assert.NotEqual(t, beforeUpdateVectors[text2vecContextionaryName3], beforeUpdateVectors[transformersName3])

				err := client.Data().Updater().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"text": "I like reading science-fiction books",
					}).
					Do(ctx)
				require.NoError(t, err)
				afterUpdateVectors := getVectors(t, client, className, id1, targetVectors...)
				require.NotEmpty(t, afterUpdateVectors[text2vecContextionaryName1])
				require.NotEmpty(t, afterUpdateVectors[text2vecContextionaryName2])
				require.NotEmpty(t, afterUpdateVectors[text2vecContextionaryName3])
				require.NotEmpty(t, afterUpdateVectors[transformersName1])
				require.NotEmpty(t, afterUpdateVectors[transformersName2])
				require.NotEmpty(t, afterUpdateVectors[transformersName3])
				for _, targetVector := range targetVectors {
					assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
				}
			})
		})
	}
}
