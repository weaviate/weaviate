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
	"fmt"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testCreateWithModulePropertiesObject(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		author := "author"
		title := "title"
		description := "description"
		genre := "genre"

		targetVectorsWithProperties := []string{
			author, title, description, genre,
		}

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()

			t.Run("create schema", func(t *testing.T) {
				ctx := context.Background()
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name: author, DataType: []string{schema.DataTypeText.String()},
						},
						{
							Name: title, DataType: []string{schema.DataTypeText.String()},
						},
						{
							Name: description, DataType: []string{schema.DataTypeText.String()},
						},
						{
							Name: genre, DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						author: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
									"properties":         []interface{}{"author"},
								},
							},
							VectorIndexType: "hnsw",
						},
						title: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
									"properties":         []interface{}{"title"},
								},
							},
							VectorIndexType: "flat",
						},
						description: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
									"properties":         []interface{}{"description"},
								},
							},
							VectorIndexType:   "hnsw",
							VectorIndexConfig: pqVectorIndexConfig(),
						},
						genre: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
									"properties":         []interface{}{"genre"},
								},
							},
							VectorIndexType:   "flat",
							VectorIndexConfig: bqFlatIndexConfig(),
						},
					},
				}

				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)

				cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
				require.NoError(t, err)
				assert.Equal(t, class.Class, cls.Class)
				require.NotEmpty(t, cls.VectorConfig)
				require.Len(t, cls.VectorConfig, 4)

				for _, name := range targetVectorsWithProperties {
					require.NotEmpty(t, cls.VectorConfig[name])
					assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
					vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
					require.True(t, ok)
					require.NotEmpty(t, vectorizerConfig)
				}
			})

			t.Run("create objects", func(t *testing.T) {
				for id, book := range fixtures.Books() {
					objWrapper, err := client.Data().Creator().
						WithClassName(className).
						WithID(id).
						WithProperties(map[string]interface{}{
							author:      book.Author,
							title:       book.Title,
							description: book.Description,
							genre:       book.Genre,
						}).
						Do(ctx)
					require.NoError(t, err)
					require.NotNil(t, objWrapper)
					assert.Len(t, objWrapper.Object.Vectors, 4)

					objs, err := client.Data().ObjectsGetter().
						WithClassName(className).
						WithID(id).
						WithVector().
						Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.NotNil(t, objs[0])
					assert.Len(t, objs[0].Vectors, 4)
					properties, ok := objs[0].Properties.(map[string]interface{})
					require.True(t, ok)
					assert.Equal(t, book.Author, properties["author"])
					assert.Equal(t, book.Title, properties["title"])
					assert.Equal(t, book.Description, properties["description"])
					assert.Equal(t, book.Genre, properties["genre"])
				}
			})

			t.Run("GraphQL get vectors", func(t *testing.T) {
				for id := range fixtures.Books() {
					resultVectors := getVectors(t, client, className, id, targetVectorsWithProperties...)
					for _, targetVector := range targetVectorsWithProperties {
						assert.NotEmpty(t, resultVectors[targetVector])
					}
				}
			})

			t.Run("GraphQL near<Media> check", func(t *testing.T) {
				for id, book := range fixtures.Books() {
					for _, targetVector := range targetVectorsWithProperties {
						nearText := client.GraphQL().NearTextArgBuilder().
							WithConcepts([]string{book.Title}).
							WithTargetVectors(targetVector)
						resultVectors := getVectorsWithNearText(t, client, className, id, nearText, targetVectorsWithProperties...)
						for _, targetVector := range targetVectorsWithProperties {
							assert.NotEmpty(t, resultVectors[targetVector])
						}
					}
				}
			})

			t.Run("GraphQL hybrid check", func(t *testing.T) {
				for _, book := range fixtures.Books() {
					raw := client.GraphQL().Raw()
					res, err := raw.WithQuery(fmt.Sprintf(""+
						"{Get {%s (hybrid: {query: %q targetVectors: %q}){title _additional{id}}}}", className, book.Title, "title")).Do(ctx)
					require.Nil(t, err)
					require.NotNil(t, res)
					require.Nil(t, res.Errors)
					require.NotNil(t, res.Data)
					titleHybrid := res.Data["Get"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["title"].(string)
					require.Equal(t, titleHybrid, book.Title)
				}
			})
			t.Run("merge object and check if vectors changed", func(t *testing.T) {
				for id, book := range fixtures.Books() {
					beforeUpdateVectors := getVectors(t, client, className, id, targetVectorsWithProperties...)
					// change only genre property
					err := client.Data().Updater().
						WithMerge().
						WithClassName(className).
						WithID(id).
						WithProperties(map[string]interface{}{
							author:      book.Author,
							title:       book.Title,
							description: book.Description,
							genre:       fmt.Sprintf("change only in %s genre", book.Genre),
						}).
						Do(ctx)
					require.NoError(t, err)
					afterUpdateVectors := getVectors(t, client, className, id, targetVectorsWithProperties...)
					for _, targetVector := range targetVectors {
						if targetVector == genre {
							assert.NotEqual(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
						} else {
							assert.Equal(t, beforeUpdateVectors[targetVector], afterUpdateVectors[targetVector])
						}
					}
				}
			})
		})
	}
}
