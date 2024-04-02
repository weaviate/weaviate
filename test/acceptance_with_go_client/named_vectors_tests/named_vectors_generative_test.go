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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testNamedVectorsWithGenerativeModules(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("named vectors with generative module", func(t *testing.T) {
			cleanup()
			// Define class
			className := "BooksGenerativeTest"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "title", DataType: []string{schema.DataTypeText.String()},
					},
					{
						Name: "description", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"title": {
						Vectorizer: map[string]interface{}{
							"text2vec-contextionary": map[string]interface{}{
								"properties":         []interface{}{"title"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
					"description": {
						Vectorizer: map[string]interface{}{
							"text2vec-transformers": map[string]interface{}{
								"properties":         []interface{}{"description"},
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
				ModuleConfig: map[string]interface{}{
					"generative-openai": map[string]interface{}{
						"model": "gpt-4",
					},
				},
			}

			t.Run("create schema", func(t *testing.T) {
				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)
			})

			t.Run("batch create objects", func(t *testing.T) {
				objs := []*models.Object{}
				for id, book := range fixtures.Books() {
					obj := &models.Object{
						Class: className,
						ID:    strfmt.UUID(id),
						Properties: map[string]interface{}{
							"title":       book.Title,
							"description": book.Description,
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
					objs, err := client.Data().ObjectsGetter().
						WithID(id).
						WithClassName(className).
						WithVector().
						Do(ctx)
					require.NoError(t, err)
					require.Len(t, objs, 1)
					require.NotNil(t, objs[0])
					require.Len(t, objs[0].Vectors, 2)
					assert.NotEmpty(t, objs[0].Vectors["title"])
					assert.NotEmpty(t, objs[0].Vectors["description"])
				}
			})
		})
	}
}
