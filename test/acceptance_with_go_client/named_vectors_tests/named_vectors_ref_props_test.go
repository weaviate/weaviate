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
	"strings"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testReferenceProperties(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}
		cleanup()

		classBookshelf := "Bookshelf"
		classBook := "Book"
		author := "author"
		title := "title"
		description := "description"
		genre := "genre"
		ofBookshelf := "ofBookshelf"
		transformers_bookshelf_name := "transformers_bookshelf_name"
		c11y_bookshelf_name := "c11y_bookshelf_name"

		bookshelfIDs := []string{
			"00000000-0000-0000-0000-00000000000a",
			"00000000-0000-0000-0000-00000000000b",
			"00000000-0000-0000-0000-00000000000c",
			"00000000-0000-0000-0000-00000000000d",
		}

		t.Run("create schema", func(t *testing.T) {
			bookshelf := &models.Class{
				Class: classBookshelf,
				Properties: []*models.Property{
					{
						Name:     "name",
						DataType: []string{"text"},
					},
				},
				InvertedIndexConfig: &models.InvertedIndexConfig{IndexTimestamps: true},
				VectorConfig: map[string]models.VectorConfig{
					c11y_bookshelf_name: {
						Vectorizer: map[string]interface{}{
							text2vecContextionary: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{"name"},
							},
						},
						VectorIndexType: "hnsw",
					},
					transformers_bookshelf_name: {
						Vectorizer: map[string]interface{}{
							text2vecContextionary: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{"name"},
							},
						},
						VectorIndexType: "flat",
					},
				},
			}
			book := &models.Class{
				Class: classBook,
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
					{
						Name: ofBookshelf, DataType: []string{classBookshelf},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					author: {
						Vectorizer: map[string]interface{}{
							text2vecContextionary: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{author},
							},
						},
						VectorIndexType: "hnsw",
					},
					title: {
						Vectorizer: map[string]interface{}{
							text2vecContextionary: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{title},
							},
						},
						VectorIndexType: "flat",
					},
					description: {
						Vectorizer: map[string]interface{}{
							text2vecTransformers: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{description},
							},
						},
						VectorIndexType:   "hnsw",
						VectorIndexConfig: pqVectorIndexConfig(),
					},
					genre: {
						Vectorizer: map[string]interface{}{
							text2vecTransformers: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{genre},
							},
						},
						VectorIndexType:   "flat",
						VectorIndexConfig: bqFlatIndexConfig(),
					},
				},
			}
			err := client.Schema().ClassCreator().WithClass(bookshelf).Do(ctx)
			assert.NoError(t, err)
			err = client.Schema().ClassCreator().WithClass(book).Do(ctx)
			require.NoError(t, err)
		})

		t.Run("import data", func(t *testing.T) {
			bookshelfs := make([]*models.Object, len(bookshelfIDs))
			for i, id := range bookshelfIDs {
				bookshelfs[i] = &models.Object{
					ID:    strfmt.UUID(id),
					Class: classBookshelf,
					Properties: map[string]interface{}{
						"name": fmt.Sprintf("Name of the bookshelf number %v", i),
					},
				}
			}
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(bookshelfs...).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)

			books := []*models.Object{}
			for id, book := range fixtures.Books() {
				books = append(books, &models.Object{
					ID:    strfmt.UUID(id),
					Class: classBook,
					Properties: map[string]interface{}{
						author:      book.Author,
						title:       book.Title,
						description: book.Description,
						genre:       book.Genre,
					},
				})
			}
			resp, err = client.Batch().ObjectsBatcher().
				WithObjects(books...).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)

			createReferences := func(t *testing.T, client *weaviate.Client,
				bookshelf *models.Object, books []*models.Object,
			) {
				ref := client.Data().ReferencePayloadBuilder().
					WithID(bookshelf.ID.String()).WithClassName(bookshelf.Class).Payload()
				for _, book := range books {
					err := client.Data().ReferenceCreator().
						WithID(book.ID.String()).
						WithClassName(book.Class).
						WithReferenceProperty(ofBookshelf).
						WithReference(ref).
						Do(context.TODO())
					assert.Nil(t, err)
				}
			}

			createReferences(t, client, bookshelfs[0], books[:10])
			createReferences(t, client, bookshelfs[1], books[10:])
		})

		t.Run("GrahQL check", func(t *testing.T) {
			targetVectors := []string{transformers_bookshelf_name, c11y_bookshelf_name}
			where := filters.Where().
				WithPath([]string{"id"}).
				WithOperator(filters.Equal).
				WithValueText(id1)
			_additional := graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
					{Name: fmt.Sprintf("vectors{%s}", strings.Join(targetVectors, " "))},
				},
			}
			ofBookshelfField := graphql.Field{
				Name: ofBookshelf,
				Fields: []graphql.Field{
					{
						Name:   "... on Bookshelf",
						Fields: []graphql.Field{_additional},
					},
				},
			}
			resp, err := client.GraphQL().Get().
				WithClassName(classBook).
				WithWhere(where).
				WithFields(ofBookshelfField).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.NotNil(t, resp.Data)
			require.Empty(t, resp.Errors)

			classMap, ok := resp.Data["Get"].(map[string]interface{})
			require.True(t, ok)

			class, ok := classMap[classBook].([]interface{})
			require.True(t, ok)

			targetVectorsMap := make(map[string][]float32)
			for i := range class {
				resultMap, ok := class[i].(map[string]interface{})
				require.True(t, ok)

				refProp, ok := resultMap[ofBookshelf].([]interface{})
				require.True(t, ok)
				require.Len(t, refProp, 1)

				aaa, ok := refProp[0].(map[string]interface{})
				require.True(t, ok)

				additional, ok := aaa["_additional"].(map[string]interface{})
				require.True(t, ok)

				vectors, ok := additional["vectors"].(map[string]interface{})
				require.True(t, ok)

				for _, targetVector := range targetVectors {
					vector, ok := vectors[targetVector].([]interface{})
					require.True(t, ok)

					vec := make([]float32, len(vector))
					for i := range vector {
						vec[i] = float32(vector[i].(float64))
					}

					targetVectorsMap[targetVector] = vec
				}
			}
			require.Len(t, targetVectorsMap, len(targetVectors))
			assert.NotEmpty(t, targetVectorsMap[c11y_bookshelf_name])
			assert.NotEmpty(t, targetVectorsMap[transformers_bookshelf_name])
		})
	}
}
