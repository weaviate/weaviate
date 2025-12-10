//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package properties

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func testDeleteProperty() func(t *testing.T) {
	return func(t *testing.T) {
		bookClass := "Book"
		author := "author"
		title := "title"
		description := "description"
		genre := "genre"

		deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
		deleteClassResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		helper.AssertRequestOk(t, deleteClassResp, err, nil)

		book := &models.Class{
			Class: bookClass,
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
					Vectorizer: map[string]any{
						"text2vec-model2vec": map[string]any{
							"vectorizeClassName": false,
							"properties":         []any{description},
						},
					},
					VectorIndexType: "hnsw",
				},
			},
		}
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(book)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

		t.Run("delete author", func(t *testing.T) {
			checkAuthorPropertyExistsInSchema := func(exists bool, message string) {
				params := clschema.NewSchemaObjectsGetParams().WithClassName(bookClass)
				resp, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
				helper.AssertRequestOk(t, resp, err, nil)
				require.NotNil(t, resp.Payload)

				authorPropertyExists := false
				for _, prop := range resp.Payload.Properties {
					if prop.Name == author {
						authorPropertyExists = true
						break
					}
				}
				assert.Equal(t, exists, authorPropertyExists, message)
			}
			checkAuthorPropertyExistsInSchema(true, "author property should exist in schema")
			// delete author property
			deleteParams := clschema.NewSchemaObjectsPropertiesDeleteParams().WithClassName(bookClass).WithPropertyName(author)
			deleteOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(deleteParams, nil)
			helper.AssertRequestOk(t, deleteOk, err, nil)
			require.Equal(t, 200, deleteOk.Code())
			checkAuthorPropertyExistsInSchema(false, "author property should not exist in schema")
		})

		t.Run("cannot delete property used for vectorization", func(t *testing.T) {
			deleteParams := clschema.NewSchemaObjectsPropertiesDeleteParams().WithClassName(bookClass).WithPropertyName(description)
			deleteOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(deleteParams, nil)
			require.Error(t, err)
			helper.AssertRequestFail(t, deleteOk, err, func() {
				var deleteErr *clschema.SchemaObjectsPropertiesDeleteUnprocessableEntity
				assert.True(t, errors.As(err, &deleteErr))
				require.NotNil(t, deleteErr)
				require.NotNil(t, deleteErr.Payload)
				require.NotEmpty(t, deleteErr.Payload.Error)
				for i := range deleteErr.Payload.Error {
					assert.Contains(t, deleteErr.Payload.Error[i].Message, "vector index author is using property for vectorization")
				}
			})
		})
	}
}
