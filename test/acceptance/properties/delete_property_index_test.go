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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clobjects "github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func testDeletePropertyIndex() func(t *testing.T) {
	return func(t *testing.T) {
		bookClass := "Book"
		author := "author"
		title := "title"
		year := "year"

		ptrBool := func(in bool) *bool {
			return &in
		}

		deleteIndex := func(t *testing.T, propertyName string, indexSearchable, indexFilterable, indexRangeFilters *bool) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).WithPropertyName(propertyName).
				WithBody(&models.DeletePropertyIndexRequest{
					IndexSearchable:   indexSearchable,
					IndexFilterable:   indexFilterable,
					IndexRangeFilters: indexRangeFilters,
				})
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		}

		deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
		deleteClassResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		helper.AssertRequestOk(t, deleteClassResp, err, nil)

		book := &models.Class{
			Class: bookClass,
			Properties: []*models.Property{
				{
					Name:            author,
					DataType:        []string{schema.DataTypeText.String()},
					IndexFilterable: ptrBool(true),
					IndexSearchable: ptrBool(true),
				},
				{
					Name:          title,
					DataType:      []string{schema.DataTypeText.String()},
					IndexInverted: ptrBool(true),
				},
				{
					Name:              year,
					DataType:          []string{schema.DataTypeNumber.String()},
					IndexFilterable:   ptrBool(true),
					IndexRangeFilters: ptrBool(true),
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				title: {
					Vectorizer: map[string]any{
						"text2vec-model2vec": map[string]any{
							"vectorizeClassName": false,
							"properties":         []any{title},
						},
					},
					VectorIndexType: "hnsw",
				},
			},
		}
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(book)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

		objCreateParams := clobjects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000001"),
				Class: bookClass,
				Properties: map[string]any{
					"author": "Frank Herbert",
					"title":  "Dune",
					"year":   1960,
				},
			})

		objCreateResp, err := helper.Client(t).Objects.ObjectsCreate(objCreateParams, nil)
		helper.AssertRequestOk(t, objCreateResp, err, nil)

		objCreateParams = clobjects.NewObjectsCreateParams().WithBody(
			&models.Object{
				ID:    strfmt.UUID("00000000-0000-0000-0000-000000000002"),
				Class: bookClass,
				Properties: map[string]any{
					"author": "Jaroslaw Grzedowicz",
					"title":  "The Lord of the Ice Garden",
					"year":   2005,
				},
			})

		objCreateResp, err = helper.Client(t).Objects.ObjectsCreate(objCreateParams, nil)
		helper.AssertRequestOk(t, objCreateResp, err, nil)

		t.Run("delete author property searchable index", func(t *testing.T) {
			deleteIndex(t, author, ptrBool(true), nil, nil)
		})

		t.Run("delete title property filterable index", func(t *testing.T) {
			deleteIndex(t, title, nil, ptrBool(true), nil)
		})

		t.Run("delete year property rangeable index", func(t *testing.T) {
			deleteIndex(t, year, nil, nil, ptrBool(true))
		})

		// t.Run("cannot update author property and add rangeable index", func(t *testing.T) {
		// 	updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
		// 		WithClassName(bookClass).WithPropertyName(author).
		// 		WithBody(&models.DeletePropertyIndexRequest{IndexRangeFilters: ptrBool(true)})
		// 	updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
		// 	require.Error(t, err)
		// 	helper.AssertRequestFail(t, updateOk, err, func() {
		// 		var deleteErr *clschema.SchemaObjectsPropertiesDeleteUnprocessableEntity
		// 		assert.True(t, errors.As(err, &deleteErr))
		// 		require.NotNil(t, deleteErr)
		// 		require.NotNil(t, deleteErr.Payload)
		// 		require.NotEmpty(t, deleteErr.Payload.Error)
		// 		for i := range deleteErr.Payload.Error {
		// 			assert.Contains(t, deleteErr.Payload.Error[i].Message, "`indexRangeFilters` is allowed only for number/int/date data types.")
		// 		}
		// 	})
		// })
	}
}
