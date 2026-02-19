//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package properties

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

func testDeletePropertyIndexEmpty() func(t *testing.T) {
	return func(t *testing.T) {
		bookClass := "BookEmpty"
		title := "title"
		size := "size"

		ptrBool := func(in bool) *bool {
			return &in
		}

		deleteIndex := func(t *testing.T, propertyName, indexName string) {
			updateParams := clschema.NewSchemaObjectsPropertiesDeleteParams().
				WithClassName(bookClass).WithPropertyName(propertyName).WithIndexName(indexName)
			updateOk, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(updateParams, nil)
			helper.AssertRequestOk(t, updateOk, err, nil)
			require.Equal(t, 200, updateOk.Code())
		}

		getProperty := func(t *testing.T, propertyName string) *models.Property {
			cls := helper.GetClass(t, bookClass)
			for _, prop := range cls.Properties {
				if prop.Name == propertyName {
					return prop
				}
			}
			t.Fatalf("property %s not found", propertyName)
			return nil
		}

		// cleanup before and after
		deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
		deleteClassResp, err := helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		helper.AssertRequestOk(t, deleteClassResp, err, nil)
		t.Cleanup(func() {
			deleteClassParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(bookClass)
			helper.Client(t).Schema.SchemaObjectsDelete(deleteClassParams, nil)
		})

		book := &models.Class{
			Class: bookClass,
			Properties: []*models.Property{
				{
					Name:            title,
					DataType:        []string{schema.DataTypeText.String()},
					IndexFilterable: ptrBool(true),
					IndexSearchable: ptrBool(true),
				},
				{
					Name:              size,
					DataType:          []string{schema.DataTypeNumber.String()},
					IndexFilterable:   ptrBool(true),
					IndexRangeFilters: ptrBool(true),
				},
			},
		}
		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(book)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

		t.Run("verify initial schema", func(t *testing.T) {
			titleProp := getProperty(t, title)
			assert.True(t, *titleProp.IndexFilterable)
			assert.True(t, *titleProp.IndexSearchable)

			sizeProp := getProperty(t, size)
			assert.True(t, *sizeProp.IndexFilterable)
			assert.True(t, *sizeProp.IndexRangeFilters)
		})

		t.Run("delete title filterable index", func(t *testing.T) {
			deleteIndex(t, title, "filterable")

			titleProp := getProperty(t, title)
			assert.False(t, *titleProp.IndexFilterable)
			assert.True(t, *titleProp.IndexSearchable)
		})

		t.Run("delete title searchable index", func(t *testing.T) {
			deleteIndex(t, title, "searchable")

			titleProp := getProperty(t, title)
			assert.False(t, *titleProp.IndexFilterable)
			assert.False(t, *titleProp.IndexSearchable)
		})

		t.Run("delete size filterable index", func(t *testing.T) {
			deleteIndex(t, size, "filterable")

			sizeProp := getProperty(t, size)
			assert.False(t, *sizeProp.IndexFilterable)
			assert.True(t, *sizeProp.IndexRangeFilters)
		})

		t.Run("delete size rangeFilters index", func(t *testing.T) {
			deleteIndex(t, size, "rangeFilters")

			sizeProp := getProperty(t, size)
			assert.False(t, *sizeProp.IndexFilterable)
			assert.False(t, *sizeProp.IndexRangeFilters)
		})
	}
}
