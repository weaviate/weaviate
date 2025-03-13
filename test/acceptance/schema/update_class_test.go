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

package test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestUpdateClassDescription(t *testing.T) {
	className := "C1"

	t.Run("delete class if exists", func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
	})

	t.Run("initially creating the class", func(t *testing.T) {
		c := &models.Class{
			Class: className,
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		assert.Nil(t, err)
	})

	newDescription := "it's updated description"

	t.Run("update class description", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, res.Payload.Description, "")

		class := res.Payload
		class.Description = newDescription
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(class)
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)
	})

	t.Run("assert update class description", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, res.Payload.Description, newDescription)
	})
}

func TestUpdatePropertyDescription(t *testing.T) {
	className := "C2"
	propName := "p1"
	nestedPropName := "np1"

	delete := func() {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
	}
	defer delete()

	delete()
	c := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     propName,
				DataType: []string{"object"},
				NestedProperties: []*models.NestedProperty{{
					Name:     nestedPropName,
					DataType: []string{"text"},
				}},
			},
		},
	}

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	assert.Nil(t, err)

	newDescription := "its updated description"

	t.Run("update property and nested property descriptions", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, "", res.Payload.Properties[0].Description)

		prop := res.Payload.Properties[0]
		prop.Description = newDescription
		prop.NestedProperties[0].Description = newDescription
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{prop},
			})
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.Nil(t, err)

		params = clschema.NewSchemaObjectsGetParams().WithClassName(className)
		res, err = helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, newDescription, res.Payload.Properties[0].Description)
		assert.Equal(t, newDescription, res.Payload.Properties[0].NestedProperties[0].Description)
	})

	t.Run("assert updated descriptions", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, newDescription, res.Payload.Properties[0].Description)
		assert.Equal(t, newDescription, res.Payload.Properties[0].NestedProperties[0].Description)
	})

	t.Run("update field other than description", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		prop := res.Payload.Properties[0]
		prop.DataType = []string{"int"}
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{prop},
			})
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
		var parsed *clschema.SchemaObjectsUpdateUnprocessableEntity
		require.ErrorAs(t, err, &parsed)
		if errors.As(err, &parsed) {
			require.Contains(t, parsed.Payload.Error[0].Message, "property fields other than description cannot be updated through updating the class")
		}
	})

	t.Run("update field other than description in nested", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)

		prop := res.Payload.Properties[0]
		prop.NestedProperties[0].DataType = []string{"int"}
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{prop},
			})
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		assert.NotNil(t, err)
		var parsed *clschema.SchemaObjectsUpdateUnprocessableEntity
		require.ErrorAs(t, err, &parsed)
		if errors.As(err, &parsed) {
			require.Contains(t, parsed.Payload.Error[0].Message, "property fields other than description cannot be updated through updating the class")
		}
	})
}
