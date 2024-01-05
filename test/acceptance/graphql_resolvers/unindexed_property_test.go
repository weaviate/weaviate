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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func Test_UnindexedProperty(t *testing.T) {
	className := "NoIndexTestClass"

	defer func() {
		delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		delResp, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
		helper.AssertRequestOk(t, delResp, err, nil)
	}()

	t.Run("creating a class with two string props", func(t *testing.T) {
		vFalse := false
		vTrue := true

		c := &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:            "name",
					DataType:        schema.DataTypeText.PropString(),
					Tokenization:    models.PropertyTokenizationWhitespace,
					IndexFilterable: &vTrue,
					IndexSearchable: &vTrue,
				},
				{
					Name:            "hiddenName",
					DataType:        schema.DataTypeText.PropString(),
					Tokenization:    models.PropertyTokenizationWhitespace,
					IndexFilterable: &vFalse,
					IndexSearchable: &vFalse,
				},
			},
		}

		params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	})

	t.Run("creating an object", func(t *testing.T) {
		params := objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				Class: className,
				ID:    "f5ffb60f-4c13-4d07-a395-829b2396c7b9",
				Properties: map[string]interface{}{
					"name":       "elephant",
					"hiddenName": "zebra",
				},
			})
		resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	})

	assertGetObjectEventually(t, "f5ffb60f-4c13-4d07-a395-829b2396c7b9")

	t.Run("searching for the indexed prop", func(t *testing.T) {
		query := `
		{
			Get {
				NoIndexTestClass(where:{
					operator: Equal,
					valueText: "elephant"
					path:["name"]
				}){
					name
					hiddenName
				}
			}
		}
		`

		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		objects := result.Get("Get", className).AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "elephant", "hiddenName": "zebra"},
		}

		assert.ElementsMatch(t, expected, objects)
	})

	t.Run("searching for the non-indexed prop", func(t *testing.T) {
		query := `
		{
			Get {
				NoIndexTestClass(where:{
					operator: Equal,
					valueText: "zebra"
					path:["hiddenName"]
				}){
					name
					hiddenName
				}
			}
		}
		`
		res, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
		require.Nil(t, err)
		assert.True(t, len(res.Errors) > 0, "this query should be impossible as the field was not indexed")
	})
}

func assertGetObjectEventually(t *testing.T, uuid strfmt.UUID) *models.Object {
	var (
		resp *objects.ObjectsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var object *models.Object

	helper.AssertRequestOk(t, resp, err, func() {
		object = resp.Payload
	})

	return object
}
