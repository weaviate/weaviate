//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UnindexedProperty(t *testing.T) {
	className := "NoIndexTestClass"

	defer func() {
		delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
		delResp, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
		helper.AssertRequestOk(t, delResp, err, nil)
	}()

	t.Run("creating a class with two string props", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"string"},
				},
				{
					Name:          "hiddenName",
					DataType:      []string{"string"},
					IndexInverted: referenceToFalse(),
				},
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
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
					valueString: "elephant"
					path:["name"]
				}){
					name
					hiddenName
				}
			}
		}
		`

		result := AssertGraphQL(t, helper.RootAuth, query)
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
					valueString: "zebra"
					path:["hiddenName"]
				}){
					name
					hiddenName
				}
			}
		}
		`
		res, err := QueryGraphQL(t, helper.RootAuth, "", query, nil)
		require.Nil(t, err)
		assert.True(t, len(res.Errors) > 0, "this query should be impossible as the field was not indexed")
	})
}

func referenceToFalse() *bool {
	b := false
	return &b
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
