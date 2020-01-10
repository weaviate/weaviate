//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_UnindexedProperty(t *testing.T) {
	className := "NoIndexTestClass"

	defer func() {
		delParams := schema.NewSchemaThingsDeleteParams().WithClassName(className)
		delResp, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
		helper.AssertRequestOk(t, delResp, err, nil)
	}()

	t.Run("creating a class with two string props", func(t *testing.T) {
		c := &models.Class{
			Class:              className,
			VectorizeClassName: true,
			Properties: []*models.Property{
				&models.Property{
					Name:     "name",
					DataType: []string{"string"},
				},
				&models.Property{
					Name:     "hiddenName",
					DataType: []string{"string"},
					Index:    referenceToFalse(),
				},
			},
		}

		params := schema.NewSchemaThingsCreateParams().WithThingClass(c)
		resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)

	})

	t.Run("creating an object", func(t *testing.T) {
		params := things.NewThingsCreateParams().WithBody(
			&models.Thing{
				Class: className,
				ID:    "f5ffb60f-4c13-4d07-a395-829b2396c7b9",
				Schema: map[string]interface{}{
					"name":       "elephant",
					"hiddenName": "zebra",
				},
			})
		resp, err := helper.Client(t).Things.ThingsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
	})

	assertGetThingEventually(t, "f5ffb60f-4c13-4d07-a395-829b2396c7b9")

	t.Run("searching for the indexed prop", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
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
		}
		`

		result := AssertGraphQL(t, helper.RootAuth, query)
		objects := result.Get("Get", "Things", className).AsSlice()

		spew.Dump(objects)

		expected := []interface{}{
			map[string]interface{}{"name": "elephant", "hiddenName": "zebra"},
		}

		assert.ElementsMatch(t, expected, objects)
	})

	t.Run("searching for the non-indexed prop", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
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

func assertGetThingEventually(t *testing.T, uuid strfmt.UUID) *models.Thing {
	var (
		resp *things.ThingsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Things.ThingsGet(things.NewThingsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var thing *models.Thing

	helper.AssertRequestOk(t, resp, err, func() {
		thing = resp.Payload
	})

	return thing
}
