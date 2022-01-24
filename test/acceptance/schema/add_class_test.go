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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

// this test prevents a regression on
// https://github.com/semi-technologies/weaviate/issues/981
func TestInvalidDataTypeInProperty(t *testing.T) {
	t.Parallel()
	className := "WrongPropertyClass"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	t.Run("trying to import empty string as data type", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				&models.Property{
					Name:     "someProperty",
					DataType: []string{""},
				},
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			parsed, ok := err.(*schema.SchemaObjectsCreateUnprocessableEntity)
			require.True(t, ok, "error should be unprocessable entity")
			assert.Equal(t, "property 'someProperty': invalid dataType: dataType cannot be an empty string",
				parsed.Payload.Error[0].Message)
		})
	})
}

func TestInvalidPropertyName(t *testing.T) {
	t.Parallel()
	className := "WrongPropertyClass"

	t.Run("asserting that this class does not exist yet", func(t *testing.T) {
		assert.NotContains(t, GetObjectClassNames(t), className)
	})

	t.Run("trying to create class with invalid property name", func(t *testing.T) {
		c := &models.Class{
			Class: className,
			Properties: []*models.Property{
				&models.Property{
					Name:     "some-property",
					DataType: []string{"string"},
				},
			},
		}

		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(c)
		resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
		helper.AssertRequestFail(t, resp, err, func() {
			parsed, ok := err.(*schema.SchemaObjectsCreateUnprocessableEntity)
			require.True(t, ok, "error should be unprocessable entity")
			assert.Equal(t, "'some-property' is not a valid property name. Property names in Weaviate "+
				"are restricted to valid GraphQL names, which must be “/[_A-Za-z][_0-9A-Za-z]*/”.",
				parsed.Payload.Error[0].Message)
		})
	})
}

func TestAddAndRemoveObjectClass(t *testing.T) {
	randomObjectClassName := "YellowCars"

	// Ensure that this name is not in the schema yet.
	t.Log("Asserting that this class does not exist yet")
	assert.NotContains(t, GetObjectClassNames(t), randomObjectClassName)

	tc := &models.Class{
		Class: randomObjectClassName,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	}

	t.Log("Creating class")
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(tc)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("Asserting that this class is now created")
	assert.Contains(t, GetObjectClassNames(t), randomObjectClassName)

	t.Run("pure http - without the auto-generated client", testGetSchemaWithoutClient)

	// Now clean up this class.
	t.Log("Remove the class")
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(randomObjectClassName)
	delResp, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the class does not exist anymore.
	assert.NotContains(t, GetObjectClassNames(t), randomObjectClassName)
}

// TODO: https://github.com/semi-technologies/weaviate/issues/973
// // This test prevents a regression on the fix for this bug:
// // https://github.com/semi-technologies/weaviate/issues/831
// func TestDeleteSingleProperties(t *testing.T) {
// 	t.Parallel()

// 	randomObjectClassName := "RedShip"

// 	// Ensure that this name is not in the schema yet.
// 	t.Log("Asserting that this class does not exist yet")
// 	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)

// 	tc := &models.Class{
// 		Class: randomThingClassName,
// 		Properties: []*models.Property{
// 			&models.Property{
// 				DataType: []string{"string"},
// 				Name:     "name",
// 			},
// 			&models.Property{
// 				DataType: []string{"string"},
// 				Name:     "description",
// 			},
// 		},
// 	}

// 	t.Log("Creating class")
// 	params := schema.NewSchemaThingsCreateParams().WithThingClass(tc)
// 	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
// 	helper.AssertRequestOk(t, resp, err, nil)

// 	t.Log("Asserting that this class is now created")
// 	assert.Contains(t, GetThingClassNames(t), randomThingClassName)

// 	t.Log("adding an instance of this particular class that uses both properties")
// 	instanceParams := things.NewThingsCreateParams().WithBody(
// 		&models.Thing{
// 			Class: randomThingClassName,
// 			Schema: map[string]interface{}{
// 				"name":        "my name",
// 				"description": "my description",
// 			},
// 		})
// 	instanceRes, err := helper.Client(t).Things.ThingsCreate(instanceParams, nil)
// 	assert.Nil(t, err, "adding a class instance should not error")

// 	t.Log("delete a single property of the class")
// 	deleteParams := schema.NewSchemaThingsPropertiesDeleteParams().
// 		WithClassName(randomThingClassName).
// 		WithPropertyName("description")
// 	_, err = helper.Client(t).Schema.SchemaThingsPropertiesDelete(deleteParams, nil)
// 	assert.Nil(t, err, "deleting the property should not error")

// 	t.Log("retrieve the class and make sure the property is gone")
// 	thing := assertGetThingEventually(t, instanceRes.Payload.ID)
// 	expectedSchema := map[string]interface{}{
// 		"name": "my name",
// 	}
// 	assert.Equal(t, expectedSchema, thing.Schema)

// 	t.Log("verifying that we can still retrieve the thing through graphQL")
// 	result := gql.AssertGraphQL(t, helper.RootAuth, "{  Get { Things { RedShip { name } } } }")
// 	ships := result.Get("Get", "Things", "RedShip").AsSlice()
// 	expectedShip := map[string]interface{}{
// 		"name": "my name",
// 	}
// 	assert.Contains(t, ships, expectedShip)

// 	t.Log("verifying other GQL/REST queries still work")
// 	gql.AssertGraphQL(t, helper.RootAuth, "{  Meta { Things { RedShip { name { count } } } } }")
// 	gql.AssertGraphQL(t, helper.RootAuth, `{  Aggregate { Things { RedShip(groupBy: ["name"]) { name { count } } } } }`)
// 	_, err = helper.Client(t).Things.ThingsList(things.NewThingsListParams(), nil)
// 	assert.Nil(t, err, "listing things should not error")

// 	t.Log("verifying we could re-add the property with the same name")
// 	readdParams := schema.NewSchemaThingsPropertiesAddParams().
// 		WithClassName(randomThingClassName).
// 		WithBody(&models.Property{
// 			Name:     "description",
// 			DataType: []string{"string"},
// 		})

// 	_, err = helper.Client(t).Schema.SchemaThingsPropertiesAdd(readdParams, nil)
// 	assert.Nil(t, err, "adding the previously deleted property again should not error")

// 	// Now clean up this class.
// 	t.Log("Remove the class")
// 	delParams := schema.NewSchemaThingsDeleteParams().WithClassName(randomThingClassName)
// 	delResp, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
// 	helper.AssertRequestOk(t, delResp, err, nil)

// 	// And verify that the class does not exist anymore.
// 	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)
// }
