/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/creativesoftwarefdn/weaviate/client/schema"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	gql "github.com/creativesoftwarefdn/weaviate/test/acceptance/graphql_resolvers"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
)

func TestAddAndRemoveThingClass(t *testing.T) {
	t.Parallel()

	randomThingClassName := "YellowCars"

	// Ensure that this name is not in the schema yet.
	t.Log("Asserting that this class does not exist yet")
	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)

	tc := &models.SemanticSchemaClass{
		Class: randomThingClassName,
	}

	t.Log("Creating class")
	params := schema.NewWeaviateSchemaThingsCreateParams().WithThingClass(tc)
	resp, err := helper.Client(t).Schema.WeaviateSchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("Asserting that this class is now created")
	assert.Contains(t, GetThingClassNames(t), randomThingClassName)

	// Now clean up this class.
	t.Log("Remove the class")
	delParams := schema.NewWeaviateSchemaThingsDeleteParams().WithClassName(randomThingClassName)
	delResp, err := helper.Client(t).Schema.WeaviateSchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the class does not exist anymore.
	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)
}

// This test prevents a regression on the fix for this bug:
// https://github.com/creativesoftwarefdn/weaviate/issues/831
func TestDeleteSingleProperties(t *testing.T) {
	t.Parallel()

	randomThingClassName := "RedShip"

	// Ensure that this name is not in the schema yet.
	t.Log("Asserting that this class does not exist yet")
	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)

	tc := &models.SemanticSchemaClass{
		Class: randomThingClassName,
		Properties: []*models.SemanticSchemaClassProperty{
			&models.SemanticSchemaClassProperty{
				DataType: []string{"string"},
				Name:     "name",
			},
			&models.SemanticSchemaClassProperty{
				DataType: []string{"string"},
				Name:     "description",
			},
		},
	}

	t.Log("Creating class")
	params := schema.NewWeaviateSchemaThingsCreateParams().WithThingClass(tc)
	resp, err := helper.Client(t).Schema.WeaviateSchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("Asserting that this class is now created")
	assert.Contains(t, GetThingClassNames(t), randomThingClassName)

	t.Log("adding an instance of this particular class that uses both properties")
	instanceParams := things.NewWeaviateThingsCreateParams().WithBody(
		&models.Thing{
			Class: randomThingClassName,
			Schema: map[string]interface{}{
				"name":        "my name",
				"description": "my description",
			},
		})
	instanceRes, err := helper.Client(t).Things.WeaviateThingsCreate(instanceParams, nil)
	assert.Nil(t, err, "adding a class instance should not error")

	t.Log("delete a single property of the class")
	deleteParams := schema.NewWeaviateSchemaThingsPropertiesDeleteParams().
		WithClassName(randomThingClassName).
		WithPropertyName("description")
	_, err = helper.Client(t).Schema.WeaviateSchemaThingsPropertiesDelete(deleteParams, nil)
	assert.Nil(t, err, "deleting the property should not error")

	t.Log("retrieve the class and make sure the property is gone")
	thing := assertGetThingEventually(t, instanceRes.Payload.ID)
	expectedSchema := map[string]interface{}{
		"name": "my name",
	}
	assert.Equal(t, expectedSchema, thing.Schema)

	t.Log("verifying that we can still retrieve the thing through graphQL")
	result := gql.AssertGraphQL(t, helper.RootAuth, "{ Local { Get { Things { RedShip { name } } } } }")
	ships := result.Get("Local", "Get", "Things", "RedShip").AsSlice()
	expectedShip := map[string]interface{}{
		"name": "my name",
	}
	assert.Contains(t, ships, expectedShip)

	t.Log("verifying other GQL/REST queries still work")
	gql.AssertGraphQL(t, helper.RootAuth, "{ Local { GetMeta { Things { RedShip { name { count } } } } } }")
	gql.AssertGraphQL(t, helper.RootAuth, `{ Local { Aggregate { Things { RedShip(groupBy: ["name"]) { name { count } } } } } }`)
	_, err = helper.Client(t).Things.WeaviateThingsList(things.NewWeaviateThingsListParams(), nil)
	assert.Nil(t, err, "listing things should not error")

	t.Log("verifying we could re-add the property with the same name")
	readdParams := schema.NewWeaviateSchemaThingsPropertiesAddParams().
		WithClassName(randomThingClassName).
		WithBody(&models.SemanticSchemaClassProperty{
			Name:     "description",
			DataType: []string{"string"},
		})

	_, err = helper.Client(t).Schema.WeaviateSchemaThingsPropertiesAdd(readdParams, nil)
	assert.Nil(t, err, "adding the previously deleted property again should not error")

	// Now clean up this class.
	t.Log("Remove the class")
	delParams := schema.NewWeaviateSchemaThingsDeleteParams().WithClassName(randomThingClassName)
	delResp, err := helper.Client(t).Schema.WeaviateSchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delResp, err, nil)

	// And verify that the class does not exist anymore.
	assert.NotContains(t, GetThingClassNames(t), randomThingClassName)
}

func assertGetThingEventually(t *testing.T, uuid strfmt.UUID) *models.Thing {
	var (
		resp *things.WeaviateThingsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithID(uuid), nil)
		return err == nil
	}

	helper.AssertEventuallyEqual(t, true, checkThunk)

	var thing *models.Thing

	helper.AssertRequestOk(t, resp, err, func() {
		thing = resp.Payload
	})

	return thing
}
