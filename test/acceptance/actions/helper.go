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

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/client/schema"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
)

const fakeActionId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

func assertCreateAction(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := actions.NewWeaviateActionsCreateParams().WithBody(actions.WeaviateActionsCreateBody{
		Action: &models.ActionCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    schema,
		},
	})

	resp, err := helper.Client(t).Actions.WeaviateActionsCreate(params, nil)

	var actionID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		actionID = resp.Payload.ActionID
	})

	return actionID
}

func assertGetAction(t *testing.T, uuid strfmt.UUID) *models.ActionGetResponse {
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(uuid), nil)

	var action *models.ActionGetResponse

	helper.AssertRequestOk(t, getResp, err, func() {
		action = getResp.Payload
	})

	return action
}

func assertGetActionEventually(t *testing.T, uuid strfmt.UUID) *models.ActionGetResponse {
	var (
		resp *actions.WeaviateActionsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(uuid), nil)
		return err == nil
	}

	helper.AssertEventuallyEqual(t, true, checkThunk)

	var action *models.ActionGetResponse

	helper.AssertRequestOk(t, resp, err, func() {
		action = resp.Payload
	})

	return action
}

func assertGetThingEventually(t *testing.T, uuid strfmt.UUID) *models.ThingGetResponse {
	var (
		resp *things.WeaviateThingsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Things.WeaviateThingsGet(things.NewWeaviateThingsGetParams().WithThingID(uuid), nil)
		return err == nil
	}

	helper.AssertEventuallyEqual(t, true, checkThunk)

	var thing *models.ThingGetResponse

	helper.AssertRequestOk(t, resp, err, func() {
		thing = resp.Payload
	})

	return thing
}

func assertCreateThing(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{
		Thing: &models.ThingCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    schema,
		},
	})

	resp, err := helper.Client(t).Things.WeaviateThingsCreate(params, nil)

	var thingID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thingID = resp.Payload.ThingID
	})

	return thingID
}

func assertGetSchema(t *testing.T) *schema.WeaviateSchemaDumpOKBody {
	getResp, err := helper.Client(t).Schema.WeaviateSchemaDump(schema.NewWeaviateSchemaDumpParams(), nil)
	var schema *schema.WeaviateSchemaDumpOKBody
	helper.AssertRequestOk(t, getResp, err, func() {
		schema = getResp.Payload
	})

	return schema
}

func assertClassInSchema(t *testing.T, schema *models.SemanticSchema, className string) *models.SemanticSchemaClass {
	for _, class := range schema.Classes {
		if class.Class == className {
			return class
		}
	}

	t.Fatalf("class %s not found in schema", className)
	return nil
}

func assertPropertyInClass(t *testing.T, class *models.SemanticSchemaClass, propertyName string) *models.SemanticSchemaClassProperty {
	for _, prop := range class.Properties {
		if prop.Name == propertyName {
			return prop
		}
	}

	t.Fatalf("property %s not found in class", propertyName)
	return nil
}
