//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

const fakeActionId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

func assertCreateAction(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := actions.NewActionsCreateParams().WithBody(
		&models.Action{
			Class:  className,
			Schema: schema,
		})

	resp, err := helper.Client(t).Actions.ActionsCreate(params, nil)

	var actionID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		actionID = resp.Payload.ID
	})

	return actionID
}

func assertGetAction(t *testing.T, uuid strfmt.UUID) *models.Action {
	getResp, err := helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)

	var action *models.Action

	helper.AssertRequestOk(t, getResp, err, func() {
		action = getResp.Payload
	})

	return action
}

func assertGetActionEventually(t *testing.T, uuid strfmt.UUID) *models.Action {
	var (
		resp *actions.ActionsGetOK
		err  error
	)

	checkThunk := func() interface{} {
		resp, err = helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)
		return err == nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	var action *models.Action

	helper.AssertRequestOk(t, resp, err, func() {
		action = resp.Payload
	})

	return action
}

func assertGetActionFailsEventually(t *testing.T, uuid strfmt.UUID) error {
	var (
		err error
	)

	checkThunk := func() interface{} {
		_, err = helper.Client(t).Actions.ActionsGet(actions.NewActionsGetParams().WithID(uuid), nil)
		return err != nil
	}

	testhelper.AssertEventuallyEqual(t, true, checkThunk)

	return err
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

func assertCreateThing(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := things.NewThingsCreateParams().WithBody(&models.Thing{
		Class:  className,
		Schema: schema,
	})

	resp, err := helper.Client(t).Things.ThingsCreate(params, nil)

	var thingID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thingID = resp.Payload.ID
	})

	return thingID
}

func assertGetSchema(t *testing.T) *schema.SchemaDumpOKBody {
	getResp, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams(), nil)
	var schema *schema.SchemaDumpOKBody
	helper.AssertRequestOk(t, getResp, err, func() {
		schema = getResp.Payload
	})

	return schema
}

func assertClassInSchema(t *testing.T, schema *models.Schema, className string) *models.Class {
	for _, class := range schema.Classes {
		if class.Class == className {
			return class
		}
	}

	t.Fatalf("class %s not found in schema", className)
	return nil
}

func assertPropertyInClass(t *testing.T, class *models.Class, propertyName string) *models.Property {
	for _, prop := range class.Properties {
		if prop.Name == propertyName {
			return prop
		}
	}

	t.Fatalf("property %s not found in class", propertyName)
	return nil
}
