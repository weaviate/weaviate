/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package test

import (
	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/client/things"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
	"testing"
)

const fakeActionId strfmt.UUID = "11111111-1111-1111-1111-111111111111"

func assertCreateAction(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := actions.NewWeaviateActionsCreateParams().WithBody(actions.WeaviateActionsCreateBody{
		Action: &models.ActionCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    schema,
		},
		Async: false,
	})

	resp, _, err := helper.Client(t).Actions.WeaviateActionsCreate(params, helper.RootAuth)

	var actionID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		actionID = resp.Payload.ActionID
	})

	return actionID
}

func assertGetAction(t *testing.T, uuid strfmt.UUID) *models.ActionGetResponse {
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(uuid), helper.RootAuth)

	var action *models.ActionGetResponse

	helper.AssertRequestOk(t, getResp, err, func() {
		action = getResp.Payload
	})

	return action
}

func assertCreateThing(t *testing.T, className string, schema map[string]interface{}) strfmt.UUID {
	params := things.NewWeaviateThingsCreateParams().WithBody(things.WeaviateThingsCreateBody{
		Thing: &models.ThingCreate{
			AtContext: "http://example.org",
			AtClass:   className,
			Schema:    schema,
		},
		Async: false,
	})

	resp, _, err := helper.Client(t).Things.WeaviateThingsCreate(params, helper.RootAuth)

	var thingID strfmt.UUID

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		thingID = resp.Payload.ThingID
	})

	return thingID
}
