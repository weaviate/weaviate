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

// Acceptance tests for actions

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func TestCanUpdateActionSetNumber(t *testing.T) {
	t.Parallel()

	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, uuid)

	schema := models.PropertySchema(map[string]interface{}{
		"testNumber": 41.0,
	})

	update := models.Action{}
	update.Schema = schema
	update.Class = "TestAction"
	update.ID = uuid

	params := actions.NewActionsUpdateParams().WithID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.ActionsUpdate(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)

	actualThunk := func() interface{} {
		updatedAction := assertGetAction(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		if updatedSchema["testNumber"] == nil {
			return nil
		}
		num, _ := updatedSchema["testNumber"].(json.Number).Float64()
		return num
	}
	helper.AssertEventuallyEqual(t, 41.0, actualThunk)
}

func TestCanUpdateActionSetString(t *testing.T) {
	t.Parallel()

	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, uuid)

	schema := models.PropertySchema(map[string]interface{}{
		"testString": "wibbly wobbly",
	})

	update := models.Action{}
	update.Schema = schema
	update.Class = "TestAction"
	update.ID = uuid

	params := actions.NewActionsUpdateParams().WithID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.ActionsUpdate(params, nil)
	helper.AssertRequestOk(t, updateResp, err, nil)

	actualThunk := func() interface{} {
		updatedAction := assertGetAction(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		return updatedSchema["testString"]
	}
	helper.AssertEventuallyEqual(t, "wibbly wobbly", actualThunk)
}

func TestCanUpdateActionSetBool(t *testing.T) {
	t.Parallel()
	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, uuid)

	schema := models.PropertySchema(map[string]interface{}{
		"testTrueFalse": true,
	})

	update := models.Action{}
	update.Schema = schema
	update.Class = "TestAction"
	update.ID = uuid

	params := actions.NewActionsUpdateParams().WithID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.ActionsUpdate(params, nil)

	helper.AssertRequestOk(t, updateResp, err, nil)

	actualThunk := func() interface{} {
		updatedAction := assertGetAction(t, uuid)
		updatedSchema := updatedAction.Schema.(map[string]interface{})
		return updatedSchema["testTrueFalse"]
	}
	helper.AssertEventuallyEqual(t, true, actualThunk)
}

func TestCanPatchActionsSetCref(t *testing.T) {
	t.Parallel()

	thingToRefID := assertCreateThing(t, "TestThing", nil)
	assertGetThingEventually(t, thingToRefID)
	actionID := assertCreateAction(t, "TestAction", nil)
	assertGetActionEventually(t, actionID)

	op := "add"
	path := "/schema/testReference"

	patch := &models.PatchDocument{
		Op:   &op,
		Path: &path,
		Value: []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/things/%s", thingToRefID),
			},
		},
	}

	// Now to try to link
	params := actions.NewActionsPatchParams().
		WithBody([]*models.PatchDocument{patch}).
		WithID(actionID)
	patchResp, err := helper.Client(t).Actions.ActionsPatch(params, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	actualThunk := func() interface{} {
		patchedAction := assertGetAction(t, actionID)

		rawCref := patchedAction.Schema.(map[string]interface{})["testReference"]
		cref := rawCref.([]interface{})[0].(map[string]interface{})

		return cref["beacon"]
	}
	helper.AssertEventuallyEqual(t, fmt.Sprintf("weaviate://localhost/things/%s", thingToRefID), actualThunk)
}
