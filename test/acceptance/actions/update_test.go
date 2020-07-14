//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

// Acceptance tests for actions

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

// run from setup_test.go
func updateActions(t *testing.T) {
	t.Run("update and set number", func(t *testing.T) {
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
		testhelper.AssertEventuallyEqual(t, 41.0, actualThunk)
	})

	t.Run("update and set string", func(t *testing.T) {
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
		testhelper.AssertEventuallyEqual(t, "wibbly wobbly", actualThunk)
	})

	t.Run("update and set bool", func(t *testing.T) {
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
		testhelper.AssertEventuallyEqual(t, true, actualThunk)
	})

	t.Run("can patch action with cref", func(t *testing.T) {
		thingToRefID := assertCreateThing(t, "ActionTestThing", nil)
		assertGetThingEventually(t, thingToRefID)
		actionID := assertCreateAction(t, "TestAction", nil)
		assertGetActionEventually(t, actionID)

		merge := &models.Action{
			Class: "TestAction",
			Schema: map[string]interface{}{
				"testReference": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", thingToRefID),
					},
				},
			},
		}

		// Now to try to link
		params := actions.NewActionsPatchParams().
			WithBody(merge).
			WithID(actionID)
		patchResp, err := helper.Client(t).Actions.ActionsPatch(params, nil)
		spew.Dump(err)
		helper.AssertRequestOk(t, patchResp, err, nil)

		actualThunk := func() interface{} {
			patchedAction := assertGetAction(t, actionID)

			rawRef, ok := patchedAction.Schema.(map[string]interface{})["testReference"]
			if !ok {
				return nil
			}

			refsSlice, ok := rawRef.([]interface{})
			if !ok {
				t.Logf("found the ref prop, but it was not a slice, but %T", refsSlice)
				t.Fail()
			}

			if len(refsSlice) != 1 {
				t.Logf("expected ref slice to have one element, but got: %d", len(refsSlice))
				t.Fail()
			}

			refMap, ok := refsSlice[0].(map[string]interface{})
			if !ok {
				t.Logf("found the ref element, but it was not a map, but %T", refsSlice[0])
				t.Fail()
			}

			return refMap["beacon"]
		}

		testhelper.AssertEventuallyEqual(t, fmt.Sprintf("weaviate://localhost/things/%s", thingToRefID), actualThunk)
	})
}
