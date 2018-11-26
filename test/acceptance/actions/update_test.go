package test

// Acceptance tests for actions

import (
	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCanUpdateActionSetNumber(t *testing.T) {
	t.Parallel()

	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})

	schema := models.Schema(map[string]interface{}{
		"testNumber": 41.0,
	})

	update := models.ActionUpdate{}
	update.Schema = schema
	update.AtClass = "TestAction"
	update.AtContext = "blurgh"

	params := actions.NewWeaviateActionUpdateParams().WithActionID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.WeaviateActionUpdate(params, helper.RootAuth)

	helper.AssertRequestOk(t, updateResp, err, nil)

	updatedAction := assertGetAction(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.Equal(t, updatedSchema["testNumber"], 41.0)
}

func TestCanUpdateActionSetString(t *testing.T) {
	t.Parallel()

	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})

	schema := models.Schema(map[string]interface{}{
		"testString": "wibbly wobbly",
	})

	update := models.ActionUpdate{}
	update.Schema = schema
	update.AtClass = "TestAction"
	update.AtContext = "blurgh"

	params := actions.NewWeaviateActionUpdateParams().WithActionID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.WeaviateActionUpdate(params, helper.RootAuth)

	helper.AssertRequestOk(t, updateResp, err, nil)

	updatedAction := assertGetAction(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.Equal(t, updatedSchema["testString"], "wibbly wobbly")
}

func TestCanUpdateActionSetBool(t *testing.T) {
	t.Parallel()
	uuid := assertCreateAction(t, "TestAction", map[string]interface{}{})

	schema := models.Schema(map[string]interface{}{
		"testBoolean": true,
	})

	update := models.ActionUpdate{}
	update.Schema = schema
	update.AtClass = "TestAction"
	update.AtContext = "blurgh"

	params := actions.NewWeaviateActionUpdateParams().WithActionID(uuid).WithBody(&update)
	updateResp, err := helper.Client(t).Actions.WeaviateActionUpdate(params, helper.RootAuth)

	helper.AssertRequestOk(t, updateResp, err, nil)

	updatedAction := assertGetAction(t, uuid)
	updatedSchema := updatedAction.Schema.(map[string]interface{})
	assert.Equal(t, updatedSchema["testBoolean"], true)
}
