package test

// Acceptance tests for actions

import (
	"testing"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
)

func TestCanCreateAction(t *testing.T) {
	t.Parallel()

	// Set all action values to compare
	actionTestString := "Test string"
	actionTestInt := 1
	actionTestBoolean := true
	actionTestNumber := 1.337
	actionTestDate := "2017-10-06T08:15:30+01:00"

	params := actions.NewWeaviateActionsCreateParams().WithBody(actions.WeaviateActionsCreateBody{
		Action: &models.ActionCreate{
			AtContext: "http://example.org",
			AtClass:   "TestAction",
			Schema: map[string]interface{}{
				"testString":   actionTestString,
				"testInt":      actionTestInt,
				"testBoolean":  actionTestBoolean,
				"testNumber":   actionTestNumber,
				"testDateTime": actionTestDate,
			},
		},
	})

	resp, _, err := helper.Client(t).Actions.WeaviateActionsCreate(params, helper.RootAuth)

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		action := resp.Payload
		assert.Regexp(t, strfmt.UUIDPattern, action.ActionID)

		schema, ok := action.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(schema["testInt"].(float64)))
		assert.Equal(t, actionTestBoolean, schema["testBoolean"])
		assert.Equal(t, actionTestNumber, schema["testNumber"])
		assert.Equal(t, actionTestDate, schema["testDateTime"])
	})
}

func TestCanCreateAndGetAction(t *testing.T) {
	t.Parallel()

	// Set all action values to compare
	actionTestString := "Test string"
	actionTestInt := 1
	actionTestBoolean := true
	actionTestNumber := 1.337
	actionTestDate := "2017-10-06T08:15:30+01:00"

	params := actions.NewWeaviateActionsCreateParams().WithBody(actions.WeaviateActionsCreateBody{
		Action: &models.ActionCreate{
			AtContext: "http://example.org",
			AtClass:   "TestAction",
			Schema: map[string]interface{}{
				"testString":   actionTestString,
				"testInt":      actionTestInt,
				"testBoolean":  actionTestBoolean,
				"testNumber":   actionTestNumber,
				"testDateTime": actionTestDate,
			},
		},
	})

	resp, _, err := helper.Client(t).Actions.WeaviateActionsCreate(params, helper.RootAuth)

	var actionId strfmt.UUID
	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		actionId = resp.Payload.ActionID
	})

	// Now fetch the action
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithActionID(actionId), helper.RootAuth)

	helper.AssertRequestOk(t, getResp, err, func() {
		action := getResp.Payload

		schema, ok := action.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(schema["testInt"].(float64)))
		assert.Equal(t, actionTestBoolean, schema["testBoolean"])
		assert.Equal(t, actionTestNumber, schema["testNumber"])
		assert.Equal(t, actionTestDate, schema["testDateTime"])
	})
}
