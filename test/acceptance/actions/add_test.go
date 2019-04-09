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

// Acceptance tests for actions

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestCanCreateAction(t *testing.T) {
	t.Parallel()

	// Set all action values to compare
	actionTestString := "Test string"
	actionTestInt := 1
	actionTestBoolean := true
	actionTestNumber := 1.337
	actionTestDate := "2017-10-06T08:15:30+01:00"

	params := actions.NewWeaviateActionsCreateParams().WithBody(
		&models.Action{
			AtContext: "http://example.org",
			AtClass:   "TestAction",
			Schema: map[string]interface{}{
				"testString":   actionTestString,
				"testInt":      actionTestInt,
				"testBoolean":  actionTestBoolean,
				"testNumber":   actionTestNumber,
				"testDateTime": actionTestDate,
			},
		})

	resp, err := helper.Client(t).Actions.WeaviateActionsCreate(params, nil)

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		action := resp.Payload
		assert.Regexp(t, strfmt.UUIDPattern, action.ID)

		schema, ok := action.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		testInt, _ := schema["testInt"].(json.Number).Int64()
		testNumber, _ := schema["testNumber"].(json.Number).Float64()

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(testInt))
		assert.Equal(t, actionTestBoolean, schema["testBoolean"])
		assert.Equal(t, actionTestNumber, testNumber)
		assert.Equal(t, actionTestDate, schema["testDateTime"])
	})
}

func TestCanCreateAndGetAction(t *testing.T) {
	t.Parallel()

	actionTestString := "Test string"
	actionTestInt := 1
	actionTestBoolean := true
	actionTestNumber := 1.337
	actionTestDate := "2017-10-06T08:15:30+01:00"

	actionID := assertCreateAction(t, "TestAction", map[string]interface{}{
		"testString":   actionTestString,
		"testInt":      actionTestInt,
		"testBoolean":  actionTestBoolean,
		"testNumber":   actionTestNumber,
		"testDateTime": actionTestDate,
	})
	assertGetActionEventually(t, actionID)

	// Now fetch the action
	getResp, err := helper.Client(t).Actions.WeaviateActionsGet(actions.NewWeaviateActionsGetParams().WithID(actionID), nil)

	helper.AssertRequestOk(t, getResp, err, func() {
		action := getResp.Payload

		schema, ok := action.Schema.(map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not an JSON object")
		}

		testInt, _ := schema["testInt"].(json.Number).Int64()
		testNumber, _ := schema["testNumber"].(json.Number).Float64()

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(testInt))
		assert.Equal(t, actionTestBoolean, schema["testBoolean"])
		assert.Equal(t, actionTestNumber, testNumber)
		assert.Equal(t, actionTestDate, schema["testDateTime"])
	})
}

func TestCanAddSingleRefAction(t *testing.T) {
	fmt.Println("before first")
	firstID := assertCreateAction(t, "TestAction", map[string]interface{}{})
	assertGetActionEventually(t, firstID)

	secondID := assertCreateAction(t, "TestActionTwo", map[string]interface{}{
		"testString": "stringy",
		"testCref": map[string]interface{}{
			"$cref": fmt.Sprintf("weaviate://localhost/actions/%s", firstID),
		},
	})

	secondAction := assertGetActionEventually(t, secondID)

	singleRef := secondAction.Schema.(map[string]interface{})["testCref"].(map[string]interface{})
	assert.Equal(t, singleRef["$cref"].(string), fmt.Sprintf("weaviate://localhost/actions/%s", firstID))
}
