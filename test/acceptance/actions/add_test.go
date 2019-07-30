//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
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

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/actions"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
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
			Class: "TestAction",
			Schema: map[string]interface{}{
				"testString":      actionTestString,
				"testWholeNumber": actionTestInt,
				"testTrueFalse":   actionTestBoolean,
				"testNumber":      actionTestNumber,
				"testDateTime":    actionTestDate,
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

		testWholeNumber, _ := schema["testWholeNumber"].(json.Number).Int64()
		testNumber, _ := schema["testNumber"].(json.Number).Float64()

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(testWholeNumber))
		assert.Equal(t, actionTestBoolean, schema["testTrueFalse"])
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
		"testString":      actionTestString,
		"testWholeNumber": actionTestInt,
		"testTrueFalse":   actionTestBoolean,
		"testNumber":      actionTestNumber,
		"testDateTime":    actionTestDate,
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

		testWholeNumber, _ := schema["testWholeNumber"].(json.Number).Int64()
		testNumber, _ := schema["testNumber"].(json.Number).Float64()

		// Check whether the returned information is the same as the data added
		assert.Equal(t, actionTestString, schema["testString"])
		assert.Equal(t, actionTestInt, int(testWholeNumber))
		assert.Equal(t, actionTestBoolean, schema["testTrueFalse"])
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
		"testReference": map[string]interface{}{
			"$cref": fmt.Sprintf("weaviate://localhost/actions/%s", firstID),
		},
	})

	secondAction := assertGetActionEventually(t, secondID)

	singleRef := secondAction.Schema.(map[string]interface{})["testReference"].(map[string]interface{})
	assert.Equal(t, singleRef["$cref"].(string), fmt.Sprintf("weaviate://localhost/actions/%s", firstID))
}
