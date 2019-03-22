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

// Acceptance tests for logging. Sets up a small fake endpoint that logs are sent to.

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/client/actions"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestCreateActionLogging(t *testing.T) {
	t.Parallel()

	// send a request
	sendCreateActionRequest(t)

	// wait for the log to be posted
	time.Sleep(3 * time.Second)

	result := retrieveLogFromMockEndpoint()

	count, ok := result.(int)

	assert.Equal(t, true, ok)
	assert.Equal(t, 1, count)
}

func sendCreateActionRequest(t *testing.T) {
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

	resp, _, err := helper.Client(t).Actions.WeaviateActionsCreate(params, nil)

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

func retrieveLogFromMockEndpoint() interface{} {
	// placeholder := []byte{0}
	testURL, err := url.Parse("http://127.0.0.1:8087/mock/count")
	if err != nil {
		panic(err)
	}
	//req, _ := http.NewRequest("GET", url, bytes.NewReader(placeholder))
	client := &http.Client{}
	resp, err := client.Get(testURL.String()) //client.Do(req)
	if err == nil {
		body, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(body)
		fmt.Println(bodyString)
		defer resp.Body.Close()
	} else {
		panic(fmt.Sprintf("%s%s", err, testURL.String()))
	}

	return resp
}
