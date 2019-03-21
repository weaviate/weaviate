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
	"net/http"
	"testing"
	"time"

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

	helper.Client(t).Actions.WeaviateActionsCreate(params, nil)
}

func retrieveLogFromMockEndpoint() interface{} {
	req, _ := http.NewRequest("GET", "127.0.0.1:8087/mock/count", nil)
	client := &http.Client{}
	resp, _ := client.Do(req)

	defer resp.Body.Close()

	return resp
}
