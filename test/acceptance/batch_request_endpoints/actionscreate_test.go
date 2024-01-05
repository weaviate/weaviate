//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// TODO: change this test to simulate a successful query response when the test dataset is implemented.

// Acceptance tests for the batch ObjectsCreate endpoint
package batch_request_endpoints

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// Test if batching is working correctly. Sends an OK batch containing two batched requests that refer to non-existing classes.
// The expected outcome is a 200 batch response containing two batched responses. These batched responses should both contain errors.
func TestBatchObjectsCreateResultsOrder(t *testing.T) {
	t.Parallel()

	classOneName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516542"
	classTwoName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516541"
	expectedResult := "class '%s' not present in schema"

	// generate objectcreate content
	object1 := &models.Object{
		Class: classOneName,
		Properties: map[string]interface{}{
			"testString": "Test string",
		},
	}
	object2 := &models.Object{
		Class: classTwoName,
		Properties: map[string]interface{}{
			"testWholeNumber": 1,
		},
	}

	testFields := "ALL"

	// generate request body
	params := batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
		Objects: []*models.Object{object1, object2},
		Fields:  []*string{&testFields},
	})

	// perform the request
	resp, err := helper.BatchClient(t).BatchObjectsCreate(params, nil)
	// ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		objectsCreateResponse := resp.Payload

		// check if the batch response contains two batched responses
		assert.Equal(t, 2, len(objectsCreateResponse))

		// check if the error message matches the expected outcome (and are therefore returned in the correct order)
		if len(objectsCreateResponse) == 2 {
			responseOne := objectsCreateResponse[0].Result.Errors.Error[0].Message
			responseTwo := objectsCreateResponse[1].Result.Errors.Error[0].Message

			fullExpectedOutcomeOne := fmt.Sprintf(expectedResult, classOneName)
			assert.Contains(t, responseOne, fullExpectedOutcomeOne)

			fullExpectedOutcomeTwo := fmt.Sprintf(expectedResult, classTwoName)
			assert.Contains(t, responseTwo, fullExpectedOutcomeTwo)
		}
	})
}
