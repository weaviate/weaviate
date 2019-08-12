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

// TODO: change this test to simulate a successful query response when the test dataset is implemented.

// Acceptance tests for the batch ThingsCreate endpoint
package batch_request_endpoints

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/client/operations"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// Test if batching is working correctly. Sends an OK batch containing two batched requests that refer to non-existing classes.
// The expected outcome is a 200 batch response containing two batched responses. These batched responses should both contain errors.
func TestBatchThingsCreateResultsOrder(t *testing.T) {
	t.Parallel()

	classOneName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516542"
	classTwoName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516541"
	expectedResult := "class '%s' not present in schema"

	// generate actioncreate content
	thing1 := &models.Thing{
		Class: classOneName,
		Schema: map[string]interface{}{
			"testString": "Test string",
		},
	}
	thing2 := &models.Thing{
		Class: classTwoName,
		Schema: map[string]interface{}{
			"testWholeNumber": 1,
		},
	}

	testFields := "ALL"

	// generate request body
	params := operations.NewBatchingThingsCreateParams().WithBody(operations.BatchingThingsCreateBody{
		Things: []*models.Thing{thing1, thing2},
		Fields: []*string{&testFields},
	})

	// perform the request
	resp, err := helper.OperationsClient(t).BatchingThingsCreate(params, nil)

	// ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {

		thingsCreateResponse := resp.Payload

		// check if the batch response contains two batched responses
		assert.Equal(t, 2, len(thingsCreateResponse))

		// check if the error message matches the expected outcome (and are therefore returned in the correct order)
		if len(thingsCreateResponse) == 2 {
			responseOne := thingsCreateResponse[0].Result.Errors.Error[0].Message
			responseTwo := thingsCreateResponse[1].Result.Errors.Error[0].Message

			fullExpectedOutcomeOne := fmt.Sprintf(expectedResult, classOneName)
			assert.Equal(t, fullExpectedOutcomeOne, responseOne)

			fullExpectedOutcomeTwo := fmt.Sprintf(expectedResult, classTwoName)
			assert.Equal(t, fullExpectedOutcomeTwo, responseTwo)
		}

	})
}
