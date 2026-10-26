//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// TODO: change this test to simulate a successful query response when the test dataset is implemented.

// Acceptance tests for the batch ObjectsCreate endpoint
package batch_request_endpoints

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/test/helper"
)

// Test if batching is working correctly. Sends an OK batch containing two batched requests that refer to non-existing classes.
// Auto-schema creates both classes, so the expected outcome is a 200 batch response containing two successful
// batched responses, in the order the objects were sent.
func TestBatchObjectsCreateResultsOrder(t *testing.T) {
	t.Parallel()

	classOneName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516542"
	classTwoName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516541"

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

	defer deleteObjectClass(t, classOneName)
	defer deleteObjectClass(t, classTwoName)

	// perform the request
	resp, err := helper.BatchClient(t).BatchObjectsCreate(params, nil)
	// ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		objectsCreateResponse := resp.Payload

		// check if the batch response contains two batched responses
		require.Len(t, objectsCreateResponse, 2)

		// check that both objects were created and are returned in the correct order
		for i, className := range []string{classOneName, classTwoName} {
			require.NotNil(t, objectsCreateResponse[i].Result)
			assert.Nil(t, objectsCreateResponse[i].Result.Errors)
			require.NotNil(t, objectsCreateResponse[i].Result.Status)
			assert.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *objectsCreateResponse[i].Result.Status)
			assert.Equal(t, className, objectsCreateResponse[i].Class)
		}
	})

	// check that auto-schema created both classes with a "default" named vector
	for _, className := range []string{classOneName, classTwoName} {
		class := helper.GetClass(t, className)
		require.NotNil(t, class)
		assert.Empty(t, class.Vectorizer)
		require.Contains(t, class.VectorConfig, modelsext.DefaultNamedVectorName)
	}
}
