package batch_request_endpoints

// Acceptance tests for the batch ActionsCreate endpoint

// There is a helper struct called ActionsCreateResult that helps to navigate through the output,
// a query generator and a few helper functions to access the ActionsCreate endpoint.
// See the end of this file for more details on how those work.

import (
	"fmt"
//	"reflect"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/client/operations"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
//	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
)

// Check if batch results are returned in the correct order by comparing result error messages to predefined outcomes.
func TestBatchActionsCreateResultsOrder(t *testing.T) {
	t.Parallel()
	
	classOneName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516541"
	classTwoName := "ItIsExtremelyUnlikelyThatThisClassActuallyExistsButJustToBeSureHereAreSomeRandomNumbers12987825624398509861298409782539802434516542"
	expectedResult := "no such class with name '%s' found in the schema. Check your schema files for which classes are available"
	
	// generate actioncreate content
	action1 := &models.ActionCreate{
		AtContext: "http://example.org",
		AtClass: classOneName,
		Schema: map[string]interface{}{
			"testString":   "Test string",
		},
	}
	action2 := &models.ActionCreate{
		AtContext: "http://example.org",
		AtClass: classTwoName,
		Schema: map[string]interface{}{
			"testInt": 1,
		},
	}

	testFields :=  "ALL"

	// generate request body
	params := operations.NewWeaviateBatchingActionsCreateParams().WithBody(operations.WeaviateBatchingActionsCreateBody{
	    Actions: []*models.ActionCreate{action1, action2},
	    Async: true,
	    Fields: []*string{&testFields},
	})

	// perform the request
	resp, _, err := helper.OperationsClient(t).WeaviateBatchingActionsCreate(params, helper.RootAuth) 

	// Ensure that the response is OK
	helper.AssertRequestOk(t, resp, err, func() {
		actionsCreateResponse := resp.Payload
		//assert.Regexp(t, strfmt.UUIDPattern, action.ActionID)

		schema, ok := actionsCreateResponse.Schema.([]map[string]interface{})
		if !ok {
			t.Fatal("The returned schema is not a JSON object")
		}
		
		// check if the batch response contains two batched responses
		assert.Equal(t, 2, len(schema))
		
		// check if the error message matches the expected outcome
		if len(schema) == 2 {
			responseOne := extractErrorMessage(t, schema[0])
			responseTwo := extractErrorMessage(t, schema[1])
			
			fullExpectedOutcomeOne := fmt.Sprintf(expectedResult, classOneName)
			assert.Equal(t, fullExpectedOutcomeOne, responseOne)
			
			fullExpectedOutcomeTwo := fmt.Sprintf(expectedResult, classTwoName)
			assert.Equal(t, fullExpectedOutcomeTwo, responseTwo)
		}
	})
}

// extractErrorMessage extracts the error message that should be nested in the JSON response
func extractErrorMessage(t *testing.T, response map[string]interface{}) string {
	responseResult, ok := response["result"].(map[string]interface{})
	if !ok {
		t.Fatal("The returned result is not a JSON object")
	}
	
	responseErrors, ok := responseResult["errors"].(map[string]interface{})
	if !ok {
		t.Fatal("The returned \"errors\" is not a JSON object")
	}
	
	responseError, ok := responseErrors["error"].([]map[string]interface{})
	if !ok {
		t.Fatal("The returned \"error\" is not a JSON object")
	}
	
	if len(responseError) != 1{
		t.Fatal("The returned \"error\" has no content")
	}
	
	responseFirstError := responseError[0]
	responseMessage, ok := responseFirstError["message"].(string)
	if !ok {
		t.Fatal("The returned \"message\" is not a JSON object")
	}
	
	return responseMessage
}
