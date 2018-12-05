package batch_request_endpoints

// Acceptance tests for the batch GraphQL endpoint

// There is a helper struct called GraphQLResult that helps to navigate through the output,
// a query generator and a few helper functions to access the GraphQL endpoint.
// See the end of this file for more details on how those work.

import (
	"fmt"
	"reflect"
	"testing"

	graphql_client "github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
)

// Check if batch results are returned in the correct order by comparing result equality to predefined outcomes.
// This includes testing whether individual requests and the batch request are handled correctly
func TestBatchGQLRequestResultsReturnedInCorrectOrder(t *testing.T) {
	t.Parallel()

	// get the actual and expected results
	query := genQuery()
	expectedBatchResult := genExpectedOutcome()
	response := queryBatchGraphqlAssertOK(t, helper.RootAuth, query)

	// check if the batch result is the correct type
	assert.Equal(t, "[]interface{}", reflect.TypeOf(response.Result).Name(), fmt.Sprintf("Batch GQL result is not of type []interface{}, received %s instead", reflect.TypeOf(response.Result)))

	// check if the batched results are correct type
	if batchResult, ok := response.Result.([]interface{}); ok {
		for index, batchedResult := range batchResult {
			assert.Equal(t, "map[string]interface{}", reflect.TypeOf(batchedResult).Name(), fmt.Sprintf("Batched GQL result with index %s is not of type map[string]interface{}, received %s instead", index, reflect.TypeOf(batchedResult)))

			// check if the batched result matches the expected result
			if actualBatchedResult, ok := batchedResult.(map[string]interface{}); ok {
				expectedBatchedResult := expectedBatchResult[index]
				assert.Equal(t, expectedBatchedResult, actualBatchedResult, fmt.Sprintf("Batched GQL result with index %s does not match the expected outcome. \nExpected: \n%s \n Received:\n%s", index, expectedBatchedResult, actualBatchedResult))
			}
		}
	}
}

// TODO: add a query when the test dataset is implemented. Make sure to implement a query returning 3 or more elements.
func genQuery() string {
	return `[{}]`
}

// TODO: add an expected outcome when the test dataset is implemented. Ensure these are provided in the same order as the requests in genQuery().
func genExpectedOutcome() []map[string]interface{} {
	return nil

}

// Helper functions

type BatchGraphQLResult struct {
	Result interface{}
}

// Perform a batch GraphQL query
func queryBatchEndpoint(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponses, error) {
	var vars interface{} = variables
	params := graphql_client.NewWeaviateGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.WeaviateGraphqlPost(params, auth)

	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a query and assert that it is successful
func queryBatchGraphqlAssertOK(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) *BatchGraphQLResult {
	response, err := queryBatchEndpoint(t, auth, "", query, nil)
	if err != nil {
		t.Fatalf("Expected the query to succeed, but failed due to: %#v", err)
	}

	if responseData, ok := response.Data.([]interface{}); ok {
		data := make([]interface{}, len(responseData))

		// get rid of models.JSONData
		for key, value := range response.Data {
			data[key] = value
		}

		return &BatchGraphQLResult{Result: data}
	}
	return nil
}
