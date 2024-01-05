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

package batch_request_endpoints

// TODO: These tests add little value, they only test one specific error case,
// but don't test any happy path at all. This should probably be removed or
// fixed. However, they do at least assure that the order of return values matches
// the order of input values.

// Acceptance tests for the batch GraphQL endpoint

// There is a helper struct called GraphQLResult that helps to navigate through the output,
// a query generator and a few helper functions to access the GraphQL endpoint.
// See the end of this file for more details on how those work.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	graphql_client "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TODO: change this test to simulate a successful query response when the test dataset is implemented.

// Check if batch results are returned in the correct order by comparing result equality to predefined outcomes.
// This includes testing whether individual requests and the batch request are handled correctly
func gqlResultsOrder(t *testing.T) {
	queryOneName := "testQuery"
	queryTwoName := "testQuery2"
	expectedResult := "Syntax Error GraphQL request (1:1) Unexpected Name \"%s\"\n\n1: %s\n   ^\n"

	// perform the query
	gqlResponse, err := queryBatchEndpoint(t, nil)
	if err != nil {
		t.Fatalf("The returned schema is not an JSON object: %v", err)
	}
	// check if the batch response contains two batched responses
	assert.Equal(t, 2, len(gqlResponse))

	// check if the error message matches the expected outcome (and are therefore returned in the correct order)
	if len(gqlResponse) == 2 {
		responseOne := gqlResponse[0].Errors[0].Message
		responseTwo := gqlResponse[1].Errors[0].Message

		fullExpectedOutcomeOne := fmt.Sprintf(expectedResult, queryOneName, queryOneName)
		assert.Equal(t, fullExpectedOutcomeOne, responseOne)

		fullExpectedOutcomeTwo := fmt.Sprintf(expectedResult, queryTwoName, queryTwoName)
		assert.Equal(t, fullExpectedOutcomeTwo, responseTwo)
	}
}

func gqlMalformedRequest(t *testing.T) {
	vars := []int{1, 2, 3}
	expectedResult := "422: expected map[string]interface{}, received %v"

	// perform the query
	gqlResponse, err := queryBatchEndpoint(t, vars)
	if err != nil {
		t.Fatalf("The returned schema is not an JSON object: %v", err)
	}
	// check if the batch response contains two batched responses
	assert.Equal(t, 2, len(gqlResponse))

	fullExpectedOutcome := fmt.Sprintf(expectedResult, vars)
	assert.Equal(t, fullExpectedOutcome, gqlResponse[0].Errors[0].Message)
	assert.Equal(t, fullExpectedOutcome, gqlResponse[1].Errors[0].Message)
}

// Helper functions
// TODO: change this to a successful query when the test dataset is implemented. Make sure to implement a query returning 3 or more elements.
// Perform a batch GraphQL query
func queryBatchEndpoint(t *testing.T, vars interface{}) (models.GraphQLResponses, error) {
	query1 := &models.GraphQLQuery{OperationName: "testQuery", Query: "testQuery", Variables: vars}
	query2 := &models.GraphQLQuery{OperationName: "testQuery2", Query: "testQuery2", Variables: vars}

	queries := models.GraphQLQueries{query1, query2}

	params := graphql_client.NewGraphqlBatchParams().WithBody(queries)
	response, err := helper.Client(t).Graphql.GraphqlBatch(params, nil)
	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}
