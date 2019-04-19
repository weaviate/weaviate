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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	libgraphql "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/operations/graphql"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/rest_api_utils"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/usecases/telemetry"
	middleware "github.com/go-openapi/runtime/middleware"
)

const error422 string = "The request is well-formed but was unable to be followed due to semantic errors."

type graphQLProvider interface {
	GetGraphQL() libgraphql.GraphQL
}

func setupGraphQLHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, gqlProvider graphQLProvider) {
	api.GraphqlWeaviateGraphqlPostHandler = graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams, principal *models.Principal) middleware.Responder {
		errorResponse := &models.ErrorResponse{}

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "query cannot be empty",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		graphQL := gqlProvider.GetGraphQL()
		if graphQL == nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: "no graphql provider present, " +
						"this is most likely because no schema is present. Import a schema first!",
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		result := graphQL.Resolve(query, operationName, variables, nil)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: fmt.Sprintf("couldn't marshal json: %s", jsonErr),
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if marshallErr != nil {
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				&models.ErrorResponseErrorItems0{
					Message: fmt.Sprintf("couldn't unmarshal json: %s\noriginal result was %#v", marshallErr, result),
				}}
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Register the request
		go func() {
			requestsLog.Register(telemetry.TypeGQL, telemetry.LocalAdd)
		}()

		// Return the response
		return graphql.NewWeaviateGraphqlPostOK().WithPayload(graphQLResponse)
	})

	api.GraphqlWeaviateGraphqlBatchHandler = graphql.WeaviateGraphqlBatchHandlerFunc(func(params graphql.WeaviateGraphqlBatchParams, principal *models.Principal) middleware.Responder {
		amountOfBatchedRequests := len(params.Body)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			return graphql.NewWeaviateGraphqlBatchUnprocessableEntity().WithPayload(errorResponse)
		}
		requestResults := make(chan rest_api_utils.UnbatchedRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		graphQL := gqlProvider.GetGraphQL()
		// Generate a goroutine for each separate request
		for requestIndex, unbatchedRequest := range params.Body {
			wg.Add(1)
			go handleUnbatchedGraphQLRequest(params.HTTPRequest.Context(), wg, graphQL, unbatchedRequest, requestIndex, &requestResults, requestsLog)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.GraphQLResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for unbatchedRequestResult := range requestResults {
			batchedRequestResponse[unbatchedRequestResult.RequestIndex] = unbatchedRequestResult.Response
		}

		return graphql.NewWeaviateGraphqlBatchOK().WithPayload(batchedRequestResponse)
	})
}

// Handle a single unbatched GraphQL request, return a tuple containing the index of the request in the batch and either the response or an error
func handleUnbatchedGraphQLRequest(ctx context.Context, wg *sync.WaitGroup, graphQL libgraphql.GraphQL, unbatchedRequest *models.GraphQLQuery, requestIndex int, requestResults *chan rest_api_utils.UnbatchedRequestResponse, requestsLog *telemetry.RequestsLog) {
	defer wg.Done()

	// Get all input from the body of the request
	query := unbatchedRequest.Query
	operationName := unbatchedRequest.OperationName
	graphQLResponse := &models.GraphQLResponse{}

	// Return an unprocessable error if the query is empty
	if query == "" {

		// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
		errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
		errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
		errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
		graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
		*requestResults <- rest_api_utils.UnbatchedRequestResponse{
			requestIndex,
			&graphQLResponse,
		}
	} else {

		// Extract any variables from the request
		var variables map[string]interface{}
		if unbatchedRequest.Variables != nil {
			variables = unbatchedRequest.Variables.(map[string]interface{})
		}

		if graphQL == nil {
			panic("graphql is nil!")
		}
		result := graphQL.Resolve(query, operationName, variables, ctx)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// Return an unprocessable error if marshalling the result to JSON failed
		if jsonErr != nil {

			// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
			errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
			errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
			errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
			graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
			*requestResults <- rest_api_utils.UnbatchedRequestResponse{
				requestIndex,
				&graphQLResponse,
			}
		} else {

			// Put the result data in a response ready object
			marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

			// Return an unprocessable error if unmarshalling the result to JSON failed
			if marshallErr != nil {

				// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
				errorCode := strconv.Itoa(graphql.WeaviateGraphqlBatchUnprocessableEntityCode)
				errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
				errors := []*models.GraphQLError{&models.GraphQLError{Message: errorMessage}}
				graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					&graphQLResponse,
				}
			} else {
				// Return the GraphQL response
				*requestResults <- rest_api_utils.UnbatchedRequestResponse{
					requestIndex,
					graphQLResponse,
				}
			}
		}
	}
}
