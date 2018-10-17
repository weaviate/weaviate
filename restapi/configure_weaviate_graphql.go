/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

// Package restapi with all rest API functions.
package restapi

import (
	"context"
	"encoding/json"
	"time"

	middleware "github.com/go-openapi/runtime/middleware"
	gographql "github.com/graphql-go/graphql"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/graphql"
)

func configureAPIGraphql(api *operations.WeaviateAPI) {
	api.GraphqlWeaviateGraphqlPostHandler = graphql.WeaviateGraphqlPostHandlerFunc(func(params graphql.WeaviateGraphqlPostParams, principal interface{}) middleware.Responder {
		defer messaging.TimeTrack(time.Now())
		messaging.DebugMessage("Starting GraphQL resolving")

		errorResponse := &models.ErrorResponse{}

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		// Add security principal to context that we pass on to the GraphQL resolver.
		graphql_context := context.WithValue(params.HTTPRequest.Context(), "principal", (principal.(*models.KeyTokenGetResponse)))

		// Do the request
		result := gographql.Do(gographql.Params{
			Schema:         *graphQL.Schema(),
			RequestString:  query,
			OperationName:  operationName,
			VariableValues: variables,
			Context:        graphql_context,
		})

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// If json gave error, return nothing.
		if jsonErr != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if marshallErr != nil {
			return graphql.NewWeaviateGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Return the response
		return graphql.NewWeaviateGraphqlPostOK().WithPayload(graphQLResponse)
	})
}
