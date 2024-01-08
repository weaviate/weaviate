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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/schema"

	middleware "github.com/go-openapi/runtime/middleware"
	tailorincgraphql "github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/gqlerrors"
	libgraphql "github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/graphql"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

const error422 string = "The request is well-formed but was unable to be followed due to semantic errors."

type gqlUnbatchedRequestResponse struct {
	RequestIndex int
	Response     *models.GraphQLResponse
}

type graphQLProvider interface {
	GetGraphQL() libgraphql.GraphQL
}

func setupGraphQLHandlers(
	api *operations.WeaviateAPI,
	gqlProvider graphQLProvider,
	m *schema.Manager,
	disabled bool,
	metrics *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) {
	metricRequestsTotal := newGraphqlRequestsTotal(metrics, logger)
	api.GraphqlGraphqlPostHandler = graphql.GraphqlPostHandlerFunc(func(params graphql.GraphqlPostParams, principal *models.Principal) middleware.Responder {
		// All requests to the graphQL API need at least permissions to read the schema. Request might have further
		// authorization requirements.

		err := m.Authorizer.Authorize(principal, "list", "schema/*")
		if err != nil {
			metricRequestsTotal.logUserError()
			switch err.(type) {
			case errors.Forbidden:
				return graphql.NewGraphqlPostForbidden().
					WithPayload(errPayloadFromSingleErr(err))
			default:
				return graphql.NewGraphqlPostUnprocessableEntity().
					WithPayload(errPayloadFromSingleErr(err))
			}
		}

		if disabled {
			metricRequestsTotal.logUserError()
			err := fmt.Errorf("graphql api is disabled")
			return graphql.NewGraphqlPostUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		errorResponse := &models.ErrorResponse{}

		// Get all input from the body of the request, as it is a POST.
		query := params.Body.Query
		operationName := params.Body.OperationName

		// If query is empty, the request is unprocessable
		if query == "" {
			metricRequestsTotal.logUserError()
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				{
					Message: "query cannot be empty",
				},
			}
			return graphql.NewGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Only set variables if exists in request
		var variables map[string]interface{}
		if params.Body.Variables != nil {
			variables = params.Body.Variables.(map[string]interface{})
		}

		graphQL := gqlProvider.GetGraphQL()
		if graphQL == nil {
			metricRequestsTotal.logUserError()
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				{
					Message: "no graphql provider present, this is most likely because no schema is present. Import a schema first!",
				},
			}
			return graphql.NewGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		ctx := params.HTTPRequest.Context()
		ctx = context.WithValue(ctx, "principal", principal)

		result := graphQL.Resolve(ctx, query,
			operationName, variables)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)
		if jsonErr != nil {
			metricRequestsTotal.logUserError()
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				{
					Message: fmt.Sprintf("couldn't marshal json: %s", jsonErr),
				},
			}
			return graphql.NewGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		// Put the data in a response ready object
		graphQLResponse := &models.GraphQLResponse{}
		marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

		// If json gave error, return nothing.
		if marshallErr != nil {
			metricRequestsTotal.logUserError()
			errorResponse.Error = []*models.ErrorResponseErrorItems0{
				{
					Message: fmt.Sprintf("couldn't unmarshal json: %s\noriginal result was %#v", marshallErr, result),
				},
			}
			return graphql.NewGraphqlPostUnprocessableEntity().WithPayload(errorResponse)
		}

		metricRequestsTotal.log(result)
		// Return the response
		return graphql.NewGraphqlPostOK().WithPayload(graphQLResponse)
	})

	api.GraphqlGraphqlBatchHandler = graphql.GraphqlBatchHandlerFunc(func(params graphql.GraphqlBatchParams, principal *models.Principal) middleware.Responder {
		amountOfBatchedRequests := len(params.Body)
		errorResponse := &models.ErrorResponse{}

		if amountOfBatchedRequests == 0 {
			metricRequestsTotal.logUserError()
			return graphql.NewGraphqlBatchUnprocessableEntity().WithPayload(errorResponse)
		}
		requestResults := make(chan gqlUnbatchedRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		ctx := params.HTTPRequest.Context()
		ctx = context.WithValue(ctx, "principal", principal)

		graphQL := gqlProvider.GetGraphQL()
		if graphQL == nil {
			metricRequestsTotal.logUserError()
			errRes := errPayloadFromSingleErr(fmt.Errorf("no graphql provider present, " +
				"this is most likely because no schema is present. Import a schema first!"))
			return graphql.NewGraphqlBatchUnprocessableEntity().WithPayload(errRes)
		}

		// Generate a goroutine for each separate request
		for requestIndex, unbatchedRequest := range params.Body {
			wg.Add(1)
			go handleUnbatchedGraphQLRequest(ctx, wg, graphQL, unbatchedRequest, requestIndex, &requestResults, metricRequestsTotal)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.GraphQLResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for unbatchedRequestResult := range requestResults {
			batchedRequestResponse[unbatchedRequestResult.RequestIndex] = unbatchedRequestResult.Response
		}

		return graphql.NewGraphqlBatchOK().WithPayload(batchedRequestResponse)
	})
}

// Handle a single unbatched GraphQL request, return a tuple containing the index of the request in the batch and either the response or an error
func handleUnbatchedGraphQLRequest(ctx context.Context, wg *sync.WaitGroup, graphQL libgraphql.GraphQL, unbatchedRequest *models.GraphQLQuery, requestIndex int, requestResults *chan gqlUnbatchedRequestResponse, metricRequestsTotal *graphqlRequestsTotal) {
	defer wg.Done()

	// Get all input from the body of the request
	query := unbatchedRequest.Query
	operationName := unbatchedRequest.OperationName
	graphQLResponse := &models.GraphQLResponse{}

	// Return an unprocessable error if the query is empty
	if query == "" {
		metricRequestsTotal.logUserError()
		// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
		errorCode := strconv.Itoa(graphql.GraphqlBatchUnprocessableEntityCode)
		errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
		errors := []*models.GraphQLError{{Message: errorMessage}}
		graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
		*requestResults <- gqlUnbatchedRequestResponse{
			requestIndex,
			&graphQLResponse,
		}
	} else {
		// Extract any variables from the request
		var variables map[string]interface{}
		if unbatchedRequest.Variables != nil {
			var ok bool
			variables, ok = unbatchedRequest.Variables.(map[string]interface{})
			if !ok {
				errorCode := strconv.Itoa(graphql.GraphqlBatchUnprocessableEntityCode)
				errorMessage := fmt.Sprintf("%s: %s", errorCode, fmt.Sprintf("expected map[string]interface{}, received %v", unbatchedRequest.Variables))

				error := []*models.GraphQLError{{Message: errorMessage}}
				graphQLResponse := models.GraphQLResponse{Data: nil, Errors: error}
				*requestResults <- gqlUnbatchedRequestResponse{
					requestIndex,
					&graphQLResponse,
				}
				return
			}
		}

		result := graphQL.Resolve(ctx, query, operationName, variables)

		// Marshal the JSON
		resultJSON, jsonErr := json.Marshal(result)

		// Return an unprocessable error if marshalling the result to JSON failed
		if jsonErr != nil {
			metricRequestsTotal.logUserError()
			// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
			errorCode := strconv.Itoa(graphql.GraphqlBatchUnprocessableEntityCode)
			errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
			errors := []*models.GraphQLError{{Message: errorMessage}}
			graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
			*requestResults <- gqlUnbatchedRequestResponse{
				requestIndex,
				&graphQLResponse,
			}
		} else {
			// Put the result data in a response ready object
			marshallErr := json.Unmarshal(resultJSON, graphQLResponse)

			// Return an unprocessable error if unmarshalling the result to JSON failed
			if marshallErr != nil {
				metricRequestsTotal.logUserError()
				// Regular error messages are returned as an error code in the request header, but that doesn't work for batched requests
				errorCode := strconv.Itoa(graphql.GraphqlBatchUnprocessableEntityCode)
				errorMessage := fmt.Sprintf("%s: %s", errorCode, error422)
				errors := []*models.GraphQLError{{Message: errorMessage}}
				graphQLResponse := models.GraphQLResponse{Data: nil, Errors: errors}
				*requestResults <- gqlUnbatchedRequestResponse{
					requestIndex,
					&graphQLResponse,
				}
			} else {
				metricRequestsTotal.log(result)
				// Return the GraphQL response
				*requestResults <- gqlUnbatchedRequestResponse{
					requestIndex,
					graphQLResponse,
				}
			}
		}
	}
}

type graphqlRequestsTotal struct {
	metrics *requestsTotalMetric
	logger  logrus.FieldLogger
}

func newGraphqlRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) *graphqlRequestsTotal {
	return &graphqlRequestsTotal{newRequestsTotalMetric(metrics, "graphql"), logger}
}

func (e *graphqlRequestsTotal) getQueryType(path []interface{}) string {
	if len(path) > 0 {
		return fmt.Sprintf("%v", path[0])
	}
	return ""
}

func (e *graphqlRequestsTotal) getClassName(path []interface{}) string {
	if len(path) > 1 {
		return fmt.Sprintf("%v", path[1])
	}
	return ""
}

func (e *graphqlRequestsTotal) getErrGraphQLUser(gqlError gqlerrors.FormattedError) (bool, *enterrors.ErrGraphQLUser) {
	if gqlError.OriginalError() != nil {
		if gqlOriginalErr, ok := gqlError.OriginalError().(*gqlerrors.Error); ok {
			if gqlOriginalErr.OriginalError != nil {
				switch err := gqlOriginalErr.OriginalError.(type) {
				case enterrors.ErrGraphQLUser:
					return e.getError(err)
				default:
					if gqlFormatted, ok := gqlOriginalErr.OriginalError.(gqlerrors.FormattedError); ok {
						if gqlFormatted.OriginalError() != nil {
							return e.getError(gqlFormatted.OriginalError())
						}
					}
				}
			}
		}
	}
	return false, nil
}

func (e *graphqlRequestsTotal) isSyntaxRelatedError(gqlError gqlerrors.FormattedError) bool {
	for _, prefix := range []string{"Syntax Error ", "Cannot query field"} {
		if strings.HasPrefix(gqlError.Message, prefix) {
			return true
		}
	}
	return false
}

func (e *graphqlRequestsTotal) getError(err error) (bool, *enterrors.ErrGraphQLUser) {
	switch e := err.(type) {
	case enterrors.ErrGraphQLUser:
		return true, &e
	default:
		return false, nil
	}
}

func (e *graphqlRequestsTotal) log(result *tailorincgraphql.Result) {
	if len(result.Errors) > 0 {
		for _, gqlErr := range result.Errors {
			if isUserError, err := e.getErrGraphQLUser(gqlErr); isUserError {
				if e.metrics != nil {
					e.metrics.RequestsTotalInc(UserError, err.ClassName(), err.QueryType())
				}
			} else if e.isSyntaxRelatedError(gqlErr) {
				if e.metrics != nil {
					e.metrics.RequestsTotalInc(UserError, "", "")
				}
			} else {
				e.logServerError(gqlErr, e.getClassName(gqlErr.Path), e.getQueryType(gqlErr.Path))
			}
		}
	} else if result.Data != nil {
		e.logOk(result.Data)
	}
}

func (e *graphqlRequestsTotal) logServerError(err error, className, queryType string) {
	e.logger.WithFields(logrus.Fields{
		"action":     "requests_total",
		"api":        "graphql",
		"query_type": queryType,
		"class_name": className,
	}).WithError(err).Error("unexpected error")
	if e.metrics != nil {
		e.metrics.RequestsTotalInc(ServerError, className, queryType)
	}
}

func (e *graphqlRequestsTotal) logUserError() {
	if e.metrics != nil {
		e.metrics.RequestsTotalInc(UserError, "", "")
	}
}

func (e *graphqlRequestsTotal) logOk(data interface{}) {
	if e.metrics != nil {
		className, queryType := e.getClassNameAndQueryType(data)
		e.metrics.RequestsTotalInc(Ok, className, queryType)
	}
}

func (e *graphqlRequestsTotal) getClassNameAndQueryType(data interface{}) (className, queryType string) {
	dataMap, ok := data.(map[string]interface{})
	if ok {
		for query, value := range dataMap {
			queryType = query
			if queryType == "Explore" {
				// Explore queries are cross class queries, we won't get a className in this case
				// there's no sense in further value investigation
				return
			}
			if value != nil {
				if valueMap, ok := value.(map[string]interface{}); ok {
					for class := range valueMap {
						className = class
						return
					}
				}
			}
		}
	}
	return
}
