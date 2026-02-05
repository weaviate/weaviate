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

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	tailorincgraphql "github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/gqlerrors"

	libgraphql "github.com/weaviate/weaviate/adapters/handlers/graphql"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/graphql"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespace"
	"github.com/weaviate/weaviate/usecases/schema"
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
		// Extract namespace from request before authorization
		ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
		if err != nil {
			return graphql.NewGraphqlPostUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// All requests to the graphQL API need at least permissions to read the schema.
		// For namespace users, check against their namespace-scoped collections instead of all collections.
		var authResources []string
		if ns != namespace.DefaultNamespace {
			nsPrefix := strings.ToUpper(ns[:1]) + ns[1:] + namespace.NamespaceSeparator
			authResources = authorization.CollectionsMetadata(nsPrefix + "*")
		} else {
			authResources = authorization.CollectionsMetadata()
		}
		err = m.Authorizer.Authorize(params.HTTPRequest.Context(), principal, authorization.READ, authResources...)
		if err != nil {
			metricRequestsTotal.logUserError()
			switch {
			case errors.As(err, &authzerrors.Forbidden{}):
				return graphql.NewGraphqlPostForbidden().
					WithPayload(errPayloadFromSingleErr(
						fmt.Errorf("due to GraphQL introspection, this role must have the permission to `read_collections` on `*` (all) collections: %w", err),
					))
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

		// Block introspection queries for namespace users to prevent schema leakage
		if ns != namespace.DefaultNamespace && isIntrospectionQuery(query) {
			metricRequestsTotal.logUserError()
			return graphql.NewGraphqlPostForbidden().
				WithPayload(errPayloadFromSingleErr(
					fmt.Errorf("introspection queries are not available for namespace-scoped users"),
				))
		}

		// Prefix class names in query with namespace for non-default namespaces
		if ns != namespace.DefaultNamespace {
			query = prefixGraphQLClassNames(query, ns)
		}

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

		ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

		result := graphQL.Resolve(ctx, query,
			operationName, variables)

		// Strip namespace prefix from response data for non-default namespaces
		if ns != namespace.DefaultNamespace {
			stripNamespacePrefixFromGraphQLResult(result, ns)
		}

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
		// Extract namespace from request before authorization
		ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
		if err != nil {
			return graphql.NewGraphqlBatchUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}

		// For namespace users, check against their namespace-scoped collections
		var batchAuthResources []string
		if ns != namespace.DefaultNamespace {
			nsPrefix := strings.ToUpper(ns[:1]) + ns[1:] + namespace.NamespaceSeparator
			batchAuthResources = authorization.Collections(nsPrefix + "*")
		} else {
			batchAuthResources = authorization.Collections()
		}
		err = m.Authorizer.Authorize(params.HTTPRequest.Context(), principal, authorization.READ, batchAuthResources...)
		if err != nil {
			return graphql.NewGraphqlBatchForbidden().WithPayload(errPayloadFromSingleErr(err))
		}

		errorResponse := &models.ErrorResponse{}
		amountOfBatchedRequests := len(params.Body)
		if amountOfBatchedRequests == 0 {
			metricRequestsTotal.logUserError()
			return graphql.NewGraphqlBatchUnprocessableEntity().WithPayload(errorResponse)
		}

		// Block introspection and prefix class names for namespace users
		if ns != namespace.DefaultNamespace {
			for _, req := range params.Body {
				if req != nil && req.Query != "" {
					if isIntrospectionQuery(req.Query) {
						return graphql.NewGraphqlBatchForbidden().
							WithPayload(errPayloadFromSingleErr(
								fmt.Errorf("introspection queries are not available for namespace-scoped users"),
							))
					}
					req.Query = prefixGraphQLClassNames(req.Query, ns)
				}
			}
		}

		requestResults := make(chan gqlUnbatchedRequestResponse, amountOfBatchedRequests)

		wg := new(sync.WaitGroup)

		ctx := params.HTTPRequest.Context()
		ctx = context.WithValue(ctx, "principal", principal)

		graphQL := gqlProvider.GetGraphQL()
		if graphQL == nil {
			metricRequestsTotal.logUserError()
			errRes := errPayloadFromSingleErr(fmt.Errorf("no graphql provider present, " +
				"this is most likely because no schema is present. Import a schema first"))
			return graphql.NewGraphqlBatchUnprocessableEntity().WithPayload(errRes)
		}

		// Generate a goroutine for each separate request
		for requestIndex, unbatchedRequest := range params.Body {
			requestIndex, unbatchedRequest := requestIndex, unbatchedRequest
			wg.Add(1)
			enterrors.GoWrapper(func() {
				handleUnbatchedGraphQLRequest(ctx, wg, graphQL, unbatchedRequest, requestIndex, &requestResults, metricRequestsTotal)
			}, logger)
		}

		wg.Wait()

		close(requestResults)

		batchedRequestResponse := make([]*models.GraphQLResponse, amountOfBatchedRequests)

		// Add the requests to the result array in the correct order
		for unbatchedRequestResult := range requestResults {
			batchedRequestResponse[unbatchedRequestResult.RequestIndex] = unbatchedRequestResult.Response
		}

		// Strip namespace prefix from batch response data
		if ns != namespace.DefaultNamespace {
			for _, resp := range batchedRequestResponse {
				if resp != nil {
					stripNamespacePrefixFromGraphQLResponse(resp, ns)
				}
			}
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
		var gqlOriginalErr *gqlerrors.Error
		if errors.As(gqlError.OriginalError(), &gqlOriginalErr) {
			if gqlOriginalErr.OriginalError != nil {
				switch {
				case errors.As(gqlOriginalErr.OriginalError, &enterrors.ErrGraphQLUser{}):
					return e.getError(gqlOriginalErr.OriginalError)
				default:
					var gqlFormatted *gqlerrors.FormattedError
					if errors.As(gqlOriginalErr.OriginalError, &gqlFormatted) {
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
	var errGraphQLUser *enterrors.ErrGraphQLUser
	switch {
	case errors.As(err, &errGraphQLUser):
		return true, errGraphQLUser
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
				return className, queryType
			}
			if value != nil {
				if valueMap, ok := value.(map[string]interface{}); ok {
					for class := range valueMap {
						className = class
						return className, queryType
					}
				}
			}
		}
	}
	return className, queryType
}

// gqlClassNameAtDepthPattern matches a capitalized identifier followed by optional
// whitespace and either '(' (arguments) or '{' (selection set). This is used to
// detect class names inside Get/Aggregate blocks at the right nesting depth.
var gqlClassNameAtDepthPattern = regexp.MustCompile(`^([A-Z][A-Za-z0-9_]*)(\s*[\({])`)

// prefixGraphQLClassNames rewrites a GraphQL query string to prefix class names
// with the namespace. For example, with namespace "tenanta":
//
//	{ Get { Articles { title } } } → { Get { Tenanta__Articles { title } } }
//
// It tracks brace depth to only prefix class names that appear at depth 2
// (one level inside Get/Aggregate blocks). Names that already have the namespace
// prefix are left unchanged.
func prefixGraphQLClassNames(query string, ns string) string {
	prefix := strings.ToUpper(ns[:1]) + ns[1:] + namespace.NamespaceSeparator

	var result strings.Builder
	result.Grow(len(query) + 64)

	depth := 0
	inGetOrAggregate := false
	targetDepth := 0
	i := 0

	for i < len(query) {
		ch := query[i]

		switch {
		case ch == '{':
			result.WriteByte(ch)
			depth++
			i++

		case ch == '}':
			result.WriteByte(ch)
			if depth == targetDepth && inGetOrAggregate {
				inGetOrAggregate = false
			}
			depth--
			i++

		case ch >= 'A' && ch <= 'Z':
			// Check if this is a keyword (Get/Aggregate) or a class name
			rest := query[i:]
			if !inGetOrAggregate {
				// Look for "Get" or "Aggregate" keyword followed by whitespace/brace
				for _, kw := range []string{"Get", "Aggregate"} {
					if strings.HasPrefix(rest, kw) && i+len(kw) < len(query) {
						next := query[i+len(kw)]
						if next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == '{' {
							inGetOrAggregate = true
							targetDepth = depth + 1
							result.WriteString(kw)
							i += len(kw)
							break
						}
					}
				}
				if inGetOrAggregate {
					continue
				}
			}

			// If we're inside Get/Aggregate at the right depth, prefix the class name
			if inGetOrAggregate && depth == targetDepth {
				if m := gqlClassNameAtDepthPattern.FindStringSubmatch(rest); len(m) >= 2 {
					className := m[1]
					if !strings.HasPrefix(className, prefix) {
						result.WriteString(prefix)
					}
					result.WriteString(className)
					i += len(className)
					continue
				}
			}

			// Not a class name to prefix; write character as-is
			result.WriteByte(ch)
			i++

		default:
			result.WriteByte(ch)
			i++
		}
	}

	return result.String()
}

// stripNamespacePrefixFromGraphQLResult strips namespace prefixes from class name
// keys in the GraphQL result Data map. This is called on the *graphql.Result before
// it gets marshalled to JSON.
func stripNamespacePrefixFromGraphQLResult(result *tailorincgraphql.Result, ns string) {
	if result == nil || result.Data == nil {
		return
	}
	dataMap, ok := result.Data.(map[string]interface{})
	if !ok {
		return
	}
	prefix := strings.ToUpper(ns[:1]) + ns[1:] + namespace.NamespaceSeparator
	for queryType, value := range dataMap {
		if valueMap, ok := value.(map[string]interface{}); ok {
			stripped := make(map[string]interface{}, len(valueMap))
			for className, classData := range valueMap {
				cleanName := className
				if strings.HasPrefix(className, prefix) {
					cleanName = className[len(prefix):]
				}
				stripped[cleanName] = classData
			}
			dataMap[queryType] = stripped
		}
	}
}

// isIntrospectionQuery checks if a GraphQL query contains introspection fields
// (__schema or __type). Namespace users are blocked from introspection to prevent
// leaking collection names and schema details from other namespaces.
func isIntrospectionQuery(query string) bool {
	return strings.Contains(query, "__schema") || strings.Contains(query, "__type")
}

// stripNamespacePrefixFromGraphQLResponse strips namespace prefixes from class name
// keys in a models.GraphQLResponse.Data map. Used for batch responses.
func stripNamespacePrefixFromGraphQLResponse(resp *models.GraphQLResponse, ns string) {
	if resp == nil || resp.Data == nil {
		return
	}
	prefix := strings.ToUpper(ns[:1]) + ns[1:] + namespace.NamespaceSeparator
	for queryType, value := range resp.Data {
		if valueMap, ok := value.(map[string]interface{}); ok {
			stripped := make(map[string]interface{}, len(valueMap))
			for className, classData := range valueMap {
				cleanName := className
				if strings.HasPrefix(className, prefix) {
					cleanName = className[len(prefix):]
				}
				stripped[cleanName] = classData
			}
			resp.Data[queryType] = stripped
		}
	}
}
