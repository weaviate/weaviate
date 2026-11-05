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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tailorincgraphql "github.com/tailor-platform/graphql"
	"github.com/tailor-platform/graphql/gqlerrors"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/graphql"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	runtimeconfig "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/schema"
)

// executedResult runs query through the real graphql executor so the error
// nesting is whatever the library produces today.
func executedResult(t *testing.T, returned error) *tailorincgraphql.Result {
	t.Helper()
	schema, err := tailorincgraphql.NewSchema(tailorincgraphql.SchemaConfig{
		Query: tailorincgraphql.NewObject(tailorincgraphql.ObjectConfig{
			Name: "Query",
			Fields: tailorincgraphql.Fields{
				"Get": &tailorincgraphql.Field{
					Type: tailorincgraphql.String,
					Resolve: func(tailorincgraphql.ResolveParams) (interface{}, error) {
						return nil, returned
					},
				},
			},
		}),
	})
	require.NoError(t, err)
	result := tailorincgraphql.Do(tailorincgraphql.Params{Schema: schema, RequestString: "{ Get }"})
	require.Len(t, result.Errors, 1)
	return result
}

func TestAttachErrorCodes(t *testing.T) {
	t.Run("Get ingress rate limit is 429 and keeps the message", func(t *testing.T) {
		result := executedResult(t, enterrors.NewErrGraphQLUser(enterrors.NewErrRateLimit(), "Get", "SomeAction"))
		response := responseFromResult(t, result)
		attachErrorCodes(result, response)

		require.Len(t, response.Errors, 1)
		assert.Equal(t, int64(http.StatusTooManyRequests), response.Errors[0].Code)
		assert.Contains(t, response.Errors[0].Message, "429 Too many requests")

		// metrics still see a user error. The executor nests the resolver error,
		// and NewErrGraphQLUser returns a value, not a pointer.
		counter := &graphqlRequestsTotal{}
		ok, userErr := counter.getErrGraphQLUser(result.Errors[0])
		require.True(t, ok)
		require.NotNil(t, userErr)
		assert.Equal(t, "Get", userErr.QueryType())
		assert.Equal(t, "SomeAction", userErr.ClassName())
	})

	t.Run("bare rate limit, as Aggregate and Explore return it, is 429", func(t *testing.T) {
		result := executedResult(t, enterrors.NewErrRateLimit())
		response := responseFromResult(t, result)
		attachErrorCodes(result, response)

		require.Len(t, response.Errors, 1)
		assert.Equal(t, int64(http.StatusTooManyRequests), response.Errors[0].Code)
		assert.Contains(t, response.Errors[0].Message, "429 Too many requests")
	})

	t.Run("plain resolver error omits the code", func(t *testing.T) {
		result := executedResult(t, enterrors.NewErrGraphQLUser(
			assert.AnError, "Get", "SomeAction"))
		response := responseFromResult(t, result)
		attachErrorCodes(result, response)

		require.Len(t, response.Errors, 1)
		assert.Zero(t, response.Errors[0].Code)
		encoded, err := json.Marshal(response.Errors[0])
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), `"code"`)
	})

	t.Run("nil inputs do not panic", func(t *testing.T) {
		assert.NotPanics(t, func() { attachErrorCodes(nil, nil) })
		assert.NotPanics(t, func() { attachErrorCodes(&tailorincgraphql.Result{}, nil) })
		assert.NotPanics(t, func() {
			attachErrorCodes(&tailorincgraphql.Result{Errors: []gqlerrors.FormattedError{gqlerrors.NewFormattedError("x")}},
				&models.GraphQLResponse{Errors: []*models.GraphQLError{nil}})
		})
	})
}

func TestGraphQLHandlerErrorCodes(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/graphql", nil)
	principal := &models.Principal{Username: "u"}

	newAPI := func(returned error) (*operations.WeaviateAPI, *logrustest.Hook) {
		// An empty query never reaches the resolver. Building a result for a
		// nil error would be a successful query, which executedResult rejects.
		var result *tailorincgraphql.Result
		if returned != nil {
			result = executedResult(t, returned)
		}
		gql := &fakeGraphQLWithResult{result: result}
		api := &operations.WeaviateAPI{}
		logger, hook := logrustest.NewNullLogger()
		setupGraphQLHandlers(api, &fakeGraphQLProvider{gql: gql},
			&schema.Manager{Authorizer: &authorization.DummyAuthorizer{}},
			runtimeconfig.NewDynamicValue(false), false, nil, logger)
		return api, hook
	}
	assertNoServerError := func(t *testing.T, hook *logrustest.Hook) {
		t.Helper()
		for _, entry := range hook.AllEntries() {
			assert.NotEqual(t, "unexpected error", entry.Message)
		}
	}

	t.Run("POST rate limit body carries code 429", func(t *testing.T) {
		api, hook := newAPI(enterrors.NewErrGraphQLUser(enterrors.NewErrRateLimit(), "Get", "SomeAction"))
		responder := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: "{ Get }"}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())

		require.Equal(t, http.StatusOK, rec.Code)
		var body models.GraphQLResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body.Errors, 1)
		assert.Equal(t, int64(http.StatusTooManyRequests), body.Errors[0].Code)
		assert.Contains(t, body.Errors[0].Message, "429 Too many requests")
		assertNoServerError(t, hook)
	})

	t.Run("POST plain error omits code", func(t *testing.T) {
		api, hook := newAPI(enterrors.NewErrGraphQLUser(assert.AnError, "Get", "SomeAction"))
		responder := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: "{ Get }"}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())

		require.Equal(t, http.StatusOK, rec.Code)
		assert.NotContains(t, rec.Body.String(), `"code"`)
		assertNoServerError(t, hook)
	})

	t.Run("empty query is HTTP 422 and the body code matches", func(t *testing.T) {
		api, _ := newAPI(nil)
		responder := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: ""}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())

		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		var body models.ErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body.Error, 1)
		assert.Equal(t, "query cannot be empty", body.Error[0].Message)
		assert.Equal(t, int64(http.StatusUnprocessableEntity), body.Error[0].Code)
	})

	t.Run("batch empty query carries code 422 inside the item", func(t *testing.T) {
		api, _ := newAPI(nil)
		responder := api.GraphqlGraphqlBatchHandler.Handle(
			graphql.GraphqlBatchParams{HTTPRequest: req, Body: models.GraphQLQueries{{Query: ""}}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())

		require.Equal(t, http.StatusOK, rec.Code)
		var body []models.GraphQLResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		require.Len(t, body, 1)
		require.Len(t, body[0].Errors, 1)
		assert.Equal(t, int64(http.StatusUnprocessableEntity), body[0].Errors[0].Code)
		assert.Contains(t, body[0].Errors[0].Message, "422:")
	})
}
