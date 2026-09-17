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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus"
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

// responseFromResult builds the client response the way the handlers do: a
// JSON round-trip of the resolved result.
func responseFromResult(t *testing.T, result *tailorincgraphql.Result) *models.GraphQLResponse {
	t.Helper()
	resultJSON, err := json.Marshal(result)
	require.NoError(t, err)
	response := &models.GraphQLResponse{}
	require.NoError(t, json.Unmarshal(resultJSON, response))
	return response
}

func TestAddDocsLinks(t *testing.T) {
	documented := fmt.Errorf("resolve Get: memory pressure: cannot load shard: %w", enterrors.ErrNotEnoughMappings)
	result := &tailorincgraphql.Result{Errors: []gqlerrors.FormattedError{
		// how the executor reports a resolver error: the returned error is
		// formatted, then located (wrapped in *gqlerrors.Error), then formatted again
		gqlerrors.FormatError(tailorincgraphql.NewLocatedErrorWithPath(gqlerrors.FormatError(documented), nil, nil)),
		gqlerrors.FormatError(documented),
		gqlerrors.FormatError(fmt.Errorf("resolve Get: something else")),
	}}
	response := responseFromResult(t, result)

	addDocsLinks(result, response)

	pristine := "resolve Get: memory pressure: cannot load shard: not enough memory mappings"
	want := pristine + " (see https://docs.weaviate.io/e/core-mem001)"
	assert.Equal(t, want, response.Errors[0].Message)
	assert.Equal(t, want, response.Errors[1].Message)
	assert.Equal(t, "resolve Get: something else", response.Errors[2].Message)
	// the result feeds the metrics logger and must keep its pristine messages
	assert.Equal(t, pristine, result.Errors[0].Message)
	assert.Equal(t, pristine, result.Errors[1].Message)

	assert.NotPanics(t, func() { addDocsLinks(nil, nil) })
	assert.NotPanics(t, func() { addDocsLinks(result, nil) })
	assert.NotPanics(t, func() { addDocsLinks(nil, response) })
	assert.NotPanics(t, func() { addDocsLinks(&tailorincgraphql.Result{}, response) })
	assert.NotPanics(t, func() { addDocsLinks(result, &models.GraphQLResponse{Errors: []*models.GraphQLError{nil}}) })
}

// Runs the real executor so the nesting of library error types is whatever
// the library does today, not what this test assumes.
func TestAddDocsLinksThroughExecutor(t *testing.T) {
	documented := fmt.Errorf("explorer: list class: search: %w", enterrors.ErrNotEnoughMappings)
	schema, err := tailorincgraphql.NewSchema(tailorincgraphql.SchemaConfig{
		Query: tailorincgraphql.NewObject(tailorincgraphql.ObjectConfig{
			Name: "Query",
			Fields: tailorincgraphql.Fields{
				"Get": &tailorincgraphql.Field{
					Type: tailorincgraphql.String,
					Resolve: func(tailorincgraphql.ResolveParams) (interface{}, error) {
						// as the Get resolver returns it
						return nil, enterrors.NewErrGraphQLUser(documented, "Get", "Demo")
					},
				},
			},
		}),
	})
	require.NoError(t, err)

	result := tailorincgraphql.Do(tailorincgraphql.Params{Schema: schema, RequestString: "{ Get }"})
	require.Len(t, result.Errors, 1)
	response := responseFromResult(t, result)

	addDocsLinks(result, response)

	assert.Equal(t, "explorer: list class: search: not enough memory mappings (see https://docs.weaviate.io/e/core-mem001)", response.Errors[0].Message)
	assert.Equal(t, "explorer: list class: search: not enough memory mappings", result.Errors[0].Message)
}

type fakeGraphQLWithResult struct{ result *tailorincgraphql.Result }

func (f *fakeGraphQLWithResult) Resolve(context.Context, string, string, map[string]interface{}) *tailorincgraphql.Result {
	return f.result
}

// assertServerLogPristine pins the server log shape: the metrics logger sees
// the resolver error without the docs link, which belongs to the response only.
func assertServerLogPristine(t *testing.T, hook *logrustest.Hook, wantErr string) {
	t.Helper()
	logged := false
	for _, entry := range hook.AllEntries() {
		if entry.Message == "unexpected error" {
			logged = true
			assert.Equal(t, wantErr, fmt.Sprintf("%v", entry.Data[logrus.ErrorKey]))
		}
		assert.NotContains(t, entry.Message, "docs.weaviate.io")
		for _, v := range entry.Data {
			assert.NotContains(t, fmt.Sprintf("%v", v), "docs.weaviate.io")
		}
	}
	assert.True(t, logged, "expected the server-error log entry")
}

// TestGraphQLHandlersAppendDocsLinks drives both the POST and batch handlers
// with a resolver returning a documented error: the response carries the docs
// link, the server log does not.
func TestGraphQLHandlersAppendDocsLinks(t *testing.T) {
	const pristine = "resolve Get: cannot load shard: not enough memory mappings"
	const want = pristine + " (see https://docs.weaviate.io/e/core-mem001)"
	// A fresh result per handler: addDocsLinks rewrites the response built from it.
	newAPI := func() (*operations.WeaviateAPI, *logrustest.Hook) {
		documented := fmt.Errorf("resolve Get: cannot load shard: %w", enterrors.ErrNotEnoughMappings)
		gql := &fakeGraphQLWithResult{result: &tailorincgraphql.Result{
			Errors: []gqlerrors.FormattedError{gqlerrors.FormatError(documented)},
		}}
		api := &operations.WeaviateAPI{}
		logger, hook := logrustest.NewNullLogger()
		setupGraphQLHandlers(api, &fakeGraphQLProvider{gql: gql},
			&schema.Manager{Authorizer: &authorization.DummyAuthorizer{}},
			runtimeconfig.NewDynamicValue(false), false, nil, logger)
		return api, hook
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/graphql", nil)
	principal := &models.Principal{Username: "u"}

	t.Run("POST", func(t *testing.T) {
		api, hook := newAPI()
		responder := api.GraphqlGraphqlPostHandler.Handle(
			graphql.GraphqlPostParams{HTTPRequest: req, Body: &models.GraphQLQuery{Query: "{ Get }"}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), want)
		assertServerLogPristine(t, hook, pristine)
	})

	t.Run("batch", func(t *testing.T) {
		api, hook := newAPI()
		responder := api.GraphqlGraphqlBatchHandler.Handle(
			graphql.GraphqlBatchParams{HTTPRequest: req, Body: models.GraphQLQueries{{Query: "{ Get }"}}}, principal)
		rec := httptest.NewRecorder()
		responder.WriteResponse(rec, runtime.JSONProducer())
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), want)
		assertServerLogPristine(t, hook, pristine)
	})
}
