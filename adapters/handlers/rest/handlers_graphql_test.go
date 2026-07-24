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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tailorincgraphql "github.com/tailor-platform/graphql"

	libgraphql "github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
)

type fakeGraphQLProvider struct {
	gql libgraphql.GraphQL
}

func (f *fakeGraphQLProvider) GetGraphQL() libgraphql.GraphQL { return f.gql }

type fakeGraphQL struct {
	variables map[string]interface{}
}

func (f *fakeGraphQL) Resolve(ctx context.Context, query, operationName string,
	variables map[string]interface{},
) *tailorincgraphql.Result {
	f.variables = variables
	return &tailorincgraphql.Result{Data: map[string]interface{}{}}
}

// TestGraphQLPostVariables asserts that a "variables" field which is not a JSON
// object is rejected as a user error rather than panicking on a type assertion.
func TestGraphQLPostVariables(t *testing.T) {
	tests := []struct {
		name      string
		variables interface{}
		wantCode  int
		wantMsg   string
		wantVars  map[string]interface{}
	}{
		{
			name:      "object",
			variables: map[string]interface{}{"limit": float64(1)},
			wantCode:  http.StatusOK,
			wantVars:  map[string]interface{}{"limit": float64(1)},
		},
		{
			name:      "absent",
			variables: nil,
			wantCode:  http.StatusOK,
			wantVars:  nil,
		},
		{
			name:      "string",
			variables: "not an object",
			wantCode:  http.StatusUnprocessableEntity,
			wantMsg:   "variables must be a JSON object, got string",
		},
		{
			name:      "array",
			variables: []interface{}{"a"},
			wantCode:  http.StatusUnprocessableEntity,
			wantMsg:   "variables must be a JSON object, got []interface {}",
		},
		{
			name:      "number",
			variables: float64(1),
			wantCode:  http.StatusUnprocessableEntity,
			wantMsg:   "variables must be a JSON object, got float64",
		},
		{
			name:      "bool",
			variables: true,
			wantCode:  http.StatusUnprocessableEntity,
			wantMsg:   "variables must be a JSON object, got bool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			gql := &fakeGraphQL{}
			api := &operations.WeaviateAPI{}

			setupGraphQLHandlers(api, &fakeGraphQLProvider{gql: gql},
				&schema.Manager{Authorizer: mocks.NewMockAuthorizer()}, false, nil, logger)

			responder := api.GraphqlGraphqlPostHandler.Handle(graphql.GraphqlPostParams{
				HTTPRequest: httptest.NewRequest(http.MethodPost, "/v1/graphql", nil),
				Body: &models.GraphQLQuery{
					Query:     "{ Get { Foo { name } } }",
					Variables: tt.variables,
				},
			}, nil)

			rec := httptest.NewRecorder()
			responder.WriteResponse(rec, runtime.JSONProducer())
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Equal(t, tt.wantVars, gql.variables)
				return
			}
			assert.Contains(t, rec.Body.String(), tt.wantMsg)
		})
	}
}
